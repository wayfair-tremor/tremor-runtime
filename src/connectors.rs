// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// quality of service utilities
pub(crate) mod qos;

/// prelude with commonly needed stuff imported
pub(crate) mod prelude;
/// Sink part of a connector
pub(crate) mod sink;
/// source part of a connector
pub(crate) mod source;

/// reconnect logic for connectors
pub(crate) mod reconnect;

/// home for connector specific function
pub(crate) mod functions;

/// google cloud pubsub/storage/auth
pub(crate) mod gcp;
/// opentelemetry
pub(crate) mod otel;
/// protobuf helpers
pub(crate) mod pb;

/// tcp server connector impl
pub(crate) mod tcp_server;

use async_std::task::{self, JoinHandle};
use beef::Cow;
use either::Either;

use crate::codec;
use crate::config::Connector as ConnectorConfig;
use crate::connectors::sink::{sink_task, Sink, SinkAddr, SinkContext, SinkMsg};
use crate::connectors::source::{source_task, Source, SourceAddr, SourceContext, SourceMsg};
use crate::errors::{Error, ErrorKind, Result};
use crate::pipeline;
use crate::postprocessor::make_postprocessors;
use crate::system::World;
use crate::url::ports::IN;
use crate::url::TremorUrl;
use crate::OpConfig;
use async_channel::bounded;
use halfbrown::{Entry, HashMap};
use reconnect::Reconnect;
use tremor_common::ids::ConnectorIdGen;

/// sender for connector manager messages
pub type Sender = async_channel::Sender<ManagerMsg>;

/// connector address
#[derive(Clone, Debug)]
pub struct Addr {
    uid: u64,
    url: TremorUrl,
    sender: async_channel::Sender<Msg>,
    source: Option<SourceAddr>,
    sink: Option<SinkAddr>,
}

impl Addr {
    pub(crate) async fn send_sink(&self, msg: SinkMsg) -> Result<()> {
        if let Some(sink) = self.sink.as_ref() {
            sink.addr.send(msg).await?
        }
        Ok(())
    }

    pub(crate) async fn send_source(&self, msg: SourceMsg) -> Result<()> {
        if let Some(source) = self.source.as_ref() {
            source.addr.send(msg).await?
        }
        Ok(())
    }
}

/// Messages a Connector instance receives and acts upon
pub enum Msg {
    /// connect 1 or more pipelines to a port
    Connect {
        /// port to which to connect
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
        /// result receiver
        result_tx: async_channel::Sender<Result<()>>,
    },
    /// disconnect pipeline `id` from the given `port`
    Disconnect {
        /// port from which to disconnect
        port: Cow<'static, str>,
        /// id of the pipeline to disconnect
        id: TremorUrl,
        /// sender to receive a boolean whether this connector is not connected to anything
        tx: async_channel::Sender<Result<bool>>,
    },
    /// initiate a reconnect attempt
    Reconnect, // times we attempted a reconnect, time to wait
    // TODO: fill as needed
    /// start the connector
    Start,
    /// pause the connector
    Pause,
    /// resume the connector after a pause
    Resume,
    /// stop the connector
    Stop,
}

/// msg used for connector creation
#[derive(Debug)]
pub struct Create {
    servant_id: TremorUrl,
    config: ConnectorConfig,
}

impl Create {
    /// constructor
    pub fn new(servant_id: TremorUrl, config: ConnectorConfig) -> Self {
        Self { servant_id, config }
    }
}

/// msg for the `ConnectorManager` handling all connectors
pub enum ManagerMsg {
    /// register a new connector type
    Register {
        /// the type of connector
        connector_type: String,
        /// the builder
        builder: Box<dyn ConnectorBuilder>,
        /// if this one is a builtin connector
        builtin: bool,
    },
    /// unregister a connector type
    Unregister(String),
    /// create a new connector
    Create {
        /// sender to send the create result to
        tx: async_channel::Sender<Result<Addr>>,
        /// the create command
        create: Create,
    },
    /// stop the connector manager
    Stop {
        /// reason
        reason: String,
    },
}

/// The connector manager - handling creation of connectors
/// and handles available connector types
pub struct Manager {
    qsize: usize,
}

impl Manager {
    /// constructor
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    /// start the manager
    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Connector manager started.");
            let mut connector_id_gen = ConnectorIdGen::new();
            let mut known_connectors: HashMap<String, (Box<dyn ConnectorBuilder>, bool)> =
                HashMap::with_capacity(16);

            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Create { tx, create }) => {
                        let url = create.servant_id.clone();
                        // lookup and instantiate connector
                        let connector = if let Some((builder, _)) =
                            known_connectors.get(&create.config.binding_type)
                        {
                            let connector_res = builder.from_config(&url, &create.config.config);
                            match connector_res {
                                Ok(connector) => connector,
                                Err(e) => {
                                    error!(
                                        "[Connector] Error instantiating connector {}: {}",
                                        &url, e
                                    );
                                    tx.send(Err(e)).await?;
                                    continue;
                                }
                            }
                        } else {
                            error!(
                                "[Connector] Connector Type '{}' unknown",
                                &create.config.binding_type
                            );
                            tx.send(Err(ErrorKind::UnknownConnectorType(
                                create.config.binding_type,
                            )
                            .into()))
                                .await?;
                            continue;
                        };
                        if let Err(e) = self
                            .connector_task(
                                tx.clone(),
                                create.servant_id,
                                connector,
                                create.config,
                                connector_id_gen.next_id(),
                            )
                            .await
                        {
                            error!(
                                "[Connector] Error spawning task for connector {}: {}",
                                &url, e
                            );
                            tx.send(Err(e)).await?;
                        }
                    }
                    Ok(ManagerMsg::Register {
                        connector_type,
                        builder,
                        builtin,
                    }) => {
                        info!("Registering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                warn!("Connector Type {} already registered.", e.key());
                            }
                            Entry::Vacant(e) => {
                                info!(
                                    "Connector Type {} registered{}.",
                                    e.key(),
                                    if builtin { " as builtin" } else { " " }
                                );
                                e.insert((builder, builtin));
                            }
                        }
                    }
                    Ok(ManagerMsg::Unregister(connector_type)) => {
                        info!("Unregistering {} Connector Type.", &connector_type);
                        match known_connectors.entry(connector_type) {
                            Entry::Occupied(e) => {
                                let (_, builtin) = e.get();
                                if *builtin {
                                    error!("Cannot unregister builtin Connector Type {}", e.key());
                                } else {
                                    info!("Connector Type {} unregistered.", e.key());
                                    e.remove_entry();
                                }
                            }
                            Entry::Vacant(e) => {
                                error!("Connector Type {} not registered", e.key());
                            }
                        }
                    }
                    Ok(ManagerMsg::Stop { reason }) => {
                        info!("Stopping Connector Manager... {}", reason);
                        break;
                    }
                    Err(e) => {
                        info!("Error! Stopping Connector Manager... {}", e);
                        break;
                    }
                }
            }
            info!("Connector Manager stopped.");
            Ok(())
        });
        (h, tx)
    }

    // instantiates the connector and starts listening for control plane messages
    async fn connector_task(
        &self,
        addr_tx: async_channel::Sender<Result<Addr>>,
        url: TremorUrl,
        mut connector: Box<dyn Connector>,
        config: ConnectorConfig,
        uid: u64,
    ) -> Result<()> {
        // channel for connector-level control plane communication
        let (msg_tx, msg_rx) = bounded(self.qsize);

        let mut reconnect: Reconnect = Reconnect::from(config.reconnect);
        let mut connectivity = Connectivity::Disconnected;

        let mut connector_state = ConnectorState::Stopped;
        dbg!(connector_state);

        ///// create source instance
        // channel for sending SourceReply to the source part of this connector
        let mut source_ctx = SourceContext {
            uid,
            url: url.clone(),
        };
        let source = connector
            .create_source(&mut source_ctx)
            .await?
            .map(|source| {
                // start source task
                let (source_tx, source_rx) = bounded(self.qsize);
                let _handle = task::spawn(source_task(source_rx, source, source_ctx));
                SourceAddr { addr: source_tx }
            });

        // create sink instance
        let codec_name = config
            .codec
            .map(|s| match s {
                Either::Left(s) => s,
                Either::Right(config) => config.name,
            })
            .unwrap_or(connector.default_codec().to_string());
        let codec = codec::lookup(&codec_name)?;
        let postprocessors =
            make_postprocessors(config.postprocessors.as_ref().unwrap_or(&vec![]))?;
        let mut sink_ctx = SinkContext {
            uid,
            url: url.clone(),
            codec,
            postprocessors,
        };

        let ctx = ConnectorContext {
            uid,
            url: url.clone(),
        };
        let sink = connector.create_sink(&mut sink_ctx).await?.map(|sink| {
            // start sink task
            let (sink_tx, sink_rx) = bounded(self.qsize);
            let _handle = task::spawn(sink_task(sink_rx, sink, sink_ctx));
            SinkAddr { addr: sink_tx }
        });

        let addr = Addr {
            uid,
            url: url.clone(),
            sender: msg_tx,
            source,
            sink,
        };
        let send_addr = addr.clone();
        connector_state = ConnectorState::Initialized;
        dbg!(&connector_state);

        task::spawn::<_, Result<()>>(async move {
            // typical 1 pipeline connected to IN, OUT, ERR
            let mut pipelines: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>> =
                HashMap::with_capacity(3);

            // connector control plane loop
            while let Ok(msg) = msg_rx.recv().await {
                match msg {
                    Msg::Connect {
                        port,
                        pipelines: mut mapping,
                        result_tx,
                    } => {
                        mapping.iter().for_each(|(url, _)| {
                            info!(
                                "[Connector::{}] Connecting {} via port {}",
                                &url, &url, &port
                            )
                        });
                        if let Some(port_pipes) = pipelines.get_mut(&port) {
                            port_pipes.append(&mut mapping);
                        } else {
                            pipelines.insert(port.clone(), mapping.clone());
                        }
                        let res: Result<()> = if port.eq_ignore_ascii_case(IN.as_ref()) {
                            // connect to source part
                            match addr.source.as_ref() {
                                Some(source) => source
                                    .addr
                                    .send(SourceMsg::Connect {
                                        port,
                                        pipelines: mapping,
                                    })
                                    .await
                                    .map_err(|e| e.into()),
                                None => Err(ErrorKind::InvalidConnect(
                                    addr.url.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else {
                            // connect to sink part
                            match addr.sink.as_ref() {
                                Some(sink) => sink
                                    .addr
                                    .send(SinkMsg::Connect {
                                        port,
                                        pipelines: mapping,
                                    })
                                    .await
                                    .map_err(|e| e.into()),
                                None => Err(ErrorKind::InvalidConnect(
                                    addr.url.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        };
                        // TODO: only send result after sink/source have finished connecting?
                        // send back the connect result
                        if let Err(e) = result_tx.send(res).await {
                            error!("Error sending connect result: {}", e);
                        }
                    }
                    Msg::Disconnect { port, id, tx } => {
                        let delete = if let Some(port_pipes) = pipelines.get_mut(&port) {
                            port_pipes.retain(|(url, _)| url != &id);
                            port_pipes.is_empty()
                        } else {
                            false
                        };
                        // make sure we can simply use `is_empty` for checking for emptiness
                        if delete {
                            pipelines.remove(&port);
                        }
                        let res: Result<()> = if port.eq_ignore_ascii_case(IN.as_ref()) {
                            // disconnect from source part
                            match addr.source.as_ref() {
                                Some(source) => source
                                    .addr
                                    .send(SourceMsg::Disconnect { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        } else {
                            // disconnect from sink part
                            match addr.sink.as_ref() {
                                Some(sink) => sink
                                    .addr
                                    .send(SinkMsg::Disconnect { port, id })
                                    .await
                                    .map_err(Error::from),
                                None => Err(ErrorKind::InvalidDisconnect(
                                    addr.url.to_string(),
                                    id.to_string(),
                                    port.clone(),
                                )
                                .into()),
                            }
                        };
                        // TODO: work out more fine grained "empty" semantics
                        tx.send(res.map(|_| pipelines.is_empty())).await?
                    }
                    Msg::Reconnect => {
                        // reconnect if we are below max_retries, otherwise bail out and fail the connector
                        info!("[Connector::{}] Connecting...", &addr.url);
                        let new = reconnect.attempt(connector.as_mut(), &ctx, &addr).await?;
                        match (&new, &connectivity) {
                            (Connectivity::Disconnected, Connectivity::Connected) => {
                                info!("[Connector::{}] Connected.", &addr.url);
                                // notify sink
                                addr.send_sink(SinkMsg::ConnectionEstablished).await?;
                            }
                            (Connectivity::Connected, Connectivity::Disconnected) => {
                                info!("[Connector::{}] Disconnected.", &addr.url);
                                addr.send_sink(SinkMsg::ConnectionLost).await?;
                            }
                            _ => {
                                debug!("[Connector::{}] No change: {:?}", &addr.url, &new)
                            }
                        }
                        connectivity = new;
                    }
                    Msg::Start => {
                        info!("[Connector::{}] Starting...", &addr.url);
                        // TODO: start connector
                        connector_state = match connector.on_start(&ctx).await {
                            Ok(new_state) => new_state,
                            Err(e) => {
                                error!("[Connector::{}] on_start Error: {}", &addr.url, e);
                                ConnectorState::Failed
                            }
                        };
                        info!(
                            "[Connector::{}] started. New state: {:?}",
                            &addr.url, &connector_state
                        );
                        // forward to source/sink if available
                        addr.send_source(SourceMsg::Start).await?;
                        addr.send_sink(SinkMsg::Start).await?;

                        // initiate connect asynchronously
                        addr.sender.send(Msg::Reconnect).await?;

                        info!("[Connector::{}] Started.", &addr.url);
                    }
                    Msg::Pause => {
                        info!("[Connector::{}] Pausing...", &addr.url);

                        connector.on_pause(&ctx).await;
                        connector_state = ConnectorState::Paused;

                        addr.send_source(SourceMsg::Pause).await?;
                        addr.send_sink(SinkMsg::Pause).await?;

                        info!("[Connector::{}] Paused.", &addr.url);
                    }
                    Msg::Resume => {
                        info!("[Connector::{}] Resuming...", &addr.url);
                        // TODO: resume
                        connector.on_resume(&ctx).await;
                        connector_state = ConnectorState::Running;

                        addr.send_source(SourceMsg::Resume).await?;
                        addr.send_sink(SinkMsg::Resume).await?;

                        info!("[Connector::{}] Resumed.", &addr.url);
                    }
                    Msg::Stop => {
                        info!("[Connector::{}] Stopping...", &addr.url);
                        // TODO: stop
                        connector.on_stop(&ctx).await;
                        connector_state = ConnectorState::Stopped;

                        addr.send_source(SourceMsg::Stop).await?;
                        addr.send_sink(SinkMsg::Stop).await?;

                        info!("[Connector::{}] Stopped.", &addr.url);
                        break;
                    }
                } // match

                // TODO: react on connector state changes
            } // while
            info!(
                "[Connector::{}] Connector Stopped. Reason: {:?}",
                &addr.url, &connector_state
            );
            Ok(())
        });
        addr_tx.send(Ok(send_addr)).await?;
        Ok(())
    }
}

/// state of a connector
#[derive(Debug, PartialEq)]
pub enum ConnectorState {
    /// connector has been initialized
    Initialized,
    /// connector is running
    Running,
    /// connector has been paused
    Paused,
    /// connector was stopped
    Stopped,
    /// connector failed to start
    Failed,
}

/// connector context
pub struct ConnectorContext {
    /// unique identifier
    pub uid: u64,
    /// url of the connector
    pub url: TremorUrl,
}

/// describes connectivity state of the connector
#[derive(Debug)]
pub enum Connectivity {
    /// connector is connected
    Connected,
    /// connector is disconnected
    Disconnected,
}

/// A Connector connects the tremor runtime to the outside world.
///
/// It can be a source of events, as such it is polled for new data.
/// It can also be a sink for events, as such events are sent to it from pipelines.
/// A connector can act as sink and source or just as one of those.
///
/// A connector encapsulates the establishment and maintenance of connections to the outside world,
/// such as tcp connections, file handles etc. etc.
///
/// It is a meta entity on top of the sink and source part.
/// The connector has its own control plane and is an artefact in the tremor repository.
/// It controls the sink and source parts which are connected to the rest of the runtime via links to pipelines.
#[async_trait::async_trait]
pub trait Connector: Send {
    /// create a source part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a source, return `Ok(None)`.
    async fn create_source(
        &mut self,
        _source_context: &mut SourceContext,
    ) -> Result<Option<Box<dyn Source>>> {
        Ok(None)
    }

    /// Create a sink part for this connector if applicable
    ///
    /// This function is called exactly once upon connector creation.
    /// If this connector does not act as a sink, return `Ok(None)`.
    async fn create_sink(
        &mut self,
        _sink_context: &mut SinkContext,
    ) -> Result<Option<Box<dyn Sink>>> {
        Ok(None)
    }

    /// Attempt to connect to the outside world
    /// Return `Ok(true)` if a connection could be established.
    /// This method will be retried if it fails or returns `Ok(false)`.
    async fn connect(&mut self, ctx: &ConnectorContext) -> Result<bool>;

    /// called once when the connector is started
    /// `connect` will be called after this for the first time, leave connection attempts in `connect`.
    async fn on_start(&mut self, ctx: &ConnectorContext) -> Result<ConnectorState>;

    /// called when the connector pauses
    async fn on_pause(&mut self, _ctx: &ConnectorContext) {}
    /// called when the connector resumes
    async fn on_resume(&mut self, _ctx: &ConnectorContext) {}
    /// called when the connector is stopped
    async fn on_stop(&mut self, _ctx: &ConnectorContext) {}

    /// returns the default codec for this connector
    fn default_codec(&self) -> &str;
}

/// something that is able to create a connector instance
pub trait ConnectorBuilder: Sync + Send {
    /// create a connector from the given `id` and `config`
    fn from_config(&self, id: &TremorUrl, config: &Option<OpConfig>) -> Result<Box<dyn Connector>>;
}

#[cfg(not(tarpaulin_include))]
pub async fn register_builtin_connectors(world: &World) -> Result<()> {
    world
        .register_builtin_connector_type("tcp_server", Box::new(tcp_server::Builder {}))
        .await?;
    Ok(())
}
