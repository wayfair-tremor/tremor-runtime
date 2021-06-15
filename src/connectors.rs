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

pub(crate) mod qos;

/// Extensions for `CNCF OpenTelemetry` support
pub mod otel;

/// Extensions for the `Google Cloud Platform`
pub mod gcp;

pub(crate) mod prelude;
/// TCP-Server connector
pub(crate) mod tcp_server;

pub(crate) mod pb;

pub(crate) mod sink;
pub(crate) mod source;

pub(crate) mod reconnect;

use async_std::task::{self, JoinHandle};
use beef::Cow;
use either::Either;

use crate::config::Connector as ConnectorConfig;
use crate::connectors::prelude::*;
use crate::connectors::sink::{sink_task, Sink, SinkAddr, SinkContext, SinkMsg};
use crate::connectors::source::{source_task, Source, SourceAddr, SourceContext, SourceMsg};

use crate::codec;
use crate::pipeline;
use crate::postprocessor::make_postprocessors;
use crate::url::ports::IN;
use crate::url::TremorUrl;
use crate::OpConfig;
use async_channel::bounded;
use halfbrown::HashMap;
use reconnect::Reconnect;
use tremor_common::ids::ConnectorIdGen;

pub type Sender = async_channel::Sender<ManagerMsg>;

#[derive(Clone, Debug)]
pub struct Addr {
    uid: u64,
    url: TremorUrl,
    sender: async_channel::Sender<Msg>,
    source: Option<SourceAddr>,
    sink: Option<SinkAddr>,
}

/// Messages a Connector instance receives and acts upon
pub enum Msg {
    Connect {
        port: Cow<'static, str>,
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
        result_tx: async_channel::Sender<Result<()>>,
    },
    Disconnect {
        port: Cow<'static, str>,
        id: TremorUrl,
        tx: async_channel::Sender<Result<bool>>,
    },
    Reconnect, // times we attempted a reconnect, time to wait
    // TODO: fill as needed
    Start,
    Pause,
    Resume,
    Stop,
}

#[derive(Debug)]
pub(crate) struct Create {
    servant_id: TremorUrl,
    config: ConnectorConfig,
}

impl Create {
    pub fn new(servant_id: TremorUrl, config: ConnectorConfig) -> Self {
        Self { servant_id, config }
    }
}

pub enum ManagerMsg {
    Create {
        tx: async_channel::Sender<Result<Addr>>,
        create: Create,
    },
    Stop {
        reason: String,
    },
}

#[cfg(not(tarpaulin_include))]
pub fn lookup(name: &str, id: &TremorUrl, config: &Option<OpConfig>) -> Result<Box<dyn Connector>> {
    match name {
        "tcp-server" => tcp_server::TcpServer::from_config(id, config),
        _ => Err(ErrorKind::UnknownConnector(name.to_string()).into()),
    }
}

pub(crate) struct Manager {
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Connector manager started.");
            let mut connector_id_gen = ConnectorIdGen::new();
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Create { tx, create }) => {
                        // TODO: actually start something
                        let url = create.servant_id.clone();
                        if let Err(e) = self
                            .connector_task(&tx, create, connector_id_gen.next_id())
                            .await
                        {
                            error!(
                                "[Connector] Error spawning task for connector {}: {}",
                                &url, e
                            );
                            tx.send(Err(e)).await?;
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
        addr_tx: &async_channel::Sender<Result<Addr>>,
        create: Create,
        uid: u64,
    ) -> Result<()> {
        // channel for connector-level control plane communication
        let (msg_tx, msg_rx) = bounded(self.qsize);

        // create connector instance
        let mut connector = lookup(
            create.config.binding_type.as_str(),
            &create.servant_id,
            &create.config.config,
        )?;

        let mut reconnect: Reconnect = Reconnect::from(create.config.reconnect);
        let mut connectivity = Connectivity::Disconnected;
        let mut connector_state = ConnectorState::Stopped;

        ///// create source instance
        // channel for sending SourceReply to the source part of this connector
        let mut source_ctx = SourceContext {
            uid,
            url: create.servant_id.clone(),
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
        let codec_name = create
            .config
            .codec
            .map(|s| match s {
                Either::Left(s) => s,
                Either::Right(config) => config.name,
            })
            .unwrap_or(connector.default_codec().to_string());
        let codec = codec::lookup(&codec_name)?;
        let postprocessors =
            make_postprocessors(create.config.postprocessors.as_ref().unwrap_or(&vec![]))?;
        let mut sink_ctx = SinkContext {
            uid,
            url: create.servant_id.clone(),
            codec,
            postprocessors,
        };

        let ctx = ConnectorContext {
            uid,
            url: create.servant_id.clone(),
        };
        let sink = connector.create_sink(&mut sink_ctx).await?.map(|sink| {
            // start sink task
            let (sink_tx, sink_rx) = bounded(self.qsize);
            let _handle = task::spawn(sink_task(sink_rx, sink, sink_ctx));
            SinkAddr { addr: sink_tx }
        });

        let addr = Addr {
            uid,
            url: create.servant_id.clone(),
            sender: msg_tx,
            source,
            sink,
        };
        let send_addr = addr.clone();
        connector_state = ConnectorState::Initialized;

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
                                // TODO: notify sink
                            }
                            (Connectivity::Connected, Connectivity::Disconnected) => {
                                info!("[Connector::{}] Disconnected.", &addr.url);
                                // TODO: notify sink
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
                        connector.on_start(&ctx).await;

                        // initiate connect asynchronously
                        addr.sender.send(Msg::Reconnect).await?;

                        info!("[Connector::{}] Started.", &addr.url);
                    }
                    Msg::Pause => {
                        info!("[Connector::{}] Pausing...", &addr.url);
                        // TODO: pause
                        connector.on_pause(&ctx).await;
                        info!("[Connector::{}] Paused.", &addr.url);
                    }
                    Msg::Resume => {
                        info!("[Connector::{}] Resuming...", &addr.url);
                        // TODO: resume
                        connector.on_resume(&ctx).await;
                        info!("[Connector::{}] Resumed.", &addr.url);
                    }
                    Msg::Stop => {
                        info!("[Connector::{}] Stopping...", &addr.url);
                        // TODO: stop
                        connector.on_stop(&ctx).await;
                        info!("[Connector::{}] Stopped.", &addr.url);
                        break;
                    }
                }
            }
            Ok(())
        });
        addr_tx.send(Ok(send_addr)).await?;
        Ok(())
    }
}

pub(crate) enum ConnectorState {
    Initialized,
    Running,
    Paused,
    Stopped,
}

pub(crate) struct ConnectorContext {
    uid: u64,
    url: TremorUrl,
}

#[derive(Debug)]
pub(crate) enum Connectivity {
    Connected,
    Disconnected,
}

#[async_trait::async_trait]
pub(crate) trait Connector: Send {
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
    /// This function will be used to re-establish connectivity, should it go down
    async fn on_start(&mut self, ctx: &ConnectorContext) -> Result<ConnectorState>;

    async fn on_pause(&mut self, _ctx: &ConnectorContext) {}
    async fn on_resume(&mut self, _ctx: &ConnectorContext) {}
    async fn on_stop(&mut self, _ctx: &ConnectorContext) {}

    fn default_codec(&self) -> &str;
}

pub(crate) trait ConnectorBuilder {
    fn from_config(id: &TremorUrl, config: &Option<OpConfig>) -> Result<Box<dyn Connector>>;
}
