// Copyright 2021, The Tremor Team
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

use async_std::task;
use std::collections::BTreeMap;

use crate::codec::{self, Codec};
use crate::config::Connector as ConnectorConfig;
use crate::errors::Result;
use crate::pipeline;
use crate::preprocessor::{make_preprocessors, Preprocessors};
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use halfbrown::HashMap;
use tremor_pipeline::{CbAction, EventId, EventOriginUri};
use tremor_value::Value;

/// Messages a Source can receive
pub enum SourceMsg {
    /// connect a pipeline
    Connect {
        /// port
        port: Cow<'static, str>,
        /// pipelines to connect
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    /// disconnect a pipeline from a port
    Disconnect {
        /// port
        port: Cow<'static, str>,
        /// url of the pipeline
        id: TremorUrl,
    },
    /// connectivity is lost in the connector
    ConnectionLost,
    /// connectivity is re-established
    ConnectionEstablished,
    /// Circuit Breaker Contraflow Event
    Cb(CbAction, EventId),
    // TODO: fill those
    /// start the source
    Start,
    /// pause the source
    Pause,
    /// resume the source
    Resume,
    /// stop the source
    Stop,
}

/// reply from `Source::on_event`
pub enum SourceReply {
    /// A normal data event with a `Vec<u8>` for data
    Data {
        /// origin uri
        origin_uri: EventOriginUri,
        /// the data
        data: Vec<u8>,
        /// metadata associated with this data
        meta: Option<Value<'static>>,
        /// allow source to override codec when pulling event
        /// the given string must be configured in the `config-map` as part of the source config
        codec_override: Option<String>,
        /// stream id of the data
        stream: u64,
    },
    /// A stream is opened
    StartStream(u64),
    /// A stream is closed
    EndStream(u64),
    /// no new data/event, wait for the given ms
    Empty(u64),
}

/// source part of a connector
#[async_trait::async_trait]
pub trait Source: Send {
    /// Pulls an event from the source if one exists
    /// determine the codec to be used
    async fn pull_data(&mut self, id: u64) -> Result<SourceReply>;
    /// This callback is called when the data provided from
    /// pull_event did not create any events, this is needed for
    /// linked sources that require a 1:1 mapping between requests
    /// and responses, we're looking at you REST
    async fn on_no_events(&mut self, _id: u64, _stream: usize) -> Result<()> {
        Ok(())
    }
    // TODO: lifecycle callbacks
    async fn on_start(&mut self, _ctx: &mut SourceContext) {}
    async fn on_pause(&mut self, _ctx: &mut SourceContext) {}
    async fn on_resume(&mut self, _ctx: &mut SourceContext) {}
    async fn on_stop(&mut self, _ctx: &mut SourceContext) {}

    // connectivity stuff
    // TODO: needed?
    /// called when connector lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &mut SourceContext) {}
    /// called when connector re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &mut SourceContext) {}

    /// Is this source transactional or can acks/fails be ignored
    fn is_transactional(&self) -> bool {
        false
    }
}

/// a source that
pub struct ChannelSource {
    rx: Receiver<SourceReply>,
    tx: Sender<SourceReply>,
}

impl ChannelSource {
    /// constructor
    pub fn new(qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        Self { tx, rx }
    }

    /// get the sender for the source
    pub fn sender(&self) -> Sender<SourceReply> {
        self.tx.clone()
    }
}

#[async_trait::async_trait()]
impl Source for ChannelSource {
    async fn pull_data(&mut self, _id: u64) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(reply) => Ok(reply),
            Err(async_channel::TryRecvError::Empty) => {
                // TODO: configure pull interval in connector config?
                Ok(SourceReply::Empty(10))
            }
            Err(e) => Err(e.into()),
        }
    }
}

// TODO make fields private and add some nice methods
/// context for a source
pub struct SourceContext {
    /// connector uid
    pub uid: u64,
    /// connector url
    pub url: TremorUrl,
}

/// address of a source
#[derive(Clone, Debug)]
pub struct SourceAddr {
    /// the actual address
    pub addr: async_channel::Sender<SourceMsg>,
}

struct StreamState {
    preprocessors: Preprocessors,
}

pub struct SourceManagerBuilder {
    qsize: usize,
    /// the configured codec
    codec: Box<dyn Codec>,
    /// the configured pre processors for the default stream
    preprocessors: Preprocessors,
    preprocessor_names: Vec<String>,
}

impl SourceManagerBuilder {
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    pub fn spawn<S>(self, source: S, ctx: SourceContext) -> Result<SourceAddr>
    where
        S: Source + Send + 'static,
    {
        let qsize = self.qsize;
        let name = ctx.url.short_id("c-src"); // connector source
        let (source_tx, source_rx) = bounded(qsize);
        let manager = SourceManager::new(source, ctx, self, source_rx);
        // spawn manager task
        task::Builder::new().name(name).spawn(manager.run())?;

        Ok(SourceAddr { addr: source_tx })
    }
}

/// create a builder for a `SourceManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SourceManager`.
pub fn builder(
    config: &ConnectorConfig,
    connector_default_codec: &str,
    qsize: usize,
) -> Result<SourceManagerBuilder> {
    // resolve codec
    let codec = if let Some(codec_config) = config.codec.as_ref() {
        codec::resolve(codec_config)?
    } else {
        codec::lookup(connector_default_codec)?
    };
    // resolve preprocessors
    let preprocessor_names = config.preprocessors.clone().unwrap_or_else(|| vec![]);
    let preprocessors = make_preprocessors(&preprocessor_names)?;
    Ok(SourceManagerBuilder {
        qsize,
        codec,
        preprocessors,
        preprocessor_names,
    })
}

/// entity driving the source task
/// and keeping the source state around
pub(crate) struct SourceManager<S>
where
    S: Source,
{
    source: S,
    ctx: SourceContext,
    rx: Receiver<SourceMsg>,
    /// the configured codec
    codec: Box<dyn Codec>,
    /// the configured pre processors for the default stream
    preprocessors: Preprocessors,
    preprocessor_names: Vec<String>,
    stream_states: BTreeMap<usize, StreamState>,
    pipelines: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>>,
    paused: bool,
    // TODO: add metrics reporter to metrics connector
}

impl<S> SourceManager<S>
where
    S: Source,
{
    fn new(
        source: S,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
        rx: Receiver<SourceMsg>,
    ) -> Self {
        let SourceManagerBuilder {
            codec,
            preprocessors,
            preprocessor_names,
            ..
        } = builder;
        Self {
            source,
            ctx,
            rx,
            codec,
            preprocessors,
            preprocessor_names,
            stream_states: BTreeMap::new(),
            pipelines: HashMap::with_capacity(2),
            paused: false,
        }
    }

    /// the source task
    ///
    /// handling control plane and data plane in a loop
    // TODO: data plane
    async fn run(mut self) -> Result<()> {
        loop {
            while let Ok(source_msg) = self.rx.recv().await {
                match source_msg {
                    SourceMsg::Connect {
                        port,
                        mut pipelines,
                    } => {
                        if let Some(pipes) = self.pipelines.get_mut(&port) {
                            pipes.append(&mut pipelines);
                        } else {
                            self.pipelines.insert(port, pipelines);
                        }
                    }
                    SourceMsg::Disconnect { id, port } => {
                        if let Some(pipes) = self.pipelines.get_mut(&port) {
                            pipes.retain(|(url, _)| url == &id)
                        }
                    }
                    SourceMsg::Start => {
                        self.paused = false;
                        self.source.on_start(&mut self.ctx).await
                    }
                    SourceMsg::Resume => {
                        self.paused = false;
                        self.source.on_resume(&mut self.ctx).await
                    }
                    SourceMsg::Pause => {
                        self.paused = true;
                        self.source.on_pause(&mut self.ctx).await
                    }
                    SourceMsg::Stop => self.source.on_stop(&mut self.ctx).await,
                    SourceMsg::ConnectionLost => {
                        self.paused = true;
                        self.source.on_connection_lost(&mut self.ctx).await
                    }
                    SourceMsg::ConnectionEstablished => {
                        self.paused = true;
                        self.source.on_connection_established(&mut self.ctx).await
                    }
                    SourceMsg::Cb(_cb, _id) => {
                        todo!()
                    }
                }
            }
        }
    }
}
