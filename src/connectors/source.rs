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
use either::Either;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use tremor_script::{EventPayload, ValueAndMeta};

use crate::codec::{self, Codec};
use crate::config::{CodecConfig, Connector as ConnectorConfig};
use crate::errors::{Error, Result};
use crate::pipeline;
use crate::preprocessor::{make_preprocessors, preprocess, Preprocessors};
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use halfbrown::HashMap;
use tremor_pipeline::{CbAction, EventId, EventOriginUri, DEFAULT_STREAM_ID};
use tremor_value::Value;
use value_trait::Builder;

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

macro_rules! stry_vec {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => return vec![::std::result::Result::Err(err)],
        }
    };
}

pub struct EventDeserializer {
    codec: Box<dyn Codec>,
    preprocessors: Preprocessors,
    codec_config: Either<String, CodecConfig>,
    preprocessor_names: Vec<String>,
    streams: BTreeMap<u64, (Box<dyn Codec>, Preprocessors)>,
}

impl EventDeserializer {
    fn build(
        codec_config: Option<Either<String, CodecConfig>>,
        default_codec: &str,
        preprocessor_names: Vec<String>,
    ) -> Result<Self> {
        let codec_config = codec_config.unwrap_or_else(|| Either::Left(default_codec.to_string()));
        let codec = codec::resolve(&codec_config)?;
        let preprocessors = make_preprocessors(preprocessor_names.as_slice())?;
        Ok(Self {
            codec,
            preprocessors,
            codec_config,
            preprocessor_names,
            streams: BTreeMap::new(),
        })
    }

    /// drop a stream and all associated deserialization state (codec and preprocessor)
    pub fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    pub fn clear(&mut self) {
        self.streams.clear()
    }

    /// deserialize event for the default stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    fn deserialize(
        &mut self,
        data: Vec<u8>,
        meta: Option<Value<'static>>,
        ingest_ns: &mut u64,
        url: &TremorUrl,
    ) -> Vec<Result<EventPayload>> {
        self.deserialize_for_stream(data, meta, ingest_ns, DEFAULT_STREAM_ID, url)
    }

    fn decode_into(
        codec: &mut Box<dyn Codec>,
        data: Vec<u8>,
        ingest_ns: u64,
        meta: &Value<'static>,
    ) -> Option<Result<EventPayload>> {
        let payload = EventPayload::try_new::<Option<Error>, _>(data, |mut_data| {
            match codec.decode(mut_data, ingest_ns) {
                Ok(None) => Err(None),
                Err(e) => Err(Some(e)),
                Ok(Some(decoded)) => Ok(ValueAndMeta::from_parts(decoded, meta.clone())),
            }
        });
        match payload {
            Ok(ep) => Some(Ok(ep)),
            Err(Some(e)) => Some(Err(e)),
            Err(None) => None,
        }
    }

    /// deserialize event for a given stream
    ///
    /// # Errors
    ///   * if serialization failed (codec or postprocessors)
    pub fn deserialize_for_stream(
        &mut self,
        data: Vec<u8>,
        meta: Option<Value<'static>>,
        ingest_ns: &mut u64,
        stream_id: u64,
        url: &TremorUrl,
    ) -> Vec<Result<EventPayload>> {
        let meta = meta.unwrap_or_else(Value::object); // empty object as default meta
        if stream_id == DEFAULT_STREAM_ID {
            match preprocess(&mut self.preprocessors, ingest_ns, data, url) {
                Ok(preprocessed) => preprocessed
                    .into_iter()
                    .filter_map(|chunk| {
                        Self::decode_into(&mut self.codec, chunk, *ingest_ns, &meta)
                    })
                    .collect(),
                Err(e) => vec![Err(e)],
            }
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (c, pps) = entry.get_mut();
                    match preprocess(pps, ingest_ns, data, url) {
                        Ok(preprocessed) => preprocessed
                            .into_iter()
                            .filter_map(|p| Self::decode_into(c, p, *ingest_ns, &meta))
                            .collect(),
                        Err(e) => vec![Err(e)],
                    }
                }
                Entry::Vacant(entry) => {
                    let codec = stry_vec!(codec::resolve(&self.codec_config));
                    let pps = stry_vec!(make_preprocessors(self.preprocessor_names.as_slice()));
                    // insert data for a new stream
                    let (codec, pps) = entry.insert((codec, pps));
                    match preprocess(pps, ingest_ns, data, url) {
                        Ok(preprocessed) => preprocessed
                            .into_iter()
                            .filter_map(|p| Self::decode_into(codec, p, *ingest_ns, &meta))
                            .collect(),
                        Err(e) => vec![Err(e)],
                    }
                }
            }
        }
    }
}

pub struct SourceManagerBuilder {
    qsize: usize,
    deserializer: EventDeserializer,
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
    let preprocessor_names = config.preprocessors.clone().unwrap_or_else(Vec::new);
    let deserializer = EventDeserializer::build(
        config.codec.clone(),
        connector_default_codec,
        preprocessor_names,
    )?;

    Ok(SourceManagerBuilder {
        qsize,
        deserializer,
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
    deserializer: EventDeserializer,
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
        let SourceManagerBuilder { deserializer, .. } = builder;
        Self {
            source,
            ctx,
            rx,
            deserializer,
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
