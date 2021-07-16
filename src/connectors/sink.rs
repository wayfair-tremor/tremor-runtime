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

use crate::codec::{self, Codec};
use crate::config::{CodecConfig, Connector as ConnectorConfig};
use crate::errors::Result;
use crate::permge::PriorityMerge;
use crate::pipeline;
use crate::postprocessor::{make_postprocessors, postprocess, Postprocessors};
use crate::url::ports::IN;
use crate::url::TremorUrl;
use async_channel::{self, bounded, Receiver, Sender};
use async_std::stream::StreamExt; // for .next() on PriorityMerge
use async_std::task;
use beef::Cow;
use bimap::BiMap;
use either::Either;
use hashbrown::HashMap;
use simd_json::ValueAccess;
use std::borrow::Borrow;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::hash::Hash;
use tremor_common::time::nanotime;
use tremor_pipeline::{CbAction, Event, EventId, OpMeta, SignalKind, DEFAULT_STREAM_ID};

use tremor_value::Value;

/// stuff a sink replies back upon an event or a signal
/// to the calling sink/connector manager
#[derive(Clone, Debug)]
pub enum SinkReply {
    /// no reply - maybe no reply yet, maybe replies come asynchronously...
    None,
    /// everything went smoothly, chill
    Ack,
    /// shit hit the fan, but only for this event, nothing big
    Fail,
    /// the whole sink became unavailable or available again
    CB(CbAction),
}

/// some basic Event data needed for generating correct contraflow messages
#[derive(Clone, Debug)]
pub struct EventCfData {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: OpMeta,
}

impl From<&Event> for EventCfData {
    fn from(event: &Event) -> Self {
        Self {
            event_id: event.id.clone(),
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.clone(),
        }
    }
}

/// Possible replies from asynchronous sinks via reply_channel from event or signal handling
pub enum AsyncSinkReply {
    Ack(EventCfData, u64),
    Fail(EventCfData),
    CB(EventCfData, CbAction),
}

/// Result for a sink function that may provide insights or response.
///
/// It can return None or Some(vec![]) if no insights/response were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
pub type ResultVec = Result<Vec<SinkReply>>;

/// connector sink - receiving events
#[async_trait::async_trait]
pub trait Sink: Send {
    /// called when receiving an event
    async fn on_event(
        &mut self,
        input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> ResultVec;
    /// called when receiving a signal
    async fn on_signal(
        &mut self,
        signal: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> ResultVec;

    // lifecycle stuff
    /// called when started
    async fn on_start(&mut self, _ctx: &mut SinkContext) {}
    /// called when paused
    async fn on_pause(&mut self, _ctx: &mut SinkContext) {}
    /// called when resumed
    async fn on_resume(&mut self, _ctx: &mut SinkContext) {}
    /// called when stopped
    async fn on_stop(&mut self, _ctx: &mut SinkContext) {}

    // connectivity stuff
    /// called when sink lost connectivity
    async fn on_connection_lost(&mut self, _ctx: &mut SinkContext) {}
    /// called when sink re-established connectivity
    async fn on_connection_established(&mut self, _ctx: &mut SinkContext) {}

    /// if `true` events are acknowledged/failed automatically by the sink manager.
    /// Such sinks should return SinkReply::None from on_event or SinkReply::Fail if they fail immediately.
    ///
    /// if `false` events need to be acked/failed manually by the sink impl
    fn auto_ack(&self) -> bool {
        true
    }

    /// if true events are sent asynchronously, not necessarily when `on_event` returns.
    /// if false events can be considered delivered once `on_event` returns.
    fn asynchronous(&self) -> bool {
        false
    }
}

/// some data for a ChannelSink stream
#[derive(Clone, Debug)]
pub struct SinkData {
    /// data to send
    pub data: Vec<Vec<u8>>,
    /// async reply utils (if required)
    pub contraflow: Option<(EventCfData, Sender<AsyncSinkReply>)>,
    /// timestamp of processing start
    pub start: u64,
}

/// messages a channel sink can receive
pub enum ChannelSinkMsg<T>
where
    T: Hash + Eq,
{
    /// add a new stream
    NewStream {
        /// the id of the stream
        stream_id: u64,
        /// stream metadata used for resolving a stream
        meta: Option<T>,
        /// sender to the actual stream handling data
        sender: Sender<SinkData>,
    },
    /// remove the stream
    RemoveStream(u64),
}

/// tracking 1 channel per stream
pub struct ChannelSink<T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'_>) -> Option<T>,
{
    streams_meta: BiMap<T, u64>,
    streams: HashMap<u64, Sender<SinkData>>,
    resolver: F,
    tx: Sender<ChannelSinkMsg<T>>,
    rx: Receiver<ChannelSinkMsg<T>>,
    reply_tx: Sender<AsyncSinkReply>,
}

impl<T, F> ChannelSink<T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// constructor
    pub fn new(qsize: usize, resolver: F, reply_tx: Sender<AsyncSinkReply>) -> Self {
        let (tx, rx) = bounded(qsize);
        let streams = HashMap::with_capacity(8);
        let streams_meta = BiMap::with_capacity(8);
        Self {
            tx,
            rx,
            streams,
            streams_meta,
            resolver,
            reply_tx,
        }
    }

    /// hand out a clone of the `Sender` to reach this sink for new streams
    pub fn sender(&self) -> Sender<ChannelSinkMsg<T>> {
        self.tx.clone()
    }

    fn handle_channels_quickly(&mut self, serializer: &mut EventSerializer) -> bool {
        self.handle_channels(serializer, false)
    }
    /// returns true, if there are no more channels to send stuff to
    fn handle_channels(
        &mut self,
        serializer: &mut EventSerializer,
        clean_closed_streams: bool,
    ) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ChannelSinkMsg::NewStream {
                    stream_id,
                    meta,
                    sender,
                } => {
                    self.streams.insert(stream_id, sender);
                    if let Some(meta) = meta {
                        self.streams_meta.insert(meta, stream_id);
                    }
                }
                ChannelSinkMsg::RemoveStream(stream_id) => {
                    self.remove_stream(stream_id);
                    serializer.drop_stream(stream_id);
                }
            }
        }
        // clean out closed streams
        if clean_closed_streams {
            for (stream_id, _) in self.streams.drain_filter(|_k, v| v.is_closed()) {
                self.streams_meta.remove_by_right(&stream_id);
                serializer.drop_stream(stream_id)
            }
        }
        self.streams.is_empty()
    }

    fn remove_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
        self.streams_meta.remove_by_right(&stream_id);
    }

    fn resolve_stream_from_meta<'lt, 'value>(
        &self,
        meta: &'lt Value<'value>,
        ctx: &SinkContext,
    ) -> Option<(&u64, &Sender<SinkData>)> {
        get_sink_meta(meta, ctx)
            .and_then(|sink_meta| (self.resolver)(sink_meta))
            .and_then(|stream_meta| self.streams_meta.get_by_left(&stream_meta))
            .and_then(|stream_id| {
                self.streams
                    .get(stream_id)
                    .map(|sender| (stream_id, sender))
            })
    }
}

/// Extract sink specific metadata from event metadata
///
/// The general path is $<RESOURCE_TYPE>.<ARTEFACT>
/// Example: `$connector.tcp_server`
fn get_sink_meta<'lt, 'value>(
    meta: &'lt Value<'value>,
    ctx: &SinkContext,
) -> Option<&'lt Value<'value>> {
    ctx.url
        .resource_type()
        .and_then(|rt| meta.get(&Cow::owned(rt.to_string())))
        .and_then(|rt_meta| {
            ctx.url
                .artefact()
                .and_then(|artefact| rt_meta.get(artefact.into()))
        })
}

#[async_trait::async_trait()]
impl<T, F> Sink for ChannelSink<T, F>
where
    T: Hash + Eq + Send + Sync,
    F: (Fn(&Value<'_>) -> Option<T>) + Send + Sync,
{
    /// FIXME: use reply_channel to only ack delivery once it is successfully sent via TCP
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        serializer: &mut EventSerializer,
        start: u64,
    ) -> ResultVec {
        // clean up
        // make sure channels for the given event are added to avoid stupid errors
        // due to channels not yet handled
        let empty = self.handle_channels_quickly(serializer);
        if empty {
            // no streams available :sob:
            return Ok(vec![SinkReply::Fail, SinkReply::CB(CbAction::Close)]);
        }

        let ingest_ns = event.ingest_ns;
        let stream_ids = event.id.get_streams(ctx.uid);

        let contraflow_utils = if event.transactional {
            Some((EventCfData::from(&event), self.reply_tx.clone()))
        } else {
            None
        };

        let mut res = Vec::with_capacity(event.len());
        for (value, meta) in event.value_meta_iter() {
            let mut errored = false;
            let mut found = false;
            let mut remove_streams = vec![];
            // route based on stream id present in event metadata or in event id (trackign the event origin)
            // resolve by checking meta for sink specific metadata
            // fallback: get all tracked stream_ids for the current connector uid
            //
            let streams = if let Some(stream) = self.resolve_stream_from_meta(meta, ctx) {
                Either::Left(std::iter::once(stream))
            } else {
                Either::Right(
                    stream_ids
                        .iter()
                        .filter_map(|sid| self.streams.get(sid).map(|sender| (sid, sender))),
                )
            };
            for (stream_id, sender) in streams {
                let data = serializer.serialize_for_stream(value, ingest_ns, *stream_id)?;
                let sink_data = SinkData {
                    data,
                    contraflow: contraflow_utils.clone(),
                    start,
                };
                found = true;
                if let Err(_) = sender.send(sink_data).await {
                    error!(
                        "[Connector::{}] Error sending to closed stream {}.",
                        &ctx.url, stream_id
                    );
                    remove_streams.push(*stream_id);
                    errored = true;
                }
            }

            res.push(if errored || !found {
                SinkReply::Fail
            } else {
                // ack is sent when channel has delivered serialized event asynchronously
                SinkReply::None
            });
            for stream_id in remove_streams {
                self.remove_stream(stream_id);
                serializer.drop_stream(stream_id);
                // TODO: stream based CB
            }
        }
        Ok(res)
    }

    async fn on_signal(
        &mut self,
        signal: Event,
        _ctx: &SinkContext,
        serializer: &mut EventSerializer,
    ) -> ResultVec {
        if let Some(SignalKind::Tick) = signal.kind {
            self.handle_channels(serializer, true);
        }
        Ok(vec![])
    }

    fn asynchronous(&self) -> bool {
        // events are delivered asynchronously on their stream tasks
        true
    }

    fn auto_ack(&self) -> bool {
        // we handle ack/fail in the asynchronous streams
        false
    }
}

/// context for the connector sink
pub struct SinkContext {
    /// the connector unique identifier
    pub uid: u64,
    /// the connector url
    pub url: TremorUrl,
}

/// messages a sink can receive
pub enum SinkMsg {
    /// receive an event to handle
    Event {
        /// the event
        event: Event,
        /// the port through which it came
        port: Cow<'static, str>,
    },
    /// receive a signal
    Signal {
        /// the signal event
        signal: Event,
    },
    /// connect some pipelines to the give port
    Connect {
        /// the port
        port: Cow<'static, str>,
        /// the pipelines
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    /// disconnect a pipeline
    Disconnect {
        /// url of the pipeline
        id: TremorUrl,
        /// the port
        port: Cow<'static, str>,
    },
    /// the connection to the outside world wasl ost
    ConnectionLost,
    /// connection established
    ConnectionEstablished,
    // TODO: fill those
    /// start the sink
    Start,
    /// pause the sink
    Pause,
    /// resume the sink
    Resume,
    /// stop the sink
    Stop,
}

/// Wrapper around all possible sink messages
/// handled in the Sink task
enum SinkMsgWrapper {
    FromSink(AsyncSinkReply),
    ToSink(SinkMsg),
}

/// address of a connector sink
#[derive(Clone, Debug)]
pub struct SinkAddr {
    /// the actual sender
    pub addr: async_channel::Sender<SinkMsg>,
}

pub struct SinkManagerBuilder {
    qsize: usize,
    serializer: EventSerializer,
    reply_channel: (
        async_channel::Sender<AsyncSinkReply>,
        async_channel::Receiver<AsyncSinkReply>,
    ),
}

impl SinkManagerBuilder {
    /// globally configured queue size
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    /// Get yourself a sender to send replies back from your concrete sink.
    ///
    /// This is especially useful if your sink handles events asynchronously
    /// and you can't reply immediately.
    pub fn reply_tx(&self) -> async_channel::Sender<AsyncSinkReply> {
        self.reply_channel.0.clone()
    }

    /// spawn your specific sink
    pub fn spawn<S>(self, sink: S, ctx: SinkContext) -> Result<SinkAddr>
    where
        S: Sink + Send + 'static,
    {
        let qsize = self.qsize;
        let name = ctx.url.short_id("c-sink"); // connector sink
        let (sink_tx, sink_rx) = bounded(qsize);
        let manager = SinkManager::new(sink, ctx, self, sink_rx);
        // spawn manager task
        task::Builder::new().name(name).spawn(manager.run())?;

        Ok(SinkAddr { addr: sink_tx })
    }
}

/// create a builder for a `SinkManager`.
/// with the generic information available in the connector
/// the builder then in a second step takes the source specific information to assemble and spawn the actual `SinkManager`.
pub(crate) fn builder(
    config: &ConnectorConfig,
    connector_default_codec: &str,
    qsize: usize,
) -> Result<SinkManagerBuilder> {
    // resolve codec and processors
    let postprocessor_names = config.postprocessors.clone().unwrap_or_else(|| vec![]);
    let serializer = EventSerializer::build(
        config.codec.clone(),
        connector_default_codec,
        postprocessor_names,
    )?;
    // the incoming channels for events are all bounded, so we can safely be unbounded here
    // TODO: actually we could have lots of CB events not bound to events here
    let reply_channel = async_channel::unbounded();
    Ok(SinkManagerBuilder {
        qsize,
        serializer,
        reply_channel,
    })
}

/// Helper for serializing events within sinks
///
/// Keeps track of codec/postprocessors for seach stream
/// Attention: Take care to clear out data for streams that are not used
pub struct EventSerializer {
    // default stream handling
    codec: Box<dyn Codec>,
    postprocessors: Postprocessors,
    // creation templates for stream handling
    codec_config: Either<String, CodecConfig>,
    postprocessor_names: Vec<String>,
    // stream data
    // TODO: clear out state from codec, postprocessors and enable reuse
    streams: BTreeMap<u64, (Box<dyn Codec>, Postprocessors)>,
}

impl EventSerializer {
    fn build(
        codec_config: Option<Either<String, CodecConfig>>,
        default_codec: &str,
        postprocessor_names: Vec<String>,
    ) -> Result<Self> {
        let codec_config = codec_config.unwrap_or_else(|| Either::Left(default_codec.to_string()));
        let codec = codec::resolve(&codec_config)?;
        let postprocessors = make_postprocessors(postprocessor_names.as_slice())?;
        Ok(Self {
            codec,
            postprocessors,
            codec_config,
            postprocessor_names,
            streams: BTreeMap::new(),
        })
    }

    fn drop_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// clear out all streams - this can lead to data loss
    /// only use when you are sure, all the streams are gone
    fn clear(&mut self) {
        self.streams.clear()
    }

    /// serialize event for the default stream
    fn serialize(&mut self, value: &Value, ingest_ns: u64) -> Result<Vec<Vec<u8>>> {
        self.serialize_for_stream(value, ingest_ns, DEFAULT_STREAM_ID)
    }

    /// serialize event for a certain stream
    fn serialize_for_stream(
        &mut self,
        value: &Value,
        ingest_ns: u64,
        stream_id: u64,
    ) -> Result<Vec<Vec<u8>>> {
        if stream_id == DEFAULT_STREAM_ID {
            postprocess(
                &mut self.postprocessors,
                ingest_ns,
                self.codec.encode(value)?,
            )
        } else {
            match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let (c, pps) = entry.get_mut();
                    postprocess(pps, ingest_ns, c.encode(value)?)
                }
                Entry::Vacant(entry) => {
                    let codec = codec::resolve(&self.codec_config)?;
                    let pps = make_postprocessors(self.postprocessor_names.as_slice())?;
                    // insert data for a new stream
                    let (c, pps2) = entry.insert((codec, pps));
                    postprocess(pps2, ingest_ns, c.encode(value)?)
                }
            }
        }
    }
}

pub(crate) struct SinkManager<S>
where
    S: Sink,
{
    sink: S,
    ctx: SinkContext,
    rx: async_channel::Receiver<SinkMsg>,
    reply_rx: async_channel::Receiver<AsyncSinkReply>,
    serializer: EventSerializer,
    /// tracking which operators all incoming events visited
    merged_operator_meta: OpMeta,
    // pipelines connected to IN port
    pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    paused: bool,
}

impl<S> SinkManager<S>
where
    S: Sink,
{
    fn new(sink: S, ctx: SinkContext, builder: SinkManagerBuilder, rx: Receiver<SinkMsg>) -> Self {
        let SinkManagerBuilder {
            serializer,
            reply_channel,
            ..
        } = builder;
        Self {
            sink,
            ctx,
            rx,
            reply_rx: reply_channel.1,
            serializer,
            merged_operator_meta: OpMeta::default(),
            pipelines: Vec::with_capacity(1), // by default 1 connected to "in" port
            paused: true,                     // instantiated in paused state
        }
    }
    async fn run(mut self) -> Result<()> {
        let from_sink = self.reply_rx.map(SinkMsgWrapper::FromSink);
        let to_sink = self.rx.map(SinkMsgWrapper::ToSink);
        let mut from_and_to_sink_channel = PriorityMerge::new(from_sink, to_sink);

        loop {
            while let Some(msg_wrapper) = from_and_to_sink_channel.next().await {
                match msg_wrapper {
                    SinkMsgWrapper::ToSink(sink_msg) => {
                        match sink_msg {
                            SinkMsg::Connect {
                                port,
                                mut pipelines,
                            } => {
                                debug_assert!(
                                    port == IN,
                                    "[Sink::{}] connected to invalid connector sink port",
                                    &self.ctx.url
                                );
                                self.pipelines.append(&mut pipelines);
                            }
                            SinkMsg::Disconnect { id, port } => {
                                debug_assert!(
                                    port == IN,
                                    "[Sink::{}] disconnected from invalid connector sink port",
                                    &self.ctx.url
                                );
                                self.pipelines.retain(|(url, _)| url != &id);
                            }
                            SinkMsg::Start => self.sink.on_start(&mut self.ctx).await,
                            SinkMsg::Resume => {
                                self.paused = false;
                                self.sink.on_resume(&mut self.ctx).await
                            }
                            SinkMsg::Pause => {
                                self.paused = true;
                                self.sink.on_pause(&mut self.ctx).await
                            }
                            SinkMsg::Stop => {
                                self.sink.on_stop(&mut self.ctx).await;
                                return Ok(());
                            }
                            SinkMsg::ConnectionEstablished => {
                                let cf =
                                    Event::cb_open(nanotime(), self.merged_operator_meta.clone());
                                // send CB restore to all pipes
                                send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                            }
                            SinkMsg::ConnectionLost => {
                                // clean out all pending stream data from EventSerializer - we assume all streams closed at this point
                                self.serializer.clear();
                                // send CB trigger to all pipes
                                let cf =
                                    Event::cb_close(nanotime(), self.merged_operator_meta.clone());
                                send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                            }
                            SinkMsg::Event { event, port } => {
                                let cf_builder = ContraflowBuilder::from(&event);

                                // FIXME: fix additional clones here for merge
                                self.merged_operator_meta.merge(event.op_meta.clone());
                                let transactional = event.transactional;
                                // TODO: increment event in metric
                                let start = nanotime();
                                let res = self
                                    .sink
                                    .on_event(
                                        port.borrow(),
                                        event,
                                        &self.ctx,
                                        &mut self.serializer,
                                        start,
                                    )
                                    .await;
                                let duration = nanotime() - start;
                                match res {
                                    Ok(replies) => {
                                        // TODO: send metric for duration
                                        handle_replies(
                                            replies,
                                            duration,
                                            cf_builder,
                                            &self.pipelines,
                                            &self.ctx.url,
                                            transactional && self.sink.auto_ack(),
                                        )
                                        .await;
                                    }
                                    Err(_e) => {
                                        // sink error that is not signalled via SinkReply::Fail (not handled)
                                        // TODO: error logging? This could fill the logs quickly. Rather emit a metrics event with the logging info?
                                        if transactional {
                                            let cf = cf_builder.into_fail();
                                            send_contraflow(&self.pipelines, &self.ctx.url, cf)
                                                .await;
                                        }
                                    }
                                };
                            }
                            SinkMsg::Signal { signal } => {
                                let cf_builder = ContraflowBuilder::from(&signal);
                                let start = nanotime();
                                let res = self
                                    .sink
                                    .on_signal(signal, &self.ctx, &mut self.serializer)
                                    .await;
                                let duration = nanotime() - start;
                                match res {
                                    Ok(replies) => {
                                        handle_replies(
                                            replies,
                                            duration,
                                            cf_builder,
                                            &self.pipelines,
                                            &self.ctx.url,
                                            false,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        // logging here is ok, as this is mostly limited to ticks (every 100ms)
                                        error!(
                                            "[Connector::{}] Error handling signal: {}",
                                            &self.ctx.url, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    SinkMsgWrapper::FromSink(reply) => {
                        // handle asynchronous sink replies
                        let cf = match reply {
                            AsyncSinkReply::Ack(data, duration) => Event::cb_ack_with_timing(
                                data.ingest_ns,
                                data.event_id,
                                data.op_meta,
                                duration,
                            ),
                            AsyncSinkReply::Fail(data) => {
                                Event::cb_fail(data.ingest_ns, data.event_id, data.op_meta)
                            }
                            AsyncSinkReply::CB(data, cb) => {
                                Event::insight(cb, data.event_id, data.ingest_ns, data.op_meta)
                            }
                        };
                        send_contraflow(&self.pipelines, &self.ctx.url, cf).await;
                    }
                }
            }
        }
    }
}

pub(crate) struct ContraflowBuilder {
    event_id: EventId,
    ingest_ns: u64,
    op_meta: OpMeta,
}

impl ContraflowBuilder {
    fn ack(&self, duration: u64) -> Event {
        Event::cb_ack_with_timing(
            self.ingest_ns,
            self.event_id.clone(),
            self.op_meta.clone(),
            duration,
        )
    }
    fn into_ack(self, duration: u64) -> Event {
        Event::cb_ack_with_timing(self.ingest_ns, self.event_id, self.op_meta, duration)
    }
    fn fail(&self) -> Event {
        Event::cb_fail(self.ingest_ns, self.event_id.clone(), self.op_meta.clone())
    }
    fn into_fail(self) -> Event {
        Event::cb_fail(self.ingest_ns, self.event_id, self.op_meta)
    }
    fn cb(&self, cb: CbAction) -> Event {
        Event::insight(
            cb,
            self.event_id.clone(),
            self.ingest_ns,
            self.op_meta.clone(),
        )
    }
    fn into_cb(self, cb: CbAction) -> Event {
        Event::insight(cb, self.event_id, self.ingest_ns, self.op_meta)
    }
}

impl From<&Event> for ContraflowBuilder {
    fn from(event: &Event) -> Self {
        ContraflowBuilder {
            event_id: event.id.clone(),
            ingest_ns: event.ingest_ns,
            op_meta: event.op_meta.clone(), // TODO: mem::swap here?
        }
    }
}

/// send contraflow back to pipelines
async fn send_contraflow(
    pipelines: &[(TremorUrl, pipeline::Addr)],
    connector_url: &TremorUrl,
    contraflow: Event,
) {
    let mut iter = pipelines.iter();
    if let Some((first_url, first_addr)) = iter.next() {
        for (url, addr) in iter {
            if let Err(e) = addr.send_insight(contraflow.clone()).await {
                error!(
                    "[Connector::{}] Error sending contraflow to {}: {}",
                    &connector_url, url, e
                );
            }
        }
        if let Err(e) = first_addr.send_insight(contraflow).await {
            error!(
                "[Connector::{}] Error sending contraflow to {}: {}",
                &connector_url, first_url, e
            );
        }
    }
}

async fn handle_replies(
    replies: Vec<SinkReply>,
    duration: u64,
    cf_builder: ContraflowBuilder,
    pipelines: &[(TremorUrl, pipeline::Addr)],
    connector_url: &TremorUrl,
    send_auto_ack: bool,
) {
    let mut reply_iter = replies.into_iter();
    if let Some(first) = reply_iter.next() {
        for reply in reply_iter {
            let contraflow = match reply {
                SinkReply::Ack => cf_builder.ack(duration),
                SinkReply::Fail => cf_builder.fail(),
                SinkReply::CB(cb) => {
                    // we do not maintain a merged op_meta here, to avoid the cost
                    // the downside is, only operators which this event passed get to know this CB event
                    // but worst case is, 1 or 2 more events are lost - totally worth it
                    cf_builder.cb(cb)
                }
                SinkReply::None => {
                    continue;
                }
            };
            send_contraflow(pipelines, connector_url, contraflow).await;
        }
        match first {
            SinkReply::Ack => {
                send_contraflow(pipelines, connector_url, cf_builder.into_ack(duration)).await
            }
            SinkReply::Fail => {
                send_contraflow(pipelines, connector_url, cf_builder.into_fail()).await
            }
            SinkReply::CB(cb) => {
                // we do not maintain a merged op_meta here, to avoid the cost
                // the downside is, only operators which this event passed get to know this CB event
                // but worst case is, 1 or 2 more events are lost - totally worth it
                send_contraflow(pipelines, connector_url, cf_builder.into_cb(cb)).await
            }
            SinkReply::None => {
                if send_auto_ack {
                    let cf = cf_builder.into_ack(duration);
                    send_contraflow(pipelines, connector_url, cf).await;
                }
            }
        };
    }
}
