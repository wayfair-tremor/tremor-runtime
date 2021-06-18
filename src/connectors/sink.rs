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

use crate::codec::Codec;
use crate::errors::Result;
use crate::pipeline;
use crate::postprocessor::{postprocess, Postprocessors};
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use bimap::BiMap;
use hashbrown::HashMap;
use simd_json::ValueAccess;
use std::hash::Hash;
use tremor_pipeline::Event;
use tremor_value::Value;

/// Circuit Breaker state change reply
pub enum CbReply {
    /// a stream became unavailable
    StreamTriggered(u64),
    /// a stream became avaiable again
    StreamRestored(u64),
    /// the whole sink became unavailable
    /// upstream onramps should stop sending
    Triggered,
    /// the whole sink became available again
    Restored,
}

/// stuff a sink replies back upon an event or a signal
/// to the calling sink/connector manager
pub enum SinkReply {
    /// everything went smoothly, chill
    Ack,
    /// shit hit the fan, but only for this event, nothing big
    Fail,
    /// it seems a single stream or the whole sink became unavaiable or available again
    CB(CbReply),
}

/// Result for a sink function that may provide insights or response.
///
/// It can return None or Some(vec![]) if no insights/response were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
pub(crate) type ResultVec = Result<Vec<SinkReply>>;

#[async_trait::async_trait]
pub(crate) trait Sink: Send {
    async fn on_event(
        &mut self,
        input: &'static str,
        event: Event,
        ctx: &mut SinkContext,
    ) -> ResultVec;
    async fn on_signal(&mut self, signal: Event, ctx: &mut SinkContext) -> ResultVec;
}

#[derive(Clone, Debug)]
pub(crate) struct SinkData {
    pub(crate) data: Vec<Vec<u8>>,
    pub(crate) event_id: u64,
}

pub(crate) enum ChannelSinkMsg<T>
where
    T: Hash + Eq,
{
    NewStream {
        stream_id: u64,
        meta: Option<T>,
        sender: Sender<SinkData>,
    },
    RemoveStream(u64),
}

/// tracking 1 channel per stream
pub(crate) struct ChannelSink<'value, T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'value>) -> Option<T>,
{
    // TODO: check if a vec w/ binary search is more performant in general (also for big sizes)
    streams_meta: BiMap<T, u64>,
    streams: HashMap<u64, Sender<SinkData>>,
    resolver: F,
    tx: Sender<ChannelSinkMsg<T>>,
    rx: Receiver<ChannelSinkMsg<T>>,
}

impl<'value, T, F> ChannelSink<'value, T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'value>) -> Option<T>,
{
    pub(crate) fn new(qsize: usize, resolver: F) -> Self {
        let (tx, rx) = bounded(qsize);
        let streams = HashMap::with_capacity(8);
        let streams_meta = BiMap::with_capacity(8);
        Self {
            tx,
            rx,
            streams,
            streams_meta,
            resolver,
        }
    }

    /// hand out a clone of the `Sender` to reach this sink for new streams
    pub(crate) fn sender(&self) -> Sender<ChannelSinkMsg<T>> {
        self.tx.clone()
    }

    /// returns true, if there are no more channels to send stuff to
    fn handle_channels(&mut self) -> bool {
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
                    // TODO: somehow make the meta map also indexed on the stream id
                    self.remove_stream(stream_id);
                }
            }
        }
        // clean out closed streams
        self.streams.retain(|_k, v| !v.is_closed());
        self.streams_meta.retain(|_k, v| !v.is_closed());
        self.streams.is_empty()
    }

    fn remove_stream(&self, stream_id: u64) {
        self.streams.remove(&stream_id);
        self.streams_meta.remove_by_right(&stream_id);
    }

    async fn send_or_remove(
        &self,
        stream_id: u64,
        sender: Sender<SinkData>,
        sink_data: SinkData,
        ctx: &SinkContext,
    ) -> Vec<SinkReply> {
        let res = Vec::with_capacity(2);
        if let Err(send_error) = sender.send(sink_data).await {
            error!(
                "[Connector::{}] Error sending to closed stream {}.",
                ctx.url, stream_id
            );
            self.remove_stream(stream_id);
            res.push(SinkReply::Fail);
            res.push(SinkReply::CB(CbReply::StreamTriggered(stream_id)));
        } else {
            res.push(SinkReply::Ack);
        }
        res
    }
}

/// Extract sink specific metadata from event metadata
///
/// The general path is $<RESOURCE_TYPE>.<ARTEFACT>
/// Example: `$connector.foo`
fn get_sink_meta<'lt, 'value>(
    meta: &'lt Value<'value>,
    ctx: &SinkContext,
) -> Option<&'lt Value<'value>> {
    ctx.url
        .resource_type()
        .and_then(|rt| meta.get(rt.to_string().into()))
        .and_then(|rt_meta| {
            ctx.url
                .artefact()
                .and_then(|artefact| rt_meta.get(artefact.into()))
        })
}

#[async_trait::async_trait()]
impl<'value, T, F> Sink for ChannelSink<'value, T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'value>) -> Option<T>,
{
    async fn on_event(
        &mut self,
        _input: &'static str,
        event: Event,
        ctx: &mut SinkContext,
    ) -> ResultVec {
        // clean up
        // moved to on_signal due to overhead for each event?
        // self.handle_channels();

        let ingest_ns = event.ingest_ns;
        let id = event.id;
        let res = Vec::with_capacity(event.len());
        for (value, meta) in event.value_meta_iter() {
            // encode and postprocess event data
            let encoded = ctx.codec.encode(value)?;
            let processed = postprocess(&mut ctx.postprocessors, ingest_ns, encoded)?;

            // route based on stream id present in event metadata or in event id (trackign the event origin)
            // resolve by checking meta for sink specific metadata
            // fallback: get all tracked stream_ids for the current connector uid
            //
            if let Some((stream_id, sender)) = get_sink_meta(meta, &ctx)
                .and_then(|sink_meta| (self.resolver)(sink_meta))
                .and_then(|stream_meta| self.streams_meta.get_by_left(&stream_meta))
                .and_then(|stream_id| {
                    self.streams
                        .get(stream_id)
                        .map(|sender| (stream_id, sender))
                })
            {
                let sink_data = SinkData {
                    data: processed,
                    event_id: id.event_id(),
                };
                res.append(
                    self.send_or_remove(*stream_id, sender, sink_data, ctx)
                        .await,
                );
            } else {
                // check event id for stream ids from this connector
                let stream_ids = id.get_streams(ctx.uid);
                let senders_iter = stream_ids
                    .into_iter()
                    .filter_map(|sid| self.streams.get(sid).map(|sender| (sid, sender)));

                if let Some((stream_id, first)) = senders_iter.next() {
                    let sink_data = SinkData {
                        data: processed,
                        event_id: id.event_id(),
                    };
                    for (stream_id, sender) in senders_iter {
                        res.append(
                            self.send_or_remove(stream_id, sender, sink_data.clone(), ctx)
                                .await,
                        );
                    }
                    res.append(self.send_or_remove(stream_id, first, sink_data, ctx).await);
                } else {
                    error!(
                        "[Connector::{}] Error resolving stream for event {}",
                        &ctx.url, &id
                    );
                    res.push(SinkReply::Fail)
                }
            }
        }
        Ok(res)
    }
    async fn on_signal(&mut self, _signal: Event, _ctx: &mut SinkContext) -> ResultVec {
        self.handle_channels();
        // TODO: handle signal
        Ok(None)
    }
}

pub(crate) struct SinkContext {
    pub(crate) uid: u64,
    pub(crate) url: TremorUrl,
    pub(crate) codec: Box<dyn Codec>,
    pub(crate) postprocessors: Postprocessors,
}

pub(crate) enum SinkMsg {
    Connect {
        port: Cow<'static, str>,
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    Disconnect {
        id: TremorUrl,
        port: Cow<'static, str>,
    },
    // TODO: fill those
    Start,
    Pause,
    Resume,
    Stop,
}

#[derive(Clone, Debug)]
pub(crate) struct SinkAddr {
    pub(crate) addr: async_channel::Sender<SinkMsg>,
}

pub(crate) async fn sink_task(
    receiver: async_channel::Receiver<SinkMsg>,
    _sink: Box<dyn Sink>,
    _ctx: SinkContext,
) -> Result<()> {
    let mut connected: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>> =
        HashMap::with_capacity(1); // 1 connected to IN port default

    while let Ok(sink_msg) = receiver.recv().await {
        match sink_msg {
            SinkMsg::Connect {
                port,
                mut pipelines,
            } => {
                if let Some(pipes) = connected.get_mut(&port) {
                    pipes.append(&mut pipelines);
                } else {
                    connected.insert(port, pipelines);
                }
            }
            SinkMsg::Disconnect { id, port } => {
                let delete = if let Some(pipes) = connected.get_mut(&port) {
                    pipes.retain(|(url, _)| url == &id);
                    pipes.is_empty()
                } else {
                    false
                };
                if delete {
                    connected.remove(&port);
                }
            }
            SinkMsg::Start => {}
            SinkMsg::Resume => {}
            SinkMsg::Pause => {}
            SinkMsg::Stop => {}
        }
    }
    Ok(())
}
