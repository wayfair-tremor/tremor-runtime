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
use tremor_common::time::nanotime;
use tremor_pipeline::Event;
use tremor_value::Value;

/// Circuit Breaker state change reply
#[derive(Clone, Debug)]
pub enum CbReply {
    /// a stream became unavailable
    StreamTriggered(u64),
    /// a stream became avaiable again
    StreamRestored(u64),
    /// the whole sink became unavailable
    /// upstream onramps should stop sending
    Triggered,
    /// the whole sink became available again
    /// upstream onramps should start sending again
    Restored,
}

/// stuff a sink replies back upon an event or a signal
/// to the calling sink/connector manager
#[derive(Clone, Debug)]
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
pub type ResultVec = Result<Vec<SinkReply>>;

/// connector sink - receiving events
#[async_trait::async_trait]
pub trait Sink: Send {
    /// called when receiving an event
    async fn on_event(
        &mut self,
        input: &'static str,
        event: Event,
        ctx: &mut SinkContext,
    ) -> ResultVec;
    /// called when receiving a signal
    async fn on_signal(&mut self, signal: Event, ctx: &mut SinkContext) -> ResultVec;

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
}

/// some data for a ChannelSink stream
#[derive(Clone, Debug)]
pub struct SinkData {
    /// data to send
    pub data: Vec<Vec<u8>>,
    /// numeric event id
    pub event_id: u64,
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
    // TODO: check if a vec w/ binary search is more performant in general (also for big sizes)
    streams_meta: BiMap<T, u64>,
    streams: HashMap<u64, Sender<SinkData>>,
    resolver: F,
    tx: Sender<ChannelSinkMsg<T>>,
    rx: Receiver<ChannelSinkMsg<T>>,
}

impl<T, F> ChannelSink<T, F>
where
    T: Hash + Eq,
    F: Fn(&Value<'_>) -> Option<T>,
{
    /// constructor
    pub fn new(qsize: usize, resolver: F) -> Self {
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
    pub fn sender(&self) -> Sender<ChannelSinkMsg<T>> {
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
                    self.remove_stream(stream_id);
                }
            }
        }
        // clean out closed streams
        for (k, _) in self.streams.drain_filter(|_k, v| v.is_closed()) {
            self.streams_meta.remove_by_right(&k);
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
/// Example: `$connector.foo`
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
    async fn on_event(
        &mut self,
        _input: &'static str,
        event: Event,
        ctx: &mut SinkContext,
    ) -> ResultVec {
        // clean up
        // moved to on_signal due to overhead for each event?
        // TODO: re-enable to ensure we handle all new/removes streams before an event in order to not miss a first event
        // self.handle_channels();

        let connector_url = ctx.url.clone();
        let ingest_ns = event.ingest_ns;
        let id = &event.id;
        let stream_ids = id.get_streams(ctx.uid);
        let mut res = Vec::with_capacity(event.len());
        for (value, meta) in event.value_meta_iter() {
            // encode and postprocess event data
            let encoded = ctx.codec.encode(value)?;
            let processed = postprocess(&mut ctx.postprocessors, ingest_ns, encoded)?;

            let mut errored = false;
            let mut remove_streams = vec![];
            // route based on stream id present in event metadata or in event id (trackign the event origin)
            // resolve by checking meta for sink specific metadata
            // fallback: get all tracked stream_ids for the current connector uid
            //
            if let Some((stream_id, sender)) = self.resolve_stream_from_meta(meta, &ctx) {
                // resolved stream by meta
                let sink_data = SinkData {
                    data: processed,
                    event_id: id.event_id(),
                };
                // TODO: externalize to async fn
                errored = if let Err(_send_error) = sender.send(sink_data).await {
                    error!(
                        "[Connector::{}] Error sending to closed stream {}.",
                        connector_url, stream_id
                    );
                    remove_streams.push(*stream_id);
                    true
                } else {
                    false
                };
            } else {
                // check event id for stream ids from this connector
                let mut senders_iter = stream_ids
                    .iter()
                    .filter_map(|sid| self.streams.get(sid).map(|sender| (sid, sender)));

                if let Some((stream_id, first)) = senders_iter.next() {
                    let sink_data = SinkData {
                        data: processed,
                        event_id: id.event_id(),
                    };
                    for (stream_id, sender) in senders_iter {
                        if let Err(_) = sender.send(sink_data.clone()).await {
                            error!(
                                "[Connector::{}] Error sending to closed stream {}.",
                                connector_url, stream_id
                            );
                            remove_streams.push(*stream_id);
                            errored = true;
                        }
                    }
                    if let Err(_) = first.send(sink_data).await {
                        error!(
                            "[Connector::{}] Error sending to closed stream {}.",
                            connector_url, stream_id
                        );
                        remove_streams.push(*stream_id);
                        // self.remove_stream(stream_id);
                        // res.push(SinkReply::CB(CbReply::StreamTriggered(stream_id)));
                        errored = true;
                    }
                } else {
                    error!(
                        "[Connector::{}] Error resolving stream for event {}",
                        connector_url, id
                    );
                    errored = true;
                }
            }
            res.push(if errored {
                SinkReply::Fail
            } else {
                SinkReply::Ack
            });
            for stream_id in remove_streams {
                self.remove_stream(stream_id);
                res.push(SinkReply::CB(CbReply::StreamTriggered(stream_id)));
            }
        }
        Ok(res)
    }

    async fn on_signal(&mut self, _signal: Event, _ctx: &mut SinkContext) -> ResultVec {
        self.handle_channels();
        // TODO: handle signal
        Ok(vec![])
    }
}

/// context for the connector sink
pub struct SinkContext {
    /// the connector unique identifier
    pub uid: u64,
    /// the connector url
    pub url: TremorUrl,
    /// the configured codec
    pub codec: Box<dyn Codec>,
    /// the configures post processors
    pub postprocessors: Postprocessors,
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

/// address of a connector sink
#[derive(Clone, Debug)]
pub struct SinkAddr {
    /// the actual sender
    pub addr: async_channel::Sender<SinkMsg>,
}

/// sink logic task
pub async fn sink_task(
    receiver: async_channel::Receiver<SinkMsg>,
    mut sink: Box<dyn Sink>,
    mut ctx: SinkContext,
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
            SinkMsg::Start => sink.on_start(&mut ctx).await,
            SinkMsg::Resume => sink.on_resume(&mut ctx).await,
            SinkMsg::Pause => sink.on_pause(&mut ctx).await,
            SinkMsg::Stop => sink.on_stop(&mut ctx).await,
            SinkMsg::ConnectionEstablished => {
                // mark as restored and send CB to all pipes
                for pipes in connected.values() {
                    for (_url, addr) in pipes {
                        addr.send_insight(Event::cb_restore(nanotime())).await?;
                    }
                }
            }
            SinkMsg::ConnectionLost => {
                // mark as triggered and send CB to all pipes
                for pipes in connected.values() {
                    for (_url, addr) in pipes {
                        addr.send_insight(Event::cb_trigger(nanotime())).await?;
                    }
                }
            }
            SinkMsg::Event { .. } => todo!(),
            SinkMsg::Signal { .. } => todo!(),
        }
    }
    Ok(())
}
