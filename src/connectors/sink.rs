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
use crate::postprocessor::Postprocessors;
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use hashbrown::HashMap;
use tremor_pipeline::Event;

pub enum SinkReply {
    Contraflow(Event),
}

/// Result for a sink function that may provide insights or response.
///
/// It can return None or Some(vec![]) if no insights/response were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
pub(crate) type ResultVec = Result<Option<Vec<SinkReply>>>;

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

pub(crate) struct SinkData {
    pub(crate) data: Vec<Vec<u8>>,
    pub(crate) stream: usize,
    pub(crate) event_id: u64,
}

pub(crate) enum ChannelSinkMsg {
    NewStream(usize, Sender<SinkData>),
    RemoveStream(usize),
}

/// tracking 1 channel per stream
pub(crate) struct ChannelSink {
    // TODO: check if a vec w/ binary search is more performant in general (also for big sizes)
    streams: HashMap<usize, Sender<SinkData>>,
    tx: Sender<ChannelSinkMsg>,
    rx: Receiver<ChannelSinkMsg>,
}

impl ChannelSink {
    pub(crate) fn new(qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        let streams = HashMap::with_capacity(8);
        Self { tx, rx, streams }
    }

    /// hand out a clone of the `Sender` to reach this sink for new streams
    pub(crate) fn sender(&self) -> Sender<ChannelSinkMsg> {
        self.tx.clone()
    }

    /// returns true, if there are no more channels to send stuff to
    fn handle_channels(&mut self) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ChannelSinkMsg::NewStream(stream_id, sender) => {
                    if !sender.is_closed() {
                        self.streams.insert(stream_id, sender);
                    }
                }
                ChannelSinkMsg::RemoveStream(stream_id) => {
                    self.streams.remove(&stream_id);
                }
            }
        }
        // clean out closed streams
        self.streams.retain(|_k, v| !v.is_closed());
        self.streams.is_empty()
    }
}

#[async_trait::async_trait()]
impl Sink for ChannelSink {
    async fn on_event(
        &mut self,
        _input: &'static str,
        _event: Event,
        _ctx: &mut SinkContext,
    ) -> ResultVec {
        self.handle_channels();
        // TODO: encode and postprocess event data
        Ok(None)
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
