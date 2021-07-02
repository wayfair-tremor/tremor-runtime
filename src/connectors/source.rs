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

use crate::errors::Result;
use crate::pipeline;
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use halfbrown::HashMap;
use tremor_pipeline::EventOriginUri;
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
    // TODO: preprocessors/interceptor-chain?
}

/// address of a source
#[derive(Clone, Debug)]
pub struct SourceAddr {
    /// the actual address
    pub addr: async_channel::Sender<SourceMsg>,
}

/// source control plane
pub async fn source_task(
    receiver: async_channel::Receiver<SourceMsg>,
    _source: Box<dyn Source>,
    _ctx: SourceContext,
) -> Result<()> {
    let mut connected: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>> =
        HashMap::with_capacity(2); // capacity for OUT and ERR port

    // check control plane messages
    while let Ok(source_msg) = receiver.recv().await {
        match source_msg {
            SourceMsg::Connect {
                port,
                mut pipelines,
            } => {
                if let Some(pipes) = connected.get_mut(&port) {
                    pipes.append(&mut pipelines);
                } else {
                    connected.insert(port, pipelines);
                }
            }
            SourceMsg::Disconnect { id, port } => {
                if let Some(pipes) = connected.get_mut(&port) {
                    pipes.retain(|(url, _)| url == &id)
                }
            }
            SourceMsg::Start => {}
            SourceMsg::Resume => {}
            SourceMsg::Pause => {}
            SourceMsg::Stop => {}
        }
    }
    Ok(())
}
