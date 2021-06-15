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

use crate::errors::{Error, Result};
use crate::pipeline;
use crate::url::TremorUrl;
use async_channel::{bounded, Receiver, Sender};
use beef::Cow;
use halfbrown::HashMap;

pub(crate) enum SourceMsg {
    Connect {
        port: Cow<'static, str>,
        pipelines: Vec<(TremorUrl, pipeline::Addr)>,
    },
    Disconnect {
        port: Cow<'static, str>,
        id: TremorUrl,
    },
    // TODO: fill those
    Start,
    Pause,
    Resume,
    Stop,
}

/// source part of a connector
#[async_trait::async_trait]
pub(crate) trait Source: Send {
    /// Pulls an event from the source if one exists
    /// determine the codec to be used
    async fn pull_data(&mut self, id: u64) -> Result<crate::source::SourceReply>;
    /// This callback is called when the data provided from
    /// pull_event did not create any events, this is needed for
    /// linked sources that require a 1:1 mapping between requests
    /// and responses, we're looking at you REST
    async fn on_no_events(&mut self, _id: u64, _stream: usize) -> Result<()> {
        Ok(())
    }
    // TODO: lifecycle callbacks
}

pub(crate) struct ChannelSource {
    rx: Receiver<crate::source::SourceReply>,
    tx: Sender<crate::source::SourceReply>,
}

impl ChannelSource {
    pub(crate) fn new(qsize: usize) -> Self {
        let (tx, rx) = bounded(qsize);
        Self { tx, rx }
    }

    pub(crate) fn sender(&self) -> Sender<crate::source::SourceReply> {
        self.tx.clone()
    }
}

#[async_trait::async_trait()]
impl Source for ChannelSource {
    async fn pull_data(&mut self, _id: u64) -> Result<crate::source::SourceReply> {
        if self.rx.is_empty() {
            // TODO: configure pull interval in connector config?
            Ok(crate::source::SourceReply::Empty(10))
        } else {
            self.rx.try_recv().map_err(Error::from)
        }
    }
}

// TODO
// TODO make fields private and add some nice methods
pub(crate) struct SourceContext {
    /// connector uid
    pub(crate) uid: u64,
    /// connector url
    pub(crate) url: TremorUrl,
    // TODO: preprocessors/interceptor-chain?
}

#[derive(Clone, Debug)]
pub(crate) struct SourceAddr {
    pub(crate) addr: async_channel::Sender<SourceMsg>,
}

/// source control plane
pub(crate) async fn source_task(
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
