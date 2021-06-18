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
use crate::connectors::prelude::*;
use crate::connectors::sink::{ChannelSink, ChannelSinkMsg};
use crate::connectors::source::ChannelSource;
use crate::source::SourceReply;
use async_channel::{bounded, Sender, TrySendError};
use async_std::net::TcpListener;
use async_std::task::{self, JoinHandle};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use tremor_common::stry;
use tremor_pipeline::EventOriginUri;

const URL_SCHEME: &str = "tremor-tcp";

#[derive(Deserialize, Debug)]
pub struct Config {
    // kept as a str, so it is re-resolved upon each connect
    host: String,
    port: u16,
    // TCP: receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

fn default_buf_size() -> usize {
    8192
}

impl ConfigImpl for Config {}

pub struct TcpServer {
    url: TremorUrl,
    config: Config,
    accept_task: Option<JoinHandle<Result<()>>>,
    sink_channel: Sender<ChannelSinkMsg>,
    source_channel: Sender<SourceReply>,
}

impl ConnectorBuilder for TcpServer {
    fn from_config(url: &TremorUrl, raw_config: &Option<OpConfig>) -> Result<Box<dyn Connector>> {
        if let Some(raw_config) = raw_config {
            let config = Config::new(raw_config)?;
            let (dummy_tx, _dummy_rx) = bounded(1);
            let (dummy_tx2, _dummy_rx) = bounded(1);
            Ok(Box::new(TcpServer {
                url: url.clone(),
                config,
                accept_task: None, // not yet started
                sink_channel: dummy_tx2,
                source_channel: dummy_tx,
            }))
        } else {
            Err(ErrorKind::MissingConfiguration(String::from("TcpServer")).into())
        }
    }
}

/// send a message to a channel
///   - first try a non-blocking send
///   - if the queue is full, we log, use the blocking version and await on it
/// dont care about send errors, just log an error
async fn send<T>(url: &TremorUrl, msg: T, tx: &Sender<T>) -> Result<()> {
    let res = match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            // TODO: emit a metric about this instead of logging
            debug!("[Connector::{}] Send queue full.", &url);
            tx.send(m).await.map_err(Error::from)
        }
        Err(e) => Err(Error::from(e)),
    };

    if let Err(e) = &res {
        error!("[Connector::{}] Error sending message: {}", url, e);
    }
    res
}

#[async_trait::async_trait()]
impl Connector for TcpServer {
    async fn on_start(&mut self, _ctx: &ConnectorContext) -> Result<ConnectorState> {
        Ok(ConnectorState::Running)
    }

    async fn create_source(
        &mut self,
        _source_context: &mut SourceContext,
    ) -> Result<Option<Box<dyn Source>>> {
        let source = ChannelSource::new(crate::QSIZE);
        self.source_channel = source.sender();
        Ok(Some(Box::new(source)))
    }

    async fn create_sink(
        &mut self,
        _sink_context: &mut SinkContext,
    ) -> Result<Option<Box<dyn Sink>>> {
        let sink = ChannelSink::new(crate::QSIZE);
        self.sink_channel = sink.sender();
        Ok(Some(Box::new(sink)))
    }

    async fn connect(&mut self, ctx: &ConnectorContext) -> Result<bool> {
        let path = vec![self.config.port.to_string()];
        let uid = ctx.uid;
        let accept_url = self.url.clone();

        let sc = self.source_channel.clone();
        let sink_tx = self.sink_channel.clone();
        let buf_size = self.config.buf_size;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.cancel().await;
        }

        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        // accept task
        self.accept_task = Some(task::spawn(async move {
            // TODO: provide utility for stream id generation
            let mut stream_id = 0_usize;
            while let Ok((stream, peer_addr)) = listener.accept().await {
                let my_id: u64 = stream_id;
                stream_id += 1;

                let (mut read_half, mut write_half) = stream.split();
                let sc_stream = sc.clone();
                let stream_url = accept_url.clone();

                let origin_uri = EventOriginUri {
                    uid,
                    scheme: URL_SCHEME.to_string(),
                    host: peer_addr.ip().to_string(),
                    port: Some(peer_addr.port()),
                    // TODO also add token_num here?
                    path: path.clone(), // captures server port
                };

                // TODO: issue metrics on send time, to detect queues running full
                stry!(send(&accept_url, SourceReply::StartStream(my_id), &sc).await);

                // spawn source stream task
                task::spawn(async move {
                    let mut buffer = Vec::with_capacity(buf_size);
                    while let Ok(bytes_read) = read_half.read(&mut buffer).await {
                        let sc_data = SourceReply::Data {
                            origin_uri: origin_uri.clone(),
                            codec_override: None,
                            stream: my_id as usize,
                            meta: None,
                            // ALLOW: we know bytes_read is smaller than or equal buf_size
                            data: buffer[0..bytes_read].to_vec(),
                        };
                        stry!(send(&stream_url, sc_data, &sc_stream).await);
                    }
                    Ok(())
                });

                let (stream_tx, stream_rx) = bounded::<SinkData>(crate::QSIZE);

                // spawn sink stream task
                task::spawn(async move {
                    while let Ok(data) = stream_rx.recv().await {
                        for chunk in data.data {
                            let slice: &[u8] = &chunk;
                            write_half.write_all(slice).await?;
                        }
                    }
                    Result::Ok(())
                });
                sink_tx
                    .send(ChannelSinkMsg::NewStream(my_id, stream_tx))
                    .await?;
            }
            // TODO: notify connector task about disconnect
            Ok(())
        }));

        Ok(true)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
