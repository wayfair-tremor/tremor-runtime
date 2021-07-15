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

#![cfg(not(tarpaulin_include))]

//! # UDP Offramp
//!
//! Sends each message as a udp datagram
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use std::time::Instant;

use crate::sink::prelude::*;
use async_std::net::UdpSocket;
use halfbrown::HashMap;

/// An offramp that write a given file
pub struct Udp {
    socket: Option<UdpSocket>,
    config: Config,
    postprocessors: Postprocessors,
}

#[derive(Deserialize, Debug)]
struct Host {
    host: String,
    port: u16,
}
impl Default for Host {
    fn default() -> Self {
        Self {
            host: String::from("0.0.0.0"),
            port: 0,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// Host to use as source
    host: String,
    port: u16,
    dst_host: Option<String>,
    dst_port: Option<u16>,
    #[serde(default = "Host::default")]
    bind: Host,
    #[serde(default = "t")]
    bound: bool,
}

fn t() -> bool {
    true
}

impl ConfigImpl for Config {}
impl Udp {
    /// Binds and possibly 'connects' the udp socket
    async fn bind(&mut self) -> Result<()> {
        if self.socket.is_none() {
            info!(
                "[Sink::UDP] binding to {}:{} ...",
                self.config.bind.host, self.config.bind.port
            );
            let socket =
                UdpSocket::bind((self.config.bind.host.as_str(), self.config.bind.port)).await?;
            info!("[Sink::UDP] bound.");
            if self.config.bound {
                info!(
                    "[Sink::UDP] set peer address to {}:{} ...",
                    self.config.host, self.config.port
                );
                socket
                    .connect((self.config.host.as_str(), self.config.port))
                    .await?;
                info!("[Sink::UDP] peer address set.");
            }
            self.socket = Some(socket);
        };
        Ok(())
    }
}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let mut config: Config = Config::new(config)?;
            if !config.bound {
                warn!("The `bound` setting of the UDP offramp is deprecated, in future re-binding will only work over the $udp metadata")
            }
            if let Some(dst_port) = config.dst_port.take() {
                warn!("The `dst_port` setting of the UDP offramp is deprecated, this will in future be `port` and current `port` will be `bind.port`.");
                config.bind.port = config.port;
                config.port = dst_port;
            }
            if let Some(dst_host) = config.dst_host.take() {
                warn!("The `dst_host` setting of the UDP offramp is deprecated, this will in future be `host` and current `host` will be `bind.host`.");
                config.bind.host = config.host;
                config.host = dst_host;
            }
            Ok(SinkManager::new_box(Udp {
                socket: None,
                config,
                postprocessors: vec![],
            }))
        } else {
            Err("UDP offramp requires a config".into())
        }
    }
}

impl Udp {
    /// serialize event and send each serialized packet
    async fn send_event(&mut self, codec: &mut dyn Codec, event: &Event) -> Result<()> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
        let ingest_ns = event.ingest_ns;
        for (value, meta) in event.value_meta_iter() {
            let raw = codec.encode(value)?;
            for processed in postprocess(&mut self.postprocessors, ingest_ns, raw)? {
                let udp = meta.get("udp");
                if let Some((host, port)) = udp.get_str("host").zip(udp.get_u16("port")) {
                    socket.send_to(&processed, (host, port)).await?;
                } else if self.config.bound {
                    socket.send(&processed).await?;
                } else {
                    warn!("using `bound` in the UDP sink config is deprecated please use $udp.host and $udp.port instead!");
                    // reaquire the destination to handle DNS changes or multi IP dns entries
                    socket
                        .send_to(&processed, (self.config.host.as_str(), self.config.port))
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for Udp {
    #[allow(clippy::cast_possible_truncation)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let processing_start = Instant::now();
        // TODO: how to properly report error metrics if we dont raise an error here?
        let replies = match self.send_event(codec, &event).await {
            Ok(()) => {
                // success
                if event.transactional {
                    Some(vec![sink::Reply::Insight(event.insight_ack_with_timing(
                        processing_start.elapsed().as_millis() as u64,
                    ))])
                } else {
                    None
                }
            }
            // for UDP we only trigger CB if there is no socket available, as we can't recover from anything else
            Err(e @ Error(ErrorKind::NoSocket, _)) => {
                // trigger CB
                error!("[Sink::UDP] Error sending event: {}.", e);
                if event.transactional {
                    Some(vec![
                        sink::Reply::Insight(event.to_fail()),
                        sink::Reply::Insight(event.insight_trigger()),
                    ])
                } else {
                    Some(vec![sink::Reply::Insight(event.insight_trigger())]) // we always send a trigger
                }
            }
            // all other errors (coded/preprocessor etc.) we just result in a fail
            Err(e) => {
                // regular error, no reason for CB
                error!("[Sink::UDP] Error sending event: {}", e);
                if event.transactional {
                    Some(vec![sink::Reply::Insight(event.to_fail())])
                } else {
                    None
                }
            }
        };
        Ok(replies)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        self.bind().await?;
        Ok(())
    }
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.socket.is_none() {
            self.bind().await?;
            Ok(Some(vec![sink::Reply::Insight(Event::cb_restore(
                signal.ingest_ns,
            ))]))
        } else {
            Ok(None)
        }
    }
    fn is_active(&self) -> bool {
        self.socket.is_some()
    }
    fn auto_ack(&self) -> bool {
        false
    }
}
