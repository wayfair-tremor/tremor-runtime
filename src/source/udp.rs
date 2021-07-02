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

use crate::source::prelude::*;
use async_std::net::UdpSocket;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The port to listen on.
    pub port: u16,
    pub host: String,
}

impl ConfigImpl for Config {}

pub struct Udp {
    pub config: Config,
    onramp_id: TremorUrl,
}

struct Int {
    config: Config,
    socket: Option<UdpSocket>,
    onramp_id: TremorUrl,
    origin_uri: EventOriginUri,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UDP")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-udp".to_string(),
            host: String::default(),
            port: None,
            path: vec![config.port.to_string()], // captures receive port
        };
        Self {
            config: config.clone(),
            socket: None,
            onramp_id,
            origin_uri,
        }
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(
        &self,
        onramp_id: &TremorUrl,
        config: &Option<YamlValue>,
    ) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Udp {
                config,
                onramp_id: onramp_id.clone(),
            }))
        } else {
            Err("Missing config for udp onramp".into())
        }
    }
}

#[async_trait::async_trait]
impl Source for Int {
    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        let mut buf = [0; 65535];

        if let Some(socket) = self.socket.as_mut() {
            match socket.recv_from(&mut buf).await {
                Ok((n, peer)) => {
                    let mut origin_uri = self.origin_uri.clone();

                    // TODO add a method in origin_uri for changes like this?
                    origin_uri.host = peer.ip().to_string();
                    origin_uri.port = Some(peer.port());
                    Ok(SourceReply::Data {
                        origin_uri,
                        // ALLOW: we get n from recv
                        data: buf[0..n].to_vec(),
                        meta: None,
                        codec_override: None,
                        stream: 0,
                    })
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        Ok(SourceReply::Empty(1))
                    } else {
                        Err(e.into())
                    }
                }
            }
        } else {
            let socket = UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?;
            info!(
                "[UDP Onramp] listening on {}:{}",
                self.config.host, self.config.port
            );
            self.socket = Some(socket);
            Ok(SourceReply::StateChange(SourceState::Connected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let socket = UdpSocket::bind((self.config.host.as_str(), self.config.port)).await?;
        info!(
            "[UDP Onramp] listening on {}:{}",
            self.config.host, self.config.port
        );
        self.socket = Some(socket);
        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Udp {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }
    fn default_codec(&self) -> &str {
        "string"
    }
}
