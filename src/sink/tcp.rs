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

//! # TCP Offramp
//!
//! Sends each message as a tcp stream
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use std::time::Instant;

use crate::sink::prelude::*;
use async_std::net::TcpStream;
use halfbrown::HashMap;

use async_tls::TlsConnector;
use rustls::ClientConfig;

use async_std::io::Write;
use either::Either;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

type Stream = Box<dyn Write + std::marker::Unpin + Send>;

/// An offramp streams over TCP/IP
pub struct Tcp {
    // TODO: check if we can/should use an enum here where we implement async_std::io::Write manually?
    stream: Option<Stream>,
    postprocessors: Postprocessors,
    config: Config,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_ttl")]
    pub ttl: u32,
    #[serde(default = "default_no_delay")]
    pub is_no_delay: bool,
    #[serde(with = "either::serde_untagged_optional", default = "Default::default")]
    pub tls: Option<Either<TLSConfig, bool>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct TLSConfig {
    cafile: Option<PathBuf>,
    domain: Option<String>,
}

fn default_no_delay() -> bool {
    true
}

fn default_ttl() -> u32 {
    64
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(SinkManager::new_box(Tcp {
                config,
                stream: None,
                postprocessors: vec![],
            }))
        } else {
            Err("TCP offramp requires a config".into())
        }
    }
}

impl Tcp {
    async fn send_event(&mut self, codec: &mut dyn Codec, event: &Event) -> Result<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::from(ErrorKind::NoSocket))?;
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            let packets = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
            for packet in packets {
                stream.write_all(&packet).await?;
            }
        }
        Ok(())
    }

    async fn connect(config: &Config) -> Result<Stream> {
        let stream = TcpStream::connect((config.host.as_str(), config.port)).await?;
        stream.set_ttl(config.ttl)?;
        stream.set_nodelay(config.is_no_delay)?;
        let s: Stream = match config.tls.as_ref() {
            Some(Either::Right(true)) => {
                debug!("Returns a TLS stream via default TLS connector");
                let tls_config = TLSConfig::default();
                let c = connector(&tls_config).await?;
                Box::new(c.connect(config.host.as_str(), stream).await?)
            }
            Some(Either::Left(tls)) => {
                debug!("Returns a TLS stream by a TLS connector started wiht cafile provided in the config");
                let c = connector(&tls).await?;
                Box::new(
                    c.connect(
                        tls.domain
                            .as_ref()
                            .map_or_else(|| config.host.as_str(), String::as_str),
                        stream,
                    )
                    .await?,
                )
            }
            Some(Either::Right(false)) | None => {
                debug!("Returns the usual TCP stream");
                Box::new(stream)
            }
        };
        Ok(s)
    }
}

#[async_trait::async_trait]
impl Sink for Tcp {
    /// We acknowledge ourself
    fn auto_ack(&self) -> bool {
        false
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let processing_start = Instant::now();
        let replies = match self.send_event(codec, &event).await {
            Ok(()) => {
                if event.transactional {
                    Some(vec![sink::Reply::Insight(event.insight_ack_with_timing(
                        processing_start.elapsed().as_millis() as u64,
                    ))])
                } else {
                    None
                }
            }
            // for TCP we always trigger the CB for IO/socket related errors
            Err(e @ Error(ErrorKind::Io(_) | ErrorKind::NoSocket, _)) => {
                debug!("[Sink::TCP] Error sending event: {}.", e);
                if event.transactional {
                    Some(vec![
                        sink::Reply::Insight(event.to_fail()),
                        sink::Reply::Insight(event.insight_trigger()),
                    ])
                } else {
                    Some(vec![sink::Reply::Insight(event.insight_trigger())]) // we always send a trigger
                }
            }
            // all other errors (codec/peprocessor etc.) just result in a fail
            Err(e) => {
                // regular error, no reason for CB
                debug!("[Sink::TCP] Error sending event: {}", e);

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
        let stream = Self::connect(&self.config).await?;
        //let stream = TcpStream::connect((self.config.host.as_str(), self.config.port)).await?;
        //stream.set_ttl(self.config.ttl)?;
        //stream.set_nodelay(self.config.is_no_delay)?;
        self.stream = Some(Box::new(stream));
        Ok(())
    }
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.stream.is_none() {
            let stream = if let Ok(stream) = Self::connect(&self.config).await {
                stream
            } else {
                return Ok(Some(vec![sink::Reply::Insight(Event::cb_trigger(
                    signal.ingest_ns,
                ))]));
            };
            self.stream = Some(stream);
            Ok(Some(vec![sink::Reply::Insight(Event::cb_restore(
                signal.ingest_ns,
            ))]))
        } else {
            Ok(None)
        }
    }
    fn is_active(&self) -> bool {
        self.stream.is_some()
    }
}

/// if we have a cafile configured, we only load it, and no other ca certificates
/// if there is no cafile configured, we load the default webpki-roots from Mozilla
async fn connector(config: &TLSConfig) -> Result<TlsConnector> {
    Ok(match config {
        TLSConfig {
            cafile: Some(cafile),
            ..
        } => {
            let mut config = ClientConfig::new();
            let file = async_std::fs::read(cafile).await?;
            let mut pem = Cursor::new(file);
            config.root_store.add_pem_file(&mut pem).map_err(|_e| {
                Error::from(ErrorKind::TLSError(format!(
                    "Invalid certificate in {}",
                    cafile.display()
                )))
            })?;
            TlsConnector::from(Arc::new(config))
        }
        TLSConfig { cafile: None, .. } => TlsConnector::default(),
    })
}
