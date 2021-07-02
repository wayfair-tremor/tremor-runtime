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

use crate::errors::{Error, ErrorKind, Result};
use crate::source::prelude::*;
use async_channel::Sender;
use async_channel::TryRecvError;
use async_std::net::TcpListener;
use async_tls::TlsAcceptor;
use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{Certificate, NoClientAuth, PrivateKey, ServerConfig};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// TODO expose this as config (would have to change buffer to be vector?)
const BUFFER_SIZE_BYTES: usize = 8192;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub port: u16,
    pub host: String,
    pub tls: Option<TLSConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TLSConfig {
    cert: PathBuf,
    key: PathBuf,
}

impl ConfigImpl for Config {}

pub struct Tcp {
    pub config: Config,
    onramp_id: TremorUrl,
}

pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<SourceReply>>,
    onramp_id: TremorUrl,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TCP")
    }
}
impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let config = config.clone();

        Self {
            uid,
            config,
            listener: None,
            onramp_id,
        }
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Tcp {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for tcp onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        self.listener.as_ref().map_or_else(
            || Ok(SourceReply::StateChange(SourceState::Disconnected)),
            |listener| match listener.try_recv() {
                Ok(r) => Ok(r),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            },
        )
    }

    async fn init(&mut self) -> Result<SourceState> {
        let listener = TcpListener::bind((self.config.host.as_str(), self.config.port)).await?;
        let (tx, rx) = bounded(crate::QSIZE);
        let uid = self.uid;
        let path = vec![self.config.port.to_string()];

        let server_config: Option<ServerConfig> = if let Some(tls_config) = self.config.tls.as_ref()
        {
            Some(load_server_config(tls_config)?)
        } else {
            None
        };
        task::spawn(async move {
            let mut stream_id = 0;
            while let Ok((stream, peer)) = listener.accept().await {
                let tx = tx.clone();
                stream_id += 1;
                let origin_uri = EventOriginUri {
                    uid,
                    scheme: "tremor-tcp".to_string(),
                    host: peer.ip().to_string(),
                    port: Some(peer.port()),
                    // TODO also add token_num here?
                    path: path.clone(), // captures server port
                };
                let tls_acceptor: Option<TlsAcceptor> = server_config
                    .clone()
                    .map(|s| TlsAcceptor::from(Arc::new(s)));
                task::spawn(async move {
                    //let (reader, writer) = &mut (&stream, &stream);
                    if let Err(e) = tx.send(SourceReply::StartStream(stream_id)).await {
                        error!("TCP Error: {}", e);
                        return;
                    }

                    if let Some(acceptor) = tls_acceptor {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                read_loop(tls_stream, tx, stream_id, origin_uri).await
                            }
                            Err(_e) => {
                                if let Err(e) = tx.send(SourceReply::EndStream(stream_id)).await {
                                    error!("TCP Error: {}", e);
                                    return;
                                }
                            }
                        }
                    } else {
                        read_loop(stream, tx, stream_id, origin_uri).await
                    };
                });
            }
        });
        self.listener = Some(rx);

        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Tcp {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

async fn read_loop(
    mut stream: impl futures::io::AsyncRead + std::marker::Unpin,
    tx: Sender<SourceReply>,
    stream_id: usize,
    origin_uri: EventOriginUri,
) {
    let mut buffer = [0; BUFFER_SIZE_BYTES];
    while let Ok(n) = stream.read(&mut buffer).await {
        if n == 0 {
            if let Err(e) = tx.send(SourceReply::EndStream(stream_id)).await {
                error!("TCP Error: {}", e);
            };
            break;
        };
        if let Err(e) = tx
            .send(SourceReply::Data {
                origin_uri: origin_uri.clone(),
                // ALLOW: we define n as part of the read
                data: buffer[0..n].to_vec(),
                meta: None, // TODO: add peer address etc. to meta
                codec_override: None,
                stream: stream_id,
            })
            .await
        {
            error!("TCP Error: {}", e);
            break;
        };
    }
}

// Load the passed certificates file
fn load_certs(path: &Path) -> Result<Vec<Certificate>> {
    let certfile = tremor_common::file::open(path)?;
    let mut reader = BufReader::new(certfile);
    certs(&mut reader).map_err(|_| {
        Error::from(ErrorKind::TLSError(format!(
            "Invalid certificate in {}",
            path.display()
        )))
    })
}

// Load the passed keys file
fn load_keys(path: &Path) -> Result<PrivateKey> {
    // prefer to load pkcs8 keys
    // this will only error if we have invalid pkcs8 key base64 or we couldnt read the file.
    let mut keys: Vec<PrivateKey> = {
        let keyfile = tremor_common::file::open(path)?;
        let mut reader = BufReader::new(keyfile);
        pkcs8_private_keys(&mut reader).map_err(|_e| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid PKCS8 Private key in {}",
                path.display()
            )))
        })
    }?;

    // only attempt to load as RSA keys if file has no pkcs8 keys
    if keys.is_empty() {
        let keyfile = tremor_common::file::open(path)?;
        let mut reader = BufReader::new(keyfile);
        keys = rsa_private_keys(&mut reader).map_err(|_e| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid RSA Private key in {}",
                path.display()
            )))
        })?;
    }

    if keys.is_empty() {
        Err(Error::from(ErrorKind::TLSError(format!(
            "No valid private keys (RSA or PKCS8) found in {}",
            path.display()
        ))))
    } else {
        // ALLOW: we know keys is not empty
        Ok(keys.remove(0))
    }
}

fn load_server_config(config: &TLSConfig) -> Result<ServerConfig> {
    let certs = load_certs(&config.cert)?;
    let keys = load_keys(&config.key)?;

    let mut server_config = ServerConfig::new(NoClientAuth::new());
    server_config
        // set this server to use one cert together with the loaded private key
        .set_single_cert(certs, keys)?;

    Ok(server_config)
}
