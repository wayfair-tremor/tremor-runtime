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

use crate::sink::nats::ConnectOptions;
use crate::source::prelude::*;
use async_nats::{Connection as NatsConnection, Subscription};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    // list of hosts
    pub hosts: Vec<String>,
    // subject to send messages to
    pub subject: String,
    // optional queue to subscribe to
    #[serde(default = "Default::default")]
    pub queue: Option<String>,
    // options to use when opening a new connection
    #[serde(default = "Default::default")]
    pub options: ConnectOptions,
}

impl ConfigImpl for Config {}

impl Config {
    async fn connection(&self) -> Result<NatsConnection> {
        let hosts = self.hosts.join(",");
        let connection = self.options.generate()?.connect(&hosts).await?;
        Ok(connection)
    }
}

pub struct Nats {
    pub config: Config,
    onramp_id: TremorUrl,
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Nats {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for nats onramp".into())
        }
    }
}

#[async_trait::async_trait]
impl Onramp for Nats {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

pub struct Int {
    uid: u64,
    onramp_id: TremorUrl,
    config: Config,
    subscription: Option<Subscription>,
    connection: Option<NatsConnection>,
    origin_uri: EventOriginUri,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NATS")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let config = config.clone();
        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-nats".to_string(),
            host: "not-connected".to_string(),
            port: None,
            path: vec![],
        };
        Self {
            uid,
            onramp_id,
            config,
            subscription: None,
            connection: None,
            origin_uri,
        }
    }
}

#[async_trait::async_trait]
impl Source for Int {
    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        if let Some(sub) = &self.subscription {
            if let Some(msg) = sub.next().await {
                let mut origin_uri = self.origin_uri.clone();
                origin_uri.path = vec![msg.subject];
                let data = msg.data;
                let mut nats_meta_data = Value::object_with_capacity(1);
                let msg_headers = msg.headers.map(|headers| {
                    let mut key_val: Value = Value::object_with_capacity(headers.len());
                    for (key, val) in headers.iter() {
                        let key = String::from(key);
                        let val: Vec<String> = val.iter().map(String::from).collect();
                        key_val.insert(key, val).ok();
                    }
                    key_val
                });
                let mut meta_data = Value::object_with_capacity(2);
                if let Some(msg_reply) = msg.reply {
                    meta_data.insert("reply", msg_reply)?;
                }
                if let Some(msg_headers) = msg_headers {
                    meta_data.insert("headers", msg_headers)?;
                }
                nats_meta_data.insert("nats", meta_data)?;
                Ok(SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(nats_meta_data),
                    codec_override: None,
                    stream: 0,
                })
            } else {
                Ok(SourceReply::StateChange(SourceState::Disconnected))
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }

    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn init(&mut self) -> Result<SourceState> {
        let nc = self.config.connection().await?;
        let sub = if let Some(queue) = &self.config.queue {
            nc.queue_subscribe(self.config.subject.as_str(), queue.as_str())
                .await?
        } else {
            nc.subscribe(self.config.subject.as_str()).await?
        };
        let first_host: Vec<&str> = if let Some(host) = self.config.hosts.first() {
            host.split(':').collect()
        } else {
            return Err(format!("[Source::{}] No hosts provided.", self.onramp_id).into());
        };
        let (host, port) = match first_host.as_slice() {
            [host] => ((*host).to_string(), None),
            [host, port] => ((*host).to_string(), Some(port.parse()?)),
            _ => {
                return Err(format!(
                    "[Source::{}] Invalid host config: {}",
                    self.onramp_id,
                    first_host.join(":")
                )
                .into())
            }
        };
        self.origin_uri = EventOriginUri {
            uid: self.uid,
            scheme: "tremor-nats".to_string(),
            host,
            port,
            path: vec![],
        };
        self.connection = Some(nc);
        self.subscription = Some(sub);
        Ok(SourceState::Connected)
    }

    async fn terminate(&mut self) {
        // we don't need to drain the subs here. closing the connection takes care of that.
        if let Some(connection) = &self.connection {
            if connection.close().await.is_err() {}
        }
    }
}
