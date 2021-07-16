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

use std::iter::FromIterator;
use std::time::Instant;

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver};
use async_nats::Connection as NatsConnection;
use async_nats::Headers;
use async_nats::Options as NatsOptions;
use halfbrown::HashMap;
use tremor_pipeline::OpMeta;

// struct containing connection options
#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
pub struct ConnectOptions {
    token: Option<String>,
    username: Option<String>,
    password: Option<String>,
    credentials_path: Option<String>,
    cert_path: Option<String>,
    key_path: Option<String>,
    name: Option<String>,
    #[serde(default = "bool::default")]
    echo: bool,
    max_reconnects: Option<usize>,
    reconnect_buffer_size: Option<usize>,
    #[serde(default = "bool::default")]
    tls: bool,
    root_cert: Option<String>,
}

impl ConnectOptions {
    pub fn generate(&self) -> Result<NatsOptions> {
        let mut options = None;
        if let Some(token) = &self.token {
            options = Some(NatsOptions::with_token(token.as_str()));
        }
        if let Some((username, password)) = self.username.as_ref().zip(self.password.as_ref()) {
            options = Some(NatsOptions::with_user_pass(
                username.as_str(),
                password.as_str(),
            ));
        }
        if let Some(credentials_path) = &self.credentials_path {
            options = Some(NatsOptions::with_credentials(credentials_path));
        }
        let mut nats_options = options.unwrap_or_default();
        if let Some((cert_path, key_path)) = self.cert_path.as_ref().zip(self.key_path.as_ref()) {
            nats_options = nats_options.client_cert(cert_path, key_path);
        }
        if let Some(name) = &self.name {
            nats_options = nats_options.with_name(name.as_str());
        }
        if let Some(max_reconnects) = self.max_reconnects {
            nats_options = nats_options.max_reconnects(max_reconnects);
        }
        if let Some(reconnect_buffer_size) = self.reconnect_buffer_size {
            nats_options = nats_options.reconnect_buffer_size(reconnect_buffer_size);
        }
        if let Some(root_cert) = &self.root_cert {
            nats_options = nats_options.add_root_certificate(root_cert);
        }
        if self.echo {
            nats_options = nats_options.no_echo();
        }
        nats_options = nats_options.tls_required(self.tls);
        Ok(nats_options)
    }
}

#[derive(Deserialize)]
pub struct Config {
    // list of hosts
    pub hosts: Vec<String>,
    // subject to send messages to
    pub subject: String,
    // options to use when opening a new connection
    #[serde(default = "Default::default")]
    pub options: ConnectOptions,
    // reply to use for the messages
    #[serde(default = "Default::default")]
    pub reply: Option<String>,
    // headers to use for the messages
    #[serde(default = "Default::default")]
    pub headers: HashMap<String, Vec<String>>,
}

impl Config {
    fn connection(&self) -> Result<NatsConnection> {
        let hosts = self.hosts.join(",");
        task::block_on(async {
            let connection = self.options.generate()?.connect(&hosts).await?;
            Ok(connection)
        })
    }
}

impl ConfigImpl for Config {}

pub struct Nats {
    sink_url: TremorUrl,
    config: Config,
    postprocessors: Postprocessors,
    reply_channel: Sender<sink::Reply>,
    connection: Option<NatsConnection>,
    error_rx: Receiver<()>,
    error_tx: Sender<()>,
    merged_meta: OpMeta,
}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let (dummy_tx, _) = bounded(1);
            let (error_tx, error_rx) = bounded(crate::QSIZE);
            Ok(SinkManager::new_box(Nats {
                sink_url: TremorUrl::from_offramp_id("nats")?,
                config,
                postprocessors: vec![],
                reply_channel: dummy_tx,
                connection: None,
                error_rx,
                error_tx,
                merged_meta: OpMeta::default(),
            }))
        } else {
            Err("Nats offramp requires a configuration.".into())
        }
    }
}

impl Nats {
    fn handle_connection(&mut self) -> Result<Option<&NatsConnection>> {
        while let Ok(()) = self.error_rx.try_recv() {
            self.connection = None;
        }
        if self.connection.is_none() {
            self.connection = Some(self.config.connection()?);
            return Ok(self.connection.as_ref());
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Sink for Nats {
    #[allow(clippy::cast_possible_truncation)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        self.handle_connection()?;
        let ingest_ns = event.ingest_ns;
        let processing_start = Instant::now();
        // evaluate here to avoid borrowing again while borrowed.
        let config_reply = self.config.reply.as_deref();
        let op_meta = &event.op_meta;
        self.merged_meta.merge(op_meta.clone());
        if let Some(connection) = &mut self.connection {
            for (value, meta) in event.value_meta_iter() {
                let encoded = codec.encode(value)?;
                let processed =
                    postprocess(self.postprocessors.as_mut_slice(), ingest_ns, encoded)?;
                let nats_meta = meta.get("nats");
                let headers = nats_meta.and_then(|v| v.get_object("headers"));
                let reply = nats_meta.and_then(|v| v.get_str("reply"));
                for payload in processed {
                    // prepare message reply
                    let message_reply = reply.or(config_reply);
                    // prepare message headers
                    let mut key_val: Vec<(&str, &str)> = Vec::with_capacity(
                        self.config.headers.len() + headers.map(HashMap::len).unwrap_or_default(),
                    );
                    for (key, val) in &self.config.headers {
                        for ele in val.iter() {
                            key_val.push((key.as_str(), ele.as_str()));
                        }
                    }
                    if let Some(headers) = headers {
                        for (key, val) in
                            headers.iter().filter_map(|(k, v)| Some((k, v.as_array()?)))
                        {
                            for ele in val.iter().filter_map(value_trait::ValueAccess::as_str) {
                                key_val.push((key, ele));
                            }
                        }
                    }
                    let message_headers = if key_val.is_empty() {
                        None
                    } else {
                        Some(Headers::from_iter(key_val))
                    };

                    let publish_result = connection
                        .publish_with_reply_or_headers(
                            self.config.subject.as_str(),
                            message_reply,
                            message_headers.as_ref(),
                            payload,
                        )
                        .await;
                    match publish_result {
                        Ok(()) => {
                            if event.transactional {
                                let mut insight = Event::cb_ack(
                                    ingest_ns,
                                    event.id.clone(),
                                    self.merged_meta.clone(),
                                );
                                insight.data = (Value::null(), literal!({ "time": processing_start.elapsed().as_millis() as u64 })).into();

                                if let Err(e) =
                                    self.reply_channel.send(sink::Reply::Insight(insight)).await
                                {
                                    error!(
                                        "[Sink::{}] Error sending insight via reply channel: {}",
                                        &self.sink_url, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("[Sink::{}] failed to send message: {}", &self.sink_url, &e);
                            if self.error_tx.send(()).await.is_err() {
                                error!(
                                    "[Sink::{}] Error notifying the system about nats error: {}",
                                    &self.sink_url, &e
                                );
                            }

                            if event.transactional {
                                if let Err(e) = self
                                    .reply_channel
                                    .send(sink::Reply::Insight(event.to_fail()))
                                    .await
                                {
                                    error!(
                                        "[Sink::{}] Error sending insight via reply_channel: {}",
                                        self.sink_url, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        match self.handle_connection() {
            Ok(Some(_connection)) => {
                let mut insight_event = Event::cb_restore(signal.ingest_ns);
                insight_event.op_meta = self.merged_meta.clone();
                Ok(Some(vec![sink::Reply::Insight(insight_event)]))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                error!(
                    "[Sink::{}] failed to establish connection: {}",
                    &self.sink_url, e
                );
                let mut insight_event = Event::cb_trigger(signal.ingest_ns);
                insight_event.op_meta = self.merged_meta.clone();
                return Ok(Some(vec![sink::Reply::Insight(insight_event)]));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        reply_channel: Sender<Reply>,
    ) -> Result<()> {
        self.connection = Some(self.config.connection()?);
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_channel = reply_channel;
        self.sink_url = sink_url.clone();
        Ok(())
    }

    fn is_active(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        false
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn terminate(&mut self) {
        if let Some(connection) = &self.connection {
            if connection.close().await.is_err() {}
        }
    }
}
