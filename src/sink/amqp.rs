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

//! # AMQP Offramp
//!
//! The `amqp` offramp allows producing events to an amqp broker.

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver};
use serde::{Deserialize};
use crate::url::TremorUrl;
use lapin::{
    options::*, Connection, ConnectionProperties, Channel, BasicProperties, PromiseChain, publisher_confirm::Confirmation
};
use halfbrown::HashMap;
use std::{
    fmt,
    time::{Instant},
};

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub amqp_addr: String,
    #[serde(default = "Default::default")]
    routing_key: String,
    #[serde(default = "Default::default")]
    exchange: String,
    publish_options: BasicPublishOptions,
    // headers to use for the messages
    #[serde(default = "Default::default")]
    pub headers: HashMap<String, Vec<String>>,
}

impl Config {
    async fn channel(&self) -> PromiseChain<Channel> {
        match Connection::connect(&self.amqp_addr, ConnectionProperties::default()).await {
            Ok(connection) => {
                connection.create_channel()
            }
            Err(error) => PromiseChain::new_with_data(Err(error)),
        }
    }
}

impl ConfigImpl for Config {}

/// Amqp offramp connector
pub struct Amqp {
    sink_url: TremorUrl,
    config: Config,
    postprocessors: Postprocessors,
    reply_channel: Sender<sink::Reply>,
    channel: Option<Channel>,
    error_rx: Receiver<()>,
    error_tx: Sender<()>,
}

impl fmt::Debug for Amqp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[Sink::{}] RoutingKey: {}", &self.sink_url, self.config.routing_key)
    }
}

impl offramp::Impl for Amqp {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let (dummy_tx, _) = bounded(1);
            let (error_tx, error_rx) = bounded(crate::QSIZE);
            Ok(SinkManager::new_box(Self {
                sink_url: TremorUrl::from_offramp_id("amqp")?, // dummy
                config,
                postprocessors: vec![],
                reply_channel: dummy_tx,
                channel: None,
                error_rx,
                error_tx,
            }))
        } else {
            Err("Amqp offramp requires a config".into())
        }
    }
}

impl Amqp {
    async fn handle_channel(&mut self) -> Result<Option<&Channel>> {
        while let Ok(()) = self.error_rx.try_recv() {
            self.channel = None;
        }
        if self.channel.is_none() {
            match self.config.channel().await.await {
                Ok(channel) => self.channel = Some(channel),
                Err(error) => return Err(error.into()),
            }
            return Ok(self.channel.as_ref());
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Sink for Amqp {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        self.handle_channel().await?;
        let ingest_ns = event.ingest_ns;
        let processing_start = Instant::now();
        /*
        // evaluate here to avoid borrowing again while borrowed.
        let config_reply = self.config.reply.as_deref();
        let op_meta = &event.op_meta;
        self.merged_meta.merge(op_meta.clone());
        */
        let insight_event = event.insight_ack();
        if let Some(channel) = &mut self.channel {
            for (value, _) in event.value_meta_iter() {
                let encoded = codec.encode(value)?;
                let processed =
                    postprocess(self.postprocessors.as_mut_slice(), ingest_ns, encoded)?;
                //let headers = meta.get("nats").and_then(|v| v.get_object("headers"));
                for payload in processed {
                    /*
                    // prepare message reply
                    let message_reply = reply.or(config_reply);
                    */
                    // prepare message headers
                    let properties = BasicProperties::default();
                    /*
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
                    */
                    let publish_result = channel.basic_publish(
                        self.config.exchange.as_str(),
                        self.config.routing_key.as_str(),
                        self.config.publish_options,
                        payload,
                        properties,
                    )
                    .await?.await?;

                    match publish_result {
                        Confirmation::NotRequested | Confirmation::Ack(_) => {
                            if event.transactional {
                                let mut insight = insight_event.clone();
                                insight.cb = CbAction::Ack;
                                let time = processing_start.elapsed().as_millis() as u64;
                                let mut m = Object::with_capacity(1);
                                m.insert("time".into(), time.into());
                                insight.data = (Value::null(), m).into();
                                self.reply_channel
                                    .send(sink::Reply::Insight(insight.clone()))
                                    .await?;
                            }
                        },
                        Confirmation::Nack(err) => {
                            match err {
                                Some(e) => error!("[Sink::{}] failed to send message: {} {}", &self.sink_url, e.reply_code, e.reply_text),
                                None => error!("[Sink::{}] failed to send message: unknown error", &self.sink_url),
                            }
                            
                            if self.error_tx.send(()).await.is_err() {
                                error!(
                                    "[Sink::{}] Error notifying the system about amqp error",
                                    &self.sink_url
                                )
                            }
                            if event.transactional {
                                let mut insight = insight_event.clone();
                                insight.cb = CbAction::Fail;
                                self.reply_channel
                                    .send(sink::Reply::Response(ERR, insight))
                                    .await?;
                            }
                        },
                    }
                }
            }
        }
        Ok(None)
    }
    fn default_codec(&self) -> &str {
        "json"
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
        self.handle_channel().await?;
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_channel = reply_channel;
        self.sink_url = sink_url.clone();
        Ok(())
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        //self.drain_fatal_errors()?;
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
    async fn terminate(&mut self) {
        if let Some(channel) = self.channel.as_ref() {
            let _res_close = channel.close(0, "terminating sink");
            let _res_confirms = channel.wait_for_confirms();
        }
        /*if self.channel.in_flight_count() > 0 {
            // wait a second in order to flush messages.
            let wait_secs = 1;
            info!(
                "[Sink::{}] Flushing messages. Waiting for {} seconds.",
                wait_secs, &self.sink_url
            );
            self.channel.flush(Duration::from_secs(1));
        }*/
        info!("[Sink::{}] Terminating.", &self.sink_url);
    }
}
