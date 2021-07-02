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

//! # Kafka Offramp
//!
//! The `kafka` offramp allows persisting events to a kafka queue.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use halfbrown::HashMap;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaError;
use rdkafka::producer::Producer;
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
};
use std::{
    fmt,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
pub struct Config {
    /// list of brokers
    pub brokers: Vec<String>,
    /// the topic to send to
    pub topic: String,
    /// a map (string keys and string values) of [librdkafka options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) (default: None) - Note this can overwrite default settings.
    ///
    /// Default settings for librdkafka:
    ///
    /// * `client.id` - `"tremor-<hostname>-0"`
    /// * `bootstrap.servers` - `brokers` from the config concatinated by `,`
    /// * `message.timeout.ms` - `"5000"`
    /// * `queue.buffering.max.ms` - `"0"` - don't buffer for lower latency (high)
    #[serde(default = "Default::default")]
    pub rdkafka_options: HashMap<String, String>,
    /// hostname to use, defaults to the hostname of the system
    #[serde(default = "d_host")]
    pub hostname: String,
    /// key to use for messages, defaults to none
    #[serde(default = "Default::default")]
    pub key: Option<String>,
}

impl Config {
    fn producer(&self) -> Result<FutureProducer> {
        let mut producer_config = ClientConfig::new();

        // ENABLE LIBRDKAFKA DEBUGGING:
        // - set librdkafka logger to debug in logger.yaml
        // - configure: debug: "all" for this onramp
        let producer_config = producer_config
            .set("client.id", &format!("tremor-{}-{}", self.hostname, 0))
            .set("bootstrap.servers", &self.brokers.join(","))
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "0"); // set to 0 for sending each message out immediately without kafka client internal batching --> low latency, busy network

        Ok(self
            .rdkafka_options
            .iter()
            .fold(producer_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
            .create()?)
    }
}

impl ConfigImpl for Config {}

fn d_host() -> String {
    hostname()
}

/// Kafka offramp connectoz
pub struct Kafka {
    sink_url: TremorUrl,
    config: Config,
    producer: FutureProducer,
    postprocessors: Postprocessors,
    reply_tx: Sender<sink::Reply>,
    error_rx: Receiver<RDKafkaError>,
    error_tx: Sender<RDKafkaError>,
}

impl fmt::Debug for Kafka {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[Sink::{}] Kafka: {}", &self.sink_url, self.config.topic)
    }
}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let producer = config.producer()?;
            let (version_n, version_s) = rdkafka::util::get_rdkafka_version();
            info!(
                "[Offramp::kafka] Starting Kafka offramp with rdkafka 0x{:08x}, {}",
                version_n, version_s
            );
            // Create the thread pool where the expensive computation will be performed.
            let (dummy_tx, _) = bounded(1);

            // TODO: does this need to be unbounded?
            let (error_tx, error_rx) = bounded(crate::QSIZE);
            Ok(SinkManager::new_box(Kafka {
                sink_url: TremorUrl::from_offramp_id("kafka")?, // dummy
                config,
                producer,
                postprocessors: vec![],
                reply_tx: dummy_tx,
                error_rx,
                error_tx,
            }))
        } else {
            Err("Kafka offramp requires a config".into())
        }
    }
}

/// Waits for actual delivery to kafka cluster and sends ack or fail.
/// Also sends fatal errors for handling in offramp task.
#[allow(clippy::cast_possible_truncation)]
async fn wait_for_delivery(
    sink_url: String,
    futures: Vec<rdkafka::producer::DeliveryFuture>,
    processing_start: Instant,
    maybe_event: Option<Event>,
    reply_tx: Sender<sink::Reply>,
    error_tx: Sender<RDKafkaError>,
) -> Result<()> {
    let cb = match futures::future::try_join_all(futures).await {
        Ok(results) => {
            if let Some((KafkaError::Transaction(rd_error), _)) =
                results.into_iter().find_map(std::result::Result::err)
            {
                error!(
                    "[Sink::{}] Error delivering kafka record: {}",
                    sink_url, &rd_error
                );
                if rd_error.is_fatal() {
                    let err_msg = format!("{}", &rd_error);
                    if error_tx.send(rd_error).await.is_err() {
                        error!(
                            "[Sink::{}] Error notifying the system about kafka error: {}",
                            &sink_url, &err_msg
                        )
                    }
                }
                CbAction::Fail
            } else {
                // all good. send ack
                CbAction::Ack
            }
        }
        Err(e) => {
            error!(
                "[Sink::{}] DeliveryFuture cancelled. Message delivery status unclear, considering it failed.: {}",
                sink_url, e
            );
            // oh noes, send fail
            CbAction::Fail
        }
    };
    if let Some(mut insight) = maybe_event {
        insight.cb = cb;
        if cb == CbAction::Ack {
            let time = processing_start.elapsed().as_millis() as u64;
            let mut m = Object::with_capacity(1);
            m.insert("time".into(), time.into());
            insight.data = (Value::null(), m).into();
        }

        if reply_tx.send(sink::Reply::Insight(insight)).await.is_err() {
            error!(
                "[Sink::{}] Error sending {:?} insight after delivery",
                sink_url, cb
            );
        }
    }
    Ok(())
}

impl Kafka {
    fn drain_fatal_errors(&mut self) -> Result<()> {
        let mut handled = false;
        while let Ok(e) = self.error_rx.try_recv() {
            if !handled {
                // only handle on first fatal error
                self.handle_fatal_error(&e)?;
                handled = true;
            }
        }
        Ok(())
    }

    fn handle_fatal_error(&mut self, fatal_error: &RDKafkaError) -> Result<()> {
        error!(
            "[Sink::{}] Fatal Error({:?}): {}",
            &self.sink_url,
            fatal_error.code(),
            fatal_error.string()
        );

        error!("[Sink::{}] Reinitiating client...", &self.sink_url);
        self.producer = self.config.producer()?;
        error!("[Sink::{}] Client reinitiated.", &self.sink_url);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for Kafka {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        // ensure we handle any fatal errors occured during last on_event invocation
        self.drain_fatal_errors()?;

        let ingest_ns = event.ingest_ns;
        let mut delivery_futures = Vec::with_capacity(event.len()); // might not be enough
        let processing_start = Instant::now();
        for (value, meta) in event.value_meta_iter() {
            let encoded = codec.encode(value)?;
            let processed = postprocess(self.postprocessors.as_mut_slice(), ingest_ns, encoded)?;
            let meta_kafka_data = meta.get_object("kafka");
            let mut meta_kafka_key = None;
            let mut meta_kafka_headers = None;
            if let Some(meta_data) = meta_kafka_data {
                meta_kafka_key = meta_data.get("key");
                meta_kafka_headers = meta_data.get("headers");
            }
            for payload in processed {
                // TODO: allow defining partition and timestamp in meta
                let mut record = FutureRecord::to(self.config.topic.as_str());
                record = record.payload(&payload);
                if let Some(kafka_key) = meta_kafka_key {
                    if let Some(kafka_key_str) = kafka_key.as_str() {
                        record = record.key(kafka_key_str);
                    }
                } else if let Some(kafka_key) = &self.config.key {
                    record = record.key(kafka_key.as_str());
                }
                if let Some(kafka_headers) = meta_kafka_headers {
                    if let Some(headers_obj) = kafka_headers.as_object() {
                        let mut headers = OwnedHeaders::new_with_capacity(headers_obj.len());
                        for (key, val) in headers_obj.iter() {
                            if let Some(val_str) = val.as_str() {
                                headers = headers.add(key, val_str);
                            }
                        }
                        record = record.headers(headers);
                    }
                }
                // send out without blocking on delivery
                match self.producer.send_result(record) {
                    Ok(delivery_future) => {
                        delivery_futures.push(delivery_future);
                    }
                    Err((e, _)) => {
                        error!(
                            "[Sink::{}] failed to enqueue message: {}",
                            &self.sink_url, e
                        );
                        if let KafkaError::Transaction(e) = e {
                            if e.is_fatal() {
                                // handle fatal errors right here, without enqueueing
                                self.handle_fatal_error(&e)?;
                            }
                        }
                        // bail out with a CB fail on enqueue error
                        if event.transactional {
                            return Ok(Some(vec![sink::Reply::Insight(event.to_fail())]));
                        }
                        return Ok(None);
                    }
                }
            }
        }
        let insight_event = if event.transactional {
            // we gonna change the success status later, if need be
            Some(event.insight_ack())
        } else {
            None
        };
        // successfully enqueued all messages
        // spawn the task waiting for delivery and send acks/fails then
        task::spawn(wait_for_delivery(
            self.sink_url.to_string(),
            delivery_futures,
            processing_start,
            insight_event,
            self.reply_tx.clone(),
            self.error_tx.clone(),
        ));
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
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_tx = reply_channel;
        self.sink_url = sink_url.clone();
        Ok(())
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        self.drain_fatal_errors()?;
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
    async fn terminate(&mut self) {
        if self.producer.in_flight_count() > 0 {
            // wait a second in order to flush messages.
            let wait_secs = 1;
            info!(
                "[Sink::{}] Flushing messages. Waiting for {} seconds.",
                wait_secs, &self.sink_url
            );
            self.producer.flush(Duration::from_secs(1));
            info!("[Sink::{}] Terminating.", &self.sink_url);
        }
    }
}
