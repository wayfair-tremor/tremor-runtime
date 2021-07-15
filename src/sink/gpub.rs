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

//! # File Offramp
//!
//! Writes events to a file, one event per line
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

#![cfg(not(tarpaulin_include))]

use crate::connectors::gcp::{pubsub, pubsub_auth};
use crate::connectors::qos::{self, QoSFacilities, SinkQoS};
use crate::sink::prelude::*;
use googapis::google::pubsub::v1::publisher_client::PublisherClient;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use halfbrown::HashMap;
use tonic::transport::Channel;
use tremor_pipeline::{EventIdGenerator, OpMeta};
use tremor_value::Value;

pub struct GoogleCloudPubSub {
    #[allow(dead_code)]
    config: Config,
    remote_publisher: Option<PublisherClient<Channel>>,
    remote_subscriber: Option<SubscriberClient<Channel>>,
    is_down: bool,
    qos_facility: Box<dyn SinkQoS>,
    reply_channel: Option<Sender<sink::Reply>>,
    is_linked: bool,
    postprocessors: Postprocessors,
    event_id_gen: EventIdGenerator,
}

#[derive(Deserialize)]
pub struct Config {}

enum PubSubCommand {
    SendMessage {
        project_id: String,
        topic_name: String,
        data: Value<'static>,
        ordering_key: String,
    },
    CreateSubscription {
        project_id: String,
        topic_name: String,
        subscription_name: String,
        enable_message_ordering: bool,
    },
    Unknown,
}

impl std::fmt::Display for PubSubCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                PubSubCommand::SendMessage {
                    project_id: _,
                    topic_name: _,
                    data: _,
                    ordering_key: _,
                } => "send_message",
                PubSubCommand::CreateSubscription {
                    project_id: _,
                    topic_name: _,
                    subscription_name: _,
                    enable_message_ordering: _,
                } => "create_subscription",
                PubSubCommand::Unknown => "Unknown",
            }
        )
    }
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        let config = Config::new(
            config
                .as_ref()
                .ok_or("Offramp Google Cloud Pubsub (pub) requires a config")?,
        )?;
        let hostport = "pubsub.googleapis.com:443";
        Ok(SinkManager::new_box(GoogleCloudPubSub {
            config,
            remote_publisher: None,  // overwritten in init
            remote_subscriber: None, // overwritten in init
            is_down: false,
            qos_facility: Box::new(QoSFacilities::recoverable(hostport.to_string())),
            reply_channel: None,
            is_linked: false,
            postprocessors: vec![],
            event_id_gen: EventIdGenerator::new(0), // Fake ID overwritten in init
        }))
    }
}

macro_rules! parse_arg {
    ($field_name: expr, $o: expr) => {
        if let Some(Value::String(snot)) = $o.get($field_name) {
            snot.to_string()
        } else {
            return Err(format!("Invalid Command, expected `{}` field", $field_name).into());
        }
    };
}

fn parse_command(value: &Value) -> Result<PubSubCommand> {
    let o = value.as_object().ok_or("Invalid Command")?;
    let cmd_name: &str = &parse_arg!("command", o);
    let command = match cmd_name {
        "send_message" => PubSubCommand::SendMessage {
            project_id: parse_arg!("project", o),
            topic_name: parse_arg!("topic", o),
            data: if let Some(data) = o.get("data") {
                data.clone_static()
            } else {
                return Err("Invalid Command, expected `data` field".into());
            },
            ordering_key: parse_arg!("ordering_key", o),
        },
        "create_subscription" => PubSubCommand::CreateSubscription {
            project_id: parse_arg!("project", o),
            topic_name: parse_arg!("topic", o),
            subscription_name: parse_arg!("subscription", o),
            enable_message_ordering: o.get("message_ordering").as_bool().unwrap_or_default(),
        },
        _ => PubSubCommand::Unknown,
    };
    Ok(command)
}

#[async_trait::async_trait]
impl Sink for GoogleCloudPubSub {
    async fn terminate(&mut self) {}

    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let remote_publisher = if let Some(remote_publisher) = &mut self.remote_publisher {
            remote_publisher
        } else {
            self.remote_publisher = Some(pubsub_auth::setup_publisher_client().await?);
            let remote_publisher = self
                .remote_publisher
                .as_mut()
                .ok_or("Publisher client error!")?;
            remote_publisher
            // TODO - Qos checks
        };

        let remote_subscriber = if let Some(remote_subscriber) = &mut self.remote_subscriber {
            remote_subscriber
        } else {
            self.remote_subscriber = Some(pubsub_auth::setup_subscriber_client().await?);
            let remote_subscriber = self
                .remote_subscriber
                .as_mut()
                .ok_or("Subscriber client error!")?;
            remote_subscriber
            // TODO - Qos checks
        };

        let maybe_correlation = event.correlation_meta();
        let mut response = Vec::new();
        for value in event.value_iter() {
            let command = parse_command(value)?;
            match command {
                PubSubCommand::SendMessage {
                    project_id,
                    topic_name,
                    data,
                    ordering_key,
                } => {
                    let mut msg: Vec<u8> = vec![];
                    let encoded = codec.encode(&data)?;
                    let mut processed =
                        postprocess(&mut self.postprocessors, event.ingest_ns, encoded)?;
                    for processed_elem in &mut processed {
                        msg.append(processed_elem);
                    }
                    response.push(make_command_response(
                        "send_message",
                        &pubsub::send_message(
                            remote_publisher,
                            &project_id,
                            &topic_name,
                            &msg,
                            &ordering_key,
                        )
                        .await?,
                    ));
                }

                PubSubCommand::CreateSubscription {
                    project_id,
                    topic_name,
                    subscription_name,
                    enable_message_ordering,
                } => {
                    let res = pubsub::create_subscription(
                        remote_subscriber,
                        &project_id,
                        &topic_name,
                        &subscription_name,
                        enable_message_ordering,
                    )
                    .await?;
                    // The commented fields can be None and throw error, and the data type was also not compatible - hence not included in response
                    let subscription = res.name;
                    let topic = res.topic;
                    // let push_config = res.push_config.ok_or("Error getting `push_config` for the created subscription")?;
                    let ack_deadline_seconds = res.ack_deadline_seconds;
                    let retain_acked_messages = res.retain_acked_messages;
                    let enable_message_ordering = res.enable_message_ordering;
                    // let labels = res.labels;
                    let message_retention_duration = res.message_retention_duration.ok_or("Error getting `message retention duration` for the created subscription")?.seconds;
                    // let expiration_policy = res.expiration_policy.ok_or("Error getting `expiration policy` for the created subscription")?;
                    let filter = res.filter;
                    // let dead_letter_policy = res.dead_letter_policy.ok_or("Error getting `dead letter policy` for the created subscription")?;
                    // let retry_policy = res.retry_policy.ok_or("Error getting `retry policy` for the created subscription")?;
                    let detached = res.detached;
                    response.push(literal!({
                        "subscription": subscription,
                        "topic": topic,
                        "ack_deadline_seconds": ack_deadline_seconds,
                        "retain_acked_messages": retain_acked_messages,
                        "enable_message_ordering": enable_message_ordering,
                        "message_retention_duration": message_retention_duration,
                        "filter": filter,
                        "detached": detached
                    }));
                }

                PubSubCommand::Unknown => {
                    warn!(
                        "Unknown Google Cloud PubSub command: `{}` attempted",
                        command.to_string()
                    );
                    return Err(format!(
                        "Unknown Google Cloud PubSub command: `{}` attempted",
                        command.to_string()
                    )
                    .into());
                }
            };
        }

        if self.is_linked {
            if let Some(reply_channel) = &self.reply_channel {
                let mut meta = Object::with_capacity(1);
                if let Some(correlation) = maybe_correlation {
                    meta.insert_nocheck("correlation".into(), correlation);
                }
                // The response vector will have only one value currently since this version of `gpub` doesn't support batches
                // So we pop that value out send it as response
                let val = response.pop().ok_or("No response received from GCP")?;
                reply_channel
                    .send(sink::Reply::Response(
                        OUT,
                        Event {
                            id: self.event_id_gen.next_id(),
                            data: (val, meta).into(),
                            ingest_ns: nanotime(),
                            origin_uri: Some(EventOriginUri {
                                uid: 0,
                                scheme: "gRPC".into(),
                                host: "".into(),
                                port: None,
                                path: vec![],
                            }),
                            kind: None,
                            is_batch: false,
                            cb: CbAction::None,
                            op_meta: OpMeta::default(),
                            transactional: false,
                        },
                    ))
                    .await?;
            }
        }

        self.is_down = false;
        Ok(Some(vec![qos::ack(&mut event)]))
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.remote_publisher = Some(pubsub_auth::setup_publisher_client().await?);
        self.remote_subscriber = Some(pubsub_auth::setup_subscriber_client().await?);
        self.event_id_gen = EventIdGenerator::new(sink_uid);
        self.postprocessors = make_postprocessors(processors.post)?;
        self.reply_channel = Some(reply_channel);
        self.is_linked = is_linked;
        Ok(())
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.is_down && self.qos_facility.probe(signal.ingest_ns) {
            self.is_down = false;
            // This means the port is connectable
            info!("Google Cloud PubSub -  sink remote endpoint - recovered and contactable");
            self.is_down = false;
            // Clone needed to make it mutable, lint is wrong
            #[allow(clippy::redundant_clone)]
            let mut signal = signal.clone();
            return Ok(Some(vec![qos::open(&mut signal)]));
        }

        Ok(None)
    }

    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
}

fn make_command_response(cmd: &str, message_id: &str) -> Value<'static> {
    literal!({
        "command": cmd,
        "message-id": message_id
    })
    .into_static()
}
