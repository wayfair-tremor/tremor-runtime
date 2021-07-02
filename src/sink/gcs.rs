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

use crate::connectors::gcp::{auth, storage};
use crate::connectors::qos::{self, QoSFacilities, SinkQoS};
use crate::sink::prelude::*;
use futures::executor::block_on;
use halfbrown::HashMap;
use http::HeaderMap;
use reqwest::Client;
use tremor_pipeline::{EventIdGenerator, OpMeta};
use tremor_value::Value;

pub struct GoogleCloudStorage {
    #[allow(dead_code)]
    config: Config,
    remote: Option<Client>,
    is_down: bool,
    qos_facility: Box<dyn SinkQoS>,
    reply_channel: Option<Sender<sink::Reply>>,
    is_linked: bool,
    preprocessors: Preprocessors,
    postprocessors: Postprocessors,
    sink_url: TremorUrl,
    event_id_gen: EventIdGenerator,
}

#[derive(Deserialize)]
pub struct Config {}

enum StorageCommand {
    Create(String, String),
    Add(String, String, Value<'static>),
    RemoveObject(String, String),
    ListBuckets(String),
    Fetch(String, String),
    Download(String, String),
    RemoveBucket(String),
    ListObjects(String),
    Unknown,
}

impl std::fmt::Display for StorageCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StorageCommand::Create(_, _) => "create_bucket",
                StorageCommand::Add(_, _, _) => "upload_object",
                StorageCommand::RemoveObject(_, _) => "remove_object",
                StorageCommand::ListBuckets(_) => "list_buckets",
                StorageCommand::Fetch(_, _) => "get_object",
                StorageCommand::Download(_, _) => "download_object",
                StorageCommand::RemoveBucket(_) => "delete_bucket",
                StorageCommand::ListObjects(_) => "list_objects",
                StorageCommand::Unknown => "Unknown",
            }
        )
    }
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let headers = HeaderMap::new();
            let remote = Some(block_on(auth::json_api_client(&headers))?);
            let hostport = "storage.googleapis.com:443";
            Ok(SinkManager::new_box(GoogleCloudStorage {
                config,
                remote,
                is_down: false,
                qos_facility: Box::new(QoSFacilities::recoverable(hostport.to_string())),
                reply_channel: None,
                is_linked: false,
                preprocessors: vec![],
                postprocessors: vec![],
                sink_url: TremorUrl::from_offramp_id("gcs")?,
                event_id_gen: EventIdGenerator::new(0), // Fake ID overwritten in init
            }))
        } else {
            Err("Offramp Google Cloud Storage requires a config".into())
        }
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

fn parse_command(value: &Value) -> Result<StorageCommand> {
    if let Value::Object(o) = value {
        let cmd_name: &str = &parse_arg!("command", o);

        let command = match cmd_name {
            "fetch" => StorageCommand::Fetch(parse_arg!("bucket", o), parse_arg!("object", o)),
            "list_buckets" => StorageCommand::ListBuckets(parse_arg!("project_id", o)),
            "list_objects" => StorageCommand::ListObjects(parse_arg!("bucket", o)),
            "upload_object" => StorageCommand::Add(
                parse_arg!("bucket", o),
                parse_arg!("object", o),
                if let Some(body) = o.get("body") {
                    body.clone().into_static()
                } else {
                    return Err("Invalid Command, expected `body` field".into());
                },
            ),
            "remove_object" => {
                StorageCommand::RemoveObject(parse_arg!("bucket", o), parse_arg!("object", o))
            }
            "create_bucket" => {
                StorageCommand::Create(parse_arg!("project_id", o), parse_arg!("bucket", o))
            }
            "remove_bucket" => StorageCommand::RemoveBucket(parse_arg!("bucket", o)),
            "download_object" => {
                StorageCommand::Download(parse_arg!("bucket", o), parse_arg!("object", o))
            }
            _ => StorageCommand::Unknown,
        };
        return Ok(command);
    }

    Err("Invalid Command".into())
}

#[async_trait::async_trait]
impl Sink for GoogleCloudStorage {
    async fn terminate(&mut self) {}

    #[allow(clippy::too_many_lines)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        let remote = if let Some(remote) = &self.remote {
            remote
        } else {
            self.remote = Some(auth::json_api_client(&HeaderMap::new()).await?);
            let remote = self.remote.as_ref().ok_or("Client error!")?;
            remote
            // TODO - Qos checks
        };

        let mut response = Vec::new();
        let maybe_correlation = event.correlation_meta();
        for value in event.value_iter() {
            let command = parse_command(value)?;
            match command {
                StorageCommand::Fetch(bucket_name, object_name) => {
                    response.push(make_command_response(
                        "fetch",
                        storage::get_object(&remote, &bucket_name, &object_name).await?,
                    ));
                }
                StorageCommand::ListBuckets(project_id) => {
                    response.push(make_command_response(
                        "list_buckets",
                        storage::list_buckets(&remote, &project_id).await?,
                    ));
                }
                StorageCommand::ListObjects(bucket_name) => {
                    response.push(make_command_response(
                        "list_objects",
                        storage::list_objects(&remote, &bucket_name).await?,
                    ));
                }
                StorageCommand::Add(bucket_name, object, body) => {
                    response.push(make_command_response(
                        "upload_object",
                        upload_object(
                            &remote,
                            &bucket_name,
                            &object,
                            &body,
                            codec,
                            event.ingest_ns,
                            &mut self.postprocessors,
                        )
                        .await?,
                    ));
                }
                StorageCommand::RemoveObject(bucket_name, object) => {
                    response.push(make_command_response(
                        "remove_object",
                        storage::delete_object(&remote, &bucket_name, &object).await?,
                    ));
                }
                StorageCommand::Create(project_id, bucket_name) => {
                    response.push(make_command_response(
                        "create_bucket",
                        storage::create_bucket(&remote, &project_id, &bucket_name).await?,
                    ));
                }
                StorageCommand::RemoveBucket(bucket_name) => {
                    response.push(make_command_response(
                        "remove_bucket",
                        storage::delete_bucket(&remote, &bucket_name).await?,
                    ));
                }
                StorageCommand::Download(bucket_name, object_name) => {
                    response.push(make_command_response(
                        "download_object",
                        download_object(
                            &remote,
                            &bucket_name,
                            &object_name,
                            &self.sink_url,
                            codec,
                            &mut self.preprocessors,
                        )
                        .await?,
                    ));
                }
                StorageCommand::Unknown => {
                    warn!(
                        "Unknown Google Cloud Storage command: `{}` attempted",
                        command.to_string()
                    );

                    return Err(format!(
                        "Unknown Google Cloud Storage command: `{}` attempted",
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

                reply_channel
                    .send(sink::Reply::Response(
                        OUT,
                        Event {
                            id: self.event_id_gen.next_id(),
                            data: (response, meta).into(),
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
        return Ok(Some(vec![qos::ack(&mut event)]));
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
        self.event_id_gen = EventIdGenerator::new(sink_uid);
        self.postprocessors = make_postprocessors(processors.post)?;
        self.preprocessors = make_preprocessors(processors.pre)?;
        self.reply_channel = Some(reply_channel);
        self.is_linked = is_linked;
        Ok(())
    }

    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.is_down && self.qos_facility.probe(signal.ingest_ns) {
            self.is_down = false;
            // This means the port is connectable
            info!("Google Cloud Storage -  sink remote endpoint - recovered and contactable");
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

async fn upload_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    data: &Value<'_>,
    codec: &dyn Codec,
    ingest_ns: u64,
    postprocessors: &mut [Box<dyn Postprocessor>],
) -> Result<Value<'static>> {
    let mut body: Vec<u8> = vec![];
    let codec_in_use = None;
    let codec = codec_in_use.unwrap_or(codec);
    let encoded = codec.encode(data)?;
    let mut processed = postprocess(postprocessors, ingest_ns, encoded)?;
    for processed_elem in &mut processed {
        body.append(processed_elem);
    }
    storage::add_object_with_slice(client, bucket_name, object_name, body).await
}

async fn download_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    sink_url: &TremorUrl,
    codec: &mut dyn Codec,
    preprocessors: &mut [Box<dyn Preprocessor>],
) -> Result<Value<'static>> {
    let response_bytes = storage::download_object(client, bucket_name, object_name).await?;
    let mut ingest_ns = nanotime();
    let preprocessed = preprocess(preprocessors, &mut ingest_ns, response_bytes, sink_url)?;
    let mut res = Vec::with_capacity(preprocessed.len());
    for pp in preprocessed {
        let mut pp = pp;
        let body = codec
            .decode(&mut pp, ingest_ns)?
            .unwrap_or_else(Value::object);
        res.push(body.into_static());
    }
    Ok(Value::Array(res))
}

fn make_command_response(cmd: &str, value: Value) -> Value<'static> {
    literal!({
        "cmd": cmd,
        "data": value
    })
    .into_static()
}
