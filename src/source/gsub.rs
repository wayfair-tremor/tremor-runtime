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

use crate::codec::Codec;
use crate::connectors::gcp::{pubsub, pubsub_auth};
use crate::source::prelude::*;
use googapis::google::pubsub::v1::subscriber_client::SubscriberClient;
use std::env;
use std::{fs::File, io::Read};
use tonic::transport::Channel;
pub struct GoogleCloudPubSub {
    config: Config,
    onramp_id: TremorUrl,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub subscription: String,
}

impl ConfigImpl for Config {}

pub struct Int {
    config: Config,
    onramp_id: TremorUrl,
    remote: Option<SubscriberClient<Channel>>,
    origin: EventOriginUri,
    project_id: String,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudPubSub")
    }
}
impl Int {
    fn from_config(onramp_id: TremorUrl, config: &Config) -> Self {
        let config: Config = config.clone();
        let remote = None;
        let project_id = "".to_string();
        let origin = EventOriginUri {
            uid: 0,
            scheme: "google-sub".to_string(),
            host: hostname(),
            port: None,
            path: vec![],
        };
        Self {
            config,
            onramp_id,
            remote,
            origin,
            project_id,
        }
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(GoogleCloudPubSub {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Onramp Google Cloud Pubsub (sub) requires a config".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        let subscription_name = &self.config.subscription;
        let mut remote = match &mut self.remote {
            Some(v) => v,
            None => return Err("Error creating the client".into()),
        };
        let project_id = &self.project_id;

        // TODO: Error Handling
        let data = pubsub::receive_message(&mut remote, &project_id, &subscription_name).await?;
        let mut res = Vec::new();
        let mut ack_ids = Vec::new();
        for msg in data.received_messages {
            let ack_id = msg.ack_id.clone();
            let data = msg
                .message
                .as_ref()
                .ok_or("Error getting pubsub message")?
                .data
                .clone();
            let message_id = msg
                .message
                .as_ref()
                .ok_or("Error getting pubsub message")?
                .message_id
                .clone();
            let mut meta = Value::object_with_capacity(2);
            meta.insert("message_id", message_id)?;
            meta.insert("acknowledgement_id", ack_id.clone())?;
            ack_ids.push(ack_id);
            res.push((data, Some(meta)));
        }
        // TODO: Construct tests to make sure that duplicate messages are not coming since acknowledgement is done after
        pubsub::acknowledge(&mut remote, &project_id, &subscription_name, ack_ids).await?;
        return Ok(SourceReply::BatchData {
            origin_uri: self.origin.clone(),
            batch_data: res,
            codec_override: None,
            stream: 0,
        });
    }

    async fn init(&mut self) -> Result<SourceState> {
        self.remote = Some(pubsub_auth::setup_subscriber_client().await?);
        let file;
        match env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Ok(val) => file = val,
            Err(_e) => file = "service account file not found".to_string(),
        }
        let mut f = File::open(file)?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;
        let json = tremor_value::parse_to_value(&mut buffer)?;
        self.project_id = match json.get_str("project_id") {
            Some(v) => v.to_string(),
            None => return Err("Error getting `project id` in gsub".into()),
        };
        Ok(SourceState::Connected)
    }

    async fn on_empty_event(&mut self, _id: u64, _stream: usize) -> Result<()> {
        Ok(())
    }

    async fn reply_event(
        &mut self,
        _event: Event,
        _codec: &dyn Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        Ok(())
    }

    fn metrics(&mut self, _t: u64) -> Vec<Event> {
        vec![]
    }

    async fn terminate(&mut self) {}

    fn trigger_breaker(&mut self) {}

    fn restore_breaker(&mut self) {}

    fn ack(&mut self, _id: u64) {}

    fn fail(&mut self, _id: u64) {}

    fn is_transactional(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl Onramp for GoogleCloudPubSub {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
