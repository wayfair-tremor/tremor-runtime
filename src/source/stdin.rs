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

use crate::{source::prelude::*, url::TremorUrl};
use async_std::io;

const INPUT_SIZE_BYTES: usize = 8192;

pub struct Stdin {
    onramp_id: TremorUrl,
}

pub struct Int {
    onramp_id: TremorUrl,
    origin_uri: EventOriginUri,
    stdin: io::Stdin,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stdin")
    }
}

impl Int {
    async fn new(uid: u64, onramp_id: TremorUrl) -> Result<Self> {
        let stdin = io::stdin();
        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-stdin".to_string(),
            host: hostname(),
            port: None,
            path: vec![],
        };
        Ok(Self {
            onramp_id,
            origin_uri,
            stdin,
        })
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, _config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        Ok(Box::new(Stdin {
            onramp_id: id.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl Onramp for Stdin {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::new(config.onramp_uid, self.onramp_id.clone()).await?;
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "string"
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        let mut input = vec![0; INPUT_SIZE_BYTES];
        if let Ok(len) = self.stdin.read(&mut input).await {
            if len == 0 {
                Ok(SourceReply::StateChange(SourceState::Disconnected))
            } else {
                Ok(SourceReply::Data {
                    origin_uri: self.origin_uri.clone(),
                    // ALLOW: len cannot be > INPUT_SIZE_BYTES
                    data: input[0..len].to_vec(),
                    meta: None,
                    codec_override: None,
                    stream: 0,
                })
            }
        } else {
            error!("[Source::{}] Failed to read from stdin.", self.onramp_id);
            Ok(SourceReply::Empty(0))
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
}
