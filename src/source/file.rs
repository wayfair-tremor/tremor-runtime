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

use crate::source::prelude::*;
use async_compression::futures::bufread::XzDecoder;
use async_std::fs::File as FSFile;
use async_std::io::prelude::*;
use async_std::io::{BufReader, Lines};
use async_std::prelude::*;
use std::process;
use tremor_common::asy::file;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    #[serde(default = "Default::default")]
    pub close_on_done: bool,
    #[serde(default = "Default::default")]
    pub sleep_on_done: u64,
}

impl ConfigImpl for Config {}

pub struct File {
    pub config: Config,
    onramp_id: TremorUrl,
}

enum ArghDyn {
    Xz(Lines<BufReader<XzDecoder<BufReader<FSFile>>>>),
    File(Lines<BufReader<FSFile>>),
}

impl ArghDyn {
    async fn next(&mut self) -> Option<std::io::Result<String>> {
        match self {
            ArghDyn::Xz(l) => l.next().await,
            ArghDyn::File(l) => l.next().await,
        }
    }
}

struct Int {
    pub config: Config,
    lines: ArghDyn,
    origin_uri: EventOriginUri,
    onramp_id: TremorUrl,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File")
    }
}
impl Int {
    const SLEEP_ON_DONE_MS: u64 = 10;

    async fn from_config(uid: u64, onramp_id: TremorUrl, config: Config) -> Result<Self> {
        let source_data_file = BufReader::new(file::open(&config.source).await?);
        let ext = file::extension(&config.source);
        let lines = if ext == Some("xz") {
            let r = BufReader::new(XzDecoder::new(source_data_file));
            ArghDyn::Xz(r.lines())
        } else {
            let r = source_data_file;
            ArghDyn::File(r.lines())
        };

        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-file".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.source.clone()],
        };
        Ok(Self {
            config,
            lines,
            origin_uri,
            onramp_id,
        })
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(File {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for file onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        if let Some(Ok(line)) = self.lines.next().await {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data: line.as_bytes().to_vec(),
                meta: None,           // TODO: add line num and filename here?
                codec_override: None, // TODO overwrite codec based on file ending or magic bytes
                stream: 0,
            })
        } else if self.config.sleep_on_done == 0 {
            if self.config.close_on_done {
                // ALLOW: This is on purpose, close when done tells the onramp to terminate when it's done with sending it's data - this is for one-offs
                process::exit(0);
            }
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        } else if self.config.sleep_on_done >= Self::SLEEP_ON_DONE_MS {
            self.config.sleep_on_done -= Self::SLEEP_ON_DONE_MS;
            Ok(SourceReply::Empty(Self::SLEEP_ON_DONE_MS))
        } else {
            let sleep = self.config.sleep_on_done;
            self.config.sleep_on_done = 0;
            Ok(SourceReply::Empty(sleep))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for File {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(
            config.onramp_uid,
            self.onramp_id.clone(),
            self.config.clone(),
        )
        .await?;
        SourceManager::start(source, config).await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
