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
use std::io::{BufRead as StdBufRead, BufReader, Read};
use std::time::Duration;
use tremor_common::file;
use xz2::read::XzDecoder;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    /// Interval in nanoseconds for coordinated emission testing
    pub interval: Option<u64>,
    /// Number of iterations to stop after
    pub iters: Option<u64>,
    #[serde(default = "Default::default")]
    pub base64: bool,
}

impl ConfigImpl for Config {}

#[derive(Clone)]
pub struct Blaster {
    pub config: Config,
    onramp_id: TremorUrl,
    data: Vec<u8>,
    acc: Acc,
    origin_uri: EventOriginUri,
}
impl std::fmt::Debug for Blaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blaster")
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let mut source_data_file = file::open(&config.source)?;
            let mut data = vec![];
            let ext = file::extension(&config.source);
            if ext == Some("xz") {
                XzDecoder::new(source_data_file).read_to_end(&mut data)?;
            } else {
                source_data_file.read_to_end(&mut data)?;
            };
            let origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-blaster".to_string(),
                host: hostname(),
                port: None,
                path: vec![config.source.clone()],
            };

            Ok(Box::new(Blaster {
                config,
                data,
                acc: Acc::default(),
                origin_uri,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[derive(Clone, Default)]
struct Acc {
    elements: Vec<Vec<u8>>,
    count: usize,
}
impl Acc {
    fn next(&mut self) -> Vec<u8> {
        // actually safe because we only get element from a slot < elements.len()
        let next = unsafe {
            self.elements
                .get_unchecked(self.count % self.elements.len())
                .clone()
        };
        self.count += 1;
        next
    }
}

#[async_trait::async_trait()]
impl Source for Blaster {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        // TODO better sleep perhaps
        if let Some(ival) = self.config.interval {
            task::sleep(Duration::from_nanos(ival)).await;
        }
        if Some(self.acc.count as u64) == self.config.iters {
            return Ok(SourceReply::StateChange(SourceState::Disconnected));
        };

        Ok(SourceReply::Data {
            origin_uri: self.origin_uri.clone(),
            data: self.acc.next(),
            meta: None,
            codec_override: None,
            stream: 0,
        })
    }
    async fn init(&mut self) -> Result<SourceState> {
        let elements: Result<Vec<Vec<u8>>> =
            StdBufRead::lines(BufReader::new(self.data.as_slice()))
                .map(|e| -> Result<Vec<u8>> {
                    if self.config.base64 {
                        Ok(base64::decode(&e?.as_bytes())?)
                    } else {
                        Ok(e?.as_bytes().to_vec())
                    }
                })
                .collect();
        self.acc.elements = elements?;
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Blaster {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        self.origin_uri.uid = config.onramp_uid;
        SourceManager::start(self.clone(), config).await
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
