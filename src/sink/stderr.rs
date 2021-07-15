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

//! # Debug offramp reporting classification statistics.
//!
//! The debug offramp periodically reports on the number of events
//! per classification.
//!
//! ## Configuration
//!
//! This operator takes no configuration

use crate::sink::prelude::*;
use async_std::io;
use halfbrown::HashMap;

pub struct StdErr {
    postprocessors: Postprocessors,
    stderr: io::Stderr,
    config: Config,
}
#[derive(Clone, Debug, Deserialize, Default)]
struct Config {
    #[serde(default = "Default::default")]
    prefix: String,

    /// write data to stderr as raw bytes, not in debug formatting
    #[serde(default = "Default::default")]
    raw: bool,
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            Ok(SinkManager::new_box(StdErr {
                postprocessors: vec![],
                stderr: io::stderr(),
                config: Config::new(config)?,
            }))
        } else {
            Ok(SinkManager::new_box(StdErr {
                postprocessors: vec![],
                stderr: io::stderr(),
                config: Config::default(),
            }))
        }
    }
}
#[async_trait::async_trait]
impl Sink for StdErr {
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let ingest_ns = event.ingest_ns;
        for value in event.value_iter() {
            let raw = codec.encode(value)?;
            for processed in postprocess(&mut self.postprocessors, ingest_ns, raw)? {
                self.stderr.write_all(self.config.prefix.as_bytes()).await?;
                if self.config.raw {
                    self.stderr.write_all(&processed).await?;
                } else if let Ok(s) = std::str::from_utf8(&processed) {
                    self.stderr.write_all(s.as_bytes()).await?;
                } else {
                    self.stderr
                        .write_all(format!("{:?}", &processed).as_bytes())
                        .await?;
                }
                self.stderr.write_all(b"\n").await?;
            }
        }
        self.stderr.flush().await?;
        Ok(None)
    }
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.postprocessors = make_postprocessors(processors.post)?;
        Ok(())
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
