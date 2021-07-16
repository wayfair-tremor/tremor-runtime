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

use crate::sink::prelude::*;
use async_std::fs::File as FSFile;
use async_std::io::prelude::*;
use halfbrown::HashMap;
use tremor_common::asy::file as cfile;

/// An offramp that write a given file
pub struct File {
    file: Option<FSFile>,
    postprocessors: Postprocessors,
    config: Config,
}

#[derive(Deserialize)]
pub struct Config {
    /// Filename to write to
    pub file: String,
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(SinkManager::new_box(File {
                file: None,
                config,
                postprocessors: vec![],
            }))
        } else {
            Err("Blackhole offramp requires a config".into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for File {
    async fn terminate(&mut self) {
        if let Some(file) = &mut self.file {
            if let Err(e) = file.flush().await {
                error!("Failed to flush file: {}", e);
            }
        }
    }

    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        if let Some(file) = &mut self.file {
            for value in event.value_iter() {
                let raw = codec.encode(value)?;
                let packets = postprocess(&mut self.postprocessors, event.ingest_ns, raw)?;
                for packet in packets {
                    file.write_all(&packet).await?;
                    file.write_all(b"\n").await?;
                }
            }
            file.flush().await?
        }
        Ok(Some(vec![sink::Reply::Insight(event.insight_ack())]))
    }
    fn default_codec(&self) -> &str {
        "json"
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
        let file = cfile::create(&self.config.file).await?;
        self.file = Some(file);
        Ok(())
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
