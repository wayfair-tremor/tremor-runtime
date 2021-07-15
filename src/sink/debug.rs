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

//! # Debug offramp reporting classification statistics.
//!
//! The debug offramp periodically reports on the number of events
//! per classification.
//!
//! ## Configuration
//!
//! This operator takes no configuration

#![cfg(not(tarpaulin_include))]

use async_std::io;
use halfbrown::HashMap;
use std::time::{Duration, Instant};
use crate::sink::prelude::*;

#[derive(Debug, Clone)]
struct DebugBucket {
    cnt: u64,
}

pub struct Debug {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    cnt: u64,
    stdout: io::Stdout,
}
pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, _config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Debug {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            cnt: 0,
            stdout: io::stdout(),
        }))
    }
}
#[async_trait::async_trait]
impl Sink for Debug {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        for (_value, meta) in event.value_meta_iter() {
            if self.last.elapsed() > self.update_time {
                self.last = Instant::now();
                self.stdout
                    .write(
                        format!(
                            "\n|{:20}| {:7}|\n|{:20}| {:7}|\n",
                            "classification", "total", "TOTAL", self.cnt
                        )
                        .as_bytes(),
                    )
                    .await?;

                self.cnt = 0;
                for (class, data) in &self.buckets {
                    self.stdout
                        .write(format!("|{:20}| {:7}|\n", class, data.cnt,).as_bytes())
                        .await?;
                }
                self.buckets.clear();
            }
            let c = meta.get_str("class").unwrap_or("<unclassified>");
            //TODO: return to entry
            if let Some(entry) = self.buckets.get_mut(c) {
                entry.cnt += 1;
            } else {
                self.buckets.insert(c.to_owned(), DebugBucket { cnt: 1 });
            }
            self.cnt += 1;
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
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
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
