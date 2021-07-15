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

//! # Exit offramp terminates runtime
//!
//! The exit offramp terminates the runtime with a system exit status
//!
//! ## Configuration
//!
//! This operator takes no configuration

#![cfg(not(tarpaulin_include))]

use halfbrown::HashMap;
use std::time::Duration;
use crate::sink::prelude::*;

pub struct Exit {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, _config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Exit {}))
    }
}

#[async_trait::async_trait]
impl Sink for Exit {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        for (value, _meta) in event.value_meta_iter() {
            if let Some(status) = value.get_i32("exit") {
                if let Some(delay) = value.get_u64("delay") {
                    task::sleep(Duration::from_millis(delay)).await;
                }
                // ALLOW: this is the supposed to exit
                std::process::exit(status);
            } else {
                return Err("Unexpected event received in exit offramp".into());
            }
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
