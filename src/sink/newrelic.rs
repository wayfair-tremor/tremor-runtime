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

//! # `NewRelic` Offramp
//!
//! Buffers and send messages to `NewRelic`
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
use std::io::{Cursor, Read, Write};

use chrono::prelude::Utc;
use halfbrown::HashMap;
use http_types::headers::{CONTENT_ENCODING, CONTENT_TYPE};
use libflate::{finish, gzip};
use log::debug;

use crate::sink::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// NewRelic license/insert_key key
    #[serde(flatten)]
    pub key: Key,
    /// Choose if logs should be compressed before sending to newrelic
    /// This avoids extra egress costs but increases CPU usage on tremor server
    #[serde(default)]
    pub compress_logs: bool,
    /// Region to use to send logs
    /// use Europe in case you need to be GDPR compliant
    #[serde(default)]
    pub region: Region,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Key {
    /// NewRelic license key
    LicenseKey(String),
    /// NewRelic insert only key
    InsertKey(String),
}

impl ConfigImpl for Config {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Region {
    Usa,
    Europe,
    #[cfg(test)]
    Test,
}

impl Default for Region {
    fn default() -> Self {
        Self::Usa
    }
}

impl Region {
    pub fn logs_url(&self) -> &str {
        match self {
            Self::Usa => "https://log-api.newrelic.com/log/v1",
            Self::Europe => "https://log-api.eu.newrelic.com/log/v1",
            #[cfg(test)]
            Self::Test => "http://localhost:23456/newrelic",
        }
    }
}

pub struct NewRelic {
    config: Config,
}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config = Config::new(config)?;
            Ok(SinkManager::new_box(NewRelic { config }))
        } else {
            Err("Missing config for newrelic offramp".into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for NewRelic {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        // TODO: Document this, if one of the log entries cannot be decoded, the whole batch will be lost because
        // of the collect::<Result<_>>
        let payload = NewRelicPayload {
            logs: event
                .value_iter()
                .map(Self::value_to_newrelic_log)
                .collect(),
        };

        debug!("Sending a batch of {} items", payload.logs.len());

        self.send(&payload).await?;
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

impl NewRelic {
    async fn send(&mut self, newrelic_payload: &NewRelicPayload<'_>) -> Result<()> {
        let (key, value) = self.auth_headers();
        let output_array = vec![newrelic_payload];
        let mut buffer = Vec::with_capacity(10240);
        {
            let mut writer = self.get_writer(&mut buffer)?;
            simd_json::to_writer::<Vec<&NewRelicPayload>, _>(&mut writer, &output_array)?;
        }

        if log::log_enabled!(log::Level::Trace) {
            if self.config.compress_logs {
                let mut decoder = libflate::gzip::Decoder::new(buffer.as_slice())?;
                let mut output = String::new();
                decoder.read_to_string(&mut output)?;
                trace!("Payload to send {}", output);
            } else {
                let output = String::from_utf8(buffer.clone())?;
                trace!("Payload to send: {}", output);
            }
        }

        debug!("sending {} bytes", buffer.len());

        let mut request = surf::post(self.config.region.logs_url())
            .header(key, value)
            .body(buffer)
            .header(CONTENT_TYPE, "application/json");

        if self.config.compress_logs {
            request = request.header(CONTENT_ENCODING, "gzip");
        }

        trace!("Request: {:?}", request);

        let mut response = request.await?;

        if !response.status().is_success() {
            let body = match response.body_string().await {
                Ok(body) => body,
                Err(err) => format!("failed to load body {}", err),
            };
            return Err(format!(
                "error sending newrelic logs\nresponse: {:?}\nreturned body: {}",
                response, body
            )
            .into());
        }

        if log::log_enabled!(log::Level::Debug) {
            let body = match response.body_string().await {
                Ok(body) => body,
                Err(err) => format!("failed to load body {}", err),
            };
            debug!("newrelic response: {:?}\nbody: {}", response, body);
        }

        Ok(())
    }

    fn auth_headers(&self) -> (&str, &str) {
        match self.config.key {
            Key::LicenseKey(ref key) => ("X-License-Key", key),
            Key::InsertKey(ref key) => ("X-Insert-Key", key),
        }
    }

    fn get_writer<'a>(&self, buffer: &'a mut Vec<u8>) -> Result<Box<dyn Write + 'a>> {
        if self.config.compress_logs {
            // Just unwrap as we are writing to a Vec
            trace!("using gzip writer");
            Ok(Box::new(finish::AutoFinishUnchecked::new(
                gzip::Encoder::new(buffer)?,
            )))
        } else {
            trace!("using plain text writer");
            Ok(Box::new(Cursor::new(buffer)))
        }
    }

    fn value_to_newrelic_log<'a>(value: &'a Value<'_>) -> NewRelicLog<'a> {
        let mut new_value = value.clone();
        let timestamp = new_value
            .remove("timestamp")
            .ok()
            .flatten()
            .as_ref()
            .as_i64()
            .unwrap_or_else(|| Utc::now().timestamp_millis());

        let message = new_value
            .remove("message")
            .ok()
            .flatten()
            .as_str()
            .unwrap_or("")
            .to_string();

        NewRelicLog {
            timestamp,
            message,
            attributes: Some(new_value),
        }
    }
}

#[derive(Debug, Serialize)]
struct NewRelicPayload<'a> {
    logs: Vec<NewRelicLog<'a>>,
}

#[derive(Debug, Serialize)]
struct NewRelicLog<'a> {
    timestamp: i64,
    message: String,
    attributes: Option<Value<'a>>,
}

#[cfg(test)]
mod tests {
    use super::{Config, Key, Region};

    #[test]
    fn config_shape() {
        let config = Config {
            key: Key::LicenseKey("asdas".into()),
            compress_logs: true,
            region: Default::default(),
        };
        let config_output = simd_json::to_string::<Config>(&config).expect("failed to dump config");
        assert_eq!(
            config_output,
            r#"{"license_key":"asdas","compress_logs":true,"region":"usa"}"#
        );
    }

    #[test]
    fn region_url() {
        assert_eq!(
            Region::default().logs_url(),
            "https://log-api.newrelic.com/log/v1"
        );
        assert_eq!(
            Region::Usa.logs_url(),
            "https://log-api.newrelic.com/log/v1"
        );
        assert_eq!(
            Region::Europe.logs_url(),
            "https://log-api.eu.newrelic.com/log/v1"
        );
    }
}
