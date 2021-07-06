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

use crate::connectors::otel::{logs, metrics, trace};

use crate::source::prelude::*;
use tremor_otelapis::all::{self, OpenTelemetryEvents};
use tremor_script::Value;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// The hostname or IP address for the remote OpenTelemetry collector endpoint
    pub host: String,
    /// The TCP port for the remote OpenTelemetry collector endpoint
    pub port: u16,
    #[serde(default = "d_true")]
    pub logs: bool,
    /// Enables the trace service
    #[serde(default = "d_true")]
    pub trace: bool,
    /// Enables the metrics service
    #[serde(default = "d_true")]
    pub metrics: bool,
}

fn d_true() -> bool {
    true
}

impl ConfigImpl for Config {}

pub struct OpenTelemetry {
    pub config: Config,
    onramp_id: TremorUrl,
}

#[allow(dead_code)]
pub struct Int {
    uid: u64,
    config: Config,
    onramp_id: TremorUrl,
    tx: tremor_otelapis::all::OpenTelemetrySender,
    rx: tremor_otelapis::all::OpenTelemetryReceiver,
    origin: EventOriginUri,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpenTelemetry")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        let (tx, rx) = bounded(128);
        let config = config.clone();
        let origin = EventOriginUri {
            uid: 0,
            scheme: "tremor-otel".to_string(),
            host: hostname(),
            port: None,
            path: vec![],
        };

        Self {
            uid,
            config,
            onramp_id,
            tx,
            rx,
            origin,
        }
    }
}

impl onramp::Impl for OpenTelemetry {
    fn from_config(id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for otel onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        match self.rx.try_recv() {
            Ok(OpenTelemetryEvents::Metrics(metrics)) => {
                if self.config.metrics {
                    let data: Value = metrics::resource_metrics_to_json(metrics);
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin.clone(),
                        data: data.into(),
                    });
                }
                warn!("Otel Source received metrics event when trace support is disabled. Dropping trace");
            }
            Ok(OpenTelemetryEvents::Logs(logs)) => {
                if self.config.logs {
                    let data: Value = logs::resource_logs_to_json(logs)?;
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin.clone(),
                        data: data.into(),
                    });
                }
                warn!(
                    "Otel Source received log event when trace support is disabled. Dropping trace"
                );
            }
            Ok(OpenTelemetryEvents::Trace(traces)) => {
                if self.config.trace {
                    let data: Value = trace::resource_spans_to_json(traces);
                    return Ok(SourceReply::Structured {
                        origin_uri: self.origin.clone(),
                        data: data.into(),
                    });
                }
                warn!("Otel Source received trace event when trace support is disabled. Dropping trace");
            }
            _ => (),
        };

        Ok(SourceReply::Empty(10))
    }

    async fn init(&mut self) -> Result<SourceState> {
        let addr = format!("{}:{}", self.config.host.as_str(), self.config.port).parse()?;
        let tx = self.tx.clone();
        task::spawn(async move {
            // Builder for gRPC server over HTTP/2 framing
            match all::make(addr, tx).await {
                Ok(_) => (),
                Err(e) => {
                    error!("Could not start gRPC service: {}", e);
                }
            }
        });
        Ok(SourceState::Connected)
    }

    async fn on_empty_event(&mut self, _id: u64, _stream: usize) -> Result<()> {
        Ok(())
    }

    async fn reply_event(
        &mut self,
        _event: Event,
        _codec: &dyn crate::codec::Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn crate::codec::Codec>>,
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
impl Onramp for OpenTelemetry {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
