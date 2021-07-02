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

use crate::connectors::otel::{logs, metrics, trace};
use crate::connectors::qos::{self, QoSFacilities, SinkQoS};
use crate::sink::prelude::*;
use halfbrown::HashMap;
use tonic::transport::Channel as TonicChannel;
use tonic::transport::Endpoint as TonicEndpoint;
use tremor_otelapis::opentelemetry::proto::collector::{
    logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
    metrics::v1::{metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest},
    trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
};

#[allow(dead_code)]
pub struct RemoteOpenTelemetryEndpoint {
    logs_client: LogsServiceClient<TonicChannel>,
    metrics_client: MetricsServiceClient<TonicChannel>,
    trace_client: TraceServiceClient<TonicChannel>,
}

#[allow(dead_code)]
pub struct OpenTelemetry {
    config: Config,
    endpoint: String,
    remote: Option<RemoteOpenTelemetryEndpoint>,
    is_down: bool,
    qos_facility: Box<dyn SinkQoS>,
}

#[derive(Deserialize)]
pub struct Config {
    /// The gRPC endpoint hostname for the remote OpenTelemetry collector endpoint
    pub host: String,
    /// The gRPC endpoint TCP port for the remote OpenTelemetry collector endpoint
    pub port: u16,
    /// Enable the log service
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

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let hostport = format!("{}:{}", config.host.clone(), config.port);
            let endpoint = format!("https://{}:{}", config.host.clone().as_str(), config.port);
            Ok(SinkManager::new_box(OpenTelemetry {
                config,
                endpoint,
                remote: None,
                is_down: false,
                qos_facility: Box::new(QoSFacilities::recoverable(hostport)),
            }))
        } else {
            Err("Offramp otel requires a config".into())
        }
    }
}

fn json_otel_logs_to_pb(json: &Value<'_>) -> Result<ExportLogsServiceRequest> {
    let pb = ExportLogsServiceRequest {
        resource_logs: logs::resource_logs_to_pb(json)?,
    };
    Ok(pb)
}

fn json_otel_trace_to_pb(json: &Value<'_>) -> Result<ExportTraceServiceRequest> {
    let pb = ExportTraceServiceRequest {
        resource_spans: trace::resource_spans_to_pb(Some(json))?,
    };
    Ok(pb)
}

fn json_otel_metrics_to_pb(json: &Value<'_>) -> Result<ExportMetricsServiceRequest> {
    let pb = ExportMetricsServiceRequest {
        resource_metrics: metrics::resource_metrics_to_pb(Some(json))?,
    };
    Ok(pb)
}

#[async_trait::async_trait]
impl Sink for OpenTelemetry {
    async fn terminate(&mut self) {}

    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        if let Some(remote) = &mut self.remote {
            // Up
            for value in event.value_iter() {
                if let Value::Object(o) = value {
                    if o.contains_key("metrics") {
                        if self.config.metrics {
                            let request = json_otel_metrics_to_pb(value)?;
                            if let Err(e) = remote.metrics_client.export(request).await {
                                error!("Failed to dispatch otel/gRPC metrics message: {}", e);
                                self.is_down = true;
                                if event.transactional {
                                    return Ok(Some(vec![
                                        qos::fail(&mut event.clone()),
                                        qos::close(&mut event),
                                    ]));
                                }
                                return Ok(Some(vec![qos::close(&mut event)]));
                            };
                            continue;
                        }
                        warn!("Otel Source received metrics event when metrics support is disabled. Dropping trace");
                    } else if o.contains_key("logs") {
                        if self.config.logs {
                            let request = json_otel_logs_to_pb(value)?;
                            if let Err(e) = remote.logs_client.export(request).await {
                                error!("Failed to dispatch otel/gRPC logs message: {}", e);
                                self.is_down = true;
                                if event.transactional {
                                    return Ok(Some(vec![
                                        qos::fail(&mut event.clone()),
                                        qos::close(&mut event),
                                    ]));
                                }
                                return Ok(Some(vec![qos::close(&mut event)]));
                            }
                            continue;
                        }
                        warn!("Otel Sink received log event when log support is disabled. Dropping trace");
                    } else if o.contains_key("trace") {
                        if self.config.trace {
                            let request = json_otel_trace_to_pb(value)?;
                            if let Err(e) = remote.trace_client.export(request).await {
                                error!("Failed to dispatch otel/gRPC logs message: {}", e);
                                self.is_down = true;
                                if event.transactional {
                                    return Ok(Some(vec![
                                        qos::fail(&mut event.clone()),
                                        qos::close(&mut event),
                                    ]));
                                }
                                return Ok(Some(vec![qos::close(&mut event)]));
                            }
                            continue;
                        }
                        warn!("Otel Sink received trace event when trace support is disabled. Dropping trace");
                    }
                }

                // Any other event structure / form is invalid - log an error
                error!("Unsupported event received by OTEL sink: {}", value);
            }

            self.is_down = false;
            return Ok(Some(if event.transactional {
                vec![qos::ack(&mut event)]
            } else {
                vec![]
            }));
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
        let channel = TonicEndpoint::from_shared(self.endpoint.clone())
            .map_err(|e| format!("Unable to connect to remote otel endpoint: {}", e))?
            .connect()
            .await;

        let channel = match channel {
            Ok(channel) => channel,
            Err(e) => return Err(format!("Unable to open remote otel channel {}", e).into()),
        };

        let logs_client = LogsServiceClient::new(channel.clone());
        let metrics_client = MetricsServiceClient::new(channel.clone());
        let trace_client = TraceServiceClient::new(channel);

        self.remote = Some(RemoteOpenTelemetryEndpoint {
            logs_client,
            metrics_client,
            trace_client,
        });

        Ok(())
    }
    async fn on_signal(&mut self, signal: Event) -> ResultVec {
        if self.is_down && self.qos_facility.probe(signal.ingest_ns) {
            self.is_down = false;
            // This means the port is connectable
            info!("CNCF OpenTelemetry -  sink remote endpoint - recovered and contactable");
            self.is_down = false;
            // Clone needed to make it mutable, lint is wrong
            #[allow(clippy::redundant_clone)]
            let mut signal = signal.clone();
            return Ok(Some(vec![qos::open(&mut signal)]));
        }

        Ok(None)
    }

    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
}
