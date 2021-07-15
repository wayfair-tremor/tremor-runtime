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

use crate::pipeline;
use crate::url::TremorUrl;
use beef::Cow;
use halfbrown::HashMap;
use tremor_pipeline::Event;
use tremor_script::prelude::*;

/// Metrics instance name
pub static mut INSTANCE: &str = "tremor";

#[derive(Debug)]
pub(crate) struct Ramp {
    r#in: u64,
    out: u64,
    err: u64,
}

/// metrics reporter for onramps and offramps
#[derive(Debug)]
pub struct RampReporter {
    /// url
    artefact_url: TremorUrl,
    /// metrics counter
    metrics: Ramp,
    /// reference to the metrics pipeline
    metrics_pipeline: Option<(TremorUrl, pipeline::Addr)>,
    /// flush interval in nano-seconds
    flush_interval: Option<u64>,
    /// last flush nano-seconds
    last_flush_ns: u64,
}

impl RampReporter {
    pub(crate) fn new(artefact_url: TremorUrl, flush_interval_s: Option<u64>) -> Self {
        Self {
            artefact_url,
            metrics: Ramp {
                r#in: 0,
                out: 0,
                err: 0,
            },
            metrics_pipeline: None,
            flush_interval: flush_interval_s.map(|n| n * 1_000_000_000),
            last_flush_ns: 0,
        }
    }

    pub(crate) fn set_metrics_pipeline(&mut self, pipeline_tuple: (TremorUrl, pipeline::Addr)) {
        self.metrics_pipeline = Some(pipeline_tuple);
    }

    pub(crate) fn increment_in(&mut self) {
        self.metrics.r#in += 1;
    }

    pub(crate) fn increment_out(&mut self) {
        self.metrics.out += 1;
    }

    pub(crate) fn increment_err(&mut self) {
        self.metrics.err += 1;
    }

    pub(crate) fn periodic_flush(&mut self, timestamp: u64) -> Option<u64> {
        if let Some(interval) = self.flush_interval {
            if timestamp >= self.last_flush_ns + interval {
                self.send(vec![
                    self.make_event(timestamp, "in", self.metrics.r#in),
                    self.make_event(timestamp, "out", self.metrics.out),
                    self.make_event(timestamp, "error", self.metrics.err),
                ]);
                self.last_flush_ns = timestamp;
                return Some(timestamp);
            }
        }
        None
    }

    #[must_use]
    fn make_event(&self, timestamp: u64, port: &'static str, count: u64) -> Event {
        let mut tags: HashMap<Cow<'static, str>, Value<'static>> = HashMap::with_capacity(2);
        tags.insert_nocheck(Cow::from("ramp"), self.artefact_url.to_string().into());
        tags.insert_nocheck(Cow::from("port"), port.into());

        let value = tremor_pipeline::influx_value(Cow::from("ramp_events"), tags, count, timestamp);
        // full metrics payload
        // TODO update origin url
        Event {
            data: value.into(),
            ingest_ns: timestamp,
            ..Event::default()
        }
    }

    // this is simple forwarding
    #[cfg(not(tarpaulin_include))]
    pub(crate) fn send(&self, events: Vec<Event>) {
        if let Some((metrics_input, metrics_addr)) = &self.metrics_pipeline {
            if let Some(input) = metrics_input.instance_port() {
                for event in events {
                    if let Err(e) = metrics_addr.try_send(pipeline::Msg::Event {
                        input: input.to_string().into(),
                        event,
                    }) {
                        error!("Failed to send to system metrics pipeline: {}", e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let mut r = RampReporter::new(TremorUrl::parse("/onramp/example/00").unwrap(), Some(1));
        r.increment_in();
        assert_eq!(r.metrics.r#in, 1);
        r.increment_out();
        assert_eq!(r.metrics.out, 1);
        r.increment_err();
        assert_eq!(r.metrics.err, 1);

        let e = r.make_event(123, "test", 42);

        let (v, _) = e.data.parts();

        assert_eq!(v["measurement"], "ramp_events");
        assert_eq!(v["tags"]["ramp"], "tremor://localhost/onramp/example/00");
        assert_eq!(v["tags"]["port"], "test");
        assert_eq!(v["fields"]["count"], 42);
        assert_eq!(v["timestamp"], 123);
        assert_eq!(r.periodic_flush(1), None);
        assert_eq!(r.periodic_flush(1_000_000_000), Some(1_000_000_000));
        assert_eq!(r.periodic_flush(1_000_000_001), None);
        assert_eq!(r.periodic_flush(2_000_000_000), Some(2_000_000_000));
    }
}
