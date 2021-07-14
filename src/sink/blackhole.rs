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

//! # Blackhole benchmarking offramp
//!
//! Offramp used for benchmarking to generate latency histograms
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

#![cfg(not(tarpaulin_include))]
// This is OK, Blackhole is benchmark only
#![allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]

use crate::sink::prelude::*;
use halfbrown::HashMap;
use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
use hdrhistogram::Histogram;
use std::fmt::Display;
use std::io::{self, stdout, Read, Write};
use std::process;
use std::result;
use std::str;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Number of seconds to collect data before the system is stopped.
    pub stop_after_secs: u64,
    /// Significant figures for the histogram
    pub significant_figures: u64,
    /// Number of seconds to warmup, events during this time are not
    /// accounted for in the latency measurements
    pub warmup_secs: u64,
}

impl ConfigImpl for Config {}

/// A null offramp that records histograms
pub struct Blackhole {
    // config: Config,
    stop_after: u64,
    warmup: u64,
    has_stop_limit: bool,
    delivered: Histogram<u64>,
    run_secs: f64,
    bytes: usize,
    count: u64,
    buf: Vec<u8>,
}

impl offramp::Impl for Blackhole {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let now_ns = nanotime();
            Ok(SinkManager::new_box(Self {
                // config: config.clone(),
                run_secs: config.stop_after_secs as f64,
                stop_after: now_ns + (config.stop_after_secs + config.warmup_secs) * 1_000_000_000,
                warmup: now_ns + config.warmup_secs * 1_000_000_000,
                has_stop_limit: config.stop_after_secs != 0,
                delivered: Histogram::new_with_bounds(
                    1,
                    100_000_000_000,
                    config.significant_figures as u8,
                )?,
                bytes: 0,
                count: 0,
                buf: Vec::with_capacity(1024),
            }))
        } else {
            Err("Blackhole offramp requires a config".into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for Blackhole {
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

    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let now_ns = nanotime();
        if self.has_stop_limit && now_ns > self.stop_after {
            let mut buf = Vec::new();
            let mut serializer = V2Serializer::new();

            serializer.serialize(&self.delivered, &mut buf)?;
            if quantiles(buf.as_slice(), stdout(), 5, 2).is_ok() {
                println!(
                    "\n\nThroughput   (data): {:.1} MB/s\nThroughput (events): {:.1}k events/s",
                    (self.bytes as f64 / self.run_secs) / (1024.0 * 1024.0),
                    (self.count as f64 / self.run_secs) / 1000.0
                );
            } else {
                eprintln!("Failed to serialize histogram");
            }
            // ALLOW: This is on purpose, we use blackhole for benchmarking, so we want it to terminate the process when done
            process::exit(0);
        };
        for value in event.value_iter() {
            if now_ns > self.warmup {
                let delta_ns = now_ns - event.ingest_ns;
                if codec.encode_into(value, &mut self.buf).is_ok() {
                    self.bytes += self.buf.len();
                } else {
                    error!("failed to encode");
                };
                self.count += 1;
                self.buf.clear();
                self.delivered.record(delta_ns)?
            }
        }
        Ok(None)
    }
    fn default_codec(&self) -> &str {
        "null"
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

fn quantiles<R: Read, W: Write>(
    mut reader: R,
    mut writer: W,
    quantile_precision: usize,
    ticks_per_half: u32,
) -> Result<()> {
    fn write_extra_data<T1: Display, T2: Display, W: Write>(
        writer: &mut W,
        label1: &str,
        data1: T1,
        label2: &str,
        data2: T2,
    ) -> result::Result<(), io::Error> {
        writer.write_all(
            format!(
                "#[{:10} = {:12.2}, {:14} = {:12.2}]\n",
                label1, data1, label2, data2
            )
            .as_ref(),
        )
    }

    let hist: Histogram<u64> = Deserializer::new().deserialize(&mut reader)?;

    writer.write_all(
        format!(
            "{:>10} {:>quantile_precision$} {:>10} {:>14}\n\n",
            "Value",
            "Percentile",
            "TotalCount",
            "1/(1-Percentile)",
            quantile_precision = quantile_precision + 2 // + 2 from leading "0." for numbers
        )
        .as_ref(),
    )?;
    let mut sum = 0;
    for v in hist.iter_quantiles(ticks_per_half) {
        sum += v.count_since_last_iteration();
        if v.quantile_iterated_to() < 1.0 {
            writer.write_all(
                format!(
                    "{:12} {:1.*} {:10} {:14.2}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    //                        v.quantile(),
                    //                        quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    1_f64 / (1_f64 - v.quantile_iterated_to()),
                )
                .as_ref(),
            )?;
        } else {
            writer.write_all(
                format!(
                    "{:12} {:1.*} {:10} {:>14}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    //                        v.quantile(),
                    //                        quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    "inf"
                )
                .as_ref(),
            )?;
        }
    }
    write_extra_data(
        &mut writer,
        "Mean",
        hist.mean(),
        "StdDeviation",
        hist.stdev(),
    )?;
    write_extra_data(&mut writer, "Max", hist.max(), "Total count", hist.len())?;
    write_extra_data(
        &mut writer,
        "Buckets",
        hist.buckets(),
        "SubBuckets",
        hist.distinct_values(),
    )?;

    Ok(())
}
