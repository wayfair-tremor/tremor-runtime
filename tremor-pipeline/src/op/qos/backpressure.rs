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

//! # Incremental backoff limiter
//!
//! The Backoff limiter will start backing off based on the maximum allowed time for results
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Outputs
//!
//! The 1st additional output is used to route data that was decided to
//! be discarded.

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use beef::Cow;
use tremor_script::prelude::*;

const OVERFLOW: Cow<'static, str> = Cow::const_str("overflow");

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// The maximum allowed timeout before backoff is applied in ms
    pub timeout: f64,
    /// A list of backoff steps in ms, wich are progressed through as long
    /// as the maximum timeout is exceeded
    ///
    /// default: `[50, 100, 250, 500, 1000, 5000, 10000]`
    #[serde(default = "d_steps")]
    pub steps: Vec<u64>,

    /// If set to true will propagate circuit breaker events when the
    /// backpressure is introduced.
    ///
    /// This should be used with caution as it does, to a degree defeate the
    /// purpose of escalating backpressure
    #[serde(default)]
    pub circuit_breaker: bool,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Output {
    backoff: u64,
    next: u64,
    output: Cow<'static, str>,
}

#[derive(Debug, Clone)]
pub struct Backpressure {
    pub config: Config,
    pub output: Output,
    pub steps: Vec<u64>,
    pub next: usize,
}

impl From<Config> for Backpressure {
    fn from(config: Config) -> Self {
        let steps = config.steps.iter().map(|v| *v * 1_000_000).collect();
        Self {
            config,
            output: Output {
                output: OUT,
                next: 0,
                backoff: 0,
            },
            steps,
            next: 0,
        }
    }
}

fn d_steps() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

pub fn next_backoff(steps: &[u64], current: u64) -> u64 {
    let mut b = 0;
    for backoff in steps {
        b = *backoff;
        if b > current {
            break;
        }
    }
    b
}

op!(BackpressureFactory(_uid, node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;
        Ok(Box::new(Backpressure::from(config)))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

impl Operator for Backpressure {
    fn on_event(
        &mut self,
        uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        event.op_meta.insert(uid, OwnedValue::null());
        // we need to mark the event as transactional in order to reliably receive CB events
        // and thus do effective backpressure
        // if we don't and the events arent transactional themselves, we won't be notified, which would render this operator useless
        event.transactional = true;
        if self.output.next <= event.ingest_ns {
            self.output.next = event.ingest_ns + self.output.backoff;
            Ok(vec![(self.output.output.clone(), event)].into())
        } else {
            Ok(vec![(OVERFLOW, event)].into())
        }
    }

    fn handles_contraflow(&self) -> bool {
        true
    }

    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(
        &mut self,
        _uid: u64,
        _state: &Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        let insights = if self.output.backoff > 0 && self.output.next <= signal.ingest_ns {
            self.output.backoff = 0;
            self.output.next = 0;
            if self.config.circuit_breaker {
                vec![Event::cb_restore(signal.ingest_ns)]
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        Ok(EventAndInsights {
            insights,
            ..EventAndInsights::default()
        })
    }

    fn on_contraflow(&mut self, uid: u64, insight: &mut Event) {
        // If the related event never touched this operator we don't take
        // action
        if !insight.op_meta.contains_key(uid) {
            return;
        }
        let (_, meta) = insight.data.parts();

        let Backpressure {
            ref mut output,
            ref steps,
            ..
        } = *self;
        let was_open = output.backoff == 0;
        let timeout = self.config.timeout;
        if meta.get("error").is_some()
            || insight.cb == CbAction::Fail
            || insight.cb == CbAction::Close
            || meta
                .get("time")
                .and_then(Value::cast_f64)
                .map_or(false, |v| v > timeout)
        {
            let backoff = next_backoff(steps, output.backoff);
            output.backoff = backoff;
            output.next = insight.ingest_ns + backoff;
        } else {
            output.backoff = 0;
            output.next = 0;
        }

        if self.config.circuit_breaker && was_open && output.backoff > 0 {
            insight.cb = CbAction::Close;
        } else if insight.cb == CbAction::Close {
            insight.cb = CbAction::None;
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn pass_wo_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Without a timeout event sent a second event,
        // it too should pass
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);
    }

    #[test]
    fn block_on_error() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();

        let mut state = Value::null();

        // Sent a first event, as all is initited clean
        // we syould see this pass
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Insert a timeout event with `time` set top `200`
        // this is over our limit of `100` so we syould move
        // one up the backup steps
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());

        let mut op_meta = OpMeta::default();
        op_meta.insert(0, OwnedValue::null());
        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // Verify that we now have a backoff of 1ms
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 1_000_000);

        // The first event was sent at exactly 1ms
        // our we should block all eventsup to
        // 1_999_999
        // this event syould overflow
        let event2 = Event {
            id: (1, 1, 2).into(),
            ingest_ns: 2_000_000 - 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);

        // On exactly 2_000_000 we should be allowed to send
        // again
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event3.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("no results");
        assert_eq!("out", out);
        assert!(event.transactional);

        // Since now the last successful event was at 2_000_000
        // the next event should overflow at 2_000_001
        let event3 = Event {
            id: (1, 1, 3).into(),
            ingest_ns: 2_000_000 + 1,
            ..Event::default()
        };
        let mut r = op
            .on_event(0, "in", &mut state, event3.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, _event) = r.pop().expect("no results");
        assert_eq!("overflow", out);
    }

    #[test]
    fn walk_backoff() {
        let mut op: Backpressure = Config {
            timeout: 100.0,
            steps: vec![1, 10, 100],
            circuit_breaker: false,
        }
        .into();
        // An contraflow that fails the timeout
        let mut m = Object::new();
        m.insert("time".into(), 200.0.into());
        let mut op_meta = OpMeta::default();
        op_meta.insert(0, OwnedValue::null());

        let mut insight = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // A contraflow that passes the timeout
        let m = literal!({
            "time": 99.0
        });
        let mut op_meta = OpMeta::default();
        op_meta.insert(0, OwnedValue::null());

        let mut insight_reset = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: (Value::null(), m).into(),
            op_meta,
            ..Event::default()
        };

        // Assert initial state
        assert_eq!(op.output.backoff, 0);
        // move one step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 1_000_000);
        // move another step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 10_000_000);
        // move another another step up
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 100_000_000);
        // We are at the highest step everything
        // should stay  the same
        op.on_contraflow(0, &mut insight);
        assert_eq!(op.output.backoff, 100_000_000);
        // Now we should reset
        op.on_contraflow(0, &mut insight_reset);
        assert_eq!(op.output.backoff, 0);
    }
}
