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

use crate::{op::prelude::*, EventId, EventIdGenerator};
use std::mem::swap;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub count: usize,
    /// The amount time between messags to flush in milliseconds
    #[serde(default = "Default::default")]
    pub timeout: Option<u64>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
pub struct Batch {
    pub config: Config,
    pub data: EventPayload,
    pub len: usize,
    pub max_delay_ns: Option<u64>,
    pub first_ns: u64,
    pub id: Cow<'static, str>,
    /// event id for the resulting batched event
    /// the resulting id will be a new distinct id and will be tracking
    /// all event ids (min and max) in the batched event
    batch_event_id: EventId,
    is_transactional: bool,
    event_id_gen: EventIdGenerator,
}

pub fn empty() -> EventPayload {
    EventPayload::new(vec![], |_| ValueAndMeta::from(Value::array()))
}

op!(BatchFactory(uid, node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
    let max_delay_ns = config.timeout.map(|max_delay_ms| max_delay_ms * 1_000_000);
    let mut idgen = EventIdGenerator::new(uid);
    Ok(Box::new(Batch {
        data: empty(),
        len: 0,
        config,
        max_delay_ns,
        first_ns: 0,
        id: node.id.clone(),
        batch_event_id: idgen.next_id(),
        is_transactional: false,
        event_id_gen: idgen,
    }))
} else {
    Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

}});

impl Operator for Batch {
    /// emit a new event once the batch is flushed
    /// with a new event id tracking all events within that batch
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        // TODO: This is ugly
        let Event {
            id,
            data,
            ingest_ns,
            is_batch,
            transactional,
            ..
        } = event;
        self.batch_event_id.track(&id);
        self.is_transactional = self.is_transactional || transactional;
        self.data.consume(
            data,
            move |this: &mut ValueAndMeta<'static>, other: ValueAndMeta<'static>| -> Result<()> {
                if let Some(ref mut a) = this.value_mut().as_array_mut() {
                    let (value, meta) = other.into_parts();
                    let e = literal!({
                        "data": {
                            "value": value,
                            "meta": meta,
                            "ingest_ns": ingest_ns,
                            "kind": Value::null(),
                            "is_batch": is_batch
                        }
                    });
                    a.push(e)
                };
                Ok(())
            },
        )?;
        self.len += 1;
        if self.len == 1 {
            self.first_ns = ingest_ns;
        };
        let flush = match self.max_delay_ns {
            Some(t) if ingest_ns - self.first_ns > t => true,
            _ => self.len == self.config.count,
        };
        if flush {
            //TODO: This is ugly
            let mut data = empty();
            swap(&mut data, &mut self.data);
            self.len = 0;

            let mut event = Event {
                id: self.event_id_gen.next_id(),
                data,
                ingest_ns: self.first_ns,
                is_batch: true,
                transactional: self.is_transactional,
                ..Event::default()
            };
            self.is_transactional = false;
            swap(&mut self.batch_event_id, &mut event.id);
            Ok(event.into())
        } else {
            Ok(EventAndInsights::default())
        }
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
        Ok(self
            .max_delay_ns
            .map_or_else(EventAndInsights::default, |delay_ns| {
                if signal.ingest_ns - self.first_ns > delay_ns {
                    // We don't want to modify the original signal we clone it to
                    // create a new event.

                    if self.len > 0 {
                        let mut data = empty();
                        swap(&mut data, &mut self.data);

                        self.len = 0; // reset len
                        let mut event = Event {
                            id: self.event_id_gen.next_id(),
                            data,
                            ingest_ns: self.first_ns,
                            is_batch: true,
                            transactional: self.is_transactional,
                            ..Event::default()
                        };
                        self.is_transactional = false;
                        swap(&mut self.batch_event_id, &mut event.id);
                        EventAndInsights::from(event)
                    } else {
                        EventAndInsights::default()
                    }
                } else {
                    EventAndInsights::default()
                }
            }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json_derive::Serialize;
    use tremor_script::Value;

    #[test]
    fn size() {
        let mut idgen = EventIdGenerator::new(0);
        let mut op = Batch {
            config: Config {
                count: 2,
                timeout: None,
            },
            first_ns: 0,
            max_delay_ns: None,
            data: empty(),
            len: 0,
            id: "badger".into(),
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };
        let event1 = Event {
            id: EventId::new(0, 0, 1),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("badger").into(),
            ..Event::default()
        };

        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 1);
        let (out, event) = r.events.pop().expect("no results");
        assert_eq!("out", out);
        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event1.data.suffix().value(), event2.data.suffix().value()]
        );

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(0, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn time() -> Result<()> {
        let node_config = NodeConfig::from_config(
            "badger",
            Config {
                count: 100,
                timeout: Some(1),
            },
        )?;
        let mut op = BatchFactory::new().from_node(42, &node_config)?;

        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        println!("{}", event1.json_string().expect(""));

        let mut state = Value::null();

        let r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event2 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::from("badger").into(),
            transactional: true,
            ..Event::default()
        };

        let mut r = op
            .on_event(0, "in", &mut state, event2.clone())
            .expect("could not run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty results");
        assert_eq!("out", out);
        assert_eq!(true, event.transactional);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(
            events,
            vec![event1.data.suffix().value(), event2.data.suffix().value()]
        );

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(0, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(0, "in", &mut state, event)
            .expect("could not run pipeline");
        assert_eq!(r.len(), 0);
        Ok(())
    }

    #[test]
    fn signal() {
        let mut idgen = EventIdGenerator::new(0);
        let mut op = Batch {
            config: Config {
                count: 100,
                timeout: Some(1),
            },
            first_ns: 0,
            max_delay_ns: Some(1_000_000),
            data: empty(),
            len: 0,
            id: "badger".into(),
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            transactional: true,
            ..Event::default()
        };

        let mut state = Value::null();

        let r = op
            .on_event(0, "in", &mut state, event1.clone())
            .expect("failed to run peipeline");
        assert_eq!(r.len(), 0);

        let mut signal = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::null().into(),
            ..Event::default()
        };

        let mut r = op
            .on_signal(0, &state, &mut signal)
            .expect("failed to run pipeline")
            .events;
        assert_eq!(r.len(), 1);
        let (out, event) = r.pop().expect("empty resultset");
        assert_eq!("out", out);
        assert_eq!(true, event.transactional);

        let events: Vec<&Value> = event.value_iter().collect();
        assert_eq!(events, vec![event1.data.suffix().value()]);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(0, "in", &mut state, event)
            .expect("failed to run pipeline");
        assert_eq!(r.len(), 0);

        let event = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let r = op
            .on_event(0, "in", &mut state, event)
            .expect("failed to run piepeline");
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn forbid_empty_batches() -> Result<()> {
        let mut idgen = EventIdGenerator::new(0);
        let mut op = Batch {
            config: Config {
                count: 2,
                timeout: Some(1),
            },
            first_ns: 0,
            max_delay_ns: Some(100_000),
            data: empty(),
            len: 0,
            id: "badger".into(),
            batch_event_id: idgen.next_id(),
            is_transactional: false,
            event_id_gen: idgen,
        };

        let mut state = Value::null();
        let mut signal = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1_000_000,
            data: Value::null().into(),
            ..Event::default()
        };

        let r = op
            .on_signal(0, &state, &mut signal)
            .expect("failed to run pipeline")
            .events;
        assert_eq!(r.len(), 0);

        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 2_000_000,
            data: Value::from("snot").into(),
            ..Event::default()
        };
        let r = op
            .on_event(0, "in", &mut state, event1)
            .expect("failed to run peipeline");
        assert_eq!(r.len(), 0);

        signal.ingest_ns = 3_000_000;
        signal.id = (1, 1, 2).into();
        let r = op
            .on_signal(0, &state, &mut signal)
            .expect("failed to run pipeline")
            .events;
        assert_eq!(r.len(), 1);

        signal.ingest_ns = 4_000_000;
        signal.id = (1, 1, 3).into();
        let r = op
            .on_signal(0, &state, &mut signal)
            .expect("failed to run pipeline")
            .events;
        assert_eq!(r.len(), 0);

        Ok(())
    }
}
