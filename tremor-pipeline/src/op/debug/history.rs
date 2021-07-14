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

use crate::op::prelude::*;
use crate::ConfigImpl;
use tremor_script::prelude::*;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Name of the event history ( path ) to track
    pub op: String,
    /// Name of the field to store data in
    pub name: String,
}

impl ConfigImpl for Config {}

op!(EventHistoryFactory(_uid, node) {
if let Some(map) = &node.config {
    let config: Config = Config::new(map)?;
    Ok(Box::new(History {
        config,
        id: node.id.clone(),
    }))
} else {
    Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())

}});

#[derive(Debug, Clone)]
pub struct History {
    pub config: Config,
    pub id: String,
}

impl Operator for History {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        mut event: Event,
    ) -> Result<EventAndInsights> {
        let Event {
            ref mut data,
            ref id,
            ..
        } = event;
        data.rent_mut(|data| {
            let (_, meta) = data.parts_mut();

            match meta
                .get_mut(self.config.name.as_str())
                .and_then(Value::as_array_mut)
            {
                Some(ref mut history) => {
                    history.push(Value::from(format!("evt: {}({})", self.config.op, id)));
                }
                None => {
                    if let Some(ref mut obj) = meta.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::from(vec![Value::from(format!(
                                "evt: {}({})",
                                self.config.op, id
                            ))]),
                        );
                    }
                }
            };
        });

        Ok(event.into())
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
        let Event {
            ref id,
            ref mut data,
            ..
        } = signal;
        data.rent_mut(|data| {
            let (_, m) = data.parts_mut();

            match m
                .get_mut(self.config.name.as_str())
                .and_then(Value::as_array_mut)
            {
                Some(ref mut history) => {
                    history.push(Value::from(format!("sig: {}({})", self.config.op, id)));
                }
                None => {
                    if let Some(ref mut obj) = m.as_object_mut() {
                        obj.insert(
                            self.config.name.clone().into(),
                            Value::from(vec![Value::from(format!(
                                "sig: {}({})",
                                self.config.op, id
                            ))]),
                        );
                    }
                }
            };
        });
        Ok(EventAndInsights::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::EventId;

    #[test]
    fn history_op_test() {
        let mut op = History {
            config: Config {
                op: "green".to_string(),
                name: "snot".to_string(),
            },
            id: "badger".into(),
        };
        let event = Event {
            id: EventId::new(0, 0, 1),
            ingest_ns: 1,
            data: (Value::from("snot"), Value::object()).into(),
            ..Event::default()
        };

        let mut state = Value::null();

        let (out, event) = op
            .on_event(0, "in", &mut state, event)
            .expect("Failed to run pipeline")
            .events
            .pop()
            .expect("Empty results");
        assert_eq!(out, "out");

        let (out, _event) = op
            .on_event(0, "in", &mut state, event)
            .expect("Failed to run pipeline")
            .events
            .pop()
            .expect("Empty results");
        assert_eq!(out, "out");

        let mut event = Event {
            id: EventId::new(0, 0, 1),
            ingest_ns: 1,
            data: (Value::from("snot"), Value::object()).into(),
            ..Event::default()
        };
        let state = Value::null();

        let _ = op.on_signal(0, &state, &mut event);
        let _ = op.on_signal(0, &state, &mut event);

        let history = event.data.suffix().meta().get(op.config.name.as_str());

        match history.as_array() {
            Some(history) => {
                assert_eq!(2, history.len());
            }
            _ => unreachable!(),
        }
    }
}
