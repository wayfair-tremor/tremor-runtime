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

use crate::{CbAction, EventId, OpMeta, SignalKind};
use std::mem::swap;
use tremor_script::prelude::*;
use tremor_script::{EventOriginUri, EventPayload, Value};

/// A tremor event
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct Event {
    /// The event ID
    pub id: EventId,
    /// The event Data
    pub data: EventPayload,
    /// Nanoseconds at when the event was ingested
    pub ingest_ns: u64,
    /// URI to identify the origin of the event
    pub origin_uri: Option<EventOriginUri>,
    /// The kind of the event
    pub kind: Option<SignalKind>,
    /// If this event is batched (containing multiple events itself)
    pub is_batch: bool,

    /// Circuit breaker action
    pub cb: CbAction,
    /// Metadata for operators
    pub op_meta: OpMeta,
    /// this needs transactional data
    pub transactional: bool,
}

impl Event {
    /// turns the event in an insight given it's success
    #[must_use]
    pub fn insight(self, success: bool) -> Event {
        Event {
            cb: success.into(),
            ingest_ns: self.ingest_ns,
            id: self.id,
            op_meta: self.op_meta,
            origin_uri: self.origin_uri,
            ..Event::default()
        }
    }

    /// Creates either a restore or trigger event
    #[must_use]
    pub fn restore_or_break(restore: bool, ingest_ns: u64) -> Self {
        if restore {
            Event::cb_restore(ingest_ns)
        } else {
            Event::cb_trigger(ingest_ns)
        }
    }

    /// Creates either a ack or fail event
    #[must_use]
    pub fn ack_or_fail(ack: bool, ingest_ns: u64, ids: EventId) -> Self {
        if ack {
            Event::cb_ack(ingest_ns, ids)
        } else {
            Event::cb_fail(ingest_ns, ids)
        }
    }

    /// Creates a new ack insight from the event, consumes the `op_meta` and
    /// `origin_uri` of the event
    #[must_use]
    pub fn insight_ack(&mut self) -> Event {
        let mut e = Event::cb_ack(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// produce a `CBAction::Ack` insight event with the given time (in ms) in the metadata
    #[must_use]
    pub fn insight_ack_with_timing(&mut self, processing_time: u64) -> Event {
        let mut e = self.insight_ack();
        let mut meta = Object::with_capacity(1);
        meta.insert("time".into(), Value::from(processing_time));
        e.data = (Value::null(), Value::from(meta)).into();
        e
    }

    /// Creates a new fail insight from the event, consumes the `op_meta` and `origin_uri` of the
    /// event
    #[must_use]
    pub fn insight_fail(&mut self) -> Event {
        let mut e = Event::cb_fail(self.ingest_ns, self.id.clone());
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a restore insight from the event, consumes the `op_meta` and `origin_uri` of the
    /// event
    #[must_use]
    pub fn insight_restore(&mut self) -> Event {
        let mut e = Event::cb_restore(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// Creates a trigger insight from the event, consums the `op_meta` and `origin_uri` of the
    /// event
    #[must_use]
    pub fn insight_trigger(&mut self) -> Event {
        let mut e = Event::cb_trigger(self.ingest_ns);
        swap(&mut e.op_meta, &mut self.op_meta);
        swap(&mut e.origin_uri, &mut self.origin_uri);
        e
    }

    /// allows to iterate over the values and metadatas
    /// in an event, if it is batched this can be multiple
    /// otherwise it's a singular event
    #[must_use]
    pub fn value_meta_iter(&self) -> ValueMetaIter {
        ValueMetaIter {
            event: self,
            idx: 0,
        }
    }
    /// Creates a new event to restore a CB
    #[must_use]
    pub fn cb_restore(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CbAction::Open,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_trigger(ingest_ns: u64) -> Self {
        Event {
            ingest_ns,
            cb: CbAction::Close,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_ack(ingest_ns: u64, id: EventId) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CbAction::Ack,
            ..Event::default()
        }
    }

    /// Creates a new event to trigger a CB
    #[must_use]
    pub fn cb_fail(ingest_ns: u64, id: EventId) -> Self {
        Event {
            ingest_ns,
            id,
            cb: CbAction::Fail,
            ..Event::default()
        }
    }

    /// Creates a CB fail insight from the given `event` (the cause of this fail)
    #[must_use]
    pub fn to_fail(&self) -> Self {
        Event {
            id: self.id.clone(),
            ingest_ns: self.ingest_ns,
            op_meta: self.op_meta.clone(),
            origin_uri: self.origin_uri.clone(),
            cb: CbAction::Fail,
            ..Event::default()
        }
    }

    /// Create a CB ack insight from the given `event` (the cause of this ack)
    #[must_use]
    pub fn to_ack(&self) -> Self {
        Event {
            id: self.id.clone(),
            ingest_ns: self.ingest_ns,
            op_meta: self.op_meta.clone(),
            origin_uri: self.origin_uri.clone(),
            cb: CbAction::Ack,
            ..Event::default()
        }
    }

    #[must_use]
    /// return the number of events contained within this event
    /// normally 1, but for batched events possibly > 1
    pub fn len(&self) -> usize {
        if self.is_batch {
            self.data.suffix().value().as_array().map_or(0, Vec::len)
        } else {
            1
        }
    }

    /// returns true if this event is batched but has no wrapped events
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.is_batch
            && self
                .data
                .suffix()
                .value()
                .as_array()
                .map_or(true, Vec::is_empty)
    }

    /// Extracts the `$correlation` metadata into a `Vec` of `Option<Value<'static>>`.
    /// We use a `Vec` to account for possibly batched events and `Option`s because single events might not have a value there.
    /// We use `Value<'static>`, which requires a clone, as we need to pass the values on to another event anyways.
    #[must_use]
    pub fn correlation_metas(&self) -> Vec<Option<Value<'static>>> {
        let mut res = Vec::with_capacity(self.len());
        for (_, meta) in self.value_meta_iter() {
            res.push(meta.get("correlation").map(Value::clone_static))
        }
        res
    }

    /// get the correlation metadata as a single value, if present
    /// creates an array value for batched events
    #[must_use]
    pub fn correlation_meta(&self) -> Option<Value<'static>> {
        if self.is_batch {
            let cms = self.correlation_metas();
            if cms.is_empty() {
                None
            } else {
                Some(Value::from(cms))
            }
        } else {
            self.data
                .suffix()
                .meta()
                .get("correlation")
                .map(Value::clone_static)
        }
    }
}

/// Iterator over the event value and metadata
/// if the event is a batch this will allow iterating
/// over all the batched events
pub struct ValueMetaIter<'value> {
    event: &'value Event,
    idx: usize,
}

// TODO: descend recursively into batched events in batched events ...
impl<'value> Iterator for ValueMetaIter<'value> {
    type Item = (&'value Value<'value>, &'value Value<'value>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .get_idx(self.idx)
                .and_then(|e| {
                    let data = e.get("data")?;
                    Some((data.get("value")?, data.get("meta")?))
                });
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = self.event.data.suffix();
            self.idx += 1;
            Some((&v.value(), &v.meta()))
        } else {
            None
        }
    }
}

impl Event {
    /// Iterate over the values in an event
    /// this will result in multiple entries
    /// if the event was batched otherwise
    /// have only a single element
    #[must_use]
    pub fn value_iter(&self) -> ValueIter {
        ValueIter {
            event: self,
            idx: 0,
        }
    }
}
/// Iterator over the values of an event
pub struct ValueIter<'value> {
    event: &'value Event,
    idx: usize,
}

impl<'value> Iterator for ValueIter<'value> {
    type Item = &'value Value<'value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.event.is_batch {
            let r = self
                .event
                .data
                .suffix()
                .value()
                .get_idx(self.idx)
                .and_then(|e| e.get("data")?.get("value"));
            self.idx += 1;
            r
        } else if self.idx == 0 {
            let v = &self.event.data.suffix().value();
            self.idx += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Result;
    use simd_json::OwnedValue;
    use tremor_script::{Object, ValueAndMeta};

    fn merge(this: &mut ValueAndMeta<'static>, other: ValueAndMeta<'static>) -> Result<()> {
        if let Some(ref mut a) = this.value_mut().as_array_mut() {
            let mut e = Object::with_capacity(7);
            // {"id":1,
            // e.insert_nocheck("id".into(), id.into());
            //  "data": {
            //      "value": "snot", "meta":{}
            //  },
            let mut data = Object::with_capacity(2);
            let (value, meta) = other.into_parts();
            data.insert_nocheck("value".into(), value);
            data.insert_nocheck("meta".into(), meta);
            e.insert_nocheck("data".into(), Value::from(data));
            //  "ingest_ns":1,
            e.insert_nocheck("ingest_ns".into(), 1.into());
            //  "kind":null,
            // kind is always null on events
            e.insert_nocheck("kind".into(), Value::null());
            //  "is_batch":false
            e.insert_nocheck("is_batch".into(), false.into());
            // }
            a.push(Value::from(e))
        };
        Ok(())
    }
    #[test]
    fn value_iters() {
        let mut b = Event {
            data: (Value::array(), 2).into(),
            is_batch: true,
            ..Event::default()
        };
        let e1 = Event {
            data: (1, 2).into(),
            ..Event::default()
        };
        let e2 = Event {
            data: (3, 4).into(),
            ..Event::default()
        };

        assert!(b.data.consume(e1.data, merge).is_ok());
        assert!(b.data.consume(e2.data, merge).is_ok());

        let mut vi = b.value_iter();
        assert_eq!(vi.next().unwrap(), &1);
        assert_eq!(vi.next().unwrap(), &3);
        assert!(vi.next().is_none());
        let mut vmi = b.value_meta_iter();
        assert_eq!(vmi.next().unwrap(), (&1.into(), &2.into()));
        assert_eq!(vmi.next().unwrap(), (&3.into(), &4.into()));
        assert!(vmi.next().is_none());
    }

    #[test]
    fn cb() {
        let mut e = Event::default();
        assert_eq!(e.clone().insight(true).cb, CbAction::Ack);
        assert_eq!(e.clone().insight(false).cb, CbAction::Fail);

        assert_eq!(
            Event::ack_or_fail(true, 0, EventId::default()).cb,
            CbAction::Ack
        );
        assert_eq!(Event::cb_ack(0, EventId::default()).cb, CbAction::Ack);
        assert_eq!(e.insight_ack().cb, CbAction::Ack);

        assert_eq!(
            Event::ack_or_fail(false, 0, EventId::default()).cb,
            CbAction::Fail
        );
        assert_eq!(Event::cb_fail(0, EventId::default()).cb, CbAction::Fail);
        assert_eq!(e.insight_fail().cb, CbAction::Fail);

        let mut clone = e.clone();
        clone.op_meta.insert(1, OwnedValue::null());
        let ack_with_timing = clone.insight_ack_with_timing(100);
        assert_eq!(ack_with_timing.cb, CbAction::Ack);
        assert!(ack_with_timing.op_meta.contains_key(1));
        let (_, m) = ack_with_timing.data.parts();
        assert_eq!(Some(100), m.get_u64("time"));

        let mut clone2 = e.clone();
        clone2.op_meta.insert(42, OwnedValue::null());
        let clone_fail = clone2.to_fail();
        assert_eq!(clone_fail.cb, CbAction::Fail);
        assert!(clone_fail.op_meta.contains_key(42));
    }

    #[test]
    fn gd() {
        let mut e = Event::default();

        assert_eq!(Event::restore_or_break(true, 0).cb, CbAction::Open);
        assert_eq!(Event::cb_restore(0).cb, CbAction::Open);
        assert_eq!(e.insight_restore().cb, CbAction::Open);

        assert_eq!(Event::restore_or_break(false, 0).cb, CbAction::Close);
        assert_eq!(Event::cb_trigger(0).cb, CbAction::Close);
        assert_eq!(e.insight_trigger().cb, CbAction::Close);
    }

    #[test]
    fn len() -> Result<()> {
        // default non-batched event
        let mut e = Event::default();
        assert_eq!(1, e.len());
        // batched event with 2 elements
        e.is_batch = true;
        let mut value = Value::array_with_capacity(2);
        value.push(Value::from(true))?; // dummy events
        value.push(Value::from(false))?;
        e.data = (value, Value::object_with_capacity(0)).into();
        assert_eq!(2, e.len());

        // batched event with non-array value
        e.data = (Value::null(), Value::object_with_capacity(0)).into();
        assert_eq!(0, e.len());
        // batched array with empty array value
        e.data = (
            Value::array_with_capacity(0),
            Value::object_with_capacity(0),
        )
            .into();
        assert_eq!(0, e.len());

        Ok(())
    }

    #[test]
    fn is_empty() -> Result<()> {
        let mut e = Event::default();
        assert_eq!(false, e.is_empty());

        e.is_batch = true;
        e.data = (Value::null(), Value::object()).into();
        assert_eq!(true, e.is_empty());

        e.data = (Value::array(), Value::object()).into();
        assert_eq!(true, e.is_empty());

        let mut value = Value::array_with_capacity(2);
        value.push(Value::from(true))?; // dummy events
        value.push(Value::from(false))?;
        e.data = (value, Value::object()).into();
        assert_eq!(false, e.is_empty());
        Ok(())
    }
    #[test]
    fn correlation_meta() -> Result<()> {
        let mut e = Event::default();
        assert!(e.correlation_meta().is_none());
        let mut m = literal!({
            "correlation": 1
        });
        e.data = (Value::null(), m.clone()).into();

        assert_eq!(e.correlation_meta().unwrap(), 1);
        let mut e2 = Event::default();
        e2.is_batch = true;
        e2.data = (Value::array(), m.clone()).into();
        e2.data.consume(e.data.clone(), merge).unwrap();
        m.try_insert("correlation", 2);
        e.data = (Value::null(), m.clone()).into();
        e2.data.consume(e.clone().data, merge).unwrap();
        assert_eq!(e2.correlation_meta().unwrap(), Value::from(vec![1, 2]));

        Ok(())
    }
}
