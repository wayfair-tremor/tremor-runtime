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

use crate::{op::prelude::*, EventId, DEFAULT_STREAM_ID};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use simd_json_derive::{Deserialize, Serialize};
use sled::IVec;
use std::io::Cursor;
use std::mem;
use std::ops::{Add, AddAssign};
use tremor_script::prelude::*;

#[derive(Clone, Copy, Default, PartialEq)]
struct Idx([u8; 8]);
#[cfg(not(tarpaulin_include))]
impl std::fmt::Debug for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Idx({})", u64::from(self))
    }
}

#[cfg(not(tarpaulin_include))]
impl std::fmt::Display for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u64::from(self))
    }
}

impl AddAssign<u64> for Idx {
    fn add_assign(&mut self, other: u64) {
        let this: u64 = u64::from(&*self);
        let res = this + other;
        // ALLOW: We know that self.0 has exactly 8 elements
        (&mut self.0[..])
            .write_u64::<BigEndian>(res)
            .unwrap_or_default();
    }
}

impl Add<u64> for Idx {
    type Output = Idx;
    fn add(self, rhs: u64) -> Self::Output {
        Idx::from(u64::from(&self) + rhs)
    }
}

impl Add<usize> for Idx {
    type Output = Idx;
    fn add(self, rhs: usize) -> Self::Output {
        Idx::from(u64::from(&self) + rhs as u64)
    }
}

impl From<&Idx> for u64 {
    fn from(i: &Idx) -> u64 {
        let mut rdr = Cursor::new(&i.0);
        rdr.read_u64::<BigEndian>().unwrap_or(0)
    }
}

impl From<IVec> for Idx {
    fn from(v: IVec) -> Self {
        let mut rdr = Cursor::new(v);
        let res: u64 = rdr.read_u64::<BigEndian>().unwrap_or(0);
        Self::from(res)
    }
}

impl From<u64> for Idx {
    fn from(v: u64) -> Self {
        debug_assert_eq!(mem::size_of::<Self>(), mem::size_of::<u64>());
        // ALLOW: we transmute this into the respectiove newtime
        unsafe { mem::transmute(v.to_be()) }
    }
}

impl Idx {
    fn set(&mut self, v: u64) {
        // ALLOW: We know that self.0 has exactly 8 elements
        (&mut self.0[..])
            .write_u64::<BigEndian>(v)
            .unwrap_or_default();
    }
    fn set_min(&mut self, v: u64) {
        if v < u64::from(&*self) {
            self.set(v)
        }
    }
}
impl AsRef<[u8]> for Idx {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&Idx> for IVec {
    fn from(i: &Idx) -> Self {
        IVec::from(&i.0)
    }
}

#[derive(Debug, Clone, Deserialize, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// Maximum number of events to read per tick/event when filling
    /// up from the persistant storage
    pub read_count: usize,

    /// The directory to store data in, if no dir is provided this will use
    /// a temporary storage that won't persist over restarts
    pub dir: Option<String>,

    /// The maximum elements to store before breaking the circuit
    /// note this is an approximation we might store a few elements
    /// above that to allow circuit breakers to kick in
    pub max_elements: Option<u64>,

    /// Maximum number of bytes the WAL is alloed to take on disk,
    /// note this is a soft maximum and might be overshot slighty
    pub max_bytes: Option<u64>,

    /// Flush to disk on every write
    flush_on_evnt: Option<bool>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
// TODO add seed value and field name as config items
/// A Write Ahead Log that will persist data to disk and feed the following operators from this disk
/// cache. It allows to run onramps that do not provide any support for delivery guarantees with
/// offramps that do.
///
/// The wal operator will intercept and generate it's own circuit breaker events. You can think about it
/// as a firewall that will protect all operators before itself from issues beyond it. On the other hand
/// it will indiscriminately consume data from sources and operators before itself until it's own
/// circuit breaking conditions are met.
///
/// At the same time will it interact with tremors guaranteed delivery system, events are only removed
/// from disk once they're acknowledged. In case of delivery failure the WAL operator will replay the
/// failed events. On the same way the WAL operator will acknowledge events that it persists to disk.
///
/// The WAL operator should be used with caution, since every event that passes through it will be
/// written to the hard drive it has a significant performance impact.
pub struct Wal {
    /// Elements currently in the event storage
    cnt: u64,
    /// general DB
    wal: sled::Db,
    /// event storage
    events_tree: sled::Tree,
    /// state storage (written, etc)
    state_tree: sled::Tree,
    /// Next read index
    read: Idx,
    /// The last
    confirmed: Option<Idx>,
    /// The configuration
    config: Config,
    /// Are we currently in a broken CB state
    broken: bool,
    /// Did we signal because we're full
    full: bool,
    /// ID of this operator
    origin_uri: Option<EventOriginUri>,
}

op!(WalFactory(_uid, node) {
    let map = node.config.as_ref().ok_or_else(|| ErrorKind::MissingOpConfig(node.id.to_string()))?;
    let config: Config = Config::new(&map)?;

    if config.max_elements.or(config.max_bytes).is_none() {
        Err(ErrorKind::BadOpConfig("WAL operator needs at least one of `max_elements` or `max_bytes` config entries.".to_string()).into())
    } else {
        Ok(Box::new(Wal::new(node.id.to_string(), config)?))
    }
});

impl Wal {
    const URI_SCHEME: &'static str = "tremor-wal";
    // tree names
    const EVENTS: &'static str = "events";
    const STATE: &'static str = "state";
    // keys in the state tree
    const CONFIRMED: &'static str = "confirmed";

    fn new(id: String, config: Config) -> Result<Self> {
        let wal = if let Some(dir) = &config.dir {
            sled::open(&dir)?
        } else {
            sled::Config::default().temporary(true).open()?
        };
        let events_tree = wal.open_tree(Self::EVENTS)?;
        let state_tree = wal.open_tree(Self::STATE)?;

        #[allow(clippy::cast_possible_truncation)]
        let confirmed = state_tree.get(Self::CONFIRMED)?.map(Idx::from);
        let read = confirmed.unwrap_or_default();
        Ok(Wal {
            cnt: events_tree.len() as u64,
            wal,
            read,
            confirmed,
            events_tree,
            state_tree,
            config,
            broken: true,
            full: false,
            origin_uri: Some(EventOriginUri {
                uid: 0,
                scheme: Self::URI_SCHEME.to_string(),
                host: "pipeline".to_string(),
                port: None,
                path: vec![id],
            }),
        })
    }
    fn limit_reached(&self) -> Result<bool> {
        let max_bytes = self.config.max_bytes.unwrap_or(u64::MAX);
        let exceeds_max_bytes = self.wal.size_on_disk()? >= max_bytes;
        Ok(self.config.max_elements.map_or(false, |me| self.cnt >= me) || exceeds_max_bytes)
    }

    fn read_events(&mut self, _now: u64) -> Result<Vec<(Cow<'static, str>, Event)>> {
        // The maximum number of entries we read
        let mut events = Vec::with_capacity(self.config.read_count as usize);

        for (num_read, e) in self.events_tree.range(self.read..).enumerate() {
            if num_read > self.config.read_count {
                break;
            }
            let (idx, mut e) = e?;
            self.read = idx.into();
            self.read += 1;
            let e_slice: &mut [u8] = &mut e;
            let mut event = Event::from_slice(e_slice)?;
            event.transactional = true;
            events.push((OUT, event))
        }
        self.gc()?;
        Ok(events)
    }

    fn store_event(&mut self, source_id: u64, mut event: Event) -> Result<()> {
        let wal_id = self.wal.generate_id()?;

        let mut write: [u8; 8] = [0_u8; 8];
        debug_assert_eq!(write.len(), mem::size_of::<u64>());
        // ALLOW: we know write as exactly 8 elements
        (&mut write[..]).write_u64::<BigEndian>(wal_id)?;

        // TODO: figure out if handling of separate streams makes sense here
        let mut new_event_id = EventId::new(source_id, DEFAULT_STREAM_ID, wal_id);
        new_event_id.track(&event.id);
        event.id = new_event_id;

        // Serialize and write the event
        let event_buf = event.json_vec()?;
        self.events_tree.insert(write, event_buf.as_slice())?;
        if self.config.flush_on_evnt.unwrap_or_default() {
            self.events_tree.flush()?;
        }
        self.cnt += 1;
        Ok(())
    }

    fn gc(&mut self) -> Result<u64> {
        let mut i = 0;
        if let Some(confirmed) = self.confirmed {
            for e in self.events_tree.range(..=confirmed) {
                i += 1;
                self.cnt -= 1;
                let (idx, _) = e?;
                self.events_tree.remove(idx)?;
            }
        }
        Ok(i)
    }
}

fn maybe_parse_ivec(e: Option<IVec>) -> Option<Event> {
    let e_slice: &mut [u8] = &mut e?;
    Event::from_slice(e_slice).ok()
}

impl Operator for Wal {
    #[cfg(not(tarpaulin_include))]
    fn handles_contraflow(&self) -> bool {
        true
    }

    #[allow(clippy::option_if_let_else)] // borrow checker
    fn on_contraflow(&mut self, u_id: u64, insight: &mut Event) {
        match insight.cb {
            CbAction::None => {}
            CbAction::Open => {
                debug!("WAL CB open.");
                self.broken = false
            }
            CbAction::Close => {
                debug!("WAL CB close");
                self.broken = true
            }
            CbAction::Ack => {
                let event_id =
                    if let Some((_stream_id, event_id)) = insight.id.get_max_by_source(u_id) {
                        event_id
                    } else {
                        // This is not for us
                        return;
                    };

                trace!("WAL confirm: {}", event_id);

                let confirmed = self.confirmed.get_or_insert_with(|| Idx::from(event_id));
                confirmed.set(event_id);
                if let Err(e) = self.state_tree.insert(Self::CONFIRMED, &*confirmed) {
                    error!("Failed to persist confirm state: {}", e);
                }
                if let Some(e) = self
                    .events_tree
                    .get(confirmed)
                    .ok()
                    .and_then(maybe_parse_ivec)
                {
                    insight.id.track(&e.id);
                }
            }
            CbAction::Fail => {
                let event_id =
                    if let Some((_stream_id, event_id)) = insight.id.get_min_by_source(u_id) {
                        event_id
                    } else {
                        // This is not for us
                        return;
                    };
                trace!("WAL fail: {}", event_id);
                self.read.set_min(event_id);

                if let Some(e) = self.confirmed.and_then(|confirmed| {
                    self.events_tree
                        .get(confirmed)
                        .ok()
                        .and_then(maybe_parse_ivec)
                }) {
                    insight.id.track(&e.id);
                }

                let current_confirmed = self.confirmed.map(|v| u64::from(&v)).unwrap_or_default();
                if event_id < current_confirmed {
                    warn!(
                        "trying to fail a message({}) that was already confirmed({})",
                        event_id, current_confirmed
                    );
                    // resetting confirmed to older event_id
                    let confirmed = Idx::from(event_id);
                    self.confirmed = Some(confirmed);
                    if let Err(e) = self.state_tree.insert(Self::CONFIRMED, &confirmed) {
                        error!("Failed to persist confirm state: {}", e);
                    }
                }
            }
        }
        insight.cb = CbAction::None;
    }

    #[cfg(not(tarpaulin_include))]
    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(
        &mut self,
        _uid: u64,
        _state: &Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        let now = signal.ingest_ns;
        // Are we currently full?
        let now_full = self.limit_reached()?;
        // If we just became full or we went from full to non full
        // update the CB status
        let insights = if self.full && !now_full {
            warn!("WAL not full any more. {} elements.", self.cnt);
            let mut e = Event::cb_restore(signal.ingest_ns);
            e.origin_uri = self.origin_uri.clone();
            vec![e]
        } else if !self.full && now_full {
            warn!("WAL full. {} elements.", self.cnt);
            let mut e = Event::cb_trigger(signal.ingest_ns);
            e.origin_uri = self.origin_uri.clone();
            vec![e]
        } else {
            vec![]
        };
        self.full = now_full;
        let events = if self.broken {
            vec![]
        } else {
            self.read_events(now)?
        };
        Ok(EventAndInsights { events, insights })
    }

    fn on_event(
        &mut self,
        uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        let id = event.id.clone();
        let ingest_ns = event.ingest_ns;
        let transactional = event.transactional;
        let op_meta = if transactional {
            Some(event.op_meta.clone())
        } else {
            None
        };
        self.store_event(uid, event)?;

        let insights = if let Some(op_meta) = op_meta {
            let mut insight = Event::cb_ack(ingest_ns, id);
            insight.op_meta = op_meta;
            vec![insight]
        } else {
            vec![]
        };
        let events = if self.broken {
            Vec::new()
        } else {
            self.read_events(ingest_ns)?
        };
        Ok(EventAndInsights { events, insights })
    }
}

#[cfg(test)]
mod test {
    use crate::EventIdGenerator;
    use crate::SignalKind;

    use super::*;
    use tempfile::Builder as TempDirBuilder;

    #[test]
    fn test_gc() -> Result<()> {
        let c = Config {
            read_count: 1,
            dir: None,
            max_elements: Some(10),
            max_bytes: None,
            flush_on_evnt: None,
        };
        let mut o = Wal::new("test".to_string(), c)?;
        let wal_uid = 0_u64;
        let source_uid = 42_u64;
        let mut idgen = EventIdGenerator::new(source_uid);

        // we start in a broken state
        let id = idgen.next_id();
        let mut e = Event::default();
        let mut state = Value::null();

        //send first event
        e.id = id.clone();
        e.transactional = true;
        let mut op_meta = OpMeta::default();
        op_meta.insert(42, OwnedValue::null());
        e.op_meta = op_meta;
        let mut r = o.on_event(wal_uid, "in", &mut state, e.clone())?;
        assert_eq!(0, r.events.len());
        assert_eq!(1, r.insights.len());
        let insight = &r.insights[0];
        assert_eq!(CbAction::Ack, insight.cb);

        assert_eq!(1, o.cnt);
        assert_eq!(None, o.confirmed);

        // Restore the CB
        let mut i = Event::cb_restore(0);
        o.on_contraflow(wal_uid, &mut i);

        // we have 1 event stored
        assert_eq!(1, o.cnt);
        assert_eq!(None, o.confirmed);

        // send second event, expect both events back
        let id2 = idgen.next_id();
        e.id = id2.clone();
        e.transactional = true;
        let mut op_meta = OpMeta::default();
        op_meta.insert(42, OwnedValue::null());
        e.op_meta = op_meta;
        r = o.on_event(wal_uid, "in", &mut state, e.clone())?;
        assert_eq!(2, r.events.len());
        assert!(
            r.events[0].1.id.is_tracking(&id),
            "not tracking the origin event"
        );
        assert!(
            r.events[1].1.id.is_tracking(&id2),
            "not tracking the origin event"
        );
        assert_eq!(1, r.insights.len());
        let insight = &r.insights[0];
        assert_eq!(CbAction::Ack, insight.cb);

        // we have two events stored
        assert_eq!(2, o.cnt);
        assert_eq!(None, o.confirmed);

        // acknowledge the first event
        i = Event::cb_ack(e.ingest_ns, r.events[0].1.id.clone());
        o.on_contraflow(wal_uid, &mut i);

        // we still have two events stored
        assert_eq!(2, o.cnt);
        assert_eq!(Some(Idx::from(id.event_id())), o.confirmed);

        // apply gc on signal
        let mut signal = Event {
            id: idgen.next_id(),
            ingest_ns: 1,
            kind: Some(SignalKind::Tick),
            ..Event::default()
        };
        let s = o.on_signal(wal_uid, &state, &mut signal)?;
        assert_eq!(0, s.events.len());
        assert_eq!(0, s.insights.len());

        // now we have 1 left
        assert_eq!(1, o.cnt);
        assert_eq!(Some(Idx::from(id.event_id())), o.confirmed);

        // acknowledge the second event
        i = Event::cb_ack(e.ingest_ns, r.events[1].1.id.clone());
        o.on_contraflow(wal_uid, &mut i);

        // still 1 left
        assert_eq!(1, o.cnt);
        assert_eq!(Some(Idx::from(id2.event_id())), o.confirmed);

        // apply gc on signal
        let mut signal2 = Event {
            id: idgen.next_id(),
            ingest_ns: 1,
            kind: Some(SignalKind::Tick),
            ..Event::default()
        };
        let s = o.on_signal(wal_uid, &state, &mut signal2)?;
        assert_eq!(0, s.events.len());
        assert_eq!(0, s.insights.len());

        // we are clean now
        assert_eq!(0, o.cnt);
        assert_eq!(Some(Idx::from(id2.event_id())), o.confirmed);
        Ok(())
    }

    #[test]
    fn rw() -> Result<()> {
        let c = Config {
            read_count: 100,
            dir: None,
            max_elements: None,
            max_bytes: Some(1024 * 1024),
            flush_on_evnt: None,
        };
        let mut o = Wal::new("test".to_string(), c)?;
        let wal_uid = 0_u64;
        let source_uid = 42_u64;
        let mut idgen = EventIdGenerator::new(source_uid);

        let mut v = Value::null();
        let mut e = Event::default();
        e.id = idgen.next_id();
        // The operator start in broken status

        // Send a first event
        let r = o.on_event(wal_uid, "in", &mut v, e.clone())?;
        // Since we are broken we should get nothing back
        assert_eq!(r.len(), 0);
        assert_eq!(r.insights.len(), 0);

        // Restore the CB
        let mut i = Event::cb_restore(0);
        o.on_contraflow(0, &mut i);

        // Send a second event
        e.id = idgen.next_id();
        e.transactional = true;
        let mut op_meta = OpMeta::default();
        op_meta.insert(42, OwnedValue::null());
        e.op_meta = op_meta;
        let mut r = o.on_event(wal_uid, "in", &mut v, e.clone())?;
        // Since we are restored we now get 2 events (1 and 2)
        assert_eq!(r.len(), 2);
        assert_eq!(r.insights.len(), 1); // we get an ack back
        let insight = r.insights.pop().expect("no insight");
        assert_eq!(insight.cb, CbAction::Ack);
        assert!(insight.op_meta.contains_key(42));
        assert!(!insight.op_meta.contains_key(wal_uid));

        // extract the ids assigned by the WAL and tracked in the event ids
        let id_e1 = r.events.first().map(|(_, event)| &event.id).unwrap();
        let id_e2 = r.events.get(1).map(|(_, event)| &event.id).unwrap();

        // Send a fail event beck to the source through the WAL, this tell the WAL that delivery of
        // 2 failed and they need to be delivered again
        let mut i = Event::default();
        i.id = id_e2.clone();
        i.cb = CbAction::Fail;
        o.on_contraflow(0, &mut i);

        // Send a third event
        e.id = idgen.next_id();
        e.transactional = false;
        let r = o.on_event(0, "in", &mut v, e.clone())?;
        // since we failed before we should see 2 events, 3 and the retransmit
        // of 2
        assert_eq!(r.len(), 2);

        // Send a fail event back to the source for the first event, this will tell the WAL that delivery of
        // 1, 2, 3 failed and they need to be delivered again
        let mut i = Event::default();
        i.id = id_e1.clone();

        i.cb = CbAction::Fail;
        o.on_contraflow(0, &mut i);

        // since we failed before we should see 3 events the retransmit of 1-3
        let r = o.on_signal(0, &v, &mut i)?;
        assert_eq!(r.len(), 3);

        o.gc()?;

        Ok(())
    }

    #[test]
    // tests that the wal works fine
    // after a restart of the tremor server
    fn restart_wal_regression() -> Result<()> {
        let temp_dir = TempDirBuilder::new()
            .prefix("tremor-pipeline-wal")
            .tempdir()?;
        let read_count = 100;
        let c = Config {
            read_count,
            dir: Some(temp_dir.path().to_string_lossy().into_owned()),
            max_elements: Some(10),
            max_bytes: Some(1024 * 1024),
            flush_on_evnt: None,
        };

        let mut v = Value::null();
        let e = Event::default();
        let wal_uid = 0;

        {
            // create the operator - first time

            let mut o1 = WalFactory::new()
                .from_node(wal_uid, &NodeConfig::from_config(&"wal-test-1", c.clone())?)?;

            // Restore the CB
            let mut i = Event::cb_restore(0);
            o1.on_contraflow(wal_uid, &mut i);

            // send a first event - not acked. so it lingers around in our WAL
            let r = o1.on_event(1, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 1);
            assert_eq!(r.insights.len(), 0);
        }

        {
            // create the operator - second time
            // simulating a tremor restart
            let mut o2 = WalFactory::new()
                .from_node(wal_uid, &NodeConfig::from_config(&"wal-test-2", c)?)?;

            // Restore the CB
            let mut i = Event::cb_restore(1);
            o2.on_contraflow(wal_uid, &mut i);

            // send a first event - not acked. so it lingers around in our WAL
            let r = o2.on_event(wal_uid, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 2);
            let id1 = &r.events[0].1.id;
            let id2 = &r.events[1].1.id;
            assert_eq!(id1.get_max_by_stream(wal_uid, 0).unwrap(), 0);
            // ensure we actually had a gap bigger than read count, which triggers the error condition
            assert!(
                id2.get_max_by_stream(wal_uid, 0).unwrap()
                    - id1.get_max_by_stream(wal_uid, 0).unwrap()
                    > read_count as u64
            );
            assert_eq!(r.insights.len(), 0);

            let r = o2.on_event(wal_uid, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 1);
        }

        Ok(())
    }

    #[test]
    fn test_invalid_config() -> Result<()> {
        let c = Config {
            read_count: 10,
            dir: None,
            max_elements: None,
            max_bytes: None,
            flush_on_evnt: None,
        };

        let r = WalFactory::new().from_node(1, &NodeConfig::from_config(&"wal-test-1", c.clone())?);
        assert!(r.is_err());
        if let Err(Error(ErrorKind::BadOpConfig(s), _)) = r {
            assert_eq!(
                "WAL operator needs at least one of `max_elements` or `max_bytes` config entries.",
                s.as_str()
            );
        }
        Ok(())
    }

    #[test]
    fn from() -> Result<()> {
        assert_eq!(42, u64::from(&(Idx::from(40u64) + 2u64)));
        assert_eq!(42, u64::from(&(Idx::from(40u64) + 2usize)));

        Ok(())
    }

    #[test]
    fn as_ref() -> Result<()> {
        let i = Idx::from(42u64);
        let s: &[u8] = i.as_ref();
        assert_eq!(&[0, 0, 0, 0, 0, 0, 0, 42u8][..], s);
        Ok(())
    }
}
