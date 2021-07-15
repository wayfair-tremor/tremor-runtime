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

use crate::source::prelude::*;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::clone::Clone;
use std::cmp::Reverse;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;
use tremor_common::time::nanotime;
use tremor_script::prelude::*;

#[derive(Deserialize, Clone)]
pub struct CronEntry {
    pub name: String,
    pub expr: String,
    pub payload: Option<YamlValue>,
}

#[derive(Clone)]
pub struct CronEntryInt {
    pub name: String,
    pub expr: String,
    pub sched: Schedule,
    pub payload: Option<Value<'static>>,
}

impl TryFrom<CronEntry> for CronEntryInt {
    type Error = crate::errors::Error;
    fn try_from(entry: CronEntry) -> Result<Self> {
        let payload = if let Some(yaml_payload) = entry.payload {
            // We use this to translate a yaml value (payload in) to tremor value (payload out)
            let mut payload = simd_json::to_vec(&yaml_payload)?;
            let tremor_payload = tremor_value::parse_to_value(&mut payload)?;
            Some(tremor_payload.into_static())
        } else {
            None
        };
        Ok(Self {
            sched: Schedule::from_str(entry.expr.as_str())?,
            name: entry.name,
            expr: entry.expr,
            payload,
        })
    }
}
impl std::fmt::Debug for CronEntryInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.name, self.expr)
    }
}

impl std::fmt::Debug for CronEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.name, self.expr)
    }
}

impl PartialEq for CronEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name) && self.expr.eq(&other.expr)
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub entries: Vec<CronEntry>,
}

impl ConfigImpl for Config {}

#[derive(Clone)]
pub struct Crononome {
    pub config: Config,
    origin_uri: EventOriginUri,
    cq: ChronomicQueue,
    onramp_id: TremorUrl,
}
impl std::fmt::Debug for Crononome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Crononome")
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-crononome".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            };

            Ok(Box::new(Crononome {
                origin_uri,
                config,
                onramp_id: id.clone(),
                cq: ChronomicQueue::default(),
            }))
        } else {
            Err("Missing config for crononome onramp".into())
        }
    }
}

#[derive(Debug, Clone)]
struct TemporalItem<I> {
    at: DateTime<Utc>,
    what: I,
}

impl<I> PartialEq for TemporalItem<I> {
    fn eq(&self, other: &Self) -> bool {
        self.at.eq(&other.at)
    }
}

impl<I> PartialOrd for TemporalItem<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.at.partial_cmp(&other.at)
    }
}

impl<I> Eq for TemporalItem<I> {
    fn assert_receiver_is_total_eq(&self) {
        self.at.assert_receiver_is_total_eq()
    }
}

impl<I> Ord for TemporalItem<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at)
    }
    fn max(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if self.at > other.at {
            self
        } else {
            other
        }
    }
    fn min(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if other.at > self.at {
            self
        } else {
            other
        }
    }
}

#[derive(Debug, Clone)]
struct TemporalPriorityQueue<I> {
    q: BinaryHeap<Reverse<TemporalItem<I>>>,
}

impl<I> TemporalPriorityQueue<I> {
    pub fn default() -> Self {
        Self {
            q: BinaryHeap::new(),
        }
    }

    pub fn enqueue(&mut self, at: TemporalItem<I>) {
        self.q.push(Reverse(at))
    }

    pub fn pop(&mut self) -> Option<TemporalItem<I>> {
        if let Some(Reverse(x)) = self.q.peek() {
            let now = Utc::now().timestamp();
            let event = x.at.timestamp();
            if event <= now {
                self.q.pop().map(|Reverse(x)| x)
            } else {
                None
            }
        } else {
            None
        }
    }
    #[cfg(test)]
    pub fn drain(&mut self) -> Vec<TemporalItem<I>> {
        let now = Utc::now().timestamp();
        let mut sched: Vec<TemporalItem<I>> = vec![];
        // for next in self.q.iter() {
        loop {
            let next = self.q.peek();
            //            let next = self.q.peek();
            match next {
                Some(Reverse(x)) => {
                    let event = x.at.timestamp();
                    if event <= now {
                        match self.q.pop() {
                            Some(Reverse(x)) => {
                                sched.push(x);
                            }
                            None => continue, // should never occur in practice
                        }
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        sched
    }
}

#[derive(Debug, Clone)]
struct ChronomicQueue {
    tpq: TemporalPriorityQueue<CronEntryInt>,
}

impl ChronomicQueue {
    pub fn default() -> Self {
        Self {
            tpq: TemporalPriorityQueue::default(),
        }
    }

    pub fn enqueue(&mut self, entry: &CronEntryInt) {
        if let Some(at) = entry.sched.upcoming(chrono::Utc).next() {
            self.tpq.enqueue(TemporalItem {
                at,
                what: entry.clone(),
            });
        }
    }
    #[cfg(test)]
    pub fn drain(&mut self) -> Vec<(String, Option<Value<'static>>)> {
        let due = self.tpq.drain();
        let mut trigger: Vec<(String, Option<Value<'static>>)> = vec![];
        for ti in &due {
            // Enqueue next scheduled event if any
            self.enqueue(&ti.what);
            trigger.push((ti.what.name.clone(), ti.what.payload.clone()));
        }
        trigger
    }
    pub fn next(&mut self) -> Option<(String, Option<Value<'static>>)> {
        self.tpq.pop().map(|ti| {
            self.enqueue(&ti.what);
            (ti.what.name, ti.what.payload)
        })
    }
}

#[async_trait::async_trait()]
impl Source for Crononome {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(trigger) = self.cq.next() {
            let mut origin_uri = self.origin_uri.clone();
            origin_uri.path.push(trigger.0.clone());

            let mut data: Value<'static> = Value::object_with_capacity(4);
            data.insert("onramp", "crononome")?;
            data.insert("ingest_ns", nanotime())?;
            data.insert("id", id)?;
            let mut tr: Value<'static> = Value::object_with_capacity(2);
            tr.insert("name", trigger.0)?;
            if let Some(payload) = trigger.1 {
                tr.insert("payload", payload)?;
            }
            data.insert("trigger", tr)?;
            Ok(SourceReply::Structured {
                origin_uri,
                data: data.into(),
            })
        } else {
            Ok(SourceReply::Empty(100))
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        for entry in &self.config.entries {
            match CronEntryInt::try_from(entry.clone()) {
                Ok(entry) => self.cq.enqueue(&entry),
                Err(e) => {
                    return Err(format!(
                        "Bad configuration in crononome - expression {} is illegal: {}",
                        entry.name, e
                    )
                    .into())
                }
            }
        }
        Ok(SourceState::Connected)
    }
}

#[async_trait::async_trait]
impl Onramp for Crononome {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        SourceManager::start(self.clone(), config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{self, DateTime};
    use std::convert::TryFrom;
    use std::time::Duration;

    #[test]
    pub fn test_deserialize_cron_entry() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"* 0 0 * * * *\"}".to_vec();
        let entry = CronEntryInt::try_from(simd_json::from_slice::<CronEntry>(s.as_mut_slice())?)?;
        assert_eq!("test", entry.name);
        assert_eq!("* 0 0 * * * *", entry.expr);
        Ok(())
    }

    #[test]
    pub fn test_deserialize_cron_entry_error() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"snot snot\"}".to_vec();
        let entry: CronEntry = simd_json::from_slice(s.as_mut_slice())?;
        assert_eq!("test", entry.name);
        assert!(CronEntryInt::try_from(entry).is_err());
        Ok(())
    }

    #[test]
    pub fn test_tpq_fill_drain() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntryInt::try_from(CronEntry {
                name: "a".to_string(),
                expr: "* * * * * * 1970".to_string(),
                payload: None,
            })?,
        };
        let n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntryInt::try_from(CronEntry {
                name: "b".to_string(),
                expr: "* * * * * * *".to_string(),
                payload: None,
            })?,
        };

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());
        let due = tpq.drain();
        assert_eq!(vec![n1.clone(), n2], due);

        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.drain().is_empty());
        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.drain().is_empty());

        Ok(())
    }

    #[test]
    pub fn test_tpq_fill_pop() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntryInt::try_from(CronEntry {
                name: "a".to_string(),
                expr: "* * * * * * 1970".to_string(),
                payload: None,
            })?,
        };
        let n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntryInt::try_from(CronEntry {
                name: "b".to_string(),
                expr: "* * * * * * *".to_string(),
                payload: None,
            })?,
        };

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());

        assert_eq!(Some(n1), tpq.pop());
        assert_eq!(Some(n2), tpq.pop());

        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.pop().is_none());
        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.pop().is_none());

        Ok(())
    }

    #[test]
    pub fn test_cq_fill_drain_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let n1 = CronEntryInt::try_from(CronEntry {
            name: "a".to_string(),
            expr: "* * * * * * 1970".to_string(), // Dates before UNIX epoch start invalid
            payload: None,
        })?;
        let n2 = CronEntryInt::try_from(CronEntry {
            name: "b".to_string(),
            expr: "* * * * * * *".to_string(),
            payload: None,
        })?;
        let n3 = CronEntryInt::try_from(CronEntry {
            name: "c".to_string(),
            expr: "* * * * * * 2038".to_string(), // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
            payload: None,
        })?;

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        let due = cq.drain();
        assert_eq!(0, due.len());

        std::thread::sleep(Duration::from_millis(1000));
        assert_eq!(1, cq.drain().len());

        std::thread::sleep(Duration::from_millis(1000));
        assert_eq!(1, cq.drain().len());

        Ok(())
    }

    #[test]
    pub fn test_cq_fill_pop_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let n1 = CronEntryInt::try_from(CronEntry {
            name: "a".to_string(),
            expr: "* * * * * * 1970".to_string(), // Dates before UNIX epoch start invalid
            payload: None,
        })?;
        let n2 = CronEntryInt::try_from(CronEntry {
            name: "b".to_string(),
            expr: "* * * * * * *".to_string(),
            payload: None,
        })?;
        let n3 = CronEntryInt::try_from(CronEntry {
            name: "c".to_string(),
            expr: "* * * * * * 2038".to_string(), // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
            payload: None,
        })?;

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        assert!(cq.next().is_none());

        std::thread::sleep(Duration::from_millis(1000));
        assert!(cq.next().is_some());
        assert!(cq.next().is_none());
        std::thread::sleep(Duration::from_millis(1000));
        assert!(cq.next().is_some());

        Ok(())
    }

    #[test]
    fn test_cron_rs() {
        // NOTE Handy to check cron.rs limitations & constraints
        use cron::{Schedule, TimeUnitSpec};
        use std::collections::Bound::{Excluded, Included};
        use std::str::FromStr;

        let expression = "* * * * * * 2015-2044";
        let schedule = Schedule::from_str(expression).expect("Failed to parse expression.");

        // Membership
        assert_eq!(true, schedule.years().includes(2031));
        assert_eq!(false, schedule.years().includes(1969));

        // Number of years specified
        assert_eq!(30, schedule.years().count());

        // Iterator
        let mut years_iter = schedule.years().iter();
        assert_eq!(Some(2015), years_iter.next());
        assert_eq!(Some(2016), years_iter.next());
        // ...

        // Range Iterator
        let mut five_year_plan = schedule.years().range((Included(2017), Excluded(2017 + 5)));
        assert_eq!(Some(2017), five_year_plan.next());
        assert_eq!(Some(2018), five_year_plan.next());
        assert_eq!(Some(2019), five_year_plan.next());
        assert_eq!(Some(2020), five_year_plan.next());
        assert_eq!(Some(2021), five_year_plan.next());
        assert_eq!(None, five_year_plan.next());
    }
}
