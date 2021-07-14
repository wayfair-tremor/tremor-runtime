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

//! Tremor event processing pipeline

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
// TODO this is needed due to a false positive in clippy
// https://github.com/rust-lang/rust/issues/83125
// we will need this in 1.53.1
#![allow(proc_macro_back_compat)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use crate::errors::{ErrorKind, Result};
use crate::op::prelude::*;
use beef::Cow;
use executable_graph::NodeConfig;
use halfbrown::HashMap;
use lazy_static::lazy_static;
use op::trickle::select::WindowImpl;
use petgraph::graph::{self, NodeIndex};
use simd_json::OwnedValue;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use std::str::FromStr;
use std::{fmt, sync::Mutex};
use tremor_script::prelude::*;

/// Pipeline Errors
pub mod errors;
mod event;
mod executable_graph;

#[macro_use]
mod macros;
pub(crate) mod op;

const COUNT: Cow<'static, str> = Cow::const_str("count");
const MEASUREMENT: Cow<'static, str> = Cow::const_str("measurement");
const TAGS: Cow<'static, str> = Cow::const_str("tags");
const FIELDS: Cow<'static, str> = Cow::const_str("fields");
const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");

/// Tools to turn tremor query into pipelines
pub mod query;
pub use crate::event::{Event, ValueIter, ValueMetaIter};
pub use crate::executable_graph::{ExecutableGraph, OperatorNode};
pub(crate) use crate::executable_graph::{NodeMetrics, State};
pub use op::{ConfigImpl, InitializableOperator, Operator};
pub use tremor_script::prelude::EventOriginUri;
pub(crate) type PortIndexMap =
    HashMap<(NodeIndex, Cow<'static, str>), Vec<(NodeIndex, Cow<'static, str>)>>;
pub(crate) type ExecPortIndexMap =
    HashMap<(usize, Cow<'static, str>), Vec<(usize, Cow<'static, str>)>>;

/// A configuration map
pub type ConfigMap = Option<serde_yaml::Value>;

/// A lookup function to used to look up operators
pub type NodeLookupFn = fn(
    config: &NodeConfig,
    uid: u64,
    defn: Option<&tremor_script::srs::Stmt>,
    node: Option<&tremor_script::srs::Stmt>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode>;

/// Stringified numeric key
/// from <https://github.com/serde-rs/json-benchmark/blob/master/src/prim_str.rs>
#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct PrimStr<T>(T)
where
    T: Copy + Ord + Display + FromStr;

impl<T> simd_json_derive::SerializeAsKey for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<T> simd_json_derive::Serialize for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    fn json_write<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        write!(writer, "\"{}\"", self.0)
    }
}

impl<'input, T> simd_json_derive::Deserialize<'input> for PrimStr<T>
where
    T: Copy + Ord + Display + FromStr,
{
    #[inline]
    fn from_tape(tape: &mut simd_json_derive::Tape<'input>) -> simd_json::Result<Self>
    where
        Self: std::marker::Sized + 'input,
    {
        if let Some(simd_json::Node::String(s)) = tape.next() {
            Ok(PrimStr(FromStr::from_str(s).map_err(|_e| {
                simd_json::Error::generic(simd_json::ErrorType::Serde("not a number".into()))
            })?))
        } else {
            Err(simd_json::Error::generic(
                simd_json::ErrorType::ExpectedNull,
            ))
        }
    }
}

/// Operator metadata
#[derive(
    Clone, Debug, Default, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct OpMeta(BTreeMap<PrimStr<u64>, OwnedValue>);

impl OpMeta {
    /// inserts a value
    pub fn insert<V>(&mut self, key: u64, value: V) -> Option<OwnedValue>
    where
        OwnedValue: From<V>,
    {
        self.0.insert(PrimStr(key), OwnedValue::from(value))
    }
    /// reads a value
    pub fn get(&mut self, key: u64) -> Option<&OwnedValue> {
        self.0.get(&PrimStr(key))
    }
    /// checks existance of a key
    #[must_use]
    pub fn contains_key(&self, key: u64) -> bool {
        self.0.contains_key(&PrimStr(key))
    }

    /// Merges two op meta maps, overwriting values with `other` on duplicates
    pub fn merge(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
    }
}

lazy_static! {
    /// Function registory for the pipeline to look up functions
    // We wrap the registry in a mutex so that we can add functions from the outside
    // if required.
    pub static ref FN_REGISTRY: Mutex<Registry> = {
        let registry: Registry = tremor_script::registry();
        Mutex::new(registry)
    };
}

pub(crate) fn common_cow(s: &str) -> beef::Cow<'static, str> {
    macro_rules! cows {
        ($target:expr, $($cow:expr),*) => {
            match $target {
                $($cow => $cow.into()),*,
                _ => beef::Cow::from($target.to_string()),
            }
        };
    }
    cows!(s, "in", "out", "err", "main")
}

/// Type of nodes
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum NodeKind {
    /// An input, this is the one end of the graph
    Input,
    /// An output, this is the other end of the graph
    Output(Cow<'static, str>),
    /// An operator
    Operator,
    /// A select statement
    Select,
    /// A Script statement
    Script,
}

impl NodeKind {
    fn skippable(&self) -> bool {
        matches!(self, Self::Operator | Self::Select | Self::Script)
    }
}

impl Default for NodeKind {
    fn default() -> Self {
        Self::Operator
    }
}

/// A circuit breaker action
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum CbAction {
    /// Nothing of note
    None,
    /// The circuit breaker is triggerd and should break
    Close,
    /// The circuit breaker is restored and should work again
    Open,
    /// Acknowledge delivery of messages up to a given ID.
    /// All messages prior to and including  this will be considered delivered.
    Ack,
    /// Fail backwards to a given ID
    /// All messages after and including this will be considered non delivered
    Fail,
}
impl Default for CbAction {
    fn default() -> Self {
        Self::None
    }
}

impl From<bool> for CbAction {
    fn from(success: bool) -> Self {
        if success {
            CbAction::Ack
        } else {
            CbAction::Fail
        }
    }
}

impl CbAction {
    /// This is a Circuit Breaker related message
    #[must_use]
    pub fn is_cb(self) -> bool {
        self == CbAction::Close || self == CbAction::Open
    }
    /// This is a Guaranteed Delivery related message
    #[must_use]
    pub fn is_gd(self) -> bool {
        self == CbAction::Ack || self == CbAction::Fail
    }
}

/// Event identifier
///
/// Events are identified by their source, the stream within that source that originated the given event
/// and an `event_id` that is unique only within the same stream.
///
/// `EventId` also tracks min and max event ids for other events in order to support batched and grouped events
/// and facilitate CB mechanics
#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub struct EventId {
    source_id: u64,
    stream_id: u64,
    event_id: u64,
    tracked_event_ids: Vec<TrackedEventIds>,
}

/// default stream id if streams dont make sense
pub const DEFAULT_STREAM_ID: u64 = 0;

impl EventId {
    #[must_use]
    /// create a new `EventId` from numeric ids
    pub fn new(source_id: u64, stream_id: u64, event_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            event_id,
            tracked_event_ids: Vec::with_capacity(0),
        }
    }

    #[must_use]
    /// return the `source_id` of this event
    /// the unique id of the source/onramp/pipeline-node where this event came from
    pub fn source_id(&self) -> u64 {
        self.source_id
    }

    /// setter for `source_id`
    pub fn set_source_id(&mut self, source_id: u64) {
        self.source_id = source_id;
    }

    #[must_use]
    /// return the `stream_id` of this event
    /// the unique id of the stream within a source/onramp/pipeline-node where this event came from
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// setter for `stream_id`
    pub fn set_stream_id(&mut self, stream_id: u64) {
        self.stream_id = stream_id;
    }

    #[must_use]
    /// return the `event_id` of this event
    /// the unique id of the event within its stream
    pub fn event_id(&self) -> u64 {
        self.event_id
    }

    /// setter for `event_id`
    pub fn set_event_id(&mut self, event_id: u64) {
        self.event_id = event_id;
    }

    /// track the min and max of the given `event_id`
    /// and also include all event ids `event_id` was tracking
    pub fn track(&mut self, event_id: &EventId) {
        self.track_ids(
            event_id.source_id,
            event_id.stream_id,
            event_id.event_id,
            event_id.event_id,
        );

        for other_tracked in &event_id.tracked_event_ids {
            match self
                .tracked_event_ids
                .binary_search_by(|probe| probe.compare(other_tracked))
            {
                Ok(idx) => {
                    // ALLOW: binary_search_by verified this idx exists
                    unsafe { self.tracked_event_ids.get_unchecked_mut(idx) }
                        .track_ids(other_tracked.min_event_id, other_tracked.max_event_id)
                }
                Err(idx) => self.tracked_event_ids.insert(idx, other_tracked.clone()),
            }
        }
    }

    /// track the given event id by its raw numeric ids
    pub fn track_id(&mut self, source_id: u64, stream_id: u64, event_id: u64) {
        self.track_ids(source_id, stream_id, event_id, event_id)
    }

    fn track_ids(&mut self, source_id: u64, stream_id: u64, min_event_id: u64, max_event_id: u64) {
        // track our own id upon first track call, so we can keep resolving min and max simpler
        if self.tracked_event_ids.is_empty() {
            self.tracked_event_ids.push(TrackedEventIds::new(
                self.source_id,
                self.stream_id,
                self.event_id,
                self.event_id,
            ));
        }
        match self
            .tracked_event_ids
            .binary_search_by(|probe| probe.compare_ids(source_id, stream_id))
        {
            Ok(idx) => {
                unsafe { self.tracked_event_ids.get_unchecked_mut(idx) }
                    .track_ids(min_event_id, max_event_id);
            }
            Err(idx) => self.tracked_event_ids.insert(
                idx,
                TrackedEventIds::new(source_id, stream_id, min_event_id, max_event_id),
            ),
        }
    }

    #[must_use]
    /// get minimum event id for a given source and stream, if it is tracked
    ///
    /// This also always checks the actual eventId, not only the tracked ones, this way we can save allocations when used within insights
    pub fn get_min_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_event_ids.is_empty()
            && self.source_id == source_id
            && self.stream_id == stream_id
        {
            Some(self.event_id)
        } else {
            self.tracked_event_ids.iter().find_map(|teid| {
                if (source_id, stream_id) == (teid.source_id, teid.stream_id) {
                    Some(teid.min_event_id)
                } else {
                    None
                }
            })
        }
    }

    #[must_use]
    /// checks if the given `EventId` is tracked by this one.
    /// Also returns true, if the `event_id` has the same id as `self`.
    pub fn is_tracking(&self, event_id: &EventId) -> bool {
        let is_same = self.source_id() == event_id.source_id()
            && self.stream_id() == event_id.stream_id()
            && self.event_id() == event_id.event_id();
        is_same
            || match self.tracked_event_ids.binary_search_by(|probe| {
                probe.compare_ids(event_id.source_id(), event_id.stream_id())
            }) {
                Ok(idx) => {
                    let entry = unsafe { self.tracked_event_ids.get_unchecked(idx) };
                    // this is only a heuristic, but is good enough for now
                    (entry.min_event_id <= event_id.event_id)
                        && (event_id.event_id <= entry.max_event_id)
                }
                Err(_) => false,
            }
    }

    #[must_use]
    /// get maximum event id for a given source and stream if we have it here
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_by_stream(&self, source_id: u64, stream_id: u64) -> Option<u64> {
        if self.tracked_event_ids.is_empty()
            && self.source_id == source_id
            && self.stream_id == stream_id
        {
            Some(self.event_id)
        } else {
            self.tracked_event_ids.iter().find_map(|teid| {
                if (source_id, stream_id) == (teid.source_id, teid.stream_id) {
                    Some(teid.max_event_id)
                } else {
                    None
                }
            })
        }
    }

    #[must_use]
    /// get the minimum tracked (`stream_id`, `event_id`)
    /// by chosing events with smaller stream id
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_min_by_source(&self, source_id: u64) -> Option<(u64, u64)> {
        // TODO: change the return type to an iterator, so we make sure to return all values for all streams
        if self.tracked_event_ids.is_empty() && self.source_id == source_id {
            Some((self.stream_id, self.event_id))
        } else {
            self.tracked_event_ids
                .iter()
                .filter(|teid| teid.source_id == source_id)
                .min_by(|teid1, teid2| teid1.stream_id.cmp(&teid2.stream_id))
                .map(|teid| (teid.stream_id, teid.min_event_id))
        }
    }

    #[must_use]
    /// get the maximum tracked (`stream_id`, `event_id`)
    /// by chosing events with bigger stream id
    ///
    /// This also always checks the actual eventId, not only the tracked ones
    pub fn get_max_by_source(&self, source_id: u64) -> Option<(u64, u64)> {
        // TODO: change the return type to an iterator, so we make sure to return all values for all streams
        if self.tracked_event_ids.is_empty() && self.source_id == source_id {
            Some((self.stream_id, self.event_id))
        } else {
            self.tracked_event_ids
                .iter()
                .filter(|teid| teid.source_id == source_id)
                .max_by(|teid1, teid2| teid1.stream_id.cmp(&teid2.stream_id))
                .map(|teid| (teid.stream_id, teid.max_event_id))
        }
    }
}

impl From<(u64, u64, u64)> for EventId {
    fn from(x: (u64, u64, u64)) -> Self {
        EventId::new(x.0, x.1, x.2)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.source_id, self.stream_id, self.event_id)?;
        if !self.tracked_event_ids.is_empty() {
            let mut iter = self.tracked_event_ids.iter();
            if let Some(ids) = iter.next() {
                write!(f, " {}", ids)?;
            }
            for ids in iter {
                write!(f, ", {}", ids)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Debug, Clone, PartialEq, Default, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
/// tracked min and max event id for
pub struct TrackedEventIds {
    /// uid of the source this event originated from
    pub source_id: u64,
    /// uid of the stream within the source this event originated from
    pub stream_id: u64,
    /// tracking min event id
    pub min_event_id: u64,
    /// tracking max event id
    pub max_event_id: u64,
}

impl TrackedEventIds {
    #[must_use]
    /// create a new instance with min and max set to `event_id`.
    pub fn new(source_id: u64, stream_id: u64, min_event_id: u64, max_event_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            min_event_id,
            max_event_id,
        }
    }

    #[must_use]
    /// create tracked ids from a single `event_id`
    pub fn from_id(source_id: u64, stream_id: u64, event_id: u64) -> Self {
        Self {
            source_id,
            stream_id,
            min_event_id: event_id,
            max_event_id: event_id,
        }
    }

    #[must_use]
    /// returns true if this struct tracks the given source and stream ids
    pub fn tracks_id(&self, source_id: u64, stream_id: u64) -> bool {
        self.source_id == source_id && self.stream_id == stream_id
    }

    #[must_use]
    /// compares against the given source and stream ids, using simple numeric ordering
    pub fn compare_ids(&self, source_id: u64, stream_id: u64) -> Ordering {
        (self.source_id, self.stream_id).cmp(&(source_id, stream_id))
    }

    #[must_use]
    /// compare source and stream ids against the ones given in `other`
    pub fn compare(&self, other: &TrackedEventIds) -> Ordering {
        (self.source_id, self.stream_id).cmp(&(other.source_id, other.stream_id))
    }

    /// track everything from the given `event_id`
    pub fn track(&mut self, event_id: &EventId) {
        #[cfg(test)]
        {
            debug_assert!(
                self.source_id == event_id.source_id,
                "incompatible source ids"
            );
            debug_assert!(
                self.stream_id == event_id.stream_id,
                "incompatible stream ids"
            );
        }
        self.track_ids(event_id.event_id, event_id.event_id);
    }

    /// track a single event id
    pub fn track_id(&mut self, event_id: u64) {
        self.track_ids(event_id, event_id);
    }

    /// track a min and max event id
    pub fn track_ids(&mut self, min_event_id: u64, max_event_id: u64) {
        self.min_event_id = self.min_event_id.min(min_event_id);
        self.max_event_id = self.max_event_id.max(max_event_id);
    }

    /// merge the other `ids` into this one
    pub fn merge(&mut self, ids: &TrackedEventIds) {
        // TODO: once https://github.com/rust-lang/rust-clippy/issues/6970 is fixed comment those in again
        #[cfg(test)]
        {
            debug_assert!(self.source_id == ids.source_id, "incompatible source ids");
            debug_assert!(self.stream_id == ids.stream_id, "incompatible stream ids");
        }
        self.track_ids(ids.min_event_id, ids.max_event_id);
    }
}

impl From<&EventId> for TrackedEventIds {
    fn from(e: &EventId) -> Self {
        Self::from_id(e.source_id, e.stream_id, e.event_id)
    }
}

impl From<(u64, u64, u64)> for TrackedEventIds {
    fn from(x: (u64, u64, u64)) -> Self {
        Self::from_id(x.0, x.1, x.2)
    }
}

impl fmt::Display for TrackedEventIds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[min={}:{}:{}, max={}:{}:{}]",
            self.source_id,
            self.stream_id,
            self.min_event_id,
            self.source_id,
            self.stream_id,
            self.max_event_id
        )
    }
}

// TODO adapt for streaming, so we maintain multiple counters per stream
#[derive(Debug, Clone, Copy, Default)]
/// for generating consecutive unique event ids
pub struct EventIdGenerator(u64, u64, u64);
impl EventIdGenerator {
    /// generate the next event id for this stream
    pub fn next_id(&mut self) -> EventId {
        let event_id = self.2;
        self.2 = self.2.wrapping_add(1);
        EventId::new(self.0, self.1, event_id)
    }

    #[must_use]
    /// create a new generator using the default stream id
    pub fn new(source_id: u64) -> Self {
        Self(source_id, DEFAULT_STREAM_ID, 0)
    }

    #[must_use]
    /// create a new generator using the given source and stream id
    pub fn with_stream(source_id: u64, stream_id: u64) -> Self {
        Self(source_id, stream_id, 0)
    }

    /// set the source id
    pub fn set_source(&mut self, source_id: u64) {
        self.0 = source_id;
    }

    /// set the stream id
    pub fn set_stream(&mut self, stream_id: u64) {
        self.1 = stream_id;
    }
}

/// The kind of signal this is
#[derive(
    Debug, Clone, Copy, PartialEq, simd_json_derive::Serialize, simd_json_derive::Deserialize,
)]
pub enum SignalKind {
    // Lifecycle
    /// Init singnal
    Init,
    /// Shutdown Signal
    Shutdown,
    // Pause, TODO debug trace
    // Resume, TODO debug trace
    // Step, TODO ( into, over, to next breakpoint )
    /// Control
    Control,
    /// Periodic Tick
    Tick,
}

// We ignore this since it's a simple lookup table
#[cfg(not(tarpaulin_include))]
fn factory(node: &NodeConfig) -> Result<Box<dyn InitializableOperator>> {
    #[cfg(feature = "bert")]
    use op::bert::{SequenceClassificationFactory, SummerizationFactory};
    use op::debug::EventHistoryFactory;
    use op::generic::{BatchFactory, CounterFactory};
    use op::grouper::BucketGrouperFactory;
    use op::identity::PassthroughFactory;
    use op::qos::{BackpressureFactory, PercentileFactory, RoundRobinFactory, WalFactory};
    let name_parts: Vec<&str> = node.op_type.split("::").collect();
    let factory = match name_parts.as_slice() {
        ["passthrough"] => PassthroughFactory::new_boxed(),
        ["debug", "history"] => EventHistoryFactory::new_boxed(),
        ["grouper", "bucket"] => BucketGrouperFactory::new_boxed(),
        ["generic", "batch"] => BatchFactory::new_boxed(),
        ["generic", "backpressure"] => {
            error!("The generic::backpressure operator is depricated, please use qos::backpressure instread.");
            BackpressureFactory::new_boxed()
        }
        ["generic", "counter"] => CounterFactory::new_boxed(),
        ["qos", "backpressure"] => BackpressureFactory::new_boxed(),
        ["qos", "roundrobin"] => RoundRobinFactory::new_boxed(),
        ["qos", "wal"] => WalFactory::new_boxed(),
        ["qos", "percentile"] => PercentileFactory::new_boxed(),
        #[cfg(feature = "bert")]
        ["bert", "sequence_classification"] => SequenceClassificationFactory::new_boxed(),
        #[cfg(feature = "bert")]
        ["bert", "summarization"] => SummerizationFactory::new_boxed(),
        [namespace, name] => {
            return Err(ErrorKind::UnknownOp((*namespace).to_string(), (*name).to_string()).into());
        }
        _ => return Err(ErrorKind::UnknownNamespace(node.op_type.clone()).into()),
    };
    Ok(factory)
}

fn operator(uid: u64, node: &NodeConfig) -> Result<Box<dyn Operator + 'static>> {
    factory(&node)?.from_node(uid, node)
}

/// Takes a name, tags and creates a influx codec compatible Value
#[must_use]
pub fn influx_value(
    metric_name: Cow<'static, str>,
    tags: HashMap<Cow<'static, str>, Value<'static>>,
    count: u64,
    timestamp: u64,
) -> Value<'static> {
    literal!({
        MEASUREMENT: metric_name,
        TAGS: tags,
        FIELDS: {
            COUNT: count
        },
        TIMESTAMP: timestamp
    })
}

#[derive(Debug, Default)]
struct Connection {
    from: Cow<'static, str>,
    to: Cow<'static, str>,
}
impl Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let from: &str = &self.from;
        let to: &str = &self.to;
        match (from, to) {
            ("out", "in") => write!(f, ""),
            ("out", to) => write!(f, "{}", to),
            (from, "in") => write!(f, "{} ", from),
            (from, to) => write!(f, "{} -> {}", from, to),
        }
    }
}

pub(crate) type ConfigGraph = graph::DiGraph<NodeConfig, Connection>;

#[cfg(test)]
mod test {
    use super::*;
    use simd_json_derive::{Deserialize, Serialize};
    #[test]
    fn prim_str() {
        let p = PrimStr(42);
        let fourtytwo = r#""42""#;
        let mut fourtytwo_s = fourtytwo.to_string();
        let mut fourtytwo_i = "42".to_string();
        assert_eq!(fourtytwo, p.json_string().unwrap());
        assert_eq!(
            p,
            PrimStr::from_slice(unsafe { fourtytwo_s.as_bytes_mut() }).unwrap()
        );
        assert!(PrimStr::<i32>::from_slice(unsafe { fourtytwo_i.as_bytes_mut() }).is_err());
    }

    #[test]
    fn op_meta_merge() {
        let mut m1 = OpMeta::default();
        let mut m2 = OpMeta::default();
        m1.insert(1, 1);
        m1.insert(2, 1);
        m2.insert(1, 2);
        m2.insert(3, 2);
        m1.merge(m2);

        assert!(m1.contains_key(1));
        assert!(m1.contains_key(2));
        assert!(m1.contains_key(3));

        assert_eq!(m1.get(1).unwrap(), &2);
        assert_eq!(m1.get(2).unwrap(), &1);
        assert_eq!(m1.get(3).unwrap(), &2);
    }

    #[test]
    fn cbaction_creation() {
        assert_eq!(CbAction::default(), CbAction::None);
        assert_eq!(CbAction::from(true), CbAction::Ack);
        assert_eq!(CbAction::from(false), CbAction::Fail);
    }

    #[test]
    fn cbaction_is_gd() {
        assert_eq!(CbAction::None.is_gd(), false);

        assert_eq!(CbAction::Fail.is_gd(), true);
        assert_eq!(CbAction::Ack.is_gd(), true);

        assert_eq!(CbAction::Open.is_gd(), false);
        assert_eq!(CbAction::Close.is_gd(), false);
    }

    #[test]
    fn cbaction_is_cb() {
        assert_eq!(CbAction::None.is_cb(), false);

        assert_eq!(CbAction::Fail.is_cb(), false);
        assert_eq!(CbAction::Ack.is_cb(), false);

        assert_eq!(CbAction::Open.is_cb(), true);
        assert_eq!(CbAction::Close.is_cb(), true);
    }

    #[test]
    fn event_ids() {
        let mut ids1 = EventId::new(1, 1, 1);
        assert_eq!(Some(1), ids1.get_max_by_stream(1, 1));
        assert_eq!(None, ids1.get_max_by_stream(1, 2));
        assert_eq!(None, ids1.get_max_by_stream(2, 1));

        let mut ids2 = EventId::new(1, 1, 2);
        assert_eq!(ids2.get_max_by_stream(1, 1), Some(2));
        assert_eq!(ids2.get_max_by_stream(1, 3), None);
        assert_eq!(ids2.get_max_by_stream(2, 1), None);

        ids1.track_id(2, DEFAULT_STREAM_ID, 1);
        ids2.track_id(2, DEFAULT_STREAM_ID, 3);

        assert_eq!(ids1.get_max_by_source(1), Some((1, 1)));
        assert_eq!(ids1.get_max_by_source(2), Some((DEFAULT_STREAM_ID, 1)));

        assert_eq!(ids2.get_min_by_source(1), Some((1, 2)));
        assert_eq!(ids2.get_min_by_source(2), Some((DEFAULT_STREAM_ID, 3)));

        ids1.track(&ids2);

        assert_eq!(ids1.get_max_by_source(1), Some((1, 2)));
        assert_eq!(ids1.get_min_by_source(1), Some((1, 1)));
        assert_eq!(ids1.get_min_by_source(3), None);
        assert_eq!(ids1.get_max_by_source(3), None);
        assert_eq!(ids1.get_max_by_source(2), Some((DEFAULT_STREAM_ID, 3)));
        assert_eq!(ids1.get_min_by_source(2), Some((DEFAULT_STREAM_ID, 1)));

        assert_eq!(ids1.get_max_by_stream(1, 1), Some(2));
        assert_eq!(ids1.get_max_by_stream(1, 2), None);
        assert_eq!(ids1.get_min_by_stream(2, DEFAULT_STREAM_ID), Some(1));
        assert_eq!(ids1.get_min_by_stream(2, 42), None);

        let id = EventId::from((1, DEFAULT_STREAM_ID, 42u64));
        assert_eq!(id.get_max_by_stream(1, DEFAULT_STREAM_ID), Some(42));
        assert_eq!(id.get_max_by_stream(5, DEFAULT_STREAM_ID), None);
    }

    #[test]
    fn tracked_event_ids() {
        let teid1 = TrackedEventIds::default();
        assert_eq!(
            (
                teid1.source_id,
                teid1.stream_id,
                teid1.min_event_id,
                teid1.max_event_id
            ),
            (0, 0, 0, 0)
        );

        let mut teid2 = TrackedEventIds::new(1, 2, 3, 4);
        let eid1 = EventId::new(1, 2, 6);
        let eid2 = EventId::new(1, 2, 1);
        teid2.track(&eid1);
        assert_eq!(teid2.max_event_id, eid1.event_id);
        assert_eq!(teid2.min_event_id, 3);

        teid2.track(&eid2);
        assert_eq!(teid2.min_event_id, eid2.event_id);
        assert_eq!(teid2.max_event_id, eid1.event_id);

        let teid3 = TrackedEventIds::from((1, 2, 19));
        teid2.merge(&teid3);

        assert_eq!(teid2.min_event_id, 1);
        assert_eq!(teid2.max_event_id, 19);

        teid2.track_id(0);
        assert_eq!(teid2.min_event_id, 0);
    }
}
