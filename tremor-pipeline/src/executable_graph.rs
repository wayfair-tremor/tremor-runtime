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

use std::{fmt, fmt::Display};

use crate::{
    common_cow,
    errors::Result,
    errors::{Error, ErrorKind},
    influx_value,
    op::{prelude::IN, trickle::select::WindowImpl},
    ConfigMap, ExecPortIndexMap, NodeLookupFn,
};
use crate::{op::EventAndInsights, Event, NodeKind, Operator};
use beef::Cow;
use halfbrown::HashMap;
use tremor_common::stry;
use tremor_script::{srs, Value};

/// Configuration for a node
#[derive(Debug, Clone, PartialOrd, Eq, Default)]
pub struct NodeConfig {
    pub(crate) id: String,
    pub(crate) kind: NodeKind,
    pub(crate) op_type: String,
    pub(crate) config: ConfigMap,
    pub(crate) defn: Option<srs::Stmt>,
    pub(crate) node: Option<srs::Stmt>,
    pub(crate) label: Option<String>,
}

impl Display for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl NodeConfig {
    fn label(&self) -> &str {
        let dflt: &str = &self.id;
        self.label.as_deref().unwrap_or(dflt)
    }

    /// Creates a `NodeConfig` from a config struct
    pub fn from_config<C, I>(id: &I, config: C) -> Result<Self>
    where
        C: serde::Serialize,
        I: ToString,
    {
        let config = serde_yaml::to_vec(&config)?;

        Ok(NodeConfig {
            id: id.to_string(),
            kind: NodeKind::Operator,
            config: serde_yaml::from_slice(&config)?,
            ..NodeConfig::default()
        })
    }
}

// We ignore stmt on equality and hasing as they're only
// carried through for implementation purposes not part
// if the identiy of a node

impl PartialEq for NodeConfig {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.kind == other.kind
            && self.op_type == other.op_type
            && self.config == other.config
    }
}

impl std::hash::Hash for NodeConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.kind.hash(state);
        self.op_type.hash(state);
        self.config.hash(state);
    }
}

impl NodeConfig {
    pub(crate) fn to_op(
        &self,
        uid: u64,
        resolver: NodeLookupFn,
        defn: Option<&srs::Stmt>,
        node: Option<&srs::Stmt>,
        window: Option<HashMap<String, WindowImpl>>,
    ) -> Result<OperatorNode> {
        resolver(&self, uid, defn, node, window)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct State {
    // for operator node-level state (ordered in the same way as nodes in the executable graph)
    ops: Vec<Value<'static>>,
}

impl State {
    /// Creates a new Sate
    pub fn new(ops: Vec<Value<'static>>) -> Self {
        Self { ops }
    }
}

/// An executable operator
#[derive(Debug)]
pub struct OperatorNode {
    /// ID of the operator
    pub id: String,
    /// Typoe of the operator
    pub kind: NodeKind,
    /// operator namepsace
    pub op_type: String,
    /// The executable operator
    pub op: Box<dyn Operator>,
    /// Tremor unique identifyer
    pub uid: u64,
}

impl Operator for OperatorNode {
    fn on_event(
        &mut self,
        _uid: u64,
        port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        self.op.on_event(self.uid, port, state, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(
        &mut self,
        _uid: u64,
        state: &Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        self.op.on_signal(self.uid, state, signal)
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, _uid: u64, contraevent: &mut Event) {
        self.op.on_contraflow(self.uid, contraevent)
    }

    fn metrics(
        &self,
        tags: &HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        self.op.metrics(tags, timestamp)
    }

    fn skippable(&self) -> bool {
        self.op.skippable()
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeMetrics {
    inputs: HashMap<Cow<'static, str>, u64>,
    outputs: HashMap<Cow<'static, str>, u64>,
}

impl NodeMetrics {
    // this makes sense since we might not need to clone the cow and
    // it might be owned so cloning early would be costly
    #[allow(clippy::ptr_arg)]
    pub(crate) fn inc_input(&mut self, input: &Cow<'static, str>) {
        self.inc_input_n(input, 1)
    }

    // this makes sense since we might not need to clone the cow and
    // it might be owned so cloning early would be costly
    #[allow(clippy::ptr_arg)]
    fn inc_input_n(&mut self, input: &Cow<'static, str>, increment: u64) {
        let (_, v) = self
            .inputs
            .raw_entry_mut()
            .from_key(input)
            .or_insert_with(|| (input.clone(), 0));
        *v += increment;
    }
    // this makes sense since we might not need to clone the cow and
    // it might be owned so cloning early would be costly
    #[allow(clippy::ptr_arg)]
    pub(crate) fn inc_output(&mut self, output: &Cow<'static, str>) {
        self.inc_output_n(output, 1)
    }

    // this makes sense since we might not need to clone the cow and
    // it might be owned so cloning early would be costly
    #[allow(clippy::ptr_arg)]
    fn inc_output_n(&mut self, output: &Cow<'static, str>, increment: u64) {
        let (_, v) = self
            .outputs
            .raw_entry_mut()
            .from_key(output)
            .or_insert_with(|| (output.clone(), 0));
        *v += increment;
    }

    fn to_value(
        &self,
        metric_name: &str,
        tags: &mut HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Vec<Value<'static>> {
        let mut res = Vec::with_capacity(self.inputs.len() + self.outputs.len());
        tags.insert("direction".into(), "input".into());
        for (k, v) in &self.inputs {
            tags.insert("port".into(), Value::from(k.clone()));
            res.push(influx_value(
                metric_name.to_string().into(),
                tags.clone(),
                *v,
                timestamp,
            ));
        }
        tags.insert("direction".into(), "output".into());
        for (k, v) in &self.outputs {
            tags.insert("port".into(), Value::from(k.clone()));
            res.push(influx_value(
                metric_name.to_string().into(),
                tags.clone(),
                *v,
                timestamp,
            ));
        }
        res
    }
}

/// An executable graph, this is the executable
/// form of a pipeline
#[derive(Debug)]
pub struct ExecutableGraph {
    /// ID of the graph
    pub id: String,
    pub(crate) graph: Vec<OperatorNode>,
    pub(crate) state: State,
    pub(crate) inputs: HashMap<Cow<'static, str>, usize>,
    pub(crate) stack: Vec<(usize, Cow<'static, str>, Event)>,
    pub(crate) signalflow: Vec<usize>,
    pub(crate) contraflow: Vec<usize>,
    pub(crate) port_indexes: ExecPortIndexMap,
    pub(crate) metrics: Vec<NodeMetrics>,
    pub(crate) metrics_idx: usize,
    pub(crate) last_metrics: u64,
    pub(crate) metric_interval: Option<u64>,
    /// snot
    pub insights: Vec<(usize, Event)>,
    /// source code of the pipeline
    pub source: Option<String>,
    /// the dot representation of the graph
    pub dot: String,
}

/// The return of a graph execution
pub type Returns = Vec<(Cow<'static, str>, Event)>;
impl ExecutableGraph {
    /// Tries to optimise a pipeline
    pub fn optimize(&mut self) -> Option<()> {
        let mut i = 0;
        while self.optimize_()? {
            i += 1;
            if i > 1000 {
                error!("Failed to optimise loop after 1000 iterations");
                return None;
            }
        }
        Some(())
    }
    fn optimize_(&mut self) -> Option<bool> {
        let mut did_chage = false;
        // remove skippable nodes from contraflow and signalflow
        self.contraflow = (*self.contraflow)
            .iter()
            .filter(|id| {
                !self
                    .graph
                    .get(**id)
                    .map(|n| n.skippable())
                    .unwrap_or_default()
            })
            .copied()
            .collect();
        self.signalflow = (*self.signalflow)
            .iter()
            .filter(|id| {
                !self
                    .graph
                    .get(**id)
                    .map(|n| n.skippable())
                    .unwrap_or_default()
            })
            .copied()
            .collect();

        let mut input_ids: Vec<usize> = Vec::new();

        // first we check the inputs, if an input points to a skippable
        // node and does not connect to more then one other node with the same
        // input name it can be removed.
        for (input_name, target) in &mut self.inputs.iter_mut() {
            let target_node = self.graph.get(*target)?;
            input_ids.push(*target);
            // the target of the input is skippable
            if target_node.skippable() {
                let mut next_nodes = self
                    .port_indexes
                    .iter()
                    .filter(|((from_id, _), _)| from_id == target);

                if let Some((_, dsts)) = next_nodes.next() {
                    // we only connect from one output
                    if next_nodes.next().is_none() && dsts.len() == 1 {
                        let (next_id, next_input) = dsts.first()?;
                        if next_input == input_name {
                            *target = *next_id;
                        }
                    }
                }
            }
        }
        // The id's of all nodes that are skippable
        let skippables: Vec<_> = self
            .graph
            .iter()
            .enumerate()
            .filter_map(|(id, e)| {
                if e.skippable() && e.kind.skippable() {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for skippable_id in &skippables {
            // We iterate over all the skippable ID's

            // We collect all the outputs that the skippable node
            // sends data to.
            // So of a passthrough sends data to node 4 and 5
            // 4 and 5 becomes the output
            let outputs: Vec<_> = self
                .port_indexes
                .iter()
                .filter_map(|((from_id, _), outputs)| {
                    if from_id == skippable_id {
                        Some(outputs)
                    } else {
                        None
                    }
                })
                .flatten()
                .cloned()
                .collect();

            // Find all nodes that connect to the skippable
            // so if node 1 and 7 connect to the passthrough
            // then this is going to be 1 and 7
            let inputs: Vec<_> = self
                .port_indexes
                .iter()
                .filter_map(|(from, connections)| {
                    if connections
                        .iter()
                        .any(|(target_id, _)| target_id == skippable_id)
                    {
                        Some(from)
                    } else {
                        None
                    }
                })
                .cloned()
                .collect();

            // We iterate over all nodes that connect to the skippable
            // we're handling.

            for i in inputs {
                // Take the nodes connections for the indexes
                let srcs: Vec<_> = self.port_indexes.remove(&i)?;
                let mut srcs1 = Vec::new();

                // We then iterate over all the destinations that input
                // node connects to
                for (src_id, src_port) in srcs {
                    if src_id == *skippable_id {
                        did_chage = true;
                        // If it is the skippable node replace this entry
                        // with all the outputs the skippable had
                        for o in &outputs {
                            srcs1.push(o.clone())
                        }
                    } else {
                        // Otherwise keep the connection untoucehd.
                        srcs1.push((src_id, src_port))
                    }
                }
                // Add the node back in
                self.port_indexes.insert(i, srcs1);
            }
        }
        Some(did_chage)
    }
    /// This is a performance critial function!
    ///
    /// # Errors
    /// Errors if the event can not be processed, or an operator fails
    pub fn enqueue(
        &mut self,
        stream_name: &str,
        event: Event,
        returns: &mut Returns,
    ) -> Result<()> {
        // Resolve the input stream or entrypoint for this enqueue operation
        if self
            .metric_interval
            .map(|ival| event.ingest_ns - self.last_metrics > ival)
            .unwrap_or_default()
        {
            let mut tags = HashMap::with_capacity(8);
            tags.insert("pipeline".into(), common_cow(&self.id).into());
            self.enqueue_metrics("events", tags, event.ingest_ns);
            self.last_metrics = event.ingest_ns;
        }
        let input = *stry!(self.inputs.get(stream_name).ok_or_else(|| {
            Error::from(ErrorKind::InvalidInputStreamName(
                stream_name.to_owned(),
                self.id.clone(),
            ))
        }));
        self.stack.push((input, IN, event));
        self.run(returns)
    }

    #[inline]
    fn run(&mut self, returns: &mut Returns) -> Result<()> {
        while stry!(self.next(returns)) {}
        returns.reverse();
        Ok(())
    }

    #[inline]
    fn next(&mut self, returns: &mut Returns) -> Result<bool> {
        if let Some((idx, port, event)) = self.stack.pop() {
            // If we have emitted a signal event we got to handle it as a signal flow
            // the signal flow will
            if event.kind.is_some() {
                stry!(self.signalflow(event));
            } else {
                // count ingres
                let node = unsafe { self.graph.get_unchecked_mut(idx) };
                if let NodeKind::Output(port) = &node.kind {
                    returns.push((port.clone(), event));
                } else {
                    // ALLOW: We know the state was initiated
                    let state = unsafe { self.state.ops.get_unchecked_mut(idx) };
                    let EventAndInsights { events, insights } =
                        stry!(node.on_event(0, &port, state, event));

                    for (out_port, _) in &events {
                        unsafe { self.metrics.get_unchecked_mut(idx) }.inc_output(out_port);
                    }
                    for insight in insights {
                        self.insights.push((idx, insight))
                    }
                    self.enqueue_events(idx, events);
                };
            }
            Ok(!self.stack.is_empty())
        } else {
            error!("next was called on an empty graph stack, this should never happen");
            Ok(false)
        }
    }

    fn enqueue_metrics(
        &mut self,
        metric_name: &str,
        mut tags: HashMap<Cow<'static, str>, Value<'static>>,
        ingest_ns: u64,
    ) {
        for (i, m) in self.metrics.iter().enumerate() {
            tags.insert("node".into(), unsafe {
                self.graph.get_unchecked(i).id.clone().into()
            });
            if let Ok(metrics) = unsafe { self.graph.get_unchecked(i) }.metrics(&tags, ingest_ns) {
                for value in metrics {
                    self.stack.push((
                        self.metrics_idx,
                        IN,
                        Event {
                            data: value.into(),
                            ingest_ns,
                            // TODO update this to point to tremor instance producing the metrics?
                            origin_uri: None,
                            ..Event::default()
                        },
                    ));
                }
            }

            for value in m.to_value(&metric_name, &mut tags, ingest_ns) {
                self.stack.push((
                    self.metrics_idx,
                    IN,
                    Event {
                        data: value.into(),
                        ingest_ns,
                        // TODO update this to point to tremor instance producing the metrics?
                        origin_uri: None,
                        ..Event::default()
                    },
                ));
            }
        }
    }
    #[inline]
    fn enqueue_events(&mut self, idx: usize, events: Vec<(Cow<'static, str>, Event)>) {
        for (out_port, event) in events {
            if let Some((last, rest)) = self
                .port_indexes
                .get(&(idx, out_port))
                .and_then(|o| o.split_last())
            {
                for (idx, in_port) in rest {
                    unsafe { self.metrics.get_unchecked_mut(*idx) }.inc_input(in_port);
                    self.stack.push((*idx, in_port.clone(), event.clone()));
                }
                let (idx, in_port) = last;
                unsafe { self.metrics.get_unchecked_mut(*idx) }.inc_input(in_port);
                self.stack.push((*idx, in_port.clone(), event))
            }
        }
    }
    /// Enque a contraflow insight
    pub fn contraflow(&mut self, mut skip_to: Option<usize>, mut insight: Event) -> Event {
        for idx in &self.contraflow {
            if skip_to.is_none() {
                let op = unsafe { self.graph.get_unchecked_mut(*idx) }; // We know this exists
                op.on_contraflow(op.uid, &mut insight);
            } else if skip_to == Some(*idx) {
                skip_to = None
            }
        }
        insight
    }
    /// Enqueue a signal
    ///
    /// # Errors
    /// if the singal fails to be processed in the singal flow or if any forward going
    /// events spawned by this signal fail to be processed
    pub fn enqueue_signal(&mut self, signal: Event, returns: &mut Returns) -> Result<()> {
        if stry!(self.signalflow(signal)) {
            stry!(self.run(returns));
        }
        Ok(())
    }

    fn signalflow(&mut self, mut signal: Event) -> Result<bool> {
        let mut has_events = false;
        // We can't use an iterator over signalfow here
        // rust refuses to let us use enqueue_events if we do
        for idx in 0..self.signalflow.len() {
            // ALLOW: we guarantee that idx exists above
            let i = unsafe { *self.signalflow.get_unchecked(idx) };
            let EventAndInsights { events, insights } = {
                let op = unsafe { self.graph.get_unchecked_mut(i) }; // We know this exists
                let state = unsafe { self.state.ops.get_unchecked(i) }; // we know this has been initialized
                stry!(op.on_signal(op.uid, state, &mut signal))
            };
            self.insights.extend(insights.into_iter().map(|cf| (i, cf)));
            has_events = has_events || !events.is_empty();
            self.enqueue_events(i, events);
        }
        Ok(has_events)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::op::{
        identity::PassthroughFactory,
        prelude::{METRICS, OUT},
    };
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    use tremor_script::prelude::*;
    #[test]
    fn node_conjfig_eq() {
        let n0 = NodeConfig::from_config(&"node", ()).unwrap();
        let n1 = NodeConfig::from_config(&"other", ()).unwrap();
        assert_eq!(n0, n0);
        assert_ne!(n0, n1);
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        let mut h3 = DefaultHasher::new();
        n0.hash(&mut h1);
        n0.hash(&mut h2);

        assert_eq!(h1.finish(), h2.finish());
        n1.hash(&mut h3);
        assert_ne!(h2.finish(), h3.finish());
    }

    fn pass(uid: u64, id: &'static str) -> OperatorNode {
        let c = NodeConfig::from_config(&"passthrough", ()).unwrap();
        OperatorNode {
            id: id.into(),
            kind: NodeKind::Operator,
            op_type: id.into(),
            op: PassthroughFactory::new_boxed().from_node(uid, &c).unwrap(),
            uid: 0,
        }
    }
    #[test]
    fn operator_node() {
        let mut n = pass(0, "passthrough");
        assert!(!n.handles_contraflow());
        assert!(!n.handles_signal());
        assert!(!n.handles_contraflow());
        let mut e = Event::default();
        let state = Value::null();
        n.on_contraflow(0, &mut e);
        assert_eq!(e, Event::default());
        assert_eq!(
            n.on_signal(0, &state, &mut e).unwrap(),
            EventAndInsights::default()
        );
        assert_eq!(e, Event::default());
        assert!(n.metrics(&HashMap::default(), 0).unwrap().is_empty());
    }

    fn test_metric<'value>(v: &Value<'value>, m: &str, c: u64) {
        assert_eq!(v.get("measurement").unwrap(), m);
        assert!(v.get("tags").unwrap().is_object());
        assert_eq!(v.get("fields").unwrap().get("count").unwrap(), &c);
    }

    #[test]
    fn node_metrics() {
        let mut m = NodeMetrics::default();
        let port: Cow<'static, str> = Cow::from("port");
        m.inc_input_n(&port, 41);
        m.inc_input(&port);
        m.inc_output(&port);
        m.inc_output_n(&port, 22);
        let mut tags = HashMap::default();
        let mut v = m.to_value("test", &mut tags, 123);
        assert_eq!(v.len(), 2);

        let mo = v.pop().unwrap();
        test_metric(&mo, "test", 23);
        assert_eq!(mo.get("timestamp").unwrap(), &123);

        let mi = v.pop().unwrap();
        test_metric(&mi, "test", 42);
        assert_eq!(mi.get("timestamp").unwrap(), &123);
    }

    #[derive(Debug)]
    struct AllOperator {}

    impl Operator for AllOperator {
        fn on_event(
            &mut self,
            _uid: u64,
            _port: &str,
            _state: &mut Value<'static>,
            event: Event,
        ) -> Result<EventAndInsights> {
            Ok(event.into())
        }
        fn handles_signal(&self) -> bool {
            true
        }

        fn on_signal(
            &mut self,
            _uid: u64,
            _state: &Value<'static>,
            _signal: &mut Event,
        ) -> Result<EventAndInsights> {
            // Make the trait signature nicer
            Ok(EventAndInsights::default())
        }

        fn handles_contraflow(&self) -> bool {
            true
        }

        fn on_contraflow(&mut self, _uid: u64, _insight: &mut Event) {}

        fn metrics(
            &self,
            _tags: &HashMap<Cow<'static, str>, Value<'static>>,
            _timestamp: u64,
        ) -> Result<Vec<Value<'static>>> {
            Ok(Vec::new())
        }

        fn skippable(&self) -> bool {
            false
        }
    }

    fn all_op(id: &'static str) -> OperatorNode {
        OperatorNode {
            id: id.into(),
            kind: NodeKind::Operator,
            op_type: "test".into(),
            op: Box::new(AllOperator {}),
            uid: 0,
        }
    }

    fn test_metrics(mut metrics: Vec<Event>, n: u64) {
        // out/in
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "out");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "in");

        // all-2/out
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "all-2");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "out");

        // all-2/in
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "all-2");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "in");

        // all-1/out
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "all-1");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "out");

        // all-1/in
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "all-1");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "in");

        // out/in
        let this = metrics.pop().unwrap();
        let (data, _) = this.data.parts();
        test_metric(data, "test-metric", n);
        assert_eq!(data.get("tags").unwrap().get("node").unwrap(), "in");
        assert_eq!(data.get("tags").unwrap().get("port").unwrap(), "out");
        assert!(metrics.is_empty());
    }

    #[test]
    fn eg_metrics() {
        let mut in_n = pass(1, "in");
        in_n.kind = NodeKind::Input;
        let mut out_n = pass(2, "out");
        out_n.kind = NodeKind::Output(OUT);
        let mut metrics_n = pass(3, "metrics");
        metrics_n.kind = NodeKind::Output(METRICS);

        // The graph is in -> 1 -> 2 -> out, we pre-stack the edges since we do not
        // need to have order in here.
        let graph = vec![in_n, all_op("all-1"), all_op("all-2"), out_n, metrics_n];

        let mut inputs = HashMap::new();
        inputs.insert("in".into(), 0);

        let mut port_indexes = ExecPortIndexMap::new();
        // link out from
        port_indexes.insert((0, "out".into()), vec![(1, "in".into())]);
        port_indexes.insert((1, "out".into()), vec![(2, "in".into())]);
        port_indexes.insert((2, "out".into()), vec![(3, "in".into())]);

        // Create metric nodes for all elements in the graph
        let metrics = vec![
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
        ];
        // Create state for all nodes
        let state = State {
            ops: vec![
                Value::null(),
                Value::null(),
                Value::null(),
                Value::null(),
                Value::null(),
            ],
        };
        let mut g = ExecutableGraph {
            id: "test".into(),
            graph,
            state,
            inputs,
            stack: vec![],
            signalflow: vec![2, 3],
            contraflow: vec![3, 2],
            port_indexes,
            metrics,
            // The index of the metrics node in our pipeline
            metrics_idx: 4,
            last_metrics: 0,
            metric_interval: Some(1),
            insights: vec![],
            source: None,
            dot: String::from(""),
        };

        // Test with one event
        let e = Event::default();
        let mut returns = Vec::new();
        g.enqueue("in", e, &mut returns).unwrap();
        assert_eq!(returns.len(), 1);
        returns.clear();

        g.enqueue_metrics("test-metric", HashMap::new(), 123);
        g.run(&mut returns).unwrap();
        let (ports, metrics): (Vec<_>, Vec<_>) = returns.drain(..).unzip();
        assert!(ports.iter().all(|v| v == "metrics"));
        test_metrics(metrics, 1);

        // Test with two events
        let e = Event::default();
        let mut returns = Vec::new();
        g.enqueue("in", e, &mut returns).unwrap();
        assert_eq!(returns.len(), 1);
        returns.clear();

        let e = Event::default();
        let mut returns = Vec::new();
        g.enqueue("in", e, &mut returns).unwrap();
        assert_eq!(returns.len(), 1);
        returns.clear();

        g.enqueue_metrics("test-metric", HashMap::new(), 123);
        g.run(&mut returns).unwrap();
        let (ports, metrics): (Vec<_>, Vec<_>) = returns.drain(..).unzip();
        assert!(ports.iter().all(|v| v == "metrics"));
        test_metrics(metrics, 3);
    }

    #[test]
    fn eg_optimize() {
        let mut in_n = pass(1, "in");
        in_n.kind = NodeKind::Input;
        let mut out_n = pass(2, "out");
        out_n.kind = NodeKind::Output(OUT);
        let mut metrics_n = pass(3, "metrics");
        metrics_n.kind = NodeKind::Output(METRICS);

        // The graph is in -> 1 -> 2 -> out, we pre-stack the edges since we do not
        // need to have order in here.
        let graph = vec![
            in_n,
            all_op("all-1"),
            pass(4, "nop"),
            all_op("all-2"),
            out_n,
            metrics_n,
        ];

        let mut inputs = HashMap::new();
        inputs.insert("in".into(), 0);

        let mut port_indexes = ExecPortIndexMap::new();
        // link out from
        port_indexes.insert((0, "out".into()), vec![(1, "in".into())]);
        port_indexes.insert((1, "out".into()), vec![(2, "in".into())]);
        port_indexes.insert((2, "out".into()), vec![(3, "in".into())]);
        port_indexes.insert((3, "out".into()), vec![(4, "in".into())]);

        // Create metric nodes for all elements in the graph
        let metrics = vec![
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
            NodeMetrics::default(),
        ];
        // Create state for all nodes
        let state = State {
            ops: vec![
                Value::null(),
                Value::null(),
                Value::null(),
                Value::null(),
                Value::null(),
                Value::null(),
            ],
        };
        let mut g = ExecutableGraph {
            id: "test".into(),
            graph,
            state,
            inputs,
            stack: vec![],
            signalflow: vec![2, 3],
            contraflow: vec![3, 2],
            port_indexes,
            metrics,
            // The index of the metrics node in our pipeline
            metrics_idx: 5,
            last_metrics: 0,
            metric_interval: Some(1),
            insights: vec![],
            source: None,
            dot: String::from(""),
        };
        assert!(g.optimize().is_some());
        // Test with one event
        let e = Event::default();
        let mut returns = Vec::new();
        g.enqueue("in", e, &mut returns).unwrap();
        assert_eq!(returns.len(), 1);
        returns.clear();

        // check that the Input was moved from 0 to 1, skipping the input
        assert_eq!(g.inputs.len(), 1);
        assert_eq!(g.inputs.get("in"), Some(&1));

        // check that node 2, a passthrough, was optimized out
        assert_eq!(
            g.port_indexes.get(&(1usize, OUT)),
            Some(&vec![(3usize, IN)])
        );
        assert_eq!(
            g.port_indexes.get(&(3usize, OUT)),
            Some(&vec![(4usize, IN)])
        );
    }
}
