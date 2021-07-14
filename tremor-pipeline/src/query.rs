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

use crate::{
    common_cow,
    errors::{Error, ErrorKind, Result},
    op::{
        self,
        identity::PassthroughFactory,
        prelude::{ERR, IN, METRICS, OUT},
        trickle::{
            operator::TrickleOperator,
            script::Script,
            select::{Groups, TrickleSelect, WindowImpl},
            simple_select::SimpleSelect,
        },
    },
    ConfigGraph, Connection, NodeConfig, NodeKind, Operator, OperatorNode, PortIndexMap,
};
use beef::Cow;
use halfbrown::HashMap;
use indexmap::IndexMap;
use petgraph::algo::is_cyclic_directed;
use tremor_common::ids::OperatorIdGen;
use tremor_script::{
    ast::{
        BaseExpr, CompilationUnit, Ident, NodeMetas, Select, SelectType, Stmt, WindowDecl,
        WindowKind,
    },
    errors::{
        query_node_duplicate_name_err, query_node_reserved_name_err, query_stream_not_defined_err,
        CompilerError,
    },
    highlighter::{Dumb, Highlighter},
    path::ModulePath,
    prelude::*,
    srs, AggrRegistry, Registry, Value,
};

const BUILTIN_NODES: [(Cow<'static, str>, NodeKind); 4] = [
    (IN, NodeKind::Input),
    (OUT, NodeKind::Output(OUT)),
    (ERR, NodeKind::Output(ERR)),
    (METRICS, NodeKind::Output(METRICS)),
];

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct InputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
    pub location: tremor_script::pos::Range,
}
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OutputPort {
    pub id: Cow<'static, str>,
    pub port: Cow<'static, str>,
    pub had_port: bool,
    pub location: tremor_script::pos::Range,
}

fn resolve_input_port(port: &(Ident, Ident), meta: &NodeMetas) -> InputPort {
    InputPort {
        id: common_cow(&port.0.id),
        port: common_cow(&port.1.id),
        had_port: true,
        location: port.0.extent(meta),
    }
}

fn resolve_output_port(port: &(Ident, Ident), meta: &NodeMetas) -> OutputPort {
    OutputPort {
        id: common_cow(&port.0.id),
        port: common_cow(&port.1.id),
        had_port: true,
        location: port.0.extent(meta),
    }
}

pub(crate) fn window_decl_to_impl(d: &WindowDecl) -> Result<WindowImpl> {
    use op::trickle::select::{TumblingWindowOnNumber, TumblingWindowOnTime};
    match &d.kind {
        WindowKind::Sliding => Err("Sliding windows are not yet implemented".into()),
        WindowKind::Tumbling => {
            let script = if d.script.is_some() { Some(d) } else { None };
            let ttl = d
                .params
                .get(WindowDecl::EVICTION_PERIOD)
                .and_then(Value::as_u64);
            let max_groups = d
                .params
                .get(WindowDecl::MAX_GROUPS)
                .and_then(Value::as_u64)
                .unwrap_or(WindowImpl::DEFAULT_MAX_GROUPS);
            let emit_empty_windows = d
                .params
                .get(WindowDecl::EMIT_EMPTY_WINDOWS)
                .and_then(Value::as_bool)
                .unwrap_or(WindowImpl::DEFAULT_EMIT_EMPTY_WINDOWS);

            match (
                d.params.get(WindowDecl::INTERVAL).and_then(Value::as_u64),
                d.params.get(WindowDecl::SIZE).and_then(Value::as_u64),
            ) {
                (Some(interval), None) => Ok(WindowImpl::from(TumblingWindowOnTime::from_stmt(
                    interval,
                    emit_empty_windows,
                    max_groups,
                    ttl,
                    script,
                ))),
                (None, Some(size)) => Ok(WindowImpl::from(TumblingWindowOnNumber::from_stmt(
                    size, max_groups, ttl, script,
                ))),
                (Some(_), Some(_)) => Err(Error::from(
                    "Bad window configuration, only one of `size` or `interval` is allowed.",
                )),
                (None, None) => Err(Error::from(
                    "Bad window configuration, either `size` or `interval` is required.",
                )),
            }
        }
    }
}
/// A Tremor Query
#[derive(Clone, Debug)]
pub struct Query(pub tremor_script::query::Query);

impl Query {
    /// Fetches the ID of the query if it was provided
    pub fn id(&self) -> Option<&str> {
        self.0
            .query
            .suffix()
            .config
            .get("id")
            .and_then(ValueAccess::as_str)
    }
    /// Source of the query
    #[must_use]
    pub fn source(&self) -> &str {
        &self.0.source
    }
    /// Parse a query
    ///
    /// # Errors
    /// if the trickle script can not be parsed
    pub fn parse(
        module_path: &ModulePath,
        script: &str,
        file_name: &str,
        cus: Vec<CompilationUnit>,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, CompilerError> {
        Ok(Self(tremor_script::query::Query::parse(
            module_path,
            file_name,
            script,
            cus,
            reg,
            aggr_reg,
        )?))
    }

    /// Turn a query into a executable pipeline graph
    ///
    /// # Errors
    /// if the graph can not be turned into a pipeline
    #[allow(clippy::too_many_lines)]
    pub fn to_pipe(&self, idgen: &mut OperatorIdGen) -> Result<crate::ExecutableGraph> {
        use crate::{ExecutableGraph, NodeMetrics, State};
        use std::iter;

        let query = self.0.suffix();

        let mut pipe_graph = ConfigGraph::new();
        let mut pipe_ops = HashMap::new();
        let mut nodes: HashMap<Cow<'static, str>, _> = HashMap::new();
        let mut links: IndexMap<OutputPort, Vec<InputPort>> = IndexMap::new();
        let mut inputs = HashMap::new();
        let mut outputs: Vec<petgraph::graph::NodeIndex> = Vec::new();

        let metric_interval = query
            .config
            .get("metrics_interval_s")
            .and_then(Value::as_u64)
            .map(|i| i * 1_000_000_000);

        let pipeline_id = query
            .config
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("<generated>");

        for (name, node_kind) in &BUILTIN_NODES {
            let id = pipe_graph.add_node(NodeConfig {
                id: name.to_string(),
                kind: node_kind.clone(),
                op_type: "passthrough".to_string(),
                ..NodeConfig::default()
            });
            nodes.insert(name.clone(), id);
            let op = pipe_graph
                .raw_nodes()
                .get(id.index())
                .ok_or_else(|| Error::from("Error finding freshly added node."))
                .and_then(|node| {
                    node.weight
                        .to_op(idgen.next_id(), supported_operators, None, None, None)
                })?;
            pipe_ops.insert(id, op);
            match node_kind {
                NodeKind::Input => {
                    inputs.insert(name.clone(), id);
                }
                NodeKind::Output(_) => outputs.push(id),
                _ => {
                    return Err(format!(
                        "Builtin node {} has unsupported node kind: {:?}",
                        name, node_kind
                    )
                    .into())
                }
            }
        }

        let mut port_indexes: PortIndexMap = HashMap::new();
        let mut select_num = 0;

        let has_builtin_node_name = make_builtin_node_name_checker();

        let stmts = self.0.extract_stmts();
        for (i, stmt) in stmts.into_iter().enumerate() {
            match stmt.suffix() {
                Stmt::Select(ref select) => {
                    let s: &Select<'_> = &select.stmt;

                    if !nodes.contains_key(&s.from.0.id) {
                        return Err(query_stream_not_defined_err(
                            s,
                            &s.from.0,
                            s.from.0.id.to_string(),
                            &query.node_meta,
                        )
                        .into());
                    }
                    let e = select.stmt.extent(&select.node_meta);
                    let mut h = Dumb::new();
                    let label = h
                        .highlight_str(self.source(), "", false, Some(e))
                        .ok()
                        .map(|_| h.to_string().trim_end().to_string());

                    let select_in = InputPort {
                        id: format!("select_{}", select_num).into(),
                        port: IN,
                        had_port: false,
                        location: s.extent(&query.node_meta),
                    };
                    let select_out = OutputPort {
                        id: format!("select_{}", select_num,).into(),
                        port: OUT,
                        had_port: false,
                        location: s.extent(&query.node_meta),
                    };
                    select_num += 1;
                    let mut from = resolve_output_port(&s.from, &query.node_meta);
                    if from.id == "in" && from.port != "out" {
                        let name: Cow<'static, str> = format!("in/{}", from.port).into();
                        from.id = name.clone();
                        if !nodes.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.to_string(),
                                kind: NodeKind::Input,
                                op_type: "passthrough".to_string(),
                                ..NodeConfig::default()
                            });
                            nodes.insert(name.clone(), id);
                            let op = pipe_graph
                                .raw_nodes()
                                .get(id.index())
                                .ok_or_else(|| Error::from("Error finding freshly added node."))
                                .and_then(|node| {
                                    node.weight.to_op(
                                        idgen.next_id(),
                                        supported_operators,
                                        None,
                                        None,
                                        None,
                                    )
                                })?;
                            pipe_ops.insert(id, op);
                            inputs.insert(name, id);
                        }
                    }
                    let mut into = resolve_input_port(&s.into, &query.node_meta);
                    if into.id == "out" && into.port != "in" {
                        let name: Cow<'static, str> = format!("out/{}", into.port).into();
                        into.id = name.clone();
                        if !nodes.contains_key(&name) {
                            let id = pipe_graph.add_node(NodeConfig {
                                id: name.to_string(),
                                label: Some(name.to_string()),
                                kind: NodeKind::Output(into.port.clone()),
                                op_type: "passthrough".to_string(),
                                ..NodeConfig::default()
                            });
                            nodes.insert(name, id);
                            let op = pipe_graph
                                .raw_nodes()
                                .get(id.index())
                                .ok_or_else(|| Error::from("Error finding freshly added node."))
                                .and_then(|node| {
                                    node.weight.to_op(
                                        idgen.next_id(),
                                        supported_operators,
                                        None,
                                        None,
                                        None,
                                    )
                                })?;

                            pipe_ops.insert(id, op);
                            outputs.push(id);
                        }
                    }

                    links.entry(from).or_default().push(select_in.clone());
                    links.entry(select_out).or_default().push(into);

                    let node = NodeConfig {
                        id: select_in.id.to_string(),
                        label,
                        kind: NodeKind::Select,
                        op_type: "trickle::select".to_string(),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());

                    let mut ww = HashMap::with_capacity(query.windows.len());
                    for (name, decl) in &query.windows {
                        ww.insert(name.clone(), window_decl_to_impl(&decl)?);
                    }
                    let op = node.to_op(
                        idgen.next_id(),
                        supported_operators,
                        None,
                        Some(&stmt),
                        Some(ww),
                    )?;
                    pipe_ops.insert(id, op);
                    nodes.insert(select_in.id.clone(), id);
                    outputs.push(id);
                }
                Stmt::Stream(s) => {
                    let name = common_cow(&s.id);
                    let id = name.clone();
                    if !nodes.contains_key(&id) {
                        let node = NodeConfig {
                            id: id.to_string(),
                            kind: NodeKind::Operator,
                            op_type: "passthrough".to_string(),
                            ..NodeConfig::default()
                        };
                        let id = pipe_graph.add_node(node.clone());
                        nodes.insert(name.clone(), id);
                        let op = node.to_op(
                            idgen.next_id(),
                            supported_operators,
                            None,
                            Some(&stmt),
                            Some(HashMap::new()),
                        )?;
                        inputs.insert(name.clone(), id);
                        pipe_ops.insert(id, op);
                        outputs.push(id);
                    };
                }
                Stmt::WindowDecl(_) | Stmt::ScriptDecl(_) | Stmt::OperatorDecl(_) => {}
                Stmt::Operator(o) => {
                    if nodes.contains_key(&common_cow(&o.id)) {
                        let error_func = if has_builtin_node_name(&common_cow(&o.id)) {
                            query_node_reserved_name_err
                        } else {
                            query_node_duplicate_name_err
                        };
                        return Err(error_func(o, o.id.clone(), &query.node_meta).into());
                    }

                    let target = o.target.clone().to_string();
                    let fqon = if o.module.is_empty() {
                        target
                    } else {
                        format!("{}::{}", o.module.join("::"), target)
                    };

                    let node = NodeConfig {
                        id: o.id.to_string(),
                        kind: NodeKind::Operator,
                        op_type: "trickle::operator".to_string(),
                        ..NodeConfig::default()
                    };
                    let id = pipe_graph.add_node(node.clone());

                    let stmt_srs =
                        srs::Stmt::try_new_from_query::<&'static str, _>(&self.0.query, |query| {
                            let mut decl = query
                                .operators
                                .get(&fqon)
                                .ok_or("operator not found")?
                                .clone();
                            if let Some(Stmt::Operator(o)) = query.stmts.get(i) {
                                if let Some(params) = &o.params {
                                    if let Some(decl_params) = decl.params.as_mut() {
                                        for (k, v) in params {
                                            decl_params.insert(k.clone(), v.clone());
                                        }
                                    } else {
                                        decl.params = Some(params.clone());
                                    }
                                }
                            };
                            let inner_stmt = Stmt::OperatorDecl(decl);

                            Ok(inner_stmt)
                        })?;

                    let that = stmt_srs;
                    let op = node.to_op(
                        idgen.next_id(),
                        supported_operators,
                        None,
                        Some(&that),
                        None,
                    )?;
                    pipe_ops.insert(id, op);
                    nodes.insert(common_cow(&o.id), id);
                    outputs.push(id);
                }
                Stmt::Script(o) => {
                    if nodes.contains_key(&common_cow(&o.id)) {
                        let error_func = if has_builtin_node_name(&common_cow(&o.id)) {
                            query_node_reserved_name_err
                        } else {
                            query_node_duplicate_name_err
                        };
                        return Err(error_func(o, o.id.clone(), &query.node_meta).into());
                    }

                    let target = o.target.clone().to_string();
                    let fqsn = if o.module.is_empty() {
                        target
                    } else {
                        format!("{}::{}", o.module.join("::"), target)
                    };

                    let stmt_srs =
                        srs::Stmt::try_new_from_query::<&'static str, _>(&self.0.query, |query| {
                            let decl = query.scripts.get(&fqsn).ok_or("script not found")?.clone();
                            let inner_stmt = Stmt::ScriptDecl(Box::new(decl));
                            Ok(inner_stmt)
                        })?;

                    let label = if let Stmt::ScriptDecl(s) = stmt_srs.suffix() {
                        let e = s.extent(&query.node_meta);
                        let mut h = Dumb::new();
                        // We're trimming the code so no spaces are at the end then adding a newline
                        // to ensure we're left justified (this is a dot thing, don't question it)
                        h.highlight_str(self.source(), "", false, Some(e))
                            .ok()
                            .map(|_| format!("{}\n", h.to_string().trim_end()))
                    } else {
                        None
                    };

                    let that_defn = stmt_srs;

                    let node = NodeConfig {
                        id: o.id.to_string(),
                        kind: NodeKind::Script,
                        label,
                        op_type: "trickle::script".to_string(),
                        defn: Some(that_defn.clone()),
                        node: Some(stmt.clone()),
                        ..NodeConfig::default()
                    };

                    let id = pipe_graph.add_node(node.clone());
                    let op = node.to_op(
                        idgen.next_id(),
                        supported_operators,
                        Some(&that_defn),
                        Some(&stmt),
                        None,
                    )?;
                    pipe_ops.insert(id, op);
                    nodes.insert(common_cow(&o.id), id);
                    outputs.push(id);
                }
            };
        }

        // Link graph edges
        for (from, tos) in &links {
            for to in tos {
                let from_idx = *nodes.get(&from.id).ok_or_else(|| {
                    query_stream_not_defined_err(
                        &from.location,
                        &from.location,
                        from.id.to_string(),
                        &query.node_meta,
                    )
                })?;
                let to_idx = *nodes.get(&to.id).ok_or_else(|| {
                    query_stream_not_defined_err(
                        &to.location,
                        &to.location,
                        to.id.to_string(),
                        &query.node_meta,
                    )
                })?;

                let from_tpl = (from_idx, from.port.clone());
                let to_tpl = (to_idx, to.port.clone());
                match port_indexes.get_mut(&from_tpl) {
                    None => {
                        port_indexes.insert(from_tpl, vec![to_tpl]);
                    }
                    Some(ports) => {
                        ports.push(to_tpl);
                    }
                }
                pipe_graph.add_edge(
                    from_idx,
                    to_idx,
                    Connection {
                        from: from.port.clone(),
                        to: to.port.clone(),
                    },
                );
            }
        }

        let dot = petgraph::dot::Dot::with_attr_getters(
            &pipe_graph,
            &[],
            &|_g, _r| "".to_string(),
            &|_g, (_i, c)| match c {
                NodeConfig {
                    kind: NodeKind::Input,
                    ..
                } => r#"shape = "rarrow""#.to_string(),
                NodeConfig {
                    kind: NodeKind::Output(_),
                    ..
                } => r#"shape = "larrow""#.to_string(),
                NodeConfig {
                    kind: NodeKind::Select,
                    ..
                } => r#"shape = "box""#.to_string(),
                NodeConfig {
                    kind: NodeKind::Script,
                    ..
                } => r#"shape = "note""#.to_string(),
                NodeConfig {
                    kind: NodeKind::Operator,
                    ..
                } => "".to_string(),
            },
        );

        // iff cycles, fail and bail
        if is_cyclic_directed(&pipe_graph) {
            Err(ErrorKind::CyclicGraphError(format!("{}", dot)).into())
        } else {
            let mut i2pos = HashMap::new();
            let mut graph = Vec::new();
            // Nodes that handle contraflow
            let mut contraflow = Vec::new();
            // Nodes that handle signals
            let mut signalflow = Vec::new();
            for (i, nx) in pipe_graph.node_indices().enumerate() {
                let op = pipe_ops
                    .remove(&nx)
                    .ok_or_else(|| format!("Invalid pipeline can't find node {:?}", &nx))?;
                i2pos.insert(nx, i);
                if op.handles_contraflow() {
                    contraflow.push(i);
                }
                if op.handles_signal() {
                    signalflow.push(i);
                }
                graph.push(op);
            }
            // since contraflow is the reverse we need to reverse it.
            contraflow.reverse();

            //pub type PortIndexMap = HashMap<(NodeIndex, String), Vec<(NodeIndex, String)>>;

            let mut port_indexes2 = HashMap::new();
            for ((i1, s1), connections) in &port_indexes {
                let connections = connections
                    .iter()
                    .filter_map(|(i, s)| Some((*i2pos.get(&i)?, s.clone())))
                    .collect();
                let k = *i2pos.get(i1).ok_or_else(|| Error::from("Invalid graph"))?;
                port_indexes2.insert((k, s1.clone()), connections);
            }

            let mut inputs2: HashMap<beef::Cow<'static, str>, usize> = HashMap::new();
            for (k, idx) in &inputs {
                let v = *i2pos.get(idx).ok_or_else(|| Error::from("Invalid graph"))?;
                inputs2.insert(k.clone(), v);
            }

            let metrics_idx = *nodes
                .get(&METRICS)
                .and_then(|idx| i2pos.get(idx))
                .ok_or_else(|| Error::from("metrics node missing"))?;
            let mut exec = ExecutableGraph {
                metrics: iter::repeat(NodeMetrics::default())
                    .take(graph.len())
                    .collect(),
                stack: Vec::with_capacity(graph.len()),
                id: pipeline_id.to_string(), // TODO make configurable
                metrics_idx,
                last_metrics: 0,
                state: State::new(iter::repeat(Value::null()).take(graph.len()).collect()),
                graph,
                inputs: inputs2,
                port_indexes: port_indexes2,
                contraflow,
                signalflow,
                metric_interval,
                insights: Vec::new(),
                source: Some(self.0.source.clone()),
                dot: format!("{}", dot),
            };
            exec.optimize();

            Ok(exec)
        }
    }
}

fn select(
    operator_uid: u64,
    config: &NodeConfig,
    node: Option<&srs::Stmt>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<Box<dyn Operator>> {
    let node = node.ok_or_else(|| {
        ErrorKind::MissingOpConfig("trickle operators require a statement".into())
    })?;
    let select_type = match node.suffix() {
        tremor_script::ast::Stmt::Select(ref select) => select.complexity(),
        _ => {
            return Err(ErrorKind::PipelineError(
                "Trying to turn a non select into a select operator".into(),
            )
            .into())
        }
    };
    match select_type {
        SelectType::Passthrough => {
            let op = PassthroughFactory::new_boxed();
            op.from_node(operator_uid, config)
        }
        SelectType::Simple => Ok(Box::new(SimpleSelect::with_stmt(config.id.clone(), &node)?)),
        SelectType::Normal => {
            let groups = Groups::new();
            let windows = windows.ok_or_else(|| {
                ErrorKind::MissingOpConfig("select operators require a window mapping".into())
            })?;
            let windows: Result<Vec<(String, WindowImpl)>> =
                if let tremor_script::ast::Stmt::Select(s) = node.suffix() {
                    s.stmt
                        .windows
                        .iter()
                        .map(|w| {
                            let fqwn = w.fqwn();
                            Ok(windows
                                .get(&fqwn)
                                .map(|imp| (w.id.to_string(), imp.clone()))
                                .ok_or_else(|| {
                                    ErrorKind::BadOpConfig(format!("Unknown window: {}", &fqwn))
                                })?)
                        })
                        .collect()
                } else {
                    Err("Declared as select but isn't a select".into())
                };

            Ok(Box::new(TrickleSelect::with_stmt(
                operator_uid,
                config.id.clone(),
                &groups,
                windows?,
                &node,
            )?))
        }
    }
}

fn operator(
    operator_uid: u64,
    config: &NodeConfig,
    node: Option<&srs::Stmt>,
) -> Result<Box<dyn Operator>> {
    let node = node.ok_or_else(|| {
        ErrorKind::MissingOpConfig("trickle operators require a statement".into())
    })?;
    Ok(Box::new(TrickleOperator::with_stmt(
        operator_uid,
        config.id.clone(),
        node,
    )?))
}

fn script(
    config: &NodeConfig,
    defn: Option<&srs::Stmt>,
    node: Option<&srs::Stmt>,
) -> Result<Box<dyn Operator>> {
    let node = node.ok_or_else(|| {
        ErrorKind::MissingOpConfig("trickle operators require a statement".into())
    })?;
    Ok(Box::new(Script::with_stmt(
        config.id.clone(),
        defn.ok_or_else(|| Error::from("Script definition missing"))?,
        node,
    )?))
}
pub(crate) fn supported_operators(
    config: &NodeConfig,
    uid: u64,
    defn: Option<&srs::Stmt>,
    node: Option<&srs::Stmt>,
    windows: Option<HashMap<String, WindowImpl>>,
) -> Result<OperatorNode> {
    let name_parts: Vec<&str> = config.op_type.split("::").collect();

    let op: Box<dyn op::Operator> = match name_parts.as_slice() {
        ["trickle", "select"] => select(uid, config, node, windows)?,
        ["trickle", "operator"] => operator(uid, config, node)?,
        ["trickle", "script"] => script(config, defn, node)?,
        _ => crate::operator(uid, &config)?,
    };
    Ok(OperatorNode {
        uid,
        id: config.id.to_string(),
        kind: config.kind.clone(),
        op_type: config.op_type.clone(),
        op,
    })
}

pub(crate) fn make_builtin_node_name_checker() -> impl Fn(&Cow<'static, str>) -> bool {
    // saving these names for reuse
    let builtin_node_names: Vec<Cow<'static, str>> =
        BUILTIN_NODES.iter().map(|(n, _)| n.clone()).collect();
    move |name| builtin_node_names.contains(name)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn query() {
        let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
        let aggr_reg = tremor_script::aggr_registry();

        let src = "select event from in into out;";
        let query = Query::parse(
            &module_path,
            src,
            "<test>",
            Vec::new(),
            &*crate::FN_REGISTRY.lock().unwrap(),
            &aggr_reg,
        )
        .unwrap();
        assert!(query.id().is_none());
        assert_eq!(query.source().trim_end(), src);

        // check that we can overwrite the id with a config variable
        let src = "#!config id = \"test\"\nselect event from in into out;";
        let query = Query::parse(
            &module_path,
            src,
            "<test>",
            Vec::new(),
            &*crate::FN_REGISTRY.lock().unwrap(),
            &aggr_reg,
        )
        .unwrap();
        assert_eq!(query.id().unwrap(), "test");
        assert_eq!(query.source().trim_end(), src);
    }

    #[test]
    fn custom_port() {
        let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
        let aggr_reg = tremor_script::aggr_registry();

        let src = "select event from in/test_in into out/test_out;";
        let q = Query::parse(
            &module_path,
            src,
            "<test>",
            Vec::new(),
            &*crate::FN_REGISTRY.lock().unwrap(),
            &aggr_reg,
        )
        .unwrap();

        let mut idgen = OperatorIdGen::new();
        let first = idgen.next_id();
        let g = q.to_pipe(&mut idgen).unwrap();
        assert!(g.inputs.contains_key("in/test_in"));
        assert_eq!(idgen.next_id(), first + g.graph.len() as u64 + 1);
        let out = g.graph.get(5).unwrap();
        assert_eq!(out.id, "out/test_out");
        assert_eq!(out.kind, NodeKind::Output("test_out".into()));
    }

    #[test]
    fn builtin_nodes() {
        let has_builtin_node_name = make_builtin_node_name_checker();
        assert!(has_builtin_node_name(&"in".into()));
        assert!(has_builtin_node_name(&"out".into()));
        assert!(has_builtin_node_name(&"err".into()));
        assert!(has_builtin_node_name(&"metrics".into()));
        assert!(!has_builtin_node_name(&"snot".into()));
        assert!(!has_builtin_node_name(&"badger".into()));
    }
}
