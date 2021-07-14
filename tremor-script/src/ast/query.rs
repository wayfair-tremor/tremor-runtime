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

pub(crate) mod raw;
use super::{
    error_generic, error_no_consts, error_no_locals, AggrRegistry, EventPath, HashMap, Helper,
    Ident, ImutExpr, ImutExprInt, InvokeAggrFn, Location, NodeMetas, Path, Registry, Result,
    Script, Serialize, Stmts, Upable, Value, Warning,
};
use super::{raw::BaseExpr, Consts};
use crate::ast::eq::AstEq;
use crate::impl_expr_mid;
use raw::WindowDefnRaw;

/// A Tremor query
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Query<'script> {
    /// Config for the query
    pub config: HashMap<String, Value<'script>>,
    /// Statements
    pub stmts: Stmts<'script>,
    /// Query Node Metadata
    pub node_meta: NodeMetas,
    /// Window declarations
    pub windows: HashMap<String, WindowDecl<'script>>,
    /// Script declarations
    pub scripts: HashMap<String, ScriptDecl<'script>>,
    /// Operators declarations
    pub operators: HashMap<String, OperatorDecl<'script>>,
}

/// Query statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Stmt<'script> {
    /// A window declaration
    WindowDecl(Box<WindowDecl<'script>>),
    /// A stream
    Stream(StreamStmt),
    /// An operator declaration
    OperatorDecl(OperatorDecl<'script>),
    /// A script declaration
    ScriptDecl(Box<ScriptDecl<'script>>),
    /// An operator creation
    Operator(OperatorStmt<'script>),
    /// A script creation
    Script(ScriptStmt<'script>),
    /// A select statement
    Select(SelectStmt<'script>),
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for Stmt<'script> {
    fn mid(&self) -> usize {
        match self {
            Stmt::WindowDecl(s) => s.mid(),
            Stmt::Stream(s) => s.mid(),
            Stmt::OperatorDecl(s) => s.mid(),
            Stmt::ScriptDecl(s) => s.mid(),
            Stmt::Operator(s) => s.mid(),
            Stmt::Script(s) => s.mid(),
            Stmt::Select(s) => s.mid(),
        }
    }
}

/// array of aggregate functions
pub type Aggregates<'a> = Vec<InvokeAggrFn<'a>>;

/// Scratch data for efficiently merging window states in a tilt-frame without additional allocations at runtime.
/// This is basically what needs to be dragged through
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AggregateScratch {
    /// aggregate states
    pub aggregates: Aggregates<'static>,
    /// transactional state for the outgoing event from a window
    pub transactional: bool,
}

impl AggregateScratch {
    /// constructor
    #[must_use]
    pub fn new(aggregates: Aggregates<'static>) -> Self {
        Self {
            aggregates,
            transactional: false,
        }
    }
}

/// A Select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SelectStmt<'script> {
    /// The select statement
    pub stmt: Box<Select<'script>>,
    /// Aggregates
    pub aggregates: Aggregates<'static>,
    /// scratches needed when executing multiple windows
    /// only necessary if we have multiple windows, otherwise empty
    pub aggregate_scratches: Option<(AggregateScratch, AggregateScratch)>,
    /// Constants
    pub consts: Consts<'script>,
    /// Number of locals
    pub locals: usize,
    /// Node metadata nodes
    pub node_meta: NodeMetas,
}
#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for SelectStmt<'script> {
    fn mid(&self) -> usize {
        self.stmt.mid()
    }
}

/// The type of a select statement
pub enum SelectType {
    /// This select statement can be turned
    /// into a passthrough node
    Passthrough,
    /// This is a simple statement without grouping
    /// or windowing
    Simple,
    /// This is a full fledged select statement
    Normal,
}

impl SelectStmt<'_> {
    /// Determine how complex a select statement is
    #[must_use]
    pub fn complexity(&self) -> SelectType {
        if self
            .stmt
            .target
            .0
            .ast_eq(&ImutExprInt::Path(Path::Event(EventPath {
                mid: 0,
                segments: vec![],
            })))
            && self.stmt.maybe_group_by.is_none()
            && self.stmt.windows.is_empty()
        {
            if self.stmt.maybe_having.is_none() && self.stmt.maybe_where.is_none() {
                SelectType::Passthrough
            } else {
                SelectType::Simple
            }
        } else {
            SelectType::Normal
        }
    }
}

/// Operator kind identifier
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorKind {
    pub(crate) mid: usize,
    /// Module of the operator
    pub module: String,
    /// Operator name
    pub operation: String,
}

impl BaseExpr for OperatorKind {
    fn mid(&self) -> usize {
        self.mid
    }
}

/// An operator declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorDecl<'script> {
    pub(crate) mid: usize,
    /// Type of the operator
    pub kind: OperatorKind,
    /// Module of the operator
    pub module: Vec<String>,
    /// Identifer for the operator
    pub id: String,
    /// Parameters for the operator
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr_mid!(OperatorDecl);

impl<'script> OperatorDecl<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqon(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

/// An operator creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct OperatorStmt<'script> {
    pub(crate) mid: usize,
    /// Id of the operator
    pub id: String,
    /// Target of the operator
    pub target: String,
    /// Module of the script
    pub module: Vec<String>,
    /// parameters of the instance
    pub params: Option<HashMap<String, Value<'script>>>,
}
impl_expr_mid!(OperatorStmt);

/// A script declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptDecl<'script> {
    pub(crate) mid: usize,
    /// Module of the script
    pub module: Vec<String>,
    /// ID of the script
    pub id: String,
    /// Parameters of a script declaration
    pub params: Option<HashMap<String, Value<'script>>>,
    /// The script itself
    pub script: Script<'script>,
}
impl_expr_mid!(ScriptDecl);

impl<'script> ScriptDecl<'script> {
    /// Calculate the fully qualified name
    #[must_use]
    pub fn fqsn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

/// A script creation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScriptStmt<'script> {
    pub(crate) mid: usize,
    /// ID of the script
    pub id: String,
    /// Target of the script
    pub target: String,
    /// Parameters of the script statement
    pub params: Option<HashMap<String, Value<'script>>>,
    /// Module path of the script
    pub module: Vec<String>,
}
impl_expr_mid!(ScriptStmt);

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum WindowKind {
    /// we're forced to make this pub because of lalrpop
    Sliding,
    /// we're forced to make this pub because of lalrpop
    Tumbling,
}

/// A window declaration
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowDecl<'script> {
    /// metadata id
    pub(crate) mid: usize,
    /// Module of the window declaration
    pub module: Vec<String>,
    /// Name of the window declaration
    pub id: String,
    /// The type of window
    pub kind: WindowKind,
    /// Parameters passed to the window
    pub params: HashMap<String, Value<'script>>,
    /// The script of the window
    pub script: Option<Script<'script>>,
}
impl_expr_mid!(WindowDecl);

impl<'script> WindowDecl<'script> {
    /// `emit_empty_windows` setting
    pub const EMIT_EMPTY_WINDOWS: &'static str = "emit_empty_windows";
    /// `eviction_period` setting
    pub const EVICTION_PERIOD: &'static str = "eviction_period";
    /// `max_groups` setting
    pub const MAX_GROUPS: &'static str = "max_groups";
    /// `interval` setting
    pub const INTERVAL: &'static str = "interval";
    /// `size` setting
    pub const SIZE: &'static str = "size";

    /// Calculate the fully qualified window name
    #[must_use]
    pub fn fqwn(&self, module: &[String]) -> String {
        if module.is_empty() {
            self.id.clone()
        } else {
            format!("{}::{}", module.join("::"), self.id)
        }
    }
}

/// A select statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Select<'script> {
    /// MetadataID of the statement
    pub mid: usize,
    /// The from clause
    pub from: (Ident<'script>, Ident<'script>),
    /// The into claus
    pub into: (Ident<'script>, Ident<'script>),
    /// The target (select part)
    pub target: ImutExpr<'script>,
    /// Where clause
    pub maybe_where: Option<ImutExpr<'script>>,

    /// Having clause
    pub maybe_having: Option<ImutExpr<'script>>,
    /// Group-By clause
    pub maybe_group_by: Option<GroupBy<'script>>,
    /// Window
    pub windows: Vec<WindowDefnRaw<'script>>,
}
impl_expr_mid!(Select);

/// A group by clause
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GroupBy<'script>(pub(crate) GroupByInt<'script>);
#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) enum GroupByInt<'script> {
    /// Expression based group by
    Expr {
        mid: usize,
        expr: ImutExprInt<'script>,
    },
    /// `set` based group by
    Set {
        mid: usize,
        items: Vec<GroupBy<'script>>,
    },
    /// `each` based group by
    Each {
        mid: usize,
        expr: ImutExprInt<'script>,
    },
}

/// A stream statement
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamStmt {
    pub(crate) mid: usize,
    /// ID if the stream
    pub id: String,
}

impl BaseExpr for StreamStmt {
    fn mid(&self) -> usize {
        self.mid
    }
}
