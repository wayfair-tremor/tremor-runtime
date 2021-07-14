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

pub(crate) mod analyzer;
/// Base definition for expressions
pub mod base_expr;
pub(crate) mod binary;
/// custom equality definition - checking for equivalence of different AST nodes
/// e.g. two different event paths with different metadata
pub mod eq;
/// Query AST
pub mod query;
pub(crate) mod raw;
mod support;
mod to_static;
mod upable;
/// collection of AST visitors
pub mod visitors;

use self::eq::AstEq;
use crate::interpreter::{exec_binary, exec_unary, AggrType, Cont, Env, ExecOpts, LocalStack};
pub use crate::lexer::CompilationUnit;
use crate::pos::{Location, Range};
use crate::prelude::*;
use crate::registry::FResult;
use crate::registry::{
    Aggr as AggrRegistry, CustomFn, Registry, TremorAggrFnWrapper, TremorFnWrapper,
};
use crate::script::Return;
use crate::{
    errors::{error_generic, error_no_consts, error_no_locals, ErrorKind, Result},
    impl_expr_ex_mid, impl_expr_mid, stry,
    tilde::Extractor,
    EventContext, KnownKey, Value, NO_AGGRS, NO_CONSTS,
};
pub(crate) use analyzer::*;
pub use base_expr::BaseExpr;
use beef::Cow;
use halfbrown::HashMap;
pub use query::*;
use raw::reduce2;
use serde::Serialize;

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
};

use upable::Upable;

use self::{
    binary::extend_bytes_from_value,
    raw::{BytesDataType, Endian},
};

pub(crate) type Exprs<'script> = Vec<Expr<'script>>;
/// A list of lexical compilation units
pub type Imports = Vec<LexicalUnit>;
/// A list of immutable expressions
pub type ImutExprs<'script> = Vec<ImutExpr<'script>>;
pub(crate) type Fields<'script> = Vec<Field<'script>>;
pub(crate) type Segments<'script> = Vec<Segment<'script>>;
pub(crate) type PatternFields<'script> = Vec<PredicatePattern<'script>>;
pub(crate) type Predicates<'script, Ex> = Vec<ClauseGroup<'script, Ex>>;
pub(crate) type PatchOperations<'script> = Vec<PatchOperation<'script>>;
pub(crate) type ComprehensionCases<'script, Ex> = Vec<ComprehensionCase<'script, Ex>>;
pub(crate) type ArrayPredicatePatterns<'script> = Vec<ArrayPredicatePattern<'script>>;
/// A vector of statements
pub type Stmts<'script> = Vec<Stmt<'script>>;

/// A generalisation of both mutable and imutable exressions
pub trait Expression: Clone + std::fmt::Debug + PartialEq + Serialize {
    /// replaces the last shadow
    fn replace_last_shadow_use(&mut self, replace_idx: usize);

    /// tests if the expression is a null literal
    fn is_null_lit(&self) -> bool;

    /// a null literal
    fn null_lit() -> Self;
}

fn shadow_name(id: usize) -> String {
    format!(" __SHADOW {}__ ", id)
}

#[derive(Default, Clone, Serialize, Debug, PartialEq)]
struct NodeMeta {
    start: Location,
    end: Location,
    name: Option<String>,
    /// Id of current compilation unit part
    cu: usize,
    terminal: bool,
}

impl From<(Location, Location, usize)> for NodeMeta {
    fn from((start, end, cu): (Location, Location, usize)) -> Self {
        Self {
            start,
            end,
            name: None,
            cu,
            terminal: false,
        }
    }
}
/// Information about node metadata
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct NodeMetas {
    nodes: Vec<NodeMeta>,
    #[serde(skip)]
    pub(crate) cus: Vec<CompilationUnit>,
}

impl<'script> NodeMetas {
    /// Initializes meta noes with a given set of
    #[must_use]
    pub fn new(cus: Vec<CompilationUnit>) -> Self {
        Self {
            nodes: Vec::new(),
            cus,
        }
    }
    pub(crate) fn add_meta(&mut self, mut start: Location, mut end: Location, cu: usize) -> usize {
        let mid = self.nodes.len();
        start.set_cu(cu);
        end.set_cu(cu);
        self.nodes.push((start, end, cu).into());
        mid
    }
    pub(crate) fn add_meta_w_name<S>(
        &mut self,
        mut start: Location,
        mut end: Location,
        name: &S,
        cu: usize,
    ) -> usize
    where
        S: ToString,
    {
        start.set_cu(cu);
        end.set_cu(cu);
        let mid = self.nodes.len();
        self.nodes.push(NodeMeta {
            start,
            end,
            cu,
            name: Some(name.to_string()),
            terminal: false,
        });
        mid
    }

    pub(crate) fn start(&self, idx: usize) -> Option<Location> {
        self.nodes.get(idx).map(|v| v.start)
    }

    pub(crate) fn end(&self, idx: usize) -> Option<Location> {
        self.nodes.get(idx).map(|v| v.end)
    }

    pub(crate) fn name(&self, idx: usize) -> Option<&str> {
        self.nodes
            .get(idx)
            .and_then(|v| v.name.as_ref())
            .map(String::as_str)
    }

    pub(crate) fn name_dflt(&self, idx: usize) -> &str {
        self.name(idx).unwrap_or("<UNKNOWN>")
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A warning generated while lexing or parsing
pub struct Warning {
    /// Outer span of the warning
    pub outer: Range,
    /// Inner span of thw warning
    pub inner: Range,
    /// Warning message
    pub msg: String,
}

impl Warning {
    fn new(inner: Range, outer: Range, msg: String) -> Self {
        Self { outer, inner, msg }
    }
    fn new_with_scope(warning_scope: Range, msg: String) -> Self {
        Self::new(warning_scope, warning_scope, msg)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Function<'script> {
    is_const: bool,
    argc: usize,
    name: Cow<'script, str>,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
/// A section of a binary
pub struct BytesPart<'script> {
    /// metadata id
    pub mid: usize,
    /// data
    pub data: ImutExpr<'script>,
    /// type we want to convert this to
    pub data_type: BytesDataType,
    /// Endianness
    pub endianess: Endian,
    /// bits allocated for this
    pub bits: u64,
}
impl_expr_mid!(BytesPart);

/// Binary semiliteral
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Bytes<'script> {
    mid: usize,
    /// Bytes
    pub value: Vec<BytesPart<'script>>,
}
impl_expr_mid!(Bytes);

impl<'script> Bytes<'script> {
    fn try_reduce(mut self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        self.value = self
            .value
            .into_iter()
            .map(|mut v| {
                v.data = v.data.0.try_reduce(helper).map(ImutExpr)?;
                Ok(v)
            })
            .collect::<Result<Vec<BytesPart>>>()?;
        if self.value.iter().all(|v| v.data.0.is_lit()) {
            let mut bytes: Vec<u8> = Vec::with_capacity(self.value.len());
            let outer = self.extent(&helper.meta);
            let mut used = 0;
            let mut buf = 0;

            for part in self.value {
                let inner = part.extent(&helper.meta);
                let value = reduce2(part.data.0, &helper)?;
                extend_bytes_from_value(
                    &outer,
                    &inner,
                    &helper.meta,
                    part.data_type,
                    part.endianess,
                    part.bits,
                    &mut buf,
                    &mut used,
                    &mut bytes,
                    &value,
                )?;
            }
            if used > 0 {
                bytes.push(buf >> (8 - used))
            }
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::Bytes(bytes.into()),
            }))
        } else {
            Ok(ImutExprInt::Bytes(self))
        }
    }
}

/// Documentation from constant
#[derive(Debug, Clone, PartialEq)]
pub struct ConstDoc {
    /// Constant name
    pub name: String,
    /// Constant documentation
    pub doc: Option<String>,
    /// Constant value type
    pub value_type: ValueType,
}

impl ToString for ConstDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}

*type*: {:?}

{}
"#,
            self.name,
            self.value_type,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from function
#[derive(Debug, Clone, PartialEq)]
pub struct FnDoc {
    /// Function name
    pub name: String,
    /// Function arguments
    pub args: Vec<String>,
    /// Function documentation
    pub doc: Option<String>,
    /// Whether the function is open or not
    // TODO clarify what open exactly is
    pub open: bool,
}

impl ToString for FnDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}({})

{}
"#,
            self.name,
            self.args.join(", "),
            self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModDoc {
    /// Module name
    pub name: String,
    /// Module documentation
    pub doc: Option<String>,
}

impl ModDoc {
    /// Prints the module documentation
    #[must_use]
    pub fn print_with_name(&self, name: &str) -> String {
        format!(
            r#"
# {}

{}
"#,
            name,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq)]
pub struct Docs {
    /// Constants
    pub consts: Vec<ConstDoc>,
    /// Functions
    pub fns: Vec<FnDoc>,
    /// Module level documentation
    pub module: Option<ModDoc>,
}

impl Default for Docs {
    fn default() -> Self {
        Self {
            consts: Vec::new(),
            fns: Vec::new(),
            module: None,
        }
    }
}

/// Constants and special keyword values
pub struct RunConsts<'run, 'script>
where
    'script: 'run,
{
    names: &'run HashMap<Vec<String>, usize>,
    values: &'run Vec<Value<'script>>,
    /// the `args` keyword
    pub args: &'run Value<'script>,
    /// the `group` keyword
    pub group: &'run Value<'script>,
    /// the `window` keyword
    pub window: &'run Value<'script>,
}

impl<'run, 'script> RunConsts<'run, 'script>
where
    'script: 'run,
{
    pub(crate) fn get(&self, idx: usize) -> Option<&Value<'script>> {
        self.values.get(idx)
    }

    pub(crate) fn with_new_args<'r>(&'r self, args: &'r Value<'script>) -> RunConsts<'r, 'script>
    where
        'run: 'r,
    {
        RunConsts {
            names: self.names,
            values: self.values,
            args,
            group: self.group,
            window: self.window,
        }
    }
}

/// Constants and special keyword values
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Consts<'script> {
    names: HashMap<Vec<String>, usize>,
    values: Vec<Value<'script>>,
    // always present 'special' constants
    /// the `args` keyword
    pub args: Value<'script>,
    /// the `group` keyword
    pub group: Value<'script>,
    /// the `window` keyword
    pub window: Value<'script>,
}

impl<'script> Consts<'script> {
    /// Generates runtime borrow of the costs
    #[must_use]
    pub fn run(&self) -> RunConsts<'_, 'script> {
        RunConsts {
            names: &self.names,
            values: &self.values,
            args: &self.args,
            group: &self.group,
            window: &self.window,
        }
    }
    pub(crate) fn new() -> Self {
        Consts {
            names: HashMap::new(),
            values: Vec::new(),
            args: Value::const_null(),
            group: Value::const_null(),
            window: Value::const_null(),
        }
    }
    fn is_const(&self, id: &[String]) -> Option<&usize> {
        self.names.get(id)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn insert(
        &mut self,
        name_v: Vec<String>,
        val: Value<'script>,
    ) -> std::result::Result<usize, usize> {
        let idx = self.values.len();

        self.names.insert(name_v, idx).map_or_else(
            || {
                self.values.push(val);
                Ok(idx)
            },
            Err,
        )
    }
    pub(crate) fn get(&self, idx: usize) -> Option<&Value<'script>> {
        self.values.get(idx)
    }
}

/// ordered collection of warnings
pub type Warnings = std::collections::BTreeSet<Warning>;

/// don't use
#[allow(clippy::struct_excessive_bools)]
pub struct Helper<'script, 'registry>
where
    'script: 'registry,
{
    reg: &'registry Registry,
    aggr_reg: &'registry AggrRegistry,
    can_emit: bool,
    is_in_aggr: bool,
    windows: HashMap<String, WindowDecl<'script>>,
    scripts: HashMap<String, ScriptDecl<'script>>,
    operators: HashMap<String, OperatorDecl<'script>>,
    aggregates: Vec<InvokeAggrFn<'script>>,
    /// Warnings
    pub warnings: Warnings,
    shadowed_vars: Vec<String>,
    func_vec: Vec<CustomFn<'script>>,
    pub(crate) locals: HashMap<String, usize>,
    pub(crate) functions: HashMap<Vec<String>, usize>,
    pub(crate) consts: Consts<'script>,
    pub(crate) meta: NodeMetas,
    docs: Docs,
    module: Vec<String>,
    possible_leaf: bool,
    fn_argc: usize,
    is_open: bool,
    file_offset: Location,
    cu: usize,
}

impl<'script, 'registry> Helper<'script, 'registry>
where
    'script: 'registry,
{
    fn is_const(&self, id: &[String]) -> Option<&usize> {
        self.consts.is_const(id)
    }

    fn add_const_doc<N: ToString>(
        &mut self,
        name: &N,
        doc: Option<Vec<Cow<'script, str>>>,
        value_type: ValueType,
    ) {
        let doc = doc.map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n"));
        self.docs.consts.push(ConstDoc {
            name: name.to_string(),
            doc,
            value_type,
        })
    }
    pub(crate) fn add_meta(&mut self, start: Location, end: Location) -> usize {
        self.meta
            .add_meta(start - self.file_offset, end - self.file_offset, self.cu)
    }
    pub(crate) fn add_meta_w_name<S>(&mut self, start: Location, end: Location, name: &S) -> usize
    where
        S: ToString,
    {
        self.meta.add_meta_w_name(
            start - self.file_offset,
            end - self.file_offset,
            name,
            self.cu,
        )
    }
    pub(crate) fn has_locals(&self) -> bool {
        self.locals
            .iter()
            .any(|(n, _)| !n.starts_with(" __SHADOW "))
    }

    pub(crate) fn swap(
        &mut self,
        aggregates: &mut Vec<InvokeAggrFn<'script>>,
        locals: &mut HashMap<String, usize>,
    ) {
        mem::swap(&mut self.aggregates, aggregates);
        mem::swap(&mut self.locals, locals);
    }

    pub(crate) fn new(
        reg: &'registry Registry,
        aggr_reg: &'registry AggrRegistry,
        cus: Vec<crate::lexer::CompilationUnit>,
    ) -> Self {
        Helper {
            reg,
            aggr_reg,
            can_emit: true,
            is_in_aggr: false,
            windows: HashMap::new(),
            scripts: HashMap::new(),
            operators: HashMap::new(),
            aggregates: Vec::new(),
            warnings: BTreeSet::new(),
            locals: HashMap::new(),
            consts: Consts::default(),
            functions: HashMap::new(),
            func_vec: Vec::new(),
            shadowed_vars: Vec::new(),
            meta: NodeMetas::new(cus),
            docs: Docs::default(),
            module: Vec::new(),
            possible_leaf: false,
            fn_argc: 0,
            is_open: false,
            file_offset: Location::default(),
            cu: 0,
        }
    }

    fn register_fun(&mut self, f: CustomFn<'script>) -> Result<usize> {
        let i = self.func_vec.len();
        let mut mf = self.module.clone();
        mf.push(f.name.clone().to_string());

        if self.functions.insert(mf, i).is_none() {
            self.func_vec.push(f);
            Ok(i)
        } else {
            Err(format!("function {} already defined.", f.name).into())
        }
    }

    fn register_shadow_var(&mut self, id: &str) -> usize {
        let r = self.reserve_shadow();
        self.shadowed_vars.push(id.to_string());
        r
    }

    fn end_shadow_var(&mut self) {
        self.shadowed_vars.pop();
    }

    fn find_shadow_var(&self, id: &str) -> Option<String> {
        let mut r = None;
        for (i, s) in self.shadowed_vars.iter().enumerate() {
            if s == id {
                //TODO: make sure we never overwrite this,
                r = Some(shadow_name(i))
            }
        }
        r
    }

    fn reserve_shadow(&mut self) -> usize {
        self.var_id(&shadow_name(self.shadowed_vars.len()))
    }

    fn reserve_2_shadow(&mut self) -> (usize, usize) {
        let l = self.shadowed_vars.len();
        let n1 = shadow_name(l);
        let n2 = shadow_name(l + 1);
        (self.var_id(&n1), self.var_id(&n2))
    }

    fn var_id(&mut self, id: &str) -> usize {
        let id = self.find_shadow_var(id).unwrap_or_else(|| id.to_string());

        self.locals.get(id.as_str()).copied().unwrap_or_else(|| {
            self.locals.insert(id.to_string(), self.locals.len());
            self.locals.len() - 1
        })
    }

    fn warn(&mut self, warning: Warning) {
        self.warnings.insert(warning);
    }
}

/// A tremor script instance
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Script<'script> {
    /// Import definitions
    pub imports: Imports,
    /// Expressions of the script
    pub exprs: Exprs<'script>,
    /// Constants defined in this script
    pub consts: Consts<'script>,
    /// Aggregate functions
    pub aggregates: Vec<InvokeAggrFn<'script>>,
    windows: HashMap<String, WindowDecl<'script>>,
    functions: Vec<CustomFn<'script>>,
    /// Locals
    pub locals: usize,
    /// Node metadata
    pub node_meta: NodeMetas,
    #[serde(skip)]
    /// Documentation from the script
    pub docs: Docs,
}

impl<'script> Script<'script> {
    /// Runs the script and evaluates to a resulting event.
    /// This expects the script to be imutable!
    ///
    /// # Errors
    /// on runtime errors or if it isn't an imutable script
    pub fn run_imut<'event>(
        &self,
        context: &crate::EventContext,
        aggr: AggrType,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
    ) -> Result<Return<'event>>
    where
        'script: 'event,
    {
        let local = LocalStack::with_size(self.locals);

        let opts = ExecOpts {
            result_needed: true,
            aggr,
        };

        let env = Env {
            context,
            consts: self.consts.run(),
            aggrs: &self.aggregates,
            meta: &self.node_meta,
            recursion_limit: crate::recursion_limit(),
        };

        self.exprs.last().map_or(Ok(Return::Drop), |expr| {
            if let Expr::Imut(imut) = expr {
                let v = stry!(imut.run(opts.with_result(), &env, event, state, meta, &local));
                Ok(Return::Emit {
                    value: v.into_owned(),
                    port: None,
                })
            } else {
                let e = expr.extent(&self.node_meta);
                error_generic(
                    &e.expand_lines(2),
                    expr,
                    &"Not an imutable expression",
                    &self.node_meta,
                )
            }
        })
    }
    /// Runs the script and evaluates to a resulting event
    ///
    /// # Errors
    /// on runtime errors
    pub fn run<'event>(
        &self,
        context: &crate::EventContext,
        aggr: AggrType,
        event: &mut Value<'event>,
        state: &mut Value<'static>,
        meta: &mut Value<'event>,
    ) -> Result<Return<'event>>
    where
        'script: 'event,
    {
        let mut local = LocalStack::with_size(self.locals);

        let mut exprs = self.exprs.iter().peekable();
        let opts = ExecOpts {
            result_needed: true,
            aggr,
        };

        let env = Env {
            context,
            consts: self.consts.run(),
            aggrs: &self.aggregates,
            meta: &self.node_meta,
            recursion_limit: crate::recursion_limit(),
        };

        while let Some(expr) = exprs.next() {
            if exprs.peek().is_none() {
                match stry!(expr.run(opts.with_result(), &env, event, state, meta, &mut local)) {
                    Cont::Drop => return Ok(Return::Drop),
                    Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                    Cont::EmitEvent(port) => {
                        return Ok(Return::EmitEvent { port });
                    }
                    Cont::Cont(v) => {
                        return Ok(Return::Emit {
                            value: v.into_owned(),
                            port: None,
                        })
                    }
                }
            }
            match stry!(expr.run(opts.without_result(), &env, event, state, meta, &mut local)) {
                Cont::Drop => return Ok(Return::Drop),
                Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                Cont::EmitEvent(port) => {
                    return Ok(Return::EmitEvent { port });
                }
                Cont::Cont(_v) => (),
            }
        }

        // We never reach here but rust can't figure that out
        Ok(Return::Drop)
    }
}

/// A lexical compilation unit
#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum LexicalUnit {
    /// Import declaration with no alias
    NakedImportDecl(Vec<raw::IdentRaw<'static>>),
    /// Import declaration with an alias
    AliasedImportDecl(Vec<raw::IdentRaw<'static>>, raw::IdentRaw<'static>),
    /// Line directive with embedded "<string> <num> ;"
    LineDirective(String),
}
// impl_expr_mid!(Ident);

/// An ident
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct Ident<'script> {
    pub(crate) mid: usize,
    /// the text of the ident
    pub id: beef::Cow<'script, str>,
}
impl_expr_mid!(Ident);

impl<'script> std::fmt::Display for Ident<'script> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl<'script> From<&'script str> for Ident<'script> {
    fn from(id: &'script str) -> Self {
        Self {
            mid: 0,
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a record structure field
pub struct Field<'script> {
    /// Id
    pub mid: usize,
    /// Name of the field
    pub name: StringLit<'script>,
    /// Value expression for the field
    pub value: ImutExprInt<'script>,
}
impl_expr_mid!(Field);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a record structure
pub struct Record<'script> {
    /// Id
    pub mid: usize,
    /// base (or static part of the cecord)
    pub base: crate::Object<'script>,
    /// Fields of this record
    pub fields: Fields<'script>,
}
impl_expr_mid!(Record);
impl<'script> Record<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        let (base, fields): (Vec<_>, Vec<_>) = self
            .fields
            .into_iter()
            .partition(|f| f.name.is_lit() && f.value.is_lit());

        let base: Result<crate::Object> = base
            .into_iter()
            .map(|f| {
                let n = f.name.try_into_cow()?;
                reduce2(f.value, &helper).map(|v| (n, v))
            })
            .collect();
        let base = base?;

        if fields.is_empty() {
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::from(base),
            }))
        } else {
            Ok(ImutExprInt::Record(Record {
                mid: self.mid,
                base,
                fields,
            }))
        }
    }

    /// Gets the expression for a given name
    #[must_use]
    pub fn get_field_expr(&self, name: &str) -> Option<&ImutExprInt> {
        self.fields.iter().find_map(|f| {
            f.name
                .as_str()
                .and_then(|n| if n == name { Some(&f.value) } else { None })
        })
    }
    /// Tries to fetch a literal from a record
    #[must_use]
    pub fn get_literal(&self, name: &str) -> Option<&Value> {
        if let ImutExprInt::Literal(Literal { value, .. }) = self.get_field_expr(name)? {
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulation of a list structure
pub struct List<'script> {
    /// Id
    pub mid: usize,
    /// Value expressions for list elements of this list
    pub exprs: ImutExprs<'script>,
}
impl_expr_mid!(List);

impl<'script> List<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        if self.exprs.iter().map(|v| &v.0).all(ImutExprInt::is_lit) {
            let elements: Result<Vec<Value>> = self
                .exprs
                .into_iter()
                .map(|v| reduce2(v.0, &helper))
                .collect();
            Ok(ImutExprInt::Literal(Literal {
                mid: self.mid,
                value: Value::from(elements?),
            }))
        } else {
            Ok(ImutExprInt::List(self))
        }
    }
}

/// A Literal
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct Literal<'script> {
    /// Id
    pub mid: usize,
    /// Literal value
    pub value: Value<'script>,
}
impl_expr_mid!(Literal);

/// Damn you public interfaces
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDecl<'script> {
    pub(crate) mid: usize,
    pub(crate) name: Ident<'script>,
    pub(crate) args: Vec<Ident<'script>>,
    pub(crate) body: Exprs<'script>,
    pub(crate) locals: usize,
    pub(crate) open: bool,
    pub(crate) inline: bool,
}
impl_expr_mid!(FnDecl);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Legal expression forms
pub enum Expr<'script> {
    /// Match expression
    Match(Box<Match<'script, Self>>),
    /// IfElse style match expression
    IfElse(Box<IfElse<'script, Self>>),
    /// In place patch expression
    PatchInPlace(Box<Patch<'script>>),
    /// In place merge expression
    MergeInPlace(Box<Merge<'script>>),
    /// Assignment expression
    Assign {
        /// Id
        mid: usize,
        /// Target
        path: Path<'script>,
        /// Value expression
        expr: Box<Self>,
    },
    /// Assignment from local expression
    AssignMoveLocal {
        /// Id
        mid: usize,
        /// Target
        path: Path<'script>,
        /// Local Index
        idx: usize,
    },
    /// A structure comprehension
    Comprehension(Box<Comprehension<'script, Self>>),
    /// A drop expression
    Drop {
        /// Id
        mid: usize,
    },
    /// An emit expression
    Emit(Box<EmitExpr<'script>>),
    /// An immutable expression
    Imut(ImutExprInt<'script>),
}

impl<'script> Expression for Expr<'script> {
    fn replace_last_shadow_use(&mut self, replace_idx: usize) {
        match self {
            Expr::Assign { path, expr, mid } => match expr.as_ref() {
                Expr::Imut(ImutExprInt::Local { idx, .. }) if idx == &replace_idx => {
                    *self = Expr::AssignMoveLocal {
                        mid: *mid,
                        idx: *idx,
                        path: path.clone(),
                    };
                }
                _ => (),
            },
            Expr::Match(m) => {
                // In each pattern we can replace the use in the last assign
                for cg in &mut m.patterns {
                    cg.replace_last_shadow_use(replace_idx);
                }
            }
            _ => (),
        }
    }

    fn is_null_lit(&self) -> bool {
        matches!(self, Expr::Imut(ImutExprInt::Literal(Literal { value, .. })) if value.is_null())
    }

    fn null_lit() -> Self {
        Expr::Imut(ImutExprInt::Literal(Literal {
            value: Value::null(),
            mid: 0,
        }))
    }
}

impl<'script> From<ImutExprInt<'script>> for Expr<'script> {
    fn from(imut: ImutExprInt<'script>) -> Expr<'script> {
        Expr::Imut(imut)
    }
}

/// An immutable expression
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutExpr<'script>(pub ImutExprInt<'script>);
#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> Expression for ImutExpr<'script> {
    fn replace_last_shadow_use(&mut self, replace_idx: usize) {
        self.0.replace_last_shadow_use(replace_idx)
    }
    fn is_null_lit(&self) -> bool {
        self.0.is_null_lit()
    }
    fn null_lit() -> Self {
        Self(ImutExprInt::null_lit())
    }
}

impl<'script> From<Literal<'script>> for ImutExpr<'script> {
    fn from(lit: Literal<'script>) -> Self {
        Self(ImutExprInt::Literal(lit))
    }
}

#[cfg(not(tarpaulin_include))] // this is a simple passthrough
impl<'script> BaseExpr for ImutExpr<'script> {
    fn mid(&self) -> usize {
        self.0.mid()
    }

    fn s(&self, meta: &NodeMetas) -> Location {
        self.0.s(meta)
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        self.0.e(meta)
    }

    fn extent(&self, meta: &NodeMetas) -> Range {
        self.0.extent(meta)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an immutable expression
pub enum ImutExprInt<'script> {
    /// Record
    Record(Record<'script>),
    /// List
    List(List<'script>),
    /// Binary operation
    Binary(Box<BinExpr<'script>>),
    /// Unary operation
    Unary(Box<UnaryExpr<'script>>),
    /// Patch
    Patch(Box<Patch<'script>>),
    /// Match
    Match(Box<Match<'script, ImutExprInt<'script>>>),
    /// Comprehension
    Comprehension(Box<Comprehension<'script, Self>>),
    /// Merge
    Merge(Box<Merge<'script>>),
    /// Path
    Path(Path<'script>),
    /// A string literal
    String(StringLit<'script>),
    /// Local - local variable
    Local {
        /// Local Index
        idx: usize,
        /// Id
        mid: usize,
        /// True, if it is declared constant
        is_const: bool,
    },
    /// Literal
    Literal(Literal<'script>),
    /// Presence
    Present {
        /// Path
        path: Path<'script>,
        /// Id
        mid: usize,
    },
    /// Function invocation
    Invoke1(Invoke<'script>),
    /// Function invocation
    Invoke2(Invoke<'script>),
    /// Function invocation
    Invoke3(Invoke<'script>),
    /// Function invocation
    Invoke(Invoke<'script>),
    /// Aggregate Function invocation
    InvokeAggr(InvokeAggr),
    /// Tail-Recursion
    Recur(Recur<'script>),
    /// Bytes
    Bytes(Bytes<'script>),
}

impl<'script> ImutExprInt<'script> {
    pub(crate) fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<Self> {
        match self {
            ImutExprInt::Unary(u) => u.try_reduce(helper),
            ImutExprInt::Bytes(b) => b.try_reduce(helper),
            ImutExprInt::Binary(b) => b.try_reduce(helper),
            ImutExprInt::List(l) => l.try_reduce(helper),
            ImutExprInt::Record(r) => r.try_reduce(helper),
            ImutExprInt::Path(p) => Ok(p.try_reduce(helper)),
            ImutExprInt::String(s) => Ok(s.try_reduce(helper)),
            ImutExprInt::Invoke1(i)
            | ImutExprInt::Invoke2(i)
            | ImutExprInt::Invoke3(i)
            | ImutExprInt::Invoke(i) => i.try_reduce(helper),
            other => Ok(other),
        }
    }
}

impl<'script> Expression for ImutExprInt<'script> {
    #[cfg(not(tarpaulin_include))] // this has no function
    fn replace_last_shadow_use(&mut self, replace_idx: usize) {
        if let ImutExprInt::Match(m) = self {
            // In each pattern we can replace the use in the last assign
            for cg in &mut m.patterns {
                cg.replace_last_shadow_use(replace_idx);
            }
        }
    }
    fn is_null_lit(&self) -> bool {
        matches!(self, ImutExprInt::Literal(Literal { value, .. }) if value.is_null())
    }
    fn null_lit() -> Self {
        Self::Literal(Literal::default())
    }
}

/// A string literal with interpolation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLit<'script> {
    /// Id
    pub mid: usize,
    /// Elements
    pub elements: StrLitElements<'script>,
}

impl<'s> From<&'s str> for StringLit<'s> {
    fn from(s: &'s str) -> Self {
        Self {
            mid: 0,
            elements: vec![StrLitElement::Lit(s.into())],
        }
    }
}

impl<'script> StringLit<'script> {
    pub(crate) fn is_lit(&self) -> bool {
        matches!(self.elements.as_slice(), [StrLitElement::Lit(_)])
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        if let [StrLitElement::Lit(l)] = self.elements.as_slice() {
            Some(&l)
        } else {
            None
        }
    }

    pub(crate) fn try_into_cow(mut self) -> Result<Cow<'script, str>> {
        if self.elements.len() != 1 {
            Err("Not a static string".into())
        } else if let Some(StrLitElement::Lit(l)) = self.elements.pop() {
            Ok(l)
        } else {
            Err("Not a static string".into())
        }
    }

    pub(crate) fn run<'run, 'event>(
        &self,
        opts: ExecOpts,
        env: &Env<'run, 'event>,
        event: &Value<'event>,
        state: &Value<'static>,
        meta: &Value<'event>,
        local: &LocalStack<'event>,
    ) -> Result<Cow<'event, str>>
    where
        'script: 'event,
    {
        // Shortcircut when we have a 1 literal string
        if let [StrLitElement::Lit(l)] = self.elements.as_slice() {
            return Ok(l.clone());
        }
        let mut out = String::with_capacity(128);
        for e in &self.elements {
            match e {
                StrLitElement::Lit(l) => out.push_str(l),
                #[cfg(not(feature = "erlang-float-testing"))]
                StrLitElement::Expr(e) => {
                    let r = stry!(e.run(opts, env, event, state, meta, local));
                    if let Some(s) = r.as_str() {
                        out.push_str(s);
                    } else {
                        out.push_str(r.encode().as_str());
                    };
                }
                // TODO: The float scenario is different in erlang and rust
                // We knowingly excluded float correctness in string interpolation
                // as we don't want to over engineer and write own format functions.
                // any suggestions are welcome
                #[cfg(feature = "erlang-float-testing")]
                #[cfg(not(tarpaulin_include))]
                crate::ast::StrLitElement::Expr(e) => {
                    let r = e.run(opts, env, event, state, meta, local)?;
                    if let Some(s) = r.as_str() {
                        out.push_str(&s);
                    } else if let Some(_f) = r.as_f64() {
                        out.push_str("42");
                    } else {
                        out.push_str(crate::utils::sorted_serialize(&r)?.as_str());
                    };
                }
            }
        }
        Ok(Cow::owned(out))
    }
    fn try_reduce(self, _helper: &Helper<'script, '_>) -> ImutExprInt<'script> {
        let StringLit { mid, mut elements } = self;
        if elements.len() == 1 {
            match elements.pop() {
                Some(StrLitElement::Lit(l)) => {
                    let value = Value::from(l);
                    return ImutExprInt::Literal(Literal { mid, value });
                }
                Some(other) => elements.push(other),
                None => (),
            }
        } else if elements.is_empty() {
            return ImutExprInt::Literal(Literal {
                mid,
                value: Value::from(""),
            });
        }
        ImutExprInt::String(StringLit { mid, elements })
    }
}
impl_expr_mid!(StringLit);

/// A part of a string literal with interpolation
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StrLitElement<'script> {
    /// A literal string
    Lit(Cow<'script, str>),
    /// An expression in a string interpolation
    Expr(ImutExprInt<'script>),
}

/// we're forced to make this pub because of lalrpop
pub type StrLitElements<'script> = Vec<StrLitElement<'script>>;

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an emit expression
pub struct EmitExpr<'script> {
    /// Id
    pub mid: usize,
    /// Value expression
    pub expr: ImutExprInt<'script>,
    /// Port name
    pub port: Option<ImutExprInt<'script>>,
}
impl_expr_mid!(EmitExpr);

#[derive(Clone, Serialize)]
/// Encapsulates a function invocation expression
pub struct Invoke<'script> {
    /// Id
    pub mid: usize,
    /// Module path
    pub module: Vec<String>,
    /// Function name
    pub fun: String,
    /// Invocable implementation
    #[serde(skip)]
    pub invocable: Invocable<'script>,
    /// Arguments
    pub args: ImutExprs<'script>,
}
impl_expr_mid!(Invoke);

impl<'script> Invoke<'script> {
    fn inline(self) -> Result<ImutExprInt<'script>> {
        self.invocable.inline(self.args, self.mid)
    }
    fn can_inline(&self) -> bool {
        self.invocable.can_inline()
    }

    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        if self.invocable.is_const() && self.args.iter().all(|f| f.0.is_lit()) {
            let ex = self.extent(&helper.meta);
            let args: Result<Vec<Value<'script>>> = self
                .args
                .into_iter()
                .map(|v| reduce2(v.0, &helper))
                .collect();
            let args = args?;
            // Construct a view into `args`, since `invoke` expects a slice of references.
            let args2: Vec<&Value<'script>> = args.iter().collect();
            let env = Env {
                context: &EventContext::default(),
                consts: NO_CONSTS.run(),
                aggrs: &NO_AGGRS,
                meta: &helper.meta,
                recursion_limit: crate::recursion_limit(),
            };

            let v = self
                .invocable
                .invoke(&env, &args2)
                .map_err(|e| e.into_err(&ex, &ex, Some(&helper.reg), &helper.meta))?
                .into_static();
            Ok(ImutExprInt::Literal(Literal {
                value: v,
                mid: self.mid,
            }))
        } else {
            Ok(match self.args.len() {
                1 => ImutExprInt::Invoke1(self),
                2 => ImutExprInt::Invoke2(self),
                3 => ImutExprInt::Invoke3(self),
                _ => ImutExprInt::Invoke(self),
            })
        }
    }
}

#[derive(Clone)]
/// An invocable expression form
pub enum Invocable<'script> {
    /// Reference to a builtin or intrinsic function
    Intrinsic(TremorFnWrapper),
    /// A user defined or standard library function
    Tremor(CustomFn<'script>),
}

impl<'script> Invocable<'script> {
    fn inline(self, args: ImutExprs<'script>, mid: usize) -> Result<ImutExprInt<'script>> {
        match self {
            Invocable::Intrinsic(_f) => Err("can't inline intrinsic".into()),
            Invocable::Tremor(f) => f.inline(args, mid),
        }
    }
    fn can_inline(&self) -> bool {
        match self {
            Invocable::Intrinsic(_f) => false,
            Invocable::Tremor(f) => f.can_inline(),
        }
    }

    fn is_const(&self) -> bool {
        match self {
            Invocable::Intrinsic(f) => f.is_const(),
            Invocable::Tremor(f) => f.is_const(),
        }
    }
    /// Invokes this invocable
    ///
    /// # Errors
    /// if the funciton fails to be invoked
    pub fn invoke<'event, 'run>(
        &'run self,
        env: &'run Env<'run, 'event>,
        args: &'run [&'run Value<'event>],
    ) -> FResult<Value<'event>>
    where
        'script: 'event,
        'event: 'run,
    {
        match self {
            Invocable::Intrinsic(f) => f.invoke(env.context, args),
            Invocable::Tremor(f) => f.invoke(env, args),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates the tail-recursion entry-point in a tail-recursive function
pub struct Recur<'script> {
    /// Id
    pub mid: usize,
    /// Arity
    pub argc: usize,
    /// True, if supports variable arguments
    pub open: bool,
    /// Capture of argument value expressions
    pub exprs: ImutExprs<'script>,
}
impl_expr_mid!(Recur);

#[derive(Clone, Serialize, PartialEq)]
/// Encapsulates an Aggregate function invocation
pub struct InvokeAggr {
    /// Id
    pub mid: usize,
    /// Module name
    pub module: String,
    /// Function name
    pub fun: String,
    /// Unique Id of this instance
    pub aggr_id: usize,
}

/// A Invocable aggregate function
#[derive(Clone, Serialize)]
pub struct InvokeAggrFn<'script> {
    pub(crate) mid: usize,
    /// The invocable function
    #[serde(skip)]
    pub invocable: TremorAggrFnWrapper,
    pub(crate) module: String,
    pub(crate) fun: String,
    /// Arguments passed to the function
    pub args: ImutExprs<'script>,
}
impl_expr_mid!(InvokeAggrFn);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a pluggable extractor expression form
pub struct TestExpr {
    /// Id
    pub mid: usize,
    /// Extractor name
    pub id: String,
    /// Extractor format
    pub test: String,
    /// Extractor plugin
    pub extractor: Extractor,
}

/// default case for a match expression
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DefaultCase<Ex: Expression> {
    /// No default case
    None,
    /// Null default case (default => null)
    Null,
    /// Many expressions
    Many {
        /// Expressions in the clause
        exprs: Vec<Ex>,
        /// last expression in the clause
        last_expr: Box<Ex>,
    },
    /// One Expression
    One(Ex),
}

/// Encapsulates a match expression form
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Match<'script, Ex: Expression + 'script> {
    /// Id
    pub mid: usize,
    /// The target of the match
    pub target: ImutExprInt<'script>,
    /// Patterns to match against the target
    pub patterns: Predicates<'script, Ex>,
    /// Default case
    pub default: DefaultCase<Ex>,
}
impl_expr_ex_mid!(Match);

/// If / Else style match
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct IfElse<'script, Ex: Expression + 'script> {
    /// Id
    pub mid: usize,
    /// The target of the match
    pub target: ImutExprInt<'script>,
    /// The if case
    pub if_clause: PredicateClause<'script, Ex>,
    /// Default/else case
    pub else_clause: DefaultCase<Ex>,
}
impl_expr_ex_mid!(IfElse);

/// Precondition for a case group
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClausePreCondition<'script> {
    /// Segments to look up (encoded as path for easier lookup)
    pub path: Path<'script>,
}

/// A group of case statements
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ClauseGroup<'script, Ex: Expression + 'script> {
    /// A simple group consisting of multiple patterns
    Simple {
        /// pre-condition for this group
        precondition: Option<ClausePreCondition<'script>>,
        /// Clauses in a group
        patterns: Vec<PredicateClause<'script, Ex>>,
    },

    /// A search tree based group
    SearchTree {
        /// pre-condition for this group
        precondition: Option<ClausePreCondition<'script>>,
        /// Clauses in a group
        tree: BTreeMap<Value<'script>, (Vec<Ex>, Ex)>,
        /// Non tree patterns
        rest: Vec<PredicateClause<'script, Ex>>,
    },
    /// A Combination of multiple groups that share a precondition
    Combined {
        /// pre-condition for this group
        precondition: Option<ClausePreCondition<'script>>,
        /// Clauses in a group
        groups: Vec<ClauseGroup<'script, Ex>>,
    },
    /// A single precondition
    Single {
        /// pre-condition for this group
        precondition: Option<ClausePreCondition<'script>>,
        /// Clauses in a group
        pattern: PredicateClause<'script, Ex>,
    },
}

// impl<'script, Ex: Expression + 'script> Default for ClauseGroup<'script, Ex> {
//     fn default() -> Self {
//         Self::Simple {
//             precondition: None,
//             patterns: Vec::new(),
//         }
//     }
// }

impl<'script, Ex: Expression + 'script> ClauseGroup<'script, Ex> {
    const MAX_OPT_RUNS: u64 = 128;
    const MIN_BTREE_SIZE: usize = 16;

    fn combinable(&self, other: &Self) -> bool {
        self.precondition().ast_eq(&other.precondition()) && self.precondition().is_some()
    }

    fn combine(&mut self, other: Self) {
        match (self, other) {
            (Self::Combined { groups, .. }, Self::Combined { groups: mut o, .. }) => {
                groups.append(&mut o)
            }
            (Self::Combined { groups, .. }, mut other) => {
                other.clear_precondition();
                groups.push(other)
            }
            (this, other) => {
                // Swap out precondition
                let mut precondition = None;
                mem::swap(&mut precondition, this.precondition_mut());
                // Set up new combined self
                let mut new = Self::Combined {
                    groups: Vec::with_capacity(2),
                    precondition,
                };
                mem::swap(&mut new, this);
                // combine old self into new self
                this.combine(new);
                // combine other into new self
                this.combine(other);
            }
        }
    }
    fn clear_precondition(&mut self) {
        *(self.precondition_mut()) = None
    }

    pub(crate) fn precondition(&self) -> Option<&ClausePreCondition<'script>> {
        match self {
            ClauseGroup::Single { precondition, .. }
            | ClauseGroup::Simple { precondition, .. }
            | ClauseGroup::SearchTree { precondition, .. }
            | ClauseGroup::Combined { precondition, .. } => precondition.as_ref(),
        }
    }

    pub(crate) fn precondition_mut(&mut self) -> &mut Option<ClausePreCondition<'script>> {
        match self {
            ClauseGroup::Single { precondition, .. }
            | ClauseGroup::Simple { precondition, .. }
            | ClauseGroup::SearchTree { precondition, .. }
            | ClauseGroup::Combined { precondition, .. } => precondition,
        }
    }

    fn replace_last_shadow_use(&mut self, replace_idx: usize) {
        match self {
            Self::Simple { patterns, .. } => {
                for PredicateClause { last_expr, .. } in patterns {
                    last_expr.replace_last_shadow_use(replace_idx)
                }
            }
            Self::SearchTree { tree, rest, .. } => {
                for p in tree.values_mut() {
                    p.1.replace_last_shadow_use(replace_idx)
                }
                for PredicateClause { last_expr, .. } in rest {
                    last_expr.replace_last_shadow_use(replace_idx)
                }
            }
            Self::Combined { groups, .. } => {
                for cg in groups {
                    cg.replace_last_shadow_use(replace_idx)
                }
            }
            Self::Single {
                pattern: PredicateClause { last_expr, .. },
                ..
            } => last_expr.replace_last_shadow_use(replace_idx),
        }
    }

    // allow this otherwise clippy complains after telling us to use matches
    #[allow(
        clippy::blocks_in_if_conditions,
        clippy::too_many_lines,
        // we allow this because of the borrow checker
        clippy::option_if_let_else
    )]
    fn optimize(&mut self, n: u64) {
        if let Self::Simple {
            patterns,
            precondition,
        } = self
        {
            if n > Self::MAX_OPT_RUNS {
                return;
            };
            let mut first_key = None;

            // if all patterns
            if patterns.iter().all(|p| {
                match p {
                    PredicateClause {
                        pattern: Pattern::Record(RecordPattern { fields, .. }),
                        mid,
                        ..
                    } if fields.len() == 1 => fields
                        .first()
                        .map(|f| {
                            // where the record key is a binary equal
                            match f {
                                PredicatePattern::Bin {
                                    kind: BinOpKind::Eq,
                                    key,
                                    ..
                                }
                                | PredicatePattern::TildeEq { key, .. } => {
                                    // and the key of this equal is the same in all patterns
                                    if let Some((first, _)) = &first_key {
                                        first == key
                                    } else {
                                        first_key = Some((key.clone(), *mid));
                                        // this is the first item so we can assume so far it's all OK
                                        true
                                    }
                                }
                                _ => false,
                            }
                        })
                        .unwrap_or_default(),
                    _ => false,
                }
            }) {
                // optimisation for:
                // match event of
                //   case %{a == "b"} =>...
                //   case %{a == "c"} =>...
                // end;
                //        TO
                // match event.a of
                //   case "b" =>...
                //   case "c" =>...
                // end;
                if let Some((key, mid)) = &first_key {
                    // We want to make sure that our key exists
                    *precondition = Some(ClausePreCondition {
                        path: Path::Local(LocalPath {
                            segments: vec![Segment::Id {
                                mid: *mid,
                                key: key.clone(),
                            }],
                            idx: 0,
                            is_const: true,
                            mid: 0,
                        }),
                    });

                    // we now have:
                    // match event.a of ...

                    for pattern in patterns {
                        let p = match pattern {
                            PredicateClause {
                                pattern: Pattern::Record(RecordPattern { fields, .. }),
                                ..
                            } => match fields.pop() {
                                Some(PredicatePattern::Bin { rhs, .. }) => Some(Pattern::Expr(rhs)),
                                Some(PredicatePattern::TildeEq { test, .. }) => {
                                    Some(Pattern::Extract(test))
                                }
                                _other => {
                                    // ALLOW: checked before in the if-condition
                                    unreachable!()
                                }
                            },
                            _ => None,
                        };

                        if let Some(p) = p {
                            pattern.pattern = p
                        }
                    }
                }
                self.optimize(n + 1);
            } else if patterns
                .iter()
                .filter(|p| {
                    matches!(
                        p,
                        PredicateClause {
                            pattern: Pattern::Expr(ImutExprInt::Literal(_)),
                            guard: None,
                            ..
                        }
                    )
                })
                .count()
                >= Self::MIN_BTREE_SIZE
            {
                // We swap out the precondition and patterns so we can construct a new self
                let mut precondition1 = None;
                mem::swap(&mut precondition1, precondition);
                let mut patterns1 = Vec::new();
                mem::swap(&mut patterns1, patterns);
                let mut rest = Vec::new();

                let mut tree = BTreeMap::new();
                for p in patterns1 {
                    match p {
                        PredicateClause {
                            pattern: Pattern::Expr(ImutExprInt::Literal(Literal { value, .. })),
                            exprs,
                            last_expr,
                            ..
                        } => {
                            tree.insert(value, (exprs, last_expr));
                        }
                        _ => rest.push(p),
                    }
                }
                *self = Self::SearchTree {
                    precondition: precondition1,
                    tree,
                    rest,
                }
            } else if patterns.len() == 1 {
                if let Some(pattern) = patterns.pop() {
                    let mut this_precondition = None;
                    mem::swap(precondition, &mut this_precondition);
                    *self = Self::Single {
                        pattern,
                        precondition: this_precondition,
                    };
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a predicate expression form
pub struct PredicateClause<'script, Ex: Expression + 'script> {
    /// Id
    pub mid: usize,
    /// Predicate pattern
    pub pattern: Pattern<'script>,
    /// Optional guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Expressions to evaluate if predicate test and guard pass
    pub exprs: Vec<Ex>,
    /// The last expression
    pub last_expr: Ex,
}

impl<'script, Ex: Expression + 'script> PredicateClause<'script, Ex> {
    fn is_exclusive_to(&self, other: &Self) -> bool {
        // If we have guards we assume they are not exclusive
        // this saves us analyzing guards
        if self.guard.is_some() || other.guard.is_some() {
            false
        } else {
            self.pattern.is_exclusive_to(&other.pattern)
        }
    }
}
impl_expr_ex_mid!(PredicateClause);

/// A group of case statements
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutClauseGroup<'script> {
    /// Clauses in a group
    pub patterns: Vec<PredicateClause<'script, ImutExpr<'script>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a path expression form
pub struct Patch<'script> {
    /// Id
    pub mid: usize,
    /// The patch target
    pub target: ImutExprInt<'script>,
    /// Operations to patch against the target
    pub operations: PatchOperations<'script>,
}
impl_expr_mid!(Patch);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates patch operation forms
pub enum PatchOperation<'script> {
    /// Insert only operation
    Insert {
        /// Field
        ident: StringLit<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Insert or update operation
    Upsert {
        /// Field
        ident: StringLit<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Update only operation
    Update {
        /// Field
        ident: StringLit<'script>,
        /// Value expression
        expr: ImutExprInt<'script>,
    },
    /// Erase operation
    Erase {
        /// Field
        ident: StringLit<'script>,
    },
    /// Copy operation
    Copy {
        /// From field
        from: StringLit<'script>,
        /// To field
        to: StringLit<'script>,
    },
    /// Move operation
    Move {
        /// Field from
        from: StringLit<'script>,
        /// Field to
        to: StringLit<'script>,
    },
    /// Merge convenience operation
    Merge {
        /// Field
        ident: StringLit<'script>,
        /// Value
        expr: ImutExprInt<'script>,
    },
    /// Tuple based merge operation
    MergeRecord {
        /// Value
        expr: ImutExprInt<'script>,
    },
    /// Merge convenience operation
    Default {
        /// Field
        ident: StringLit<'script>,
        /// Value
        expr: ImutExprInt<'script>,
    },
    /// Tuple based merge operation
    DefaultRecord {
        /// Value
        expr: ImutExprInt<'script>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a merge form
pub struct Merge<'script> {
    /// Id
    pub mid: usize,
    /// Target of the merge
    pub target: ImutExprInt<'script>,
    /// Value expression computing content to merge into the target
    pub expr: ImutExprInt<'script>,
}
impl_expr_mid!(Merge);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a structure comprehension form
pub struct Comprehension<'script, Ex: Expression + 'script> {
    /// Id
    pub mid: usize,
    /// Key binding
    pub key_id: usize,
    /// Value binding
    pub val_id: usize,
    /// Target of the comprehension
    pub target: ImutExprInt<'script>,
    /// Case applications against target elements
    pub cases: ComprehensionCases<'script, Ex>,
}
impl_expr_ex_mid!(Comprehension);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a comprehension case application
pub struct ComprehensionCase<'script, Ex: Expression + 'script> {
    /// Id
    pub mid: usize,
    /// Key binding
    pub key_name: Cow<'script, str>,
    /// Value binding
    pub value_name: Cow<'script, str>,
    /// Guard expression
    pub guard: Option<ImutExprInt<'script>>,
    /// Case application against target on passing guard
    pub exprs: Vec<Ex>,
    /// Last case application against target on passing guard
    pub last_expr: Ex,
}
impl_expr_ex_mid!(ComprehensionCase);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates predicate pattern form
pub enum Pattern<'script> {
    //Predicate(PredicatePattern<'script>),
    /// Record pattern
    Record(RecordPattern<'script>),
    /// Array pattern
    Array(ArrayPattern<'script>),
    /// Expression
    Expr(ImutExprInt<'script>),
    /// Assignment pattern
    Assign(AssignPattern<'script>),
    /// Tuple pattern
    Tuple(TuplePattern<'script>),
    /// A extractor
    Extract(Box<TestExpr>),
    /// Don't care condition
    DoNotCare,
    /// Gates if no other pattern matches
    Default,
}
impl<'script> Pattern<'script> {
    fn is_default(&self) -> bool {
        matches!(self, Pattern::Default)
            || matches!(self, Pattern::DoNotCare)
            || if let Pattern::Assign(AssignPattern { pattern, .. }) = self {
                pattern.as_ref() == &Pattern::DoNotCare
            } else {
                false
            }
    }
    fn is_assign(&self) -> bool {
        matches!(self, Pattern::Assign(_))
    }
    fn is_exclusive_to(&self, other: &Self) -> bool {
        match (self, other) {
            // Two literals that are different are distinct
            (Pattern::Expr(ImutExprInt::Literal(l1)), Pattern::Expr(ImutExprInt::Literal(l2))) => {
                l1 != l2
            }
            // For record patterns we compare directly
            (Pattern::Record(r1), Pattern::Record(r2)) => {
                r1.is_exclusive_to(r2) || r2.is_exclusive_to(r1)
            }
            // for assignments we compare internal value
            (Pattern::Assign(AssignPattern { pattern, .. }), p2) => pattern.is_exclusive_to(p2),
            (p1, Pattern::Assign(AssignPattern { pattern, .. })) => p1.is_exclusive_to(pattern),
            // else we're just not accepting equality
            (
                Pattern::Tuple(TuplePattern { exprs: exprs1, .. }),
                Pattern::Tuple(TuplePattern { exprs: exprs2, .. }),
            ) => exprs1
                .iter()
                .zip(exprs2.iter())
                .any(|(e1, e2)| e1.is_exclusive_to(e2) || e2.is_exclusive_to(e1)),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a predicate pattern form
pub enum PredicatePattern<'script> {
    /// Structural application
    TildeEq {
        /// Assignment bind point
        assign: Cow<'script, str>,
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        test: Box<TestExpr>,
    },
    /// Binary predicate
    Bin {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Rhs
        rhs: ImutExprInt<'script>,
        /// Binary operation kind
        kind: BinOpKind,
    },
    /// Record search pattern
    RecordPatternEq {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        pattern: RecordPattern<'script>,
    },
    /// Array search pattern
    ArrayPatternEq {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Predicate
        pattern: ArrayPattern<'script>,
    },
    /// Field presence
    FieldPresent {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
    },
    /// Field absence
    FieldAbsent {
        /// Lhs
        lhs: Cow<'script, str>,
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
    },
}

impl<'script> PredicatePattern<'script> {
    fn is_exclusive_to(&self, other: &Self) -> bool {
        match (self, other) {
            (
                PredicatePattern::Bin {
                    lhs: lhs1,
                    kind: BinOpKind::Eq,
                    rhs: rhs1,
                    ..
                },
                PredicatePattern::Bin {
                    lhs: lhs2,
                    kind: BinOpKind::Eq,
                    rhs: rhs2,
                    ..
                },
            ) if lhs1 == lhs2 && !rhs1.ast_eq(rhs2) => true,
            (
                PredicatePattern::Bin { lhs: lhs1, .. },
                PredicatePattern::FieldAbsent { lhs: lhs2, .. },
            ) if lhs1 == lhs2 => true,
            (
                PredicatePattern::FieldPresent { lhs: lhs1, .. },
                PredicatePattern::FieldAbsent { lhs: lhs2, .. },
            ) if lhs1 == lhs2 => true,
            (
                PredicatePattern::Bin {
                    lhs: lhs1,
                    kind: BinOpKind::Eq,
                    rhs: ImutExprInt::Literal(Literal { value, .. }),
                    ..
                },
                PredicatePattern::TildeEq {
                    lhs: lhs2, test, ..
                },
            ) if lhs1 == lhs2 => test.extractor.is_exclusive_to(value),
            (
                PredicatePattern::TildeEq {
                    lhs: lhs2, test, ..
                },
                PredicatePattern::Bin {
                    lhs: lhs1,
                    kind: BinOpKind::Eq,
                    rhs: ImutExprInt::Literal(Literal { value, .. }),
                    ..
                },
            ) if lhs1 == lhs2 => test.extractor.is_exclusive_to(value),
            (
                PredicatePattern::TildeEq {
                    lhs: lhs1,
                    test: test1,
                    ..
                },
                PredicatePattern::TildeEq {
                    lhs: lhs2,
                    test: test2,
                    ..
                },
            ) if lhs1 == lhs2 => match (test1.as_ref(), test2.as_ref()) {
                // For two prefix extractors we know that if one isn't the prefix for the
                // other they are exclusive
                (
                    TestExpr {
                        extractor: Extractor::Prefix(p1),
                        ..
                    },
                    TestExpr {
                        extractor: Extractor::Prefix(p2),
                        ..
                    },
                ) => !(p1.starts_with(p2.as_str()) || p2.starts_with(p1.as_str())),
                // For two suffix extractors we know that if one isn't the suffix for the
                // other they are exclusive
                (
                    TestExpr {
                        extractor: Extractor::Suffix(p1),
                        ..
                    },
                    TestExpr {
                        extractor: Extractor::Suffix(p2),
                        ..
                    },
                ) => !(p1.ends_with(p2.as_str()) || p2.ends_with(p1.as_str())),
                _ => false,
            },
            (_l, _r) => false,
        }
    }
    /// Get key
    #[must_use]
    pub fn key(&self) -> &KnownKey<'script> {
        use PredicatePattern::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
        };
        match self {
            TildeEq { key, .. }
            | Bin { key, .. }
            | RecordPatternEq { key, .. }
            | ArrayPatternEq { key, .. }
            | FieldPresent { key, .. }
            | FieldAbsent { key, .. } => &key,
        }
    }

    fn lhs(&self) -> &Cow<'script, str> {
        use PredicatePattern::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
        };
        match self {
            TildeEq { lhs, .. }
            | Bin { lhs, .. }
            | RecordPatternEq { lhs, .. }
            | ArrayPatternEq { lhs, .. }
            | FieldPresent { lhs, .. }
            | FieldAbsent { lhs, .. } => &lhs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a record pattern
pub struct RecordPattern<'script> {
    /// Id
    pub mid: usize,
    /// Pattern fields
    pub fields: PatternFields<'script>,
}

impl<'script> RecordPattern<'script> {
    fn is_exclusive_to(&self, other: &Self) -> bool {
        if self.fields.len() == 1 && other.fields.len() == 1 {
            self.fields
                .first()
                .and_then(|f1| Some((f1, other.fields.first()?)))
                .map(|(f1, f2)| f1.is_exclusive_to(f2) || f2.is_exclusive_to(f1))
                .unwrap_or_default()
        } else {
            false
        }
    }
}
impl_expr_mid!(RecordPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an array predicate pattern
pub enum ArrayPredicatePattern<'script> {
    /// Expression
    Expr(ImutExprInt<'script>),
    /// Tilde predicate
    Tilde(Box<TestExpr>),
    /// Nested record pattern
    Record(RecordPattern<'script>),
    /// Don't care condition
    Ignore,
}

impl<'script> ArrayPredicatePattern<'script> {
    fn is_exclusive_to(&self, other: &Self) -> bool {
        match (self, other) {
            (ArrayPredicatePattern::Record(r1), ArrayPredicatePattern::Record(r2)) => {
                r1.is_exclusive_to(r2) || r2.is_exclusive_to(r1)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an array pattern
pub struct ArrayPattern<'script> {
    /// Id
    pub mid: usize,
    /// Predicates
    pub exprs: ArrayPredicatePatterns<'script>,
}
impl_expr_mid!(ArrayPattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates an assignment pattern
pub struct AssignPattern<'script> {
    /// Bind point
    pub id: Cow<'script, str>,
    /// Local index
    pub idx: usize,
    /// Nested predicate pattern
    pub pattern: Box<Pattern<'script>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a positional tuple pattern
pub struct TuplePattern<'script> {
    /// Id
    pub mid: usize,
    /// Predicates
    pub exprs: ArrayPredicatePatterns<'script>,
    /// True, if the pattern supports variable arguments
    pub open: bool,
}
impl_expr_mid!(TuplePattern);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Represents a path-like-structure
pub enum Path<'script> {
    /// A constant path
    Const(LocalPath<'script>),
    /// A local path
    Local(LocalPath<'script>),
    /// The current event
    Event(EventPath<'script>),
    /// The captured program state, minus const and local state
    State(StatePath<'script>),
    /// Runtime type information ( meta-state )
    Meta(MetadataPath<'script>),
    /// Special reserved path
    Reserved(ReservedPath<'script>),
}

impl<'script> Path<'script> {
    /// Get segments as slice
    #[must_use]
    pub fn segments(&self) -> &Segments<'script> {
        match self {
            Path::Const(path) | Path::Local(path) => &path.segments,
            Path::Meta(path) => &path.segments,
            Path::Event(path) => &path.segments,
            Path::State(path) => &path.segments,
            Path::Reserved(path) => path.segments(),
        }
    }

    /// Get segments as slice
    #[must_use]
    pub fn segments_mut(&mut self) -> &mut Segments<'script> {
        match self {
            Path::Const(path) | Path::Local(path) => &mut path.segments,
            Path::Meta(path) => &mut path.segments,
            Path::Event(path) => &mut path.segments,
            Path::State(path) => &mut path.segments,
            Path::Reserved(path) => path.segments_mut(),
        }
    }
    fn try_reduce(self, helper: &Helper<'script, '_>) -> ImutExprInt<'script> {
        match self {
            Path::Const(LocalPath {
                is_const: true,
                segments,
                idx,
                mid,
            }) if segments.is_empty() => {
                if let Some(v) = helper.consts.get(idx) {
                    let lit = Literal {
                        mid,
                        value: v.clone(),
                    };
                    ImutExprInt::Literal(lit)
                } else {
                    // ALLOW: if something is is_const: true it is a constant.
                    unreachable!()
                }
            }
            other => ImutExprInt::Path(other),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A Path segment
pub enum Segment<'script> {
    /// An identifier
    Id {
        /// Key
        #[serde(skip)]
        key: KnownKey<'script>,
        /// Id
        mid: usize,
    },
    /// A numeric index
    Idx {
        /// Index
        idx: usize,
        /// id
        mid: usize,
    },
    /// An element
    Element {
        /// Value Expression
        expr: ImutExprInt<'script>,
        /// Id
        mid: usize,
    },
    /// A range
    Range {
        /// Lower-inclusive
        lower_mid: usize,
        /// Max-exclusive
        upper_mid: usize,
        /// Id
        mid: usize,
        /// Start of range value expression
        range_start: Box<ImutExprInt<'script>>,
        /// End of range value expression
        range_end: Box<ImutExprInt<'script>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
/// A path local to the current program
pub struct LocalPath<'script> {
    /// Local Index
    pub idx: usize,
    /// True, if declared const
    pub is_const: bool,
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(LocalPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// A metadata path
pub struct MetadataPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(MetadataPath);

/// Reserved keyword path
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ReservedPath<'script> {
    /// `args` keyword
    Args {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
    /// `window` keyword
    Window {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
    /// `group` keyword
    Group {
        /// Id
        mid: usize,
        /// Segments
        segments: Segments<'script>,
    },
}

impl<'script> ReservedPath<'script> {
    fn segments(&self) -> &Segments<'script> {
        match self {
            ReservedPath::Args { segments, .. }
            | ReservedPath::Window { segments, .. }
            | ReservedPath::Group { segments, .. } => segments,
        }
    }
    fn segments_mut(&mut self) -> &mut Segments<'script> {
        match self {
            ReservedPath::Args { segments, .. }
            | ReservedPath::Window { segments, .. }
            | ReservedPath::Group { segments, .. } => segments,
        }
    }
}

impl<'script> BaseExpr for ReservedPath<'script> {
    fn mid(&self) -> usize {
        match self {
            ReservedPath::Args { mid, .. }
            | ReservedPath::Window { mid, .. }
            | ReservedPath::Group { mid, .. } => *mid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// The path representing the current in-flight event
pub struct EventPath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(EventPath);

#[derive(Clone, Debug, PartialEq, Serialize)]
/// The path representing captured program state
pub struct StatePath<'script> {
    /// Id
    pub mid: usize,
    /// Segments
    pub segments: Segments<'script>,
}
impl_expr_mid!(StatePath);

/// we're forced to make this pub because of lalrpop
#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum BinOpKind {
    /// we're forced to make this pub because of lalrpop
    Or,
    /// we're forced to make this pub because of lalrpop
    Xor,
    /// we're forced to make this pub because of lalrpop
    And,

    /// we're forced to make this pub because of lalrpop
    BitOr,
    /// we're forced to make this pub because of lalrpop
    BitXor,
    /// we're forced to make this pub because of lalrpop
    BitAnd,

    /// we're forced to make this pub because of lalrpop
    Eq,
    /// we're forced to make this pub because of lalrpop
    NotEq,

    /// we're forced to make this pub because of lalrpop
    Gte,
    /// we're forced to make this pub because of lalrpop
    Gt,
    /// we're forced to make this pub because of lalrpop
    Lte,
    /// we're forced to make this pub because of lalrpop
    Lt,

    /// we're forced to make this pub because of lalrpop
    RBitShiftSigned,
    /// we're forced to make this pub because of lalrpop
    RBitShiftUnsigned,
    /// we're forced to make this pub because of lalrpop
    LBitShift,

    /// we're forced to make this pub because of lalrpop
    Add,
    /// we're forced to make this pub because of lalrpop
    Sub,
    /// we're forced to make this pub because of lalrpop
    Mul,
    /// we're forced to make this pub because of lalrpop
    Div,
    /// we're forced to make this pub because of lalrpop
    Mod,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a binary expression form
pub struct BinExpr<'script> {
    /// Id
    pub mid: usize,
    /// The operation kind
    pub kind: BinOpKind,
    /// The Left-hand-side operand
    pub lhs: ImutExprInt<'script>,
    /// The Right-hand-side operand
    pub rhs: ImutExprInt<'script>,
}
impl_expr_mid!(BinExpr);

impl<'script> BinExpr<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        match &self {
            BinExpr { lhs, rhs, .. } if lhs.is_lit() && rhs.is_lit() => {
                let l = reduce2(lhs.clone(), helper)?;
                let r = reduce2(rhs.clone(), helper)?;
                let value =
                    exec_binary(&self, &self, &helper.meta, self.kind, &l, &r)?.into_owned();
                let lit = Literal {
                    mid: self.mid,
                    value,
                };
                Ok(ImutExprInt::Literal(lit))
            }
            _ => Ok(ImutExprInt::Binary(Box::new(self))),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum UnaryOpKind {
    /// we're forced to make this pub because of lalrpop
    Plus,
    /// we're forced to make this pub because of lalrpop
    Minus,
    /// we're forced to make this pub because of lalrpop
    Not,
    /// we're forced to make this pub because of lalrpop
    BitNot,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Encapsulates a unary expression form
pub struct UnaryExpr<'script> {
    /// Id
    pub mid: usize,
    /// The operation kind
    pub kind: UnaryOpKind,
    /// The operand
    pub expr: ImutExprInt<'script>,
}
impl_expr_mid!(UnaryExpr);

impl<'script> UnaryExpr<'script> {
    fn try_reduce(self, helper: &Helper<'script, '_>) -> Result<ImutExprInt<'script>> {
        match &self {
            UnaryExpr {
                expr, mid, kind, ..
            } if expr.is_lit() => {
                let expr = reduce2(expr.clone(), &helper)?;
                let value = if let Some(v) = exec_unary(*kind, &expr) {
                    v.into_owned()
                } else {
                    let ex = self.extent(&helper.meta);
                    let outer = ex.expand_lines(2);
                    let e = ErrorKind::InvalidUnary(outer, ex, *kind, expr.value_type());
                    return Err(e.into());
                };

                let lit = Literal { mid: *mid, value };
                Ok(ImutExprInt::Literal(lit))
            }
            _ => Ok(ImutExprInt::Unary(Box::new(self))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ConstDoc, FnDoc, ModDoc};
    use crate::prelude::*;

    fn v(s: &'static str) -> super::ImutExprInt<'static> {
        super::ImutExprInt::Literal(super::Literal {
            mid: 0,
            value: Value::from(s),
        })
    }
    #[test]
    fn const_doc() {
        let c = ConstDoc {
            name: "const test".into(),
            doc: Some("hello".into()),
            value_type: ValueType::Null,
        };
        assert_eq!(
            c.to_string(),
            r#"
### const test

*type*: Null

hello
"#
        );
    }

    #[test]
    fn fn_doc() {
        let c = FnDoc {
            name: "fn_test".into(),
            args: vec!["snot".into(), "badger".into()],
            doc: Some("hello".into()),
            open: false,
        };
        assert_eq!(
            c.to_string(),
            r#"
### fn_test(snot, badger)

hello
"#
        );
    }

    #[test]
    fn mod_doc() {
        let c = ModDoc {
            name: "test mod".into(),
            doc: Some("hello".into()),
        };
        assert_eq!(
            c.print_with_name(&c.name),
            r#"
# test mod

hello
"#
        );
    }

    #[test]
    fn record() {
        let f1 = super::Field {
            mid: 0,
            name: "snot".into(),
            value: v("badger"),
        };
        let f2 = super::Field {
            mid: 0,
            name: "badger".into(),
            value: v("snot"),
        };

        let r = super::Record {
            base: crate::Object::new(),
            mid: 0,
            fields: vec![f1, f2],
        };

        assert_eq!(r.get_field_expr("snot"), Some(&v("badger")));
        assert_eq!(r.get_field_expr("nots"), None);

        assert_eq!(
            r.get_literal("badger").and_then(ValueAccess::as_str),
            Some("snot")
        );
        assert_eq!(r.get_field_expr("adgerb"), None);
    }
}
