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

#![doc(hidden)]
// We want to keep the names here
#![allow(clippy::module_name_repetitions)]

use super::{
    base_expr, eq::AstEq, query, ArrayPattern, ArrayPredicatePattern, AssignPattern, BinExpr,
    BinOpKind, Bytes, ClauseGroup, Comprehension, ComprehensionCase, Costly, DefaultCase, EmitExpr,
    EventPath, Expr, Expression, Field, FnDecl, FnDoc, Helper, Ident, IfElse, ImutExpr,
    ImutExprInt, Invocable, Invoke, InvokeAggr, InvokeAggrFn, List, Literal, LocalPath, Match,
    Merge, MetadataPath, ModDoc, NodeMetas, Patch, PatchOperation, Path, Pattern, PredicateClause,
    PredicatePattern, Record, RecordPattern, Recur, ReservedPath, Script, Segment, StatePath,
    StrLitElement, StringLit, TestExpr, TuplePattern, UnaryExpr, UnaryOpKind, Warning,
};
use super::{upable::Upable, BytesPart};
use crate::impl_expr;
use crate::pos::{Location, Range};
use crate::prelude::*;
use crate::registry::CustomFn;
use crate::tilde::Extractor;
use crate::{
    errors::{
        err_generic, error_generic, error_missing_effector, error_oops, Error, ErrorKind, Result,
    },
    impl_expr_exraw,
};
use crate::{KnownKey, Value};
pub use base_expr::BaseExpr;
use beef::Cow;
use halfbrown::HashMap;
pub use query::*;
use serde::Serialize;

/// A raw script we got to put this here because of silly lalrpoop focing it to be public
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScriptRaw<'script> {
    exprs: ExprsRaw<'script>,
    doc: Option<Vec<Cow<'script, str>>>,
}

impl<'script> ScriptRaw<'script> {
    pub(crate) fn new(exprs: ExprsRaw<'script>, doc: Option<Vec<Cow<'script, str>>>) -> Self {
        Self { exprs, doc }
    }
    pub(crate) fn up_script<'registry>(
        self,
        mut helper: &mut Helper<'script, 'registry>,
    ) -> Result<Script<'script>> {
        let mut exprs = vec![];
        let last_idx = self.exprs.len() - 1;
        for (i, e) in self.exprs.into_iter().enumerate() {
            match e {
                ExprRaw::Module(m) => {
                    m.define(&mut helper)?;
                }
                ExprRaw::Const {
                    name,
                    expr,
                    start,
                    end,
                    comment,
                } => {
                    let name_v = vec![name.to_string()];
                    let r = Range::from((start, end));

                    let expr = expr.up(&mut helper)?.try_reduce(&helper)?;
                    let v = reduce2(expr, &helper)?;
                    let value_type = v.value_type();

                    let idx = helper.consts.insert(name_v, v).map_err(|_old| {
                        Error::from(ErrorKind::DoubleConst(
                            r.expand_lines(2),
                            r,
                            name.to_string(),
                        ))
                    })?;
                    if i == last_idx {
                        exprs.push(Expr::Imut(ImutExprInt::Local {
                            is_const: true,
                            idx,
                            mid: helper.add_meta_w_name(start, end, &name),
                        }))
                    }
                    helper.add_const_doc(&name, comment, value_type);
                }
                ExprRaw::FnDecl(f) => {
                    helper.docs.fns.push(f.doc());
                    let f = f.up(&mut helper)?;
                    helper.register_fun(f.into())?;
                }
                other => exprs.push(other.up(&mut helper)?),
            }
        }

        // We make sure the if we return `event` we turn it into `emit event`
        // While this is not required logically it allows us to
        // take advantage of the `emit event` optimisation
        if let Some(e) = exprs.last_mut() {
            if let Expr::Imut(ImutExprInt::Path(Path::Event(p))) = e {
                if p.segments.is_empty() {
                    let expr = EmitExpr {
                        mid: p.mid(),
                        expr: ImutExprInt::Path(Path::Event(p.clone())),
                        port: None,
                    };
                    *e = Expr::Emit(Box::new(expr));
                }
            }
        } else {
            let expr = EmitExpr {
                mid: 0,
                expr: ImutExprInt::Path(Path::Event(EventPath {
                    mid: 0,
                    segments: vec![],
                })),
                port: None,
            };
            exprs.push(Expr::Emit(Box::new(expr)))
        }

        helper.docs.module = Some(ModDoc {
            name: "self".into(),
            doc: self
                .doc
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        });

        Ok(Script {
            imports: vec![], // Compiled out
            exprs,
            consts: helper.consts.clone(),
            aggregates: helper.aggregates.clone(),
            windows: helper.windows.clone(),
            locals: helper.locals.len(),
            node_meta: helper.meta.clone(),
            functions: helper.func_vec.clone(),
            docs: helper.docs.clone(),
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Clone, Copy)]
pub enum BytesDataType {
    SignedInteger,
    UnsignedInteger,
    Binary,
}

impl Default for BytesDataType {
    fn default() -> Self {
        BytesDataType::UnsignedInteger
    }
}

#[derive(Debug, PartialEq, Serialize, Clone, Copy)]
pub enum Endian {
    Little,
    Big,
}

impl Default for Endian {
    fn default() -> Self {
        Endian::Big
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct BytesPartRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub data: ImutExprRaw<'script>,
    pub data_type: IdentRaw<'script>,
    pub bits: Option<i64>,
}
impl_expr!(BytesPartRaw);

impl<'script> Default for BytesPartRaw<'script> {
    fn default() -> Self {
        BytesPartRaw {
            start: Location::default(),
            end: Location::default(),
            data: ImutExprRaw::Literal(LiteralRaw::default()),
            data_type: IdentRaw::default(),
            bits: None,
        }
    }
}
impl<'script> Upable<'script> for BytesPartRaw<'script> {
    type Target = BytesPart<'script>;
    // We allow this for casting the bits
    #[allow(clippy::cast_sign_loss)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let data_type: Vec<&str> = self.data_type.id.split('-').collect();
        let (data_type, endianess) = match data_type.as_slice() {
            ["binary"] => (BytesDataType::Binary, Endian::Big),
            []
            | ["" | "integer" | "big" | "unsigned"]
            | ["unsigned" | "big", "integer"]
            | ["big", "unsigned", "integer"] => (BytesDataType::UnsignedInteger, Endian::Big),
            ["signed", "integer"] | ["big", "signed", "integer"] => {
                (BytesDataType::SignedInteger, Endian::Big)
            }
            ["little"] | ["little", "integer"] | ["little", "unsigned", "integer"] => {
                (BytesDataType::UnsignedInteger, Endian::Little)
            }
            ["little", "signed", "integer"] => (BytesDataType::SignedInteger, Endian::Little),
            other => {
                return Err(err_generic(
                    &self,
                    &self,
                    &format!("Not a valid data type: '{}'", other.join("-")),
                    &helper.meta,
                ))
            }
        };
        let bits = if let Some(bits) = self.bits {
            if bits <= 0 || bits > 64 {
                return Err(err_generic(
                    &self,
                    &self,
                    &format!("negative bits or bits > 64 are are not allowed: {}", bits),
                    &helper.meta,
                ));
            }
            bits as u64
        } else {
            match data_type {
                BytesDataType::SignedInteger | BytesDataType::UnsignedInteger => 8,
                BytesDataType::Binary => 0,
            }
        };

        Ok(BytesPart {
            mid: helper.add_meta(self.start, self.end),
            data: self.data.up(helper).map(ImutExpr)?,
            data_type,
            endianess,
            bits,
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct BytesRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub bytes: Vec<BytesPartRaw<'script>>,
}
impl_expr!(BytesRaw);

impl<'script> Upable<'script> for BytesRaw<'script> {
    type Target = Bytes<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Bytes {
            mid: helper.add_meta(self.start, self.end),
            value: self
                .bytes
                .into_iter()
                .map(|b| b.up(helper))
                .collect::<Result<_>>()?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct ModuleRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub name: IdentRaw<'script>,
    pub exprs: ExprsRaw<'script>,
    pub doc: Option<Vec<Cow<'script, str>>>,
}
impl_expr!(ModuleRaw);

impl<'script> ModuleRaw<'script> {
    pub(crate) fn define<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<()> {
        helper.module.push(self.name.id.to_string());
        for e in self.exprs {
            match e {
                ExprRaw::Module(m) => {
                    m.define(helper)?;
                }
                ExprRaw::Const {
                    name,
                    expr,
                    start,
                    end,
                    ..
                } => {
                    let mut name_v = helper.module.clone();
                    name_v.push(name.to_string());
                    let expr = expr.up(helper)?;
                    let v = reduce2(expr, &helper)?;
                    helper.consts.insert(name_v, v).map_err(|_old| {
                        let r = Range::from((start, end));
                        ErrorKind::DoubleConst(r.expand_lines(2), r, name.to_string())
                    })?;
                }
                ExprRaw::FnDecl(f) => {
                    let f = f.up(helper)?;
                    let f = CustomFn {
                        name: f.name.id,
                        args: f.args.iter().map(|i| i.id.to_string()).collect(),
                        locals: f.locals,
                        body: f.body,
                        is_const: false, // TODO: we should find a way to examine this
                        open: f.open,
                        inline: f.inline,
                    };

                    helper.register_fun(f)?;
                }
                // ALLOW: the gramer doesn't allow this
                _ => unreachable!("Can't have expressions inside of modules"),
            }
        }
        helper.module.pop();
        Ok(())
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Debug, PartialEq, Serialize, Clone, Default)]
pub struct IdentRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub id: beef::Cow<'script, str>,
}
impl_expr!(IdentRaw);

impl<'script> Upable<'script> for IdentRaw<'script> {
    type Target = Ident<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Self::Target {
            mid: helper.add_meta_w_name(self.start, self.end, &self.id),
            id: self.id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FieldRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: StringLitRaw<'script>,
    pub(crate) value: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for FieldRaw<'script> {
    type Target = Field<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let name = self.name.up(helper)?;
        Ok(Field {
            mid: helper.add_meta(self.start, self.end),
            name,
            value: self.value.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) fields: FieldsRaw<'script>,
}
impl_expr!(RecordRaw);

impl<'script> Upable<'script> for RecordRaw<'script> {
    type Target = Record<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Record {
            base: crate::Object::new(),
            mid: helper.add_meta(self.start, self.end),
            fields: self.fields.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ListRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ImutExprsRaw<'script>,
}
impl_expr!(ListRaw);

impl<'script> Upable<'script> for ListRaw<'script> {
    type Target = List<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(List {
            mid: helper.add_meta(self.start, self.end),
            exprs: self.exprs.up(helper)?.into_iter().map(ImutExpr).collect(),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct LiteralRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) value: Value<'script>,
}

impl_expr!(LiteralRaw);
impl<'script> Upable<'script> for LiteralRaw<'script> {
    type Target = Literal<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Literal {
            mid: helper.add_meta(self.start, self.end),
            value: self.value,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StringLitRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) elements: StrLitElementsRaw<'script>,
}

impl<'script> Upable<'script> for StringLitRaw<'script> {
    type Target = StringLit<'script>;

    fn up<'registry>(mut self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mid = helper.add_meta(self.start, self.end);

        self.elements.reverse();
        let mut new = Vec::with_capacity(self.elements.len());
        for e in self.elements {
            let next = match e {
                StrLitElementRaw::Expr(e) => {
                    let i = e.up(helper)?;
                    let i = i.try_reduce(helper)?;
                    match i {
                        #[cfg(not(feature = "erlang-float-testing"))]
                        ImutExprInt::Literal(l) => l.value.as_str().map_or_else(
                            || StrLitElement::Lit(l.value.encode().into()),
                            |s| StrLitElement::Lit(s.to_string().into()),
                        ),
                        // TODO: The float scenario is different in erlang and rust
                        // We knowingly excluded float correctness in string interpolation
                        // as we don't want to over engineer and write own format functions.
                        // any suggestions are welcome
                        #[cfg(feature = "erlang-float-testing")]
                        #[cfg(not(tarpaulin_include))]
                        ImutExprInt::Literal(l) => l.value.as_str().map_or_else(
                            || {
                                if let Some(_f) = l.value.as_f64() {
                                    StrLitElement::Lit("42".into())
                                } else {
                                    StrLitElement::Lit(
                                        crate::utils::sorted_serialize(&l.value)
                                            .unwrap_or_default()
                                            .into(),
                                    )
                                }
                            },
                            |s| StrLitElement::Lit(s.to_string().into()),
                        ),
                        _ => StrLitElement::Expr(i),
                    }
                }
                StrLitElementRaw::Lit(l) => StrLitElement::Lit(l),
            };
            if let StrLitElement::Lit(next_lit) = next {
                // We need this because otherwise we run into lifetime issues
                #[allow(clippy::option_if_let_else)]
                if let Some(prev) = new.pop() {
                    match prev {
                        StrLitElement::Lit(l) => {
                            let mut o = l.into_owned();
                            o.push_str(&next_lit);
                            new.push(StrLitElement::Lit(o.into()))
                        }
                        prev @ StrLitElement::Expr(..) => {
                            new.push(prev);
                            new.push(StrLitElement::Lit(next_lit))
                        }
                    }
                } else {
                    new.push(StrLitElement::Lit(next_lit))
                }
            } else {
                new.push(next)
            }
        }
        Ok(StringLit { mid, elements: new })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StrLitElementRaw<'script> {
    Lit(Cow<'script, str>),
    Expr(ImutExprRaw<'script>),
}

impl<'script> From<ImutExprRaw<'script>> for StrLitElementRaw<'script> {
    fn from(e: ImutExprRaw<'script>) -> Self {
        StrLitElementRaw::Expr(e)
    }
}

impl<'script> From<Cow<'script, str>> for StrLitElementRaw<'script> {
    fn from(e: Cow<'script, str>) -> Self {
        StrLitElementRaw::Lit(e)
    }
}

impl<'script> From<&'script str> for StrLitElementRaw<'script> {
    fn from(e: &'script str) -> Self {
        StrLitElementRaw::Lit(e.into())
    }
}

/// we're forced to make this pub because of lalrpop
pub type StrLitElementsRaw<'script> = Vec<StrLitElementRaw<'script>>;

pub(crate) fn reduce2<'script>(
    expr: ImutExprInt<'script>,
    helper: &Helper,
) -> Result<Value<'script>> {
    match expr {
        ImutExprInt::Literal(Literal { value: v, .. }) => Ok(v),
        ImutExprInt::Local {
            is_const: true,
            idx,
            ..
        } => Ok(Value::from(idx)),
        other => Err(ErrorKind::NotConstant(
            other.extent(&helper.meta),
            other.extent(&helper.meta).expand_lines(2),
        )
        .into()),
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Const {
        /// we're forced to make this pub because of lalrpop
        name: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
        /// we're forced to make this pub because of lalrpop
        comment: Option<Vec<Cow<'script, str>>>,
    },
    /// we're forced to make this pub because of lalrpop
    Module(ModuleRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    MatchExpr(Box<MatchRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Assign(Box<AssignRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ComprehensionRaw<'script, Self>>),
    Drop {
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
    },
    /// we're forced to make this pub because of lalrpop
    Emit(Box<EmitExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    FnDecl(AnyFnRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Imut(ImutExprRaw<'script>),
}
impl<'script> ExpressionRaw<'script> for ExprRaw<'script> {}

impl<'script> Upable<'script> for ExprRaw<'script> {
    type Target = Expr<'script>;
    #[allow(clippy::too_many_lines)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(match self {
            ExprRaw::FnDecl(_) | ExprRaw::Const { .. } | ExprRaw::Module(ModuleRaw { .. }) => {
                // ALLOW: There is no code path that leads here,
                unreachable!()
            }
            ExprRaw::MatchExpr(m) => match m.up(helper)? {
                Match {
                    mid,
                    target,
                    mut patterns,
                    default,
                } if patterns.len() == 1
                    && patterns
                        .first()
                        .map(|cg| {
                            if let ClauseGroup::Simple {
                                precondition: None,
                                patterns,
                            } = cg
                            {
                                patterns.len() == 1
                            } else {
                                false
                            }
                        })
                        .unwrap_or_default() =>
                {
                    if let Some(ClauseGroup::Simple {
                        precondition: None,
                        mut patterns,
                    }) = patterns.pop()
                    {
                        if let Some(if_clause) = patterns.pop() {
                            let ie = IfElse {
                                mid,
                                target,
                                if_clause,
                                else_clause: default,
                            };
                            Expr::IfElse(Box::new(ie))
                        } else {
                            return Err("Invalid group clause with 0 patterns".into());
                        }
                    } else {
                        // ALLOW: we check patterns.len() above
                        unreachable!()
                    }
                }
                m => Expr::Match(Box::new(m)),
            },
            ExprRaw::Assign(a) => {
                let path = a.path.up(helper)?;
                let mid = helper.add_meta(a.start, a.end);
                match a.expr.up(helper)? {
                    Expr::Imut(ImutExprInt::Merge(m)) => {
                        if path.ast_eq(&m.target) {
                            Expr::MergeInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                mid,
                                path,
                                expr: Box::new(ImutExprInt::Merge(m).into()),
                            }
                        }
                    }
                    Expr::Imut(ImutExprInt::Patch(m)) => {
                        if path.ast_eq(&m.target) {
                            Expr::PatchInPlace(Box::new(*m))
                        } else {
                            Expr::Assign {
                                mid,
                                path,
                                expr: Box::new(ImutExprInt::Patch(m).into()),
                            }
                        }
                    }
                    expr => Expr::Assign {
                        mid,
                        path,
                        expr: Box::new(expr),
                    },
                }
            }
            ExprRaw::Comprehension(c) => Expr::Comprehension(Box::new(c.up(helper)?)),
            ExprRaw::Drop { start, end } => {
                let mid = helper.add_meta(start, end);
                if !helper.can_emit {
                    return Err(ErrorKind::InvalidDrop(
                        Range(start, end).expand_lines(2),
                        Range(start, end),
                    )
                    .into());
                }
                Expr::Drop { mid }
            }
            ExprRaw::Emit(e) => Expr::Emit(Box::new(e.up(helper)?)),
            ExprRaw::Imut(i) => i.up(helper)?.into(),
        })
    }
}

#[cfg(not(tarpaulin_include))] // just dispatch
impl<'script> BaseExpr for ExprRaw<'script> {
    fn mid(&self) -> usize {
        0
    }

    fn s(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Const { start, .. } | ExprRaw::Drop { start, .. } => *start,
            ExprRaw::Module(e) => e.s(meta),
            ExprRaw::MatchExpr(e) => e.s(meta),
            ExprRaw::Assign(e) => e.s(meta),
            ExprRaw::Comprehension(e) => e.s(meta),
            ExprRaw::Emit(e) => e.s(meta),
            ExprRaw::FnDecl(e) => e.s(meta),
            ExprRaw::Imut(e) => e.s(meta),
        }
    }

    fn e(&self, meta: &NodeMetas) -> Location {
        match self {
            ExprRaw::Const { end, .. } | ExprRaw::Drop { end, .. } => *end,
            ExprRaw::Module(e) => e.e(meta),
            ExprRaw::MatchExpr(e) => e.e(meta),
            ExprRaw::Assign(e) => e.e(meta),
            ExprRaw::Comprehension(e) => e.e(meta),
            ExprRaw::Emit(e) => e.e(meta),
            ExprRaw::FnDecl(e) => e.e(meta),
            ExprRaw::Imut(e) => e.e(meta),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FnDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) body: ExprsRaw<'script>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
}
impl_expr!(FnDeclRaw);

impl<'script> FnDeclRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc {
        FnDoc {
            name: self.name.id.to_string(),
            args: self.args.iter().map(|a| a.id.to_string()).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for FnDeclRaw<'script> {
    type Target = FnDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut locals: HashMap<_, _> = self
            .args
            .iter()
            .enumerate()
            .map(|(i, a)| (a.id.to_string(), i))
            .collect();

        helper.can_emit = false;
        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.swap(&mut aggrs, &mut locals);
        helper.possible_leaf = true;
        let body = self.body.up(helper)?;
        helper.possible_leaf = false;
        helper.swap(&mut aggrs, &mut locals);
        helper.can_emit = can_emit;
        let name = self.name.up(helper)?;
        Ok(FnDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &name.id),
            name,
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
            open: self.open,
            inline: self.inline,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AnyFnRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Match(MatchFnDeclRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Normal(FnDeclRaw<'script>),
}
impl<'script> AnyFnRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc {
        match self {
            AnyFnRaw::Match(f) => f.doc(),
            AnyFnRaw::Normal(f) => f.doc(),
        }
    }
}

impl<'script> Upable<'script> for AnyFnRaw<'script> {
    type Target = FnDecl<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            AnyFnRaw::Normal(f) => f.up(helper),
            AnyFnRaw::Match(f) => f.up(helper),
        }
    }
}

#[cfg(not(tarpaulin_include))] // just dispatch
impl<'script> BaseExpr for AnyFnRaw<'script> {
    fn mid(&self) -> usize {
        0
    }
    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.start,
            AnyFnRaw::Normal(m) => m.start,
        }
    }
    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            AnyFnRaw::Match(m) => m.end,
            AnyFnRaw::Normal(m) => m.end,
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchFnDeclRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) name: IdentRaw<'script>,
    pub(crate) args: Vec<IdentRaw<'script>>,
    pub(crate) cases: Vec<PredicateClauseRaw<'script, ExprRaw<'script>>>,
    pub(crate) doc: Option<Vec<Cow<'script, str>>>,
    pub(crate) open: bool,
    pub(crate) inline: bool,
}
impl_expr!(MatchFnDeclRaw);

impl<'script> MatchFnDeclRaw<'script> {
    pub(crate) fn doc(&self) -> FnDoc {
        FnDoc {
            name: self.name.id.to_string(),
            args: self.args.iter().map(|a| a.id.to_string()).collect(),
            open: self.open,
            doc: self
                .doc
                .clone()
                .map(|d| d.iter().map(|l| l.trim()).collect::<Vec<_>>().join("\n")),
        }
    }
}

impl<'script> Upable<'script> for MatchFnDeclRaw<'script> {
    type Target = FnDecl<'script>;
    fn up<'registry>(mut self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let can_emit = helper.can_emit;
        let mut aggrs = Vec::new();
        let mut locals = HashMap::new();

        for (i, a) in self.args.iter().enumerate() {
            locals.insert(a.id.to_string(), i);
        }

        helper.is_open = self.open;
        helper.fn_argc = self.args.len();

        helper.can_emit = false;

        helper.swap(&mut aggrs, &mut locals);

        let target = self
            .args
            .iter()
            .map(|a| {
                ImutExprRaw::Path(PathRaw::Local(LocalPathRaw {
                    start: a.start,
                    end: a.end,
                    segments: vec![SegmentRaw::from_id(a.clone())],
                }))
            })
            .collect();

        let mut patterns = Vec::new();

        std::mem::swap(&mut self.cases, &mut patterns);

        let patterns = patterns
            .into_iter()
            .map(|mut c: PredicateClauseRaw<_>| {
                if c.pattern != PatternRaw::Default {
                    let mut exprs: Vec<_> = self
                        .args
                        .iter()
                        .enumerate()
                        .map(|(i, a)| {
                            ExprRaw::Assign(Box::new(AssignRaw {
                                start: c.start,
                                end: c.end,
                                path: PathRaw::Local(LocalPathRaw {
                                    start: a.start,
                                    end: a.end,
                                    segments: vec![SegmentRaw::from_id(a.clone())],
                                }),
                                expr: ExprRaw::Imut(ImutExprRaw::Path(PathRaw::Local(
                                    LocalPathRaw {
                                        start: a.start,
                                        end: a.end,
                                        segments: vec![
                                            SegmentRaw::from_str(FN_RES_NAME, a.start, a.end),
                                            SegmentRaw::from_usize(i, a.start, a.end),
                                        ],
                                    },
                                ))),
                            }))
                        })
                        .collect();
                    exprs.append(&mut c.exprs);
                    c.exprs = exprs;
                }
                c
            })
            .collect();

        let body = ExprRaw::MatchExpr(Box::new(MatchRaw {
            start: self.start,
            end: self.end,
            target: ImutExprRaw::List(Box::new(ListRaw {
                start: self.start,
                end: self.end,
                exprs: target,
            })),
            patterns,
        }));
        helper.possible_leaf = true;
        let body = body.up(helper)?;
        helper.possible_leaf = false;
        let body = vec![body];

        helper.swap(&mut aggrs, &mut locals);
        helper.can_emit = can_emit;
        let name = self.name.up(helper)?;
        Ok(FnDecl {
            mid: helper.add_meta_w_name(self.start, self.end, &name),
            name,
            args: self.args.up(helper)?,
            body,
            locals: locals.len(),
            open: self.open,
            inline: self.inline,
        })
    }
}

/// A raw expression
pub trait ExpressionRaw<'script>:
    Clone + std::fmt::Debug + PartialEq + Serialize + Upable<'script>
where
    <Self as Upable<'script>>::Target: Expression + 'script,
{
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ImutExprRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Record(Box<RecordRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    List(Box<ListRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Patch(Box<PatchRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Merge(Box<MergeRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Match(Box<MatchRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Comprehension(Box<ComprehensionRaw<'script, Self>>),
    /// we're forced to make this pub because of lalrpop
    Path(PathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Binary(Box<BinExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Unary(Box<UnaryExprRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Literal(LiteralRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Invoke(InvokeRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Present {
        /// we're forced to make this pub because of lalrpop
        path: PathRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        start: Location,
        /// we're forced to make this pub because of lalrpop
        end: Location,
    },
    /// we're forced to make this pub because of lalrpop
    String(StringLitRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Recur(RecurRaw<'script>),
    /// bytes
    Bytes(BytesRaw<'script>),
}
impl<'script> ExpressionRaw<'script> for ImutExprRaw<'script> {}

impl<'script> Upable<'script> for ImutExprRaw<'script> {
    type Target = ImutExprInt<'script>;
    #[allow(clippy::too_many_lines)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        let r = match self {
            ImutExprRaw::Recur(r) => {
                helper.possible_leaf = was_leaf;
                ImutExprInt::Recur(r.up(helper)?)
            }
            ImutExprRaw::Binary(b) => {
                ImutExprInt::Binary(Box::new(b.up(helper)?)).try_reduce(helper)?
            }
            ImutExprRaw::Unary(u) => {
                ImutExprInt::Unary(Box::new(u.up(helper)?)).try_reduce(helper)?
            }
            ImutExprRaw::String(s) => ImutExprInt::String(s.up(helper)?).try_reduce(helper)?,
            ImutExprRaw::Record(r) => ImutExprInt::Record(r.up(helper)?).try_reduce(helper)?,
            ImutExprRaw::List(l) => ImutExprInt::List(l.up(helper)?).try_reduce(helper)?,
            ImutExprRaw::Patch(p) => {
                ImutExprInt::Patch(Box::new(p.up(helper)?)).try_reduce(helper)?
            }
            ImutExprRaw::Merge(m) => {
                ImutExprInt::Merge(Box::new(m.up(helper)?)).try_reduce(helper)?
            }
            ImutExprRaw::Present { path, start, end } => ImutExprInt::Present {
                path: path.up(helper)?,
                mid: helper.add_meta(start, end),
            }
            .try_reduce(helper)?,
            ImutExprRaw::Path(p) => match p.up(helper)? {
                Path::Local(LocalPath {
                    is_const,
                    mid,
                    idx,
                    ref segments,
                }) if segments.is_empty() => ImutExprInt::Local { mid, idx, is_const },
                p => ImutExprInt::Path(p),
            }
            .try_reduce(helper)?,
            ImutExprRaw::Literal(l) => ImutExprInt::Literal(l.up(helper)?).try_reduce(helper)?,
            ImutExprRaw::Invoke(i) => {
                if i.is_aggregate(helper) {
                    ImutExprInt::InvokeAggr(i.into_aggregate().up(helper)?)
                } else {
                    let i = i.up(helper)?;
                    let i = if i.can_inline() {
                        i.inline()?
                    } else {
                        match i.args.len() {
                            1 => ImutExprInt::Invoke1(i),
                            2 => ImutExprInt::Invoke2(i),
                            3 => ImutExprInt::Invoke3(i),
                            _ => ImutExprInt::Invoke(i),
                        }
                    };
                    i.try_reduce(helper)?
                }
            }
            ImutExprRaw::Match(m) => {
                helper.possible_leaf = was_leaf;
                ImutExprInt::Match(Box::new(m.up(helper)?))
            }
            ImutExprRaw::Comprehension(c) => ImutExprInt::Comprehension(Box::new(c.up(helper)?)),
            ImutExprRaw::Bytes(b) => ImutExprInt::Bytes(b.up(helper)?).try_reduce(helper)?,
        };
        helper.possible_leaf = was_leaf;
        Ok(r)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecurRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub exprs: ImutExprsRaw<'script>,
}
impl_expr!(RecurRaw);

impl<'script> Upable<'script> for RecurRaw<'script> {
    type Target = Recur<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        if !was_leaf {
            return Err(ErrorKind::InvalidRecur(
                self.extent(&helper.meta).expand_lines(2),
                self.extent(&helper.meta),
            )
            .into());
        };
        if (helper.is_open && helper.fn_argc < self.exprs.len())
            || (!helper.is_open && helper.fn_argc != self.exprs.len())
        {
            return error_generic(
                &self,
                &self,
                &format!(
                    "Wrong number of arguments expected {} but got {}",
                    helper.fn_argc,
                    self.exprs.len()
                ),
                &helper.meta,
            );
        }
        let exprs = self.exprs.up(helper)?.into_iter().map(ImutExpr).collect();
        helper.possible_leaf = was_leaf;

        Ok(Recur {
            mid: helper.add_meta(self.start, self.end),
            argc: helper.fn_argc,
            open: helper.is_open,

            exprs,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmitExprRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub expr: ImutExprRaw<'script>,
    pub port: Option<ImutExprRaw<'script>>,
}
impl_expr!(EmitExprRaw);

impl<'script> Upable<'script> for EmitExprRaw<'script> {
    type Target = EmitExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if !helper.can_emit {
            return Err(ErrorKind::InvalidEmit(
                self.extent(&helper.meta).expand_lines(2),
                self.extent(&helper.meta),
            )
            .into());
        }
        Ok(EmitExpr {
            mid: helper.add_meta(self.start, self.end),
            expr: self.expr.up(helper)?,
            port: self.port.up(helper)?,
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) path: PathRaw<'script>,
    pub(crate) expr: ExprRaw<'script>,
}
impl_expr!(AssignRaw);

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PredicateClauseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) pattern: PatternRaw<'script>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: Vec<Ex>,
}

impl<'script, Ex> Upable<'script> for PredicateClauseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = PredicateClause<'script, <Ex as Upable<'script>>::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let was_leaf = helper.possible_leaf;
        helper.possible_leaf = false;
        // We run the pattern first as this might reserve a local shadow
        let pattern = self.pattern.up(helper)?;
        let guard = self.guard.up(helper)?;
        helper.possible_leaf = was_leaf;
        let mut exprs = self.exprs.up(helper)?;

        // If we are in an assign pattern we'd have created
        // a shadow variable, this needs to be undoine at the end
        if pattern.is_assign() {
            helper.end_shadow_var();
        }

        if let Pattern::Assign(AssignPattern { idx, .. }) = &pattern {
            if let Some(expr) = exprs.last_mut() {
                expr.replace_last_shadow_use(*idx);
            };
        }

        let span = Range::from((self.start, self.end));
        let last_expr = exprs
            .pop()
            .ok_or_else(|| error_missing_effector(&span, &span, &helper.meta))?;

        Ok(PredicateClause {
            mid: helper.add_meta(self.start, self.end),
            pattern,
            guard,
            exprs,
            last_expr,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PatchRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) operations: PatchOperationsRaw<'script>,
}

impl<'script> Upable<'script> for PatchRaw<'script> {
    type Target = Patch<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let operations = self.operations.up(helper)?;

        Ok(Patch {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            operations,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatchOperationRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Insert {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Upsert {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Update {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Erase {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Copy {
        /// we're forced to make this pub because of lalrpop
        from: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: StringLitRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Move {
        /// we're forced to make this pub because of lalrpop
        from: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        to: StringLitRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Merge {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    MergeRecord {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    Default {
        /// we're forced to make this pub because of lalrpop
        ident: StringLitRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    DefaultRecord {
        /// we're forced to make this pub because of lalrpop
        expr: ImutExprRaw<'script>,
    },
}

impl<'script> Upable<'script> for PatchOperationRaw<'script> {
    type Target = PatchOperation<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatchOperationRaw::{Copy, Erase, Insert, Merge, MergeRecord, Move, Update, Upsert};
        Ok(match self {
            Insert { ident, expr } => PatchOperation::Insert {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Upsert { ident, expr } => PatchOperation::Upsert {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Update { ident, expr } => PatchOperation::Update {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            Erase { ident } => PatchOperation::Erase {
                ident: ident.up(helper)?,
            },
            Copy { from, to } => PatchOperation::Copy {
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Move { from, to } => PatchOperation::Move {
                from: from.up(helper)?,
                to: to.up(helper)?,
            },
            Merge { expr, ident, .. } => PatchOperation::Merge {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            MergeRecord { expr } => PatchOperation::MergeRecord {
                expr: expr.up(helper)?,
            },
            PatchOperationRaw::Default { ident, expr } => PatchOperation::Default {
                ident: ident.up(helper)?,
                expr: expr.up(helper)?,
            },
            PatchOperationRaw::DefaultRecord { expr } => PatchOperation::DefaultRecord {
                expr: expr.up(helper)?,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MergeRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub expr: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for MergeRaw<'script> {
    type Target = Merge<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(Merge {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            expr: self.expr.up(helper)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub cases: ComprehensionCasesRaw<'script, Ex>,
}
impl_expr_exraw!(ComprehensionRaw);

impl<'script, Ex> Upable<'script> for ComprehensionRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = Comprehension<'script, Ex::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // We compute the target before shadowing the key and value

        let target = self.target.up(helper)?;

        // We know that each case will have a key and a value as a shadowed
        // variable so we reserve two ahead of time so we know what id's those
        // will be.
        let (key_id, val_id) = helper.reserve_2_shadow();

        let cases = self.cases.up(helper)?;

        Ok(Comprehension {
            mid: helper.add_meta(self.start, self.end),
            target,
            cases,
            key_id,
            val_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub target: ImutExprRaw<'script>,
    pub cases: ImutComprehensionCasesRaw<'script>,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComprehensionCaseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: Vec<Ex>,
}

impl<'script, Ex> Upable<'script> for ComprehensionCaseRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = ComprehensionCase<'script, Ex::Target>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        // regiter key and value as shadowed variables
        let key_idx = helper.register_shadow_var(&self.key_name);
        let val_idx = helper.register_shadow_var(&self.value_name);

        let guard = self.guard.up(helper)?;
        let mut exprs = self.exprs.up(helper)?;

        if let Some(expr) = exprs.last_mut() {
            expr.replace_last_shadow_use(key_idx);
            expr.replace_last_shadow_use(val_idx);
        };

        // unregister them again
        helper.end_shadow_var();
        helper.end_shadow_var();
        let span = Range::from((self.start, self.end));
        let last_expr = exprs
            .pop()
            .ok_or_else(|| error_missing_effector(&span, &span, &helper.meta))?;
        Ok(ComprehensionCase {
            mid: helper.add_meta(self.start, self.end),
            key_name: self.key_name,
            value_name: self.value_name,
            guard,
            exprs,
            last_expr,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ImutComprehensionCaseRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) key_name: Cow<'script, str>,
    pub(crate) value_name: Cow<'script, str>,
    pub(crate) guard: Option<ImutExprRaw<'script>>,
    pub(crate) exprs: ImutExprsRaw<'script>,
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Record(RecordPatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Array(ArrayPatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Tuple(TuplePatternRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Expr(ImutExprRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Assign(AssignPatternRaw<'script>),
    /// a test expression
    Extract(TestExprRaw),
    /// we're forced to make this pub because of lalrpop
    DoNotCare,
    /// we're forced to make this pub because of lalrpop
    Default,
}

impl<'script> Upable<'script> for PatternRaw<'script> {
    type Target = Pattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PatternRaw::{Array, Assign, Default, DoNotCare, Expr, Extract, Record, Tuple};
        Ok(match self {
            //Predicate(pp) => Pattern::Predicate(pp.up(helper)?),
            Record(rp) => Pattern::Record(rp.up(helper)?),
            Array(ap) => Pattern::Array(ap.up(helper)?),
            Tuple(tp) => Pattern::Tuple(tp.up(helper)?),
            Expr(expr) => Pattern::Expr(expr.up(helper)?),
            Assign(ap) => Pattern::Assign(ap.up(helper)?),
            Extract(e) => Pattern::Extract(Box::new(e.up(helper)?)),
            DoNotCare => Pattern::DoNotCare,
            Default => Pattern::Default,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PredicatePatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    TildeEq {
        /// we're forced to make this pub because of lalrpop
        assign: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        test: TestExprRaw,
    },
    /// we're forced to make this pub because of lalrpop
    Bin {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        rhs: ImutExprRaw<'script>,
        /// we're forced to make this pub because of lalrpop
        kind: BinOpKind,
    },
    /// we're forced to make this pub because of lalrpop
    RecordPatternEq {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        pattern: RecordPatternRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    ArrayPatternEq {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
        /// we're forced to make this pub because of lalrpop
        pattern: ArrayPatternRaw<'script>,
    },
    /// we're forced to make this pub because of lalrpop
    FieldPresent {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
    },
    /// we're forced to make this pub because of lalrpop
    FieldAbsent {
        /// we're forced to make this pub because of lalrpop
        lhs: Cow<'script, str>,
    },
}

impl<'script> Upable<'script> for PredicatePatternRaw<'script> {
    type Target = PredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PredicatePatternRaw::{
            ArrayPatternEq, Bin, FieldAbsent, FieldPresent, RecordPatternEq, TildeEq,
        };
        Ok(match self {
            TildeEq { assign, lhs, test } => PredicatePattern::TildeEq {
                assign,
                key: KnownKey::from(lhs.clone()),
                lhs,
                test: Box::new(test.up(helper)?),
            },
            Bin { lhs, rhs, kind } => PredicatePattern::Bin {
                key: KnownKey::from(lhs.clone()),
                lhs,
                rhs: rhs.up(helper)?,
                kind,
            },
            RecordPatternEq { lhs, pattern } => PredicatePattern::RecordPatternEq {
                key: KnownKey::from(lhs.clone()),
                lhs,
                pattern: pattern.up(helper)?,
            },
            ArrayPatternEq { lhs, pattern } => PredicatePattern::ArrayPatternEq {
                key: KnownKey::from(lhs.clone()),
                lhs,
                pattern: pattern.up(helper)?,
            },
            FieldPresent { lhs } => PredicatePattern::FieldPresent {
                key: KnownKey::from(lhs.clone()),
                lhs,
            },
            FieldAbsent { lhs } => PredicatePattern::FieldAbsent {
                key: KnownKey::from(lhs.clone()),
                lhs,
            },
        })
    }
}
/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordPatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) fields: PatternFieldsRaw<'script>,
}

impl<'script> Upable<'script> for RecordPatternRaw<'script> {
    type Target = RecordPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let fields = self.fields.up(helper)?;
        let present_fields: Vec<Cow<str>> = fields
            .iter()
            .filter_map(|f| {
                if let PredicatePattern::FieldPresent { lhs, .. } = f {
                    Some(lhs.clone())
                } else {
                    None
                }
            })
            .collect();

        let absent_fields: Vec<Cow<str>> = fields
            .iter()
            .filter_map(|f| {
                if let PredicatePattern::FieldAbsent { lhs, .. } = f {
                    Some(lhs.clone())
                } else {
                    None
                }
            })
            .collect();

        for present in &present_fields {
            let duplicated = fields.iter().any(|f| {
                if let PredicatePattern::FieldPresent { .. } = f {
                    false
                } else {
                    f.lhs() == present
                }
            });
            if duplicated {
                let extent = (self.start, self.end).into();
                helper.warn(Warning::new(
                    extent,
                    extent.expand_lines(2),
                    format!("The field {} is checked with both present and another extractor, this is redundant as extractors imply presence. It may also overwrite the result of the extractor.", present),
                ));
            }
        }

        for absent in &absent_fields {
            let duplicated = fields.iter().any(|f| {
                if let PredicatePattern::FieldAbsent { .. } = f {
                    false
                } else {
                    f.lhs() == absent
                }
            });
            if duplicated {
                let extent = (self.start, self.end).into();
                helper.warn(Warning::new(
                    extent,
                    extent.expand_lines(2),
                    format!("The field {} is checked with both absence and another extractor, this test can never be true.", absent),
                ))
            }
        }

        Ok(RecordPattern {
            mid: helper.add_meta(self.start, self.end),
            fields,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ArrayPredicatePatternRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Expr(ImutExprRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Tilde(TestExprRaw),
    /// we're forced to make this pub because of lalrpop
    Record(RecordPatternRaw<'script>),
    /// Ignore
    Ignore,
}
impl<'script> Upable<'script> for ArrayPredicatePatternRaw<'script> {
    type Target = ArrayPredicatePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use ArrayPredicatePatternRaw::{Expr, Ignore, Record, Tilde};
        Ok(match self {
            Expr(expr) => ArrayPredicatePattern::Expr(expr.up(helper)?),
            Tilde(te) => ArrayPredicatePattern::Tilde(Box::new(te.up(helper)?)),
            Record(rp) => ArrayPredicatePattern::Record(rp.up(helper)?),
            Ignore => ArrayPredicatePattern::Ignore,
            //Array(ap) => ArrayPredicatePattern::Array(ap),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArrayPatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
}

impl<'script> Upable<'script> for ArrayPatternRaw<'script> {
    type Target = ArrayPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(ArrayPattern {
            mid: helper.add_meta(self.start, self.end),
            exprs,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TuplePatternRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) exprs: ArrayPredicatePatternsRaw<'script>,
    pub(crate) open: bool,
}

impl<'script> Upable<'script> for TuplePatternRaw<'script> {
    type Target = TuplePattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let exprs = self.exprs.up(helper)?;
        Ok(TuplePattern {
            mid: helper.add_meta(self.start, self.end),
            exprs,
            open: self.open,
        })
    }
}

pub(crate) const FN_RES_NAME: &str = "__fn_assign_this_is_ugly";

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AssignPatternRaw<'script> {
    pub(crate) id: Cow<'script, str>,
    pub(crate) pattern: Box<PatternRaw<'script>>,
}

impl<'script> Upable<'script> for AssignPatternRaw<'script> {
    type Target = AssignPattern<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(AssignPattern {
            idx: helper.register_shadow_var(&self.id),
            id: self.id,
            pattern: Box::new(self.pattern.up(helper)?),
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PathRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Local(LocalPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Event(EventPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    State(StatePathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Meta(MetadataPathRaw<'script>),
    /// we're forced to make this pub because of lalrpop
    Const(ConstPathRaw<'script>),
    /// Special reserved path
    Reserved(ReservedPathRaw<'script>),
}

impl<'script> Upable<'script> for PathRaw<'script> {
    type Target = Path<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        use PathRaw::{Const, Event, Local, Meta, Reserved, State};
        Ok(match self {
            Local(p) => {
                let p = p.up(helper)?;
                if p.is_const {
                    Path::Const(p)
                } else {
                    Path::Local(p)
                }
            }
            Const(p) => Path::Const(p.up(helper)?),
            Event(p) => Path::Event(p.up(helper)?),
            State(p) => Path::State(p.up(helper)?),
            Meta(p) => Path::Meta(p.up(helper)?),
            Reserved(p) => Path::Reserved(p.up(helper)?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// we're forced to make this pub because of lalrpop
pub struct SegmentRangeRaw<'script> {
    pub(crate) start_lower: Location,
    pub(crate) range_start: ImutExprRaw<'script>,
    pub(crate) end_lower: Location,
    pub(crate) start_upper: Location,
    pub(crate) range_end: ImutExprRaw<'script>,
    pub(crate) end_upper: Location,
}

impl<'script> Upable<'script> for SegmentRangeRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let SegmentRangeRaw {
            start_lower,
            range_start,
            end_lower,
            start_upper,
            range_end,
            end_upper,
        } = self;

        let lower_mid = helper.add_meta(start_lower, end_lower);
        let upper_mid = helper.add_meta(start_upper, end_upper);
        let mid = helper.add_meta(start_lower, end_upper);
        Ok(Segment::Range {
            lower_mid,
            upper_mid,
            range_start: Box::new(range_start.up(helper)?),
            range_end: Box::new(range_end.up(helper)?),
            mid,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SegmentElementRaw<'script> {
    pub(crate) expr: ImutExprRaw<'script>,
    pub(crate) start: Location,
    pub(crate) end: Location,
}

impl<'script> Upable<'script> for SegmentElementRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let SegmentElementRaw { expr, start, end } = self;
        let expr = expr.up(helper)?;
        let r = expr.extent(&helper.meta);
        match expr {
            ImutExprInt::Literal(l) => match reduce2(ImutExprInt::Literal(l), &helper)? {
                Value::String(id) => {
                    let mid = helper.add_meta_w_name(start, end, &id);
                    Ok(Segment::Id {
                        key: KnownKey::from(id.clone()),
                        mid,
                    })
                }
                other => {
                    if let Some(idx) = other.as_usize() {
                        let mid = helper.add_meta(start, end);
                        Ok(Segment::Idx { idx, mid })
                    } else {
                        Err(ErrorKind::TypeConflict(
                            r.expand_lines(2),
                            r,
                            other.value_type(),
                            vec![ValueType::I64, ValueType::String],
                        )
                        .into())
                    }
                }
            },
            expr => Ok(Segment::Element {
                mid: helper.add_meta(start, end),
                expr,
            }),
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SegmentRaw<'script> {
    /// we're forced to make this pub because of lalrpop
    Element(Box<SegmentElementRaw<'script>>),
    /// we're forced to make this pub because of lalrpop
    Range(Box<SegmentRangeRaw<'script>>),
}

impl<'script> Upable<'script> for SegmentRaw<'script> {
    type Target = Segment<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        match self {
            SegmentRaw::Element(e) => e.up(helper),
            SegmentRaw::Range(r) => r.up(helper),
        }
    }
}

impl<'script> SegmentRaw<'script> {
    pub fn from_id(id: IdentRaw<'script>) -> Self {
        SegmentRaw::Element(Box::new(SegmentElementRaw {
            start: id.start,
            end: id.end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start: id.start,
                end: id.end,
                value: Value::from(id.id),
            }),
        }))
    }
    pub fn from_str(id: &'script str, start: Location, end: Location) -> Self {
        SegmentRaw::Element(Box::new(SegmentElementRaw {
            start,
            end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start,
                end,
                value: Value::from(id),
            }),
        }))
    }
    pub fn from_usize(id: usize, start: Location, end: Location) -> Self {
        SegmentRaw::Element(Box::new(SegmentElementRaw {
            start,
            end,
            expr: ImutExprRaw::Literal(LiteralRaw {
                start,
                end,
                value: Value::from(id),
            }),
        }))
    }
}

impl<'script> From<ImutExprRaw<'script>> for ExprRaw<'script> {
    fn from(imut: ImutExprRaw<'script>) -> ExprRaw<'script> {
        ExprRaw::Imut(imut)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConstPathRaw<'script> {
    pub(crate) module: Vec<IdentRaw<'script>>,
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl_expr!(ConstPathRaw);

impl<'script> Upable<'script> for ConstPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { mid, .. }) = segments.next() {
            let segments = segments.collect();
            let id = helper.meta.name_dflt(mid).to_string();
            let mid = helper.add_meta_w_name(self.start, self.end, &id);
            let mut module_direct: Vec<String> =
                self.module.iter().map(|m| m.id.to_string()).collect();
            let mut module = helper.module.clone();
            module.append(&mut module_direct);
            module.push(id);
            if let Some(idx) = helper.is_const(&module) {
                Ok(LocalPath {
                    is_const: true,
                    idx: *idx,
                    mid,
                    segments,
                })
            } else {
                error_generic(
                    &(self.start, self.end),
                    &(self.start, self.end),
                    &format!(
                        "The constant {} (absolute path) is not defined.",
                        module.join("::")
                    ),
                    &helper.meta,
                )
            }
        } else {
            // We should never encounter this
            error_oops(
                &(self.start, self.end),
                0xdead_0007,
                "Empty local path",
                &helper.meta,
            )
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ReservedPathRaw<'script> {
    Args {
        start: Location,
        end: Location,
        segments: SegmentsRaw<'script>,
    },
    Window {
        start: Location,
        end: Location,
        segments: SegmentsRaw<'script>,
    },
    Group {
        start: Location,
        end: Location,
        segments: SegmentsRaw<'script>,
    },
}
impl<'script> BaseExpr for ReservedPathRaw<'script> {
    fn s(&self, _meta: &NodeMetas) -> Location {
        match self {
            ReservedPathRaw::Args { start, .. }
            | ReservedPathRaw::Window { start, .. }
            | ReservedPathRaw::Group { start, .. } => *start,
        }
    }

    fn e(&self, _meta: &NodeMetas) -> Location {
        match self {
            ReservedPathRaw::Args { end, .. }
            | ReservedPathRaw::Window { end, .. }
            | ReservedPathRaw::Group { end, .. } => *end,
        }
    }

    fn mid(&self) -> usize {
        0
    }
}
impl<'script> Upable<'script> for ReservedPathRaw<'script> {
    type Target = ReservedPath<'script>;

    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let r = match self {
            ReservedPathRaw::Args {
                start,
                end,
                segments,
            } => ReservedPath::Args {
                mid: helper.add_meta_w_name(start, end, &"args"),
                segments: segments.up(helper)?,
            },
            ReservedPathRaw::Window {
                start,
                end,
                segments,
            } => ReservedPath::Window {
                mid: helper.add_meta_w_name(start, end, &"window"),
                segments: segments.up(helper)?,
            },
            ReservedPathRaw::Group {
                start,
                end,
                segments,
            } => ReservedPath::Group {
                mid: helper.add_meta_w_name(start, end, &"group"),
                segments: segments.up(helper)?,
            },
        };
        Ok(r)
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LocalPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl_expr!(LocalPathRaw);

impl<'script> Upable<'script> for LocalPathRaw<'script> {
    type Target = LocalPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        let mut segments = segments.into_iter();
        if let Some(Segment::Id { mid, .. }) = segments.next() {
            let segments = segments.collect();
            let id = helper.meta.name_dflt(mid).to_string();
            let mid = helper.add_meta_w_name(self.start, self.end, &id);

            let mut rel_path = helper.module.clone();
            rel_path.push(id.to_string());
            let (idx, is_const) = helper
                .is_const(&rel_path)
                .copied()
                .map_or_else(|| (helper.var_id(&id), false), |idx| (idx, true));
            Ok(LocalPath {
                idx,
                is_const,
                mid,
                segments,
            })
        } else {
            // We should never encounter this
            error_oops(
                &(self.start, self.end),
                0xdead_0008,
                "Empty local path",
                &helper.meta,
            )
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for MetadataPathRaw<'script> {
    type Target = MetadataPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(MetadataPath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EventPathRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for EventPathRaw<'script> {
    type Target = EventPath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(EventPath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatePathRaw<'script> {
    pub start: Location,
    pub end: Location,
    pub segments: SegmentsRaw<'script>,
}
impl<'script> Upable<'script> for StatePathRaw<'script> {
    type Target = StatePath<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let segments = self.segments.up(helper)?;
        Ok(StatePath {
            mid: helper.add_meta(self.start, self.end),
            segments,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BinExprRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: BinOpKind,
    pub(crate) lhs: ImutExprRaw<'script>,
    pub(crate) rhs: ImutExprRaw<'script>,
    // query_stream_not_defined(stmt: &Box<S>, inner: &I, name: String)
}

impl<'script> Upable<'script> for BinExprRaw<'script> {
    type Target = BinExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(BinExpr {
            mid: helper.add_meta(self.start, self.end),
            kind: self.kind,
            lhs: self.lhs.up(helper)?,
            rhs: self.rhs.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct UnaryExprRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) kind: UnaryOpKind,
    pub(crate) expr: ImutExprRaw<'script>,
}

impl<'script> Upable<'script> for UnaryExprRaw<'script> {
    type Target = UnaryExpr<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        Ok(UnaryExpr {
            mid: helper.add_meta(self.start, self.end),
            kind: self.kind,
            expr: self.expr.up(helper)?,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MatchRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) target: ImutExprRaw<'script>,
    pub(crate) patterns: PredicatesRaw<'script, Ex>,
}
impl_expr_exraw!(MatchRaw);

// Sort clauses, move up cheaper clauses as long as they're exclusive
fn sort_clauses<Ex: Expression>(patterns: &mut [PredicateClause<Ex>]) {
    for i in (0..patterns.len()).rev() {
        for j in (1..=i).rev() {
            #[allow(
                // No clippy, we really mean j.exclusive(k) || k.exclusive(j)...
                clippy::suspicious_operation_groupings,
                // also what is wrong with you clippy ...
                clippy::blocks_in_if_conditions
            )]
            if patterns
                .get(j)
                .and_then(|jv| Some((jv, patterns.get(j - 1)?)))
                .map(|(j, k)| {
                    j.cost() <= k.cost() && (j.is_exclusive_to(k) || k.is_exclusive_to(j))
                })
                .unwrap_or_default()
            {
                patterns.swap(j, j - 1)
            }
        }
    }
}

impl<'script, Ex> Upable<'script> for MatchRaw<'script, Ex>
where
    <Ex as Upable<'script>>::Target: Expression + 'script,
    Ex: ExpressionRaw<'script> + 'script,
{
    type Target = Match<'script, Ex::Target>;
    #[allow(clippy::too_many_lines)]
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mut patterns: Vec<PredicateClause<_>> = self
            .patterns
            .into_iter()
            .map(|v| v.up(helper))
            .collect::<Result<_>>()?;

        let defaults = patterns
            .iter()
            .filter(|p| p.pattern.is_default() && p.guard.is_none())
            .count();
        match defaults {
                0 => helper.warn(Warning::new_with_scope(
                    Range(self.start, self.end),
                    "This match expression has no default clause, if the other clauses do not cover all possibilities this will lead to events being discarded with runtime errors.".into()
                )),
                x if x > 1 => helper.warn(Warning::new_with_scope(
                    Range(self.start, self.end),
                    "A match statement with more then one default clause will never reach any but the first default clause.".into()
                )),
            _ => ()
        };
        // If the last statement is a global default we can simply remove it
        let default = if let Some(PredicateClause {
            pattern: Pattern::Default,
            guard: None,
            exprs,
            last_expr,
            ..
        }) = patterns.last_mut()
        {
            let mut es = Vec::new();
            let mut last = Ex::Target::null_lit();
            std::mem::swap(exprs, &mut es);
            std::mem::swap(last_expr, &mut last);
            if es.is_empty() {
                if last.is_null_lit() {
                    DefaultCase::Null
                } else {
                    DefaultCase::One(last)
                }
            } else {
                DefaultCase::Many {
                    exprs: es,
                    last_expr: Box::new(last),
                }
            }
        } else {
            DefaultCase::None
        };

        if default != DefaultCase::None {
            patterns.pop();
        }

        // This shortcuts for if / else style matches
        if patterns.len() == 1 {
            let patterns = patterns
                .into_iter()
                .map(|p| ClauseGroup::Simple {
                    precondition: None,
                    patterns: vec![p],
                })
                .collect();
            return Ok(Match {
                mid: helper.add_meta(self.start, self.end),
                target: self.target.up(helper)?,
                patterns,
                default,
            });
        }
        sort_clauses(&mut patterns);
        let mut groups = Vec::new();
        let mut group: Vec<PredicateClause<_>> = Vec::new();

        for p in patterns {
            if group
                .iter()
                .all(|g| p.is_exclusive_to(g) || g.is_exclusive_to(&p))
            {
                group.push(p);
            } else {
                group.sort_by_key(Costly::cost);

                let mut g = ClauseGroup::Simple {
                    patterns: group,
                    precondition: None,
                };
                g.optimize(0);
                groups.push(g);
                group = vec![p];
            }
        }
        group.sort_by_key(Costly::cost);
        let mut g = ClauseGroup::Simple {
            patterns: group,
            precondition: None,
        };
        g.optimize(0);
        groups.push(g);

        let mut patterns: Vec<ClauseGroup<_>> = Vec::new();

        for g in groups {
            #[allow(clippy::option_if_let_else)]
            if let Some(last) = patterns.last_mut() {
                if last.combinable(&g) {
                    last.combine(g);
                } else {
                    patterns.push(g);
                }
            } else {
                patterns.push(g);
            }
        }
        Ok(Match {
            mid: helper.add_meta(self.start, self.end),
            target: self.target.up(helper)?,
            patterns,
            default,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: Vec<String>,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
}
impl_expr!(InvokeRaw);

impl<'script> Upable<'script> for InvokeRaw<'script> {
    type Target = Invoke<'script>;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if self.module.first() == Some(&String::from("core")) && self.module.len() == 2 {
            // we know a second module exists
            let module = self.module.get(1).cloned().unwrap_or_default();

            let invocable = helper
                .reg
                .find(&module, &self.fun)
                .map_err(|e| e.into_err(&self, &self, Some(&helper.reg), &helper.meta))?;
            let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
            let mf = format!("{}::{}", self.module.join("::"), self.fun);
            Ok(Invoke {
                mid: helper.add_meta_w_name(self.start, self.end, &mf),
                module: self.module,
                fun: self.fun,
                invocable: Invocable::Intrinsic(invocable.clone()),
                args,
            })
        } else {
            // Absolute locability from without a set of nested modules
            let mut abs_module = helper.module.clone();
            abs_module.extend_from_slice(&self.module);
            abs_module.push(self.fun.clone());

            // of the form: [mod, mod1, name] - where the list of idents is effectively a fully qualified resource name
            if let Some(f) = helper.functions.get(&abs_module) {
                if let Some(f) = helper.func_vec.get(*f) {
                    let invocable = Invocable::Tremor(f.clone());
                    let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
                    let mf = abs_module.join("::");
                    Ok(Invoke {
                        mid: helper.add_meta_w_name(self.start, self.end, &mf),
                        module: self.module,
                        fun: self.fun,
                        invocable,
                        args,
                    })
                } else {
                    let inner: Range = (self.start, self.end).into();
                    let outer: Range = inner.expand_lines(3);
                    Err(
                        ErrorKind::MissingFunction(outer, inner, self.module, self.fun, None)
                            .into(),
                    )
                }
            } else {
                let inner: Range = (self.start, self.end).into();
                let outer: Range = inner.expand_lines(3);
                Err(ErrorKind::MissingFunction(outer, inner, self.module, self.fun, None).into())
            }
        }
    }
}

impl<'script> InvokeRaw<'script> {
    fn is_aggregate<'registry>(&self, helper: &mut Helper<'script, 'registry>) -> bool {
        if self.module.first() == Some(&String::from("aggr")) && self.module.len() == 2 {
            let module = self.module.get(1).cloned().unwrap_or_default();
            helper.aggr_reg.find(&module, &self.fun).is_ok()
        } else {
            false
        }
    }

    fn into_aggregate(self) -> InvokeAggrRaw<'script> {
        let module = self.module.get(1).cloned().unwrap_or_default();
        InvokeAggrRaw {
            start: self.start,
            end: self.end,
            module,
            fun: self.fun,
            args: self.args,
        }
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct InvokeAggrRaw<'script> {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) module: String,
    pub(crate) fun: String,
    pub(crate) args: ImutExprsRaw<'script>,
}
impl_expr!(InvokeAggrRaw);

impl<'script> Upable<'script> for InvokeAggrRaw<'script> {
    type Target = InvokeAggr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        if helper.is_in_aggr {
            return Err(ErrorKind::AggrInAggr(
                self.extent(&helper.meta),
                self.extent(&helper.meta).expand_lines(2),
            )
            .into());
        };
        helper.is_in_aggr = true;
        let invocable = helper
            .aggr_reg
            .find(&self.module, &self.fun)
            .map_err(|e| e.into_err(&self, &self, Some(&helper.reg), &helper.meta))?
            .clone();
        if !invocable.valid_arity(self.args.len()) {
            return Err(ErrorKind::BadArity(
                self.extent(&helper.meta),
                self.extent(&helper.meta).expand_lines(2),
                self.module.clone(),
                self.fun.clone(),
                invocable.arity(),
                self.args.len(),
            )
            .into());
        }
        if let Some(warning) = invocable.warning() {
            helper.warn(Warning::new_with_scope(self.extent(&helper.meta), warning));
        }
        let aggr_id = helper.aggregates.len();
        let args = self.args.up(helper)?.into_iter().map(ImutExpr).collect();
        let mf = format!("{}::{}", self.module, self.fun);
        let invoke_meta_id = helper.add_meta_w_name(self.start, self.end, &mf);

        helper.aggregates.push(InvokeAggrFn {
            mid: invoke_meta_id,
            invocable,
            args,
            module: self.module.clone(),
            fun: self.fun.clone(),
        });
        helper.is_in_aggr = false;
        let aggr_meta_id = helper.add_meta_w_name(self.start, self.end, &mf);
        Ok(InvokeAggr {
            mid: aggr_meta_id,
            module: self.module,
            fun: self.fun,
            aggr_id,
        })
    }
}

/// we're forced to make this pub because of lalrpop
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TestExprRaw {
    pub(crate) start: Location,
    pub(crate) end: Location,
    pub(crate) id: String,
    pub(crate) test: String,
}

impl<'script> Upable<'script> for TestExprRaw {
    type Target = TestExpr;
    fn up<'registry>(self, helper: &mut Helper<'script, 'registry>) -> Result<Self::Target> {
        let mid = helper.add_meta(self.start, self.end);
        match Extractor::new(&self.id, &self.test) {
            Ok(ex) => Ok(TestExpr {
                id: self.id,
                test: self.test,
                extractor: ex,
                mid,
            }),
            Err(e) => Err(ErrorKind::InvalidExtractor(
                self.extent(&helper.meta).expand_lines(2),
                self.extent(&helper.meta),
                self.id,
                self.test,
                e.msg,
            )
            .into()),
        }
    }
}

pub type ExprsRaw<'script> = Vec<ExprRaw<'script>>;
pub type ImutExprsRaw<'script> = Vec<ImutExprRaw<'script>>;
pub type FieldsRaw<'script> = Vec<FieldRaw<'script>>;
pub type SegmentsRaw<'script> = Vec<SegmentRaw<'script>>;
pub type PatternFieldsRaw<'script> = Vec<PredicatePatternRaw<'script>>;
pub type PredicatesRaw<'script, Ex> = Vec<PredicateClauseRaw<'script, Ex>>;
pub type PatchOperationsRaw<'script> = Vec<PatchOperationRaw<'script>>;
pub type ComprehensionCasesRaw<'script, Ex> = Vec<ComprehensionCaseRaw<'script, Ex>>;
pub type ImutComprehensionCasesRaw<'script> = Vec<ImutComprehensionCaseRaw<'script>>;
pub type ArrayPredicatePatternsRaw<'script> = Vec<ArrayPredicatePatternRaw<'script>>;
pub type WithExprsRaw<'script> = Vec<(IdentRaw<'script>, ImutExprRaw<'script>)>;

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn default() {
        assert_eq!(Endian::default(), Endian::Big);
        assert_eq!(BytesDataType::default(), BytesDataType::UnsignedInteger);
    }
}
