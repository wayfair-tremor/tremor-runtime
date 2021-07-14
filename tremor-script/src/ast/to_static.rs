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

// This is just one big identify function with a few Cow::owned in it
#![cfg(not(tarpaulin_include))]

use beef::Cow;
use tremor_value::Value;

use crate::CustomFn;

use super::{
    ArrayPattern, ArrayPredicatePattern, AssignPattern, BinExpr, Bytes, BytesPart, ClauseGroup,
    ClausePreCondition, Comprehension, ComprehensionCase, Consts, DefaultCase, EmitExpr, EventPath,
    Expr, Field, IfElse, ImutExpr, ImutExprInt, Invocable, Invoke, InvokeAggrFn, List, Literal,
    LocalPath, Match, Merge, MetadataPath, OperatorDecl, Patch, PatchOperation, Path, Pattern,
    PredicateClause, PredicatePattern, Record, RecordPattern, Recur, ReservedPath, Script, Segment,
    StatePath, StrLitElement, StringLit, TuplePattern, UnaryExpr,
};

use super::query::WindowDecl;

impl<'script> ImutExpr<'script> {
    pub(crate) fn into_static(self) -> ImutExpr<'static> {
        ImutExpr(self.0.into_static())
    }
}
impl<'script> ImutExprInt<'script> {
    pub(crate) fn into_static(self) -> ImutExprInt<'static> {
        match self {
            ImutExprInt::Record(e) => ImutExprInt::Record(e.into_static()),
            ImutExprInt::List(e) => ImutExprInt::List(e.into_static()),
            ImutExprInt::Binary(e) => ImutExprInt::Binary(Box::new(e.into_static())),
            ImutExprInt::Unary(e) => ImutExprInt::Unary(Box::new(e.into_static())),
            ImutExprInt::Patch(e) => ImutExprInt::Patch(Box::new(e.into_static())),
            ImutExprInt::Match(e) => ImutExprInt::Match(Box::new(e.into_static())),
            ImutExprInt::Comprehension(e) => ImutExprInt::Comprehension(Box::new(e.into_static())),
            ImutExprInt::Merge(e) => ImutExprInt::Merge(Box::new(e.into_static())),
            ImutExprInt::Path(p) => ImutExprInt::Path(p.into_static()),
            ImutExprInt::String(e) => ImutExprInt::String(e.into_static()),
            ImutExprInt::Local { idx, mid, is_const } => ImutExprInt::Local { idx, mid, is_const },
            ImutExprInt::Literal(l) => ImutExprInt::Literal(l.into_static()),
            ImutExprInt::Present { path, mid } => ImutExprInt::Present {
                path: path.into_static(),
                mid,
            },
            ImutExprInt::Invoke1(i) => ImutExprInt::Invoke1(i.into_static()),
            ImutExprInt::Invoke2(i) => ImutExprInt::Invoke2(i.into_static()),
            ImutExprInt::Invoke3(i) => ImutExprInt::Invoke3(i.into_static()),
            ImutExprInt::Invoke(i) => ImutExprInt::Invoke(i.into_static()),
            ImutExprInt::InvokeAggr(e) => ImutExprInt::InvokeAggr(e),
            ImutExprInt::Recur(r) => ImutExprInt::Recur(r.into_static()),
            ImutExprInt::Bytes(e) => ImutExprInt::Bytes(e.into_static()),
        }
    }
}
impl<'script> Recur<'script> {
    fn into_static(self) -> Recur<'static> {
        let Recur {
            mid,
            argc,
            open,
            exprs,
        } = self;
        Recur {
            mid,
            argc,
            open,
            exprs: exprs.into_iter().map(ImutExpr::into_static).collect(),
        }
    }
}

impl<'script> Literal<'script> {
    fn into_static(self) -> Literal<'static> {
        let Literal { mid, value } = self;
        Literal {
            mid,
            value: value.into_static(),
        }
    }
}

impl<'script> Path<'script> {
    fn into_static(self) -> Path<'static> {
        match self {
            Path::Const(p) => Path::Const(p.into_static()),
            Path::Local(p) => Path::Local(p.into_static()),
            Path::Event(p) => Path::Event(p.into_static()),
            Path::State(p) => Path::State(p.into_static()),
            Path::Meta(p) => Path::Meta(p.into_static()),
            Path::Reserved(p) => Path::Reserved(p.into_static()),
        }
    }
}

impl<'script> Invoke<'script> {
    fn into_static(self) -> Invoke<'static> {
        let Invoke {
            mid,
            module,
            fun,
            invocable,
            args,
        } = self;
        Invoke {
            mid,
            module,
            fun,
            invocable: invocable.into_static(),
            args: args.into_iter().map(ImutExpr::into_static).collect(),
        }
    }
}

impl<'script> Invocable<'script> {
    fn into_static(self) -> Invocable<'static> {
        match self {
            Invocable::Intrinsic(i) => Invocable::Intrinsic(i),
            Invocable::Tremor(f) => Invocable::Tremor(f.into_static()),
        }
    }
}

impl<'script> CustomFn<'script> {
    fn into_static(self) -> CustomFn<'static> {
        let CustomFn {
            name,
            body,
            args,
            open,
            locals,
            is_const,
            inline,
        } = self;
        CustomFn {
            name: Cow::owned(name.to_string()),
            body: body.into_iter().map(Expr::into_static).collect(),
            args,
            open,
            locals,
            is_const,
            inline,
        }
    }
}

impl<'script> Expr<'script> {
    pub(crate) fn into_static(self) -> Expr<'static> {
        match self {
            Expr::Match(e) => Expr::Match(Box::new(e.into_static())),
            Expr::IfElse(e) => Expr::IfElse(Box::new(e.into_static())),
            Expr::PatchInPlace(e) => Expr::PatchInPlace(Box::new(e.into_static())),
            Expr::MergeInPlace(e) => Expr::MergeInPlace(Box::new(e.into_static())),
            Expr::Assign { mid, path, expr } => Expr::Assign {
                mid,
                path: path.into_static(),
                expr: Box::new(expr.into_static()),
            },
            Expr::AssignMoveLocal { mid, path, idx } => Expr::AssignMoveLocal {
                mid,
                path: path.into_static(),
                idx,
            },
            Expr::Comprehension(e) => Expr::Comprehension(Box::new(e.into_static())),
            Expr::Drop { mid } => Expr::Drop { mid },
            Expr::Emit(e) => Expr::Emit(Box::new(e.into_static())),
            Expr::Imut(e) => Expr::Imut(e.into_static()),
        }
    }
}

impl<'script> Record<'script> {
    pub(crate) fn into_static(self) -> Record<'static> {
        let Record { mid, fields, base } = self;
        let v: Value<'static> = Value::from(base).into_static();
        let base = if let Value::Object(v) = v {
            *v
        } else {
            // ALLOW: we know this isn't reachable as we create v above
            unreachable!()
        };

        Record {
            base,
            mid,
            fields: fields.into_iter().map(Field::into_static).collect(),
        }
    }
}

impl<'script> List<'script> {
    pub(crate) fn into_static(self) -> List<'static> {
        let List { mid, exprs } = self;
        List {
            mid,
            exprs: exprs.into_iter().map(ImutExpr::into_static).collect(),
        }
    }
}

impl<'script> BinExpr<'script> {
    pub(crate) fn into_static(self) -> BinExpr<'static> {
        let BinExpr {
            mid,
            kind,
            lhs,
            rhs,
        } = self;
        BinExpr {
            mid,
            kind,
            lhs: lhs.into_static(),
            rhs: rhs.into_static(),
        }
    }
}

impl<'script> UnaryExpr<'script> {
    pub(crate) fn into_static(self) -> UnaryExpr<'static> {
        let UnaryExpr { mid, kind, expr } = self;
        UnaryExpr {
            mid,
            kind,
            expr: expr.into_static(),
        }
    }
}

impl<'script> Patch<'script> {
    pub(crate) fn into_static(self) -> Patch<'static> {
        let Patch {
            mid,
            target,
            operations,
        } = self;
        Patch {
            mid,
            target: target.into_static(),
            operations: operations
                .into_iter()
                .map(PatchOperation::into_static)
                .collect(),
        }
    }
}

impl<'script> Merge<'script> {
    pub(crate) fn into_static(self) -> Merge<'static> {
        let Merge { mid, target, expr } = self;
        Merge {
            mid,
            target: target.into_static(),
            expr: expr.into_static(),
        }
    }
}

impl<'script> Match<'script, ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> Match<'static, ImutExprInt<'static>> {
        let Match {
            mid,
            target,
            patterns,
            default,
        } = self;
        Match {
            mid,
            target: target.into_static(),
            patterns: patterns
                .into_iter()
                .map(ClauseGroup::<ImutExprInt>::into_static)
                .collect(),
            default: default.into_static(),
        }
    }
}
impl<'script> Match<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> Match<'static, Expr<'static>> {
        let Match {
            mid,
            target,
            patterns,
            default,
        } = self;
        Match {
            mid,
            target: target.into_static(),
            patterns: patterns
                .into_iter()
                .map(ClauseGroup::<Expr>::into_static)
                .collect(),
            default: default.into_static(),
        }
    }
}

// impl<'script> IfElse<'script, ImutExprInt<'script>> {
//     pub(crate) fn into_static(self) -> IfElse<'static, ImutExprInt<'static>> {
//         let IfElse {
//             mid,
//             target,
//             if_clause,
//             else_clause,
//         } = self;
//         IfElse {
//             mid,
//             target: target.into_static(),
//             if_clause: if_clause.into_static(),
//             else_clause: else_clause.into_static(),
//         }
//     }
// }

impl<'script> IfElse<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> IfElse<'static, Expr<'static>> {
        let IfElse {
            mid,
            target,
            if_clause,
            else_clause,
        } = self;
        IfElse {
            mid,
            target: target.into_static(),
            if_clause: if_clause.into_static(),
            else_clause: else_clause.into_static(),
        }
    }
}

impl<'script> ClauseGroup<'script, ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> ClauseGroup<'static, ImutExprInt<'static>> {
        match self {
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => ClauseGroup::Simple {
                precondition: precondition.map(ClausePreCondition::into_static),
                patterns: patterns
                    .into_iter()
                    .map(PredicateClause::<ImutExprInt>::into_static)
                    .collect(),
            },
            ClauseGroup::SearchTree {
                precondition,
                tree,
                rest,
            } => ClauseGroup::SearchTree {
                precondition: precondition.map(ClausePreCondition::into_static),
                tree: tree
                    .into_iter()
                    .map(|(v, (es, e))| {
                        (
                            v.into_static(),
                            (
                                es.into_iter()
                                    .map(ImutExprInt::into_static)
                                    .collect::<Vec<_>>(),
                                e.into_static(),
                            ),
                        )
                    })
                    .collect(),
                rest: rest
                    .into_iter()
                    .map(PredicateClause::<ImutExprInt>::into_static)
                    .collect(),
            },
            ClauseGroup::Combined {
                precondition,
                groups,
            } => ClauseGroup::Combined {
                precondition: precondition.map(ClausePreCondition::into_static),
                groups: groups
                    .into_iter()
                    .map(ClauseGroup::<ImutExprInt>::into_static)
                    .collect(),
            },
            ClauseGroup::Single {
                precondition,
                pattern,
            } => ClauseGroup::Single {
                precondition: precondition.map(ClausePreCondition::into_static),
                pattern: pattern.into_static(),
            },
        }
    }
}

impl<'script> ClauseGroup<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> ClauseGroup<'static, Expr<'static>> {
        match self {
            ClauseGroup::Simple {
                precondition,
                patterns,
            } => ClauseGroup::Simple {
                precondition: precondition.map(ClausePreCondition::into_static),
                patterns: patterns
                    .into_iter()
                    .map(PredicateClause::<Expr>::into_static)
                    .collect(),
            },
            ClauseGroup::SearchTree {
                precondition,
                tree,
                rest,
            } => ClauseGroup::SearchTree {
                precondition: precondition.map(ClausePreCondition::into_static),
                tree: tree
                    .into_iter()
                    .map(|(v, (es, e))| {
                        (
                            v.into_static(),
                            (
                                es.into_iter().map(Expr::into_static).collect::<Vec<_>>(),
                                e.into_static(),
                            ),
                        )
                    })
                    .collect(),
                rest: rest
                    .into_iter()
                    .map(PredicateClause::<Expr>::into_static)
                    .collect(),
            },
            ClauseGroup::Combined {
                precondition,
                groups,
            } => ClauseGroup::Combined {
                precondition: precondition.map(ClausePreCondition::into_static),
                groups: groups
                    .into_iter()
                    .map(ClauseGroup::<Expr>::into_static)
                    .collect(),
            },
            ClauseGroup::Single {
                precondition,
                pattern,
            } => ClauseGroup::Single {
                precondition: precondition.map(ClausePreCondition::into_static),
                pattern: pattern.into_static(),
            },
        }
    }
}

impl<'script> PredicateClause<'script, ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> PredicateClause<'static, ImutExprInt<'static>> {
        let PredicateClause {
            mid,
            pattern,
            guard,
            exprs,
            last_expr,
        } = self;

        PredicateClause {
            mid,
            pattern: pattern.into_static(),
            guard: guard.map(ImutExprInt::into_static),
            exprs: exprs.into_iter().map(ImutExprInt::into_static).collect(),
            last_expr: last_expr.into_static(),
        }
    }
}

impl<'script> PredicateClause<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> PredicateClause<'static, Expr<'static>> {
        let PredicateClause {
            mid,
            pattern,
            guard,
            exprs,
            last_expr,
        } = self;

        PredicateClause {
            mid,
            pattern: pattern.into_static(),
            guard: guard.map(ImutExprInt::into_static),
            exprs: exprs.into_iter().map(Expr::into_static).collect(),
            last_expr: last_expr.into_static(),
        }
    }
}

impl<'script> DefaultCase<ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> DefaultCase<ImutExprInt<'static>> {
        match self {
            DefaultCase::None => DefaultCase::None,
            DefaultCase::Null => DefaultCase::Null,
            DefaultCase::Many { exprs, last_expr } => DefaultCase::Many {
                exprs: exprs.into_iter().map(ImutExprInt::into_static).collect(),
                last_expr: Box::new(last_expr.into_static()),
            },
            DefaultCase::One(e) => DefaultCase::One(e.into_static()),
        }
    }
}

impl<'script> DefaultCase<Expr<'script>> {
    pub(crate) fn into_static(self) -> DefaultCase<Expr<'static>> {
        match self {
            DefaultCase::None => DefaultCase::None,
            DefaultCase::Null => DefaultCase::Null,
            DefaultCase::Many { exprs, last_expr } => DefaultCase::Many {
                exprs: exprs.into_iter().map(Expr::into_static).collect(),
                last_expr: Box::new(last_expr.into_static()),
            },
            DefaultCase::One(e) => DefaultCase::One(e.into_static()),
        }
    }
}

impl<'script> Comprehension<'script, ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> Comprehension<'static, ImutExprInt<'static>> {
        let Comprehension {
            mid,
            key_id,
            val_id,
            target,
            cases,
        } = self;
        Comprehension {
            mid,
            key_id,
            val_id,
            target: target.into_static(),
            cases: cases
                .into_iter()
                .map(ComprehensionCase::<ImutExprInt>::into_static)
                .collect(),
        }
    }
}
impl<'script> Comprehension<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> Comprehension<'static, Expr<'static>> {
        let Comprehension {
            mid,
            key_id,
            val_id,
            target,
            cases,
        } = self;
        Comprehension {
            mid,
            key_id,
            val_id,
            target: target.into_static(),
            cases: cases
                .into_iter()
                .map(ComprehensionCase::<Expr>::into_static)
                .collect(),
        }
    }
}

impl<'script> ComprehensionCase<'script, ImutExprInt<'script>> {
    pub(crate) fn into_static(self) -> ComprehensionCase<'static, ImutExprInt<'static>> {
        let ComprehensionCase {
            mid,
            key_name,
            value_name,
            guard,
            exprs,
            last_expr,
        } = self;
        ComprehensionCase {
            mid,
            key_name: Cow::owned(key_name.to_string()),
            value_name: Cow::owned(value_name.to_string()),
            guard: guard.map(ImutExprInt::into_static),
            exprs: exprs.into_iter().map(ImutExprInt::into_static).collect(),
            last_expr: last_expr.into_static(),
        }
    }
}

impl<'script> ComprehensionCase<'script, Expr<'script>> {
    pub(crate) fn into_static(self) -> ComprehensionCase<'static, Expr<'static>> {
        let ComprehensionCase {
            mid,
            key_name,
            value_name,
            guard,
            exprs,
            last_expr,
        } = self;
        ComprehensionCase {
            mid,
            key_name: Cow::owned(key_name.to_string()),
            value_name: Cow::owned(value_name.to_string()),
            guard: guard.map(ImutExprInt::into_static),
            exprs: exprs.into_iter().map(Expr::into_static).collect(),
            last_expr: last_expr.into_static(),
        }
    }
}

impl<'script> StringLit<'script> {
    pub(crate) fn into_static(self) -> StringLit<'static> {
        let StringLit { mid, elements } = self;
        StringLit {
            mid,
            elements: elements
                .into_iter()
                .map(StrLitElement::into_static)
                .collect(),
        }
    }
}

impl<'script> StrLitElement<'script> {
    pub(crate) fn into_static(self) -> StrLitElement<'static> {
        match self {
            StrLitElement::Lit(e) => StrLitElement::Lit(Cow::owned(e.to_string())),
            StrLitElement::Expr(e) => StrLitElement::Expr(e.into_static()),
        }
    }
}

impl<'script> Bytes<'script> {
    pub(crate) fn into_static(self) -> Bytes<'static> {
        let Bytes { mid, value } = self;
        Bytes {
            mid,
            value: value.into_iter().map(BytesPart::into_static).collect(),
        }
    }
}

impl<'script> BytesPart<'script> {
    pub(crate) fn into_static(self) -> BytesPart<'static> {
        let BytesPart {
            mid,
            data,
            data_type,
            endianess,
            bits,
        } = self;
        BytesPart {
            mid,
            data: data.into_static(),
            data_type,
            endianess,
            bits,
        }
    }
}

impl<'script> LocalPath<'script> {
    pub(crate) fn into_static(self) -> LocalPath<'static> {
        let LocalPath {
            idx,
            is_const,
            mid,
            segments,
        } = self;
        LocalPath {
            idx,
            is_const,
            mid,
            segments: segments.into_iter().map(Segment::into_static).collect(),
        }
    }
}

impl<'script> EventPath<'script> {
    pub(crate) fn into_static(self) -> EventPath<'static> {
        let EventPath { mid, segments } = self;
        EventPath {
            mid,
            segments: segments.into_iter().map(Segment::into_static).collect(),
        }
    }
}
impl<'script> StatePath<'script> {
    pub(crate) fn into_static(self) -> StatePath<'static> {
        let StatePath { mid, segments } = self;
        StatePath {
            mid,
            segments: segments.into_iter().map(Segment::into_static).collect(),
        }
    }
}

impl<'script> MetadataPath<'script> {
    pub(crate) fn into_static(self) -> MetadataPath<'static> {
        let MetadataPath { mid, segments } = self;
        MetadataPath {
            mid,
            segments: segments.into_iter().map(Segment::into_static).collect(),
        }
    }
}

impl<'script> ReservedPath<'script> {
    pub(crate) fn into_static(self) -> ReservedPath<'static> {
        match self {
            ReservedPath::Args { mid, segments } => ReservedPath::Args {
                mid,
                segments: segments.into_iter().map(Segment::into_static).collect(),
            },
            ReservedPath::Window { mid, segments } => ReservedPath::Window {
                mid,
                segments: segments.into_iter().map(Segment::into_static).collect(),
            },
            ReservedPath::Group { mid, segments } => ReservedPath::Group {
                mid,
                segments: segments.into_iter().map(Segment::into_static).collect(),
            },
        }
    }
}
impl<'script> Segment<'script> {
    pub(crate) fn into_static(self) -> Segment<'static> {
        match self {
            Segment::Id { key, mid } => Segment::Id {
                key: key.into_static(),
                mid,
            },
            Segment::Idx { idx, mid } => Segment::Idx { idx, mid },
            Segment::Element { expr, mid } => Segment::Element {
                expr: expr.into_static(),
                mid,
            },
            Segment::Range {
                lower_mid,
                upper_mid,
                mid,
                range_start,
                range_end,
            } => Segment::Range {
                lower_mid,
                upper_mid,
                mid,
                range_start: Box::new(range_start.into_static()),
                range_end: Box::new(range_end.into_static()),
            },
        }
    }
}

impl<'script> EmitExpr<'script> {
    pub(crate) fn into_static(self) -> EmitExpr<'static> {
        let EmitExpr { mid, expr, port } = self;
        EmitExpr {
            mid,
            expr: expr.into_static(),
            port: port.map(ImutExprInt::into_static),
        }
    }
}

impl<'script> Field<'script> {
    pub(crate) fn into_static(self) -> Field<'static> {
        let Field { mid, name, value } = self;
        Field {
            mid,
            name: name.into_static(),
            value: value.into_static(),
        }
    }
}

impl<'script> PatchOperation<'script> {
    pub(crate) fn into_static(self) -> PatchOperation<'static> {
        match self {
            PatchOperation::Insert { ident, expr } => PatchOperation::Insert {
                ident: ident.into_static(),
                expr: expr.into_static(),
            },
            PatchOperation::Upsert { ident, expr } => PatchOperation::Upsert {
                ident: ident.into_static(),
                expr: expr.into_static(),
            },
            PatchOperation::Update { ident, expr } => PatchOperation::Update {
                ident: ident.into_static(),
                expr: expr.into_static(),
            },
            PatchOperation::Erase { ident } => PatchOperation::Erase {
                ident: ident.into_static(),
            },
            PatchOperation::Copy { from, to } => PatchOperation::Copy {
                from: from.into_static(),
                to: to.into_static(),
            },
            PatchOperation::Move { from, to } => PatchOperation::Move {
                from: from.into_static(),
                to: to.into_static(),
            },
            PatchOperation::Merge { ident, expr } => PatchOperation::Merge {
                ident: ident.into_static(),
                expr: expr.into_static(),
            },
            PatchOperation::MergeRecord { expr } => PatchOperation::MergeRecord {
                expr: expr.into_static(),
            },
            PatchOperation::Default { ident, expr } => PatchOperation::Default {
                ident: ident.into_static(),
                expr: expr.into_static(),
            },
            PatchOperation::DefaultRecord { expr } => PatchOperation::DefaultRecord {
                expr: expr.into_static(),
            },
        }
    }
}

impl<'script> ClausePreCondition<'script> {
    pub(crate) fn into_static(self) -> ClausePreCondition<'static> {
        let ClausePreCondition { path } = self;
        ClausePreCondition {
            path: path.into_static(),
        }
    }
}

impl<'script> Pattern<'script> {
    pub(crate) fn into_static(self) -> Pattern<'static> {
        match self {
            Pattern::Record(e) => Pattern::Record(e.into_static()),
            Pattern::Array(e) => Pattern::Array(e.into_static()),
            Pattern::Expr(e) => Pattern::Expr(e.into_static()),
            Pattern::Assign(e) => Pattern::Assign(e.into_static()),
            Pattern::Tuple(e) => Pattern::Tuple(e.into_static()),
            Pattern::Extract(e) => Pattern::Extract(e),
            Pattern::DoNotCare => Pattern::DoNotCare,
            Pattern::Default => Pattern::Default,
        }
    }
}

impl<'script> RecordPattern<'script> {
    pub(crate) fn into_static(self) -> RecordPattern<'static> {
        let RecordPattern { mid, fields } = self;
        RecordPattern {
            mid,
            fields: fields
                .into_iter()
                .map(PredicatePattern::into_static)
                .collect(),
        }
    }
}

impl<'script> ArrayPattern<'script> {
    pub(crate) fn into_static(self) -> ArrayPattern<'static> {
        let ArrayPattern { mid, exprs } = self;
        ArrayPattern {
            mid,
            exprs: exprs
                .into_iter()
                .map(ArrayPredicatePattern::into_static)
                .collect(),
        }
    }
}

impl<'script> AssignPattern<'script> {
    pub(crate) fn into_static(self) -> AssignPattern<'static> {
        let AssignPattern { id, idx, pattern } = self;
        AssignPattern {
            id: Cow::owned(id.to_string()),
            idx,
            pattern: Box::new(pattern.into_static()),
        }
    }
}

impl<'script> TuplePattern<'script> {
    pub(crate) fn into_static(self) -> TuplePattern<'static> {
        let TuplePattern { mid, exprs, open } = self;
        TuplePattern {
            mid,
            exprs: exprs
                .into_iter()
                .map(ArrayPredicatePattern::into_static)
                .collect(),
            open,
        }
    }
}

impl<'script> PredicatePattern<'script> {
    pub(crate) fn into_static(self) -> PredicatePattern<'static> {
        match self {
            PredicatePattern::TildeEq {
                assign,
                lhs,
                key,
                test,
            } => PredicatePattern::TildeEq {
                assign: Cow::owned(assign.to_string()),
                lhs: Cow::owned(lhs.to_string()),
                key: key.into_static(),
                test,
            },
            PredicatePattern::Bin {
                lhs,
                key,
                rhs,
                kind,
            } => PredicatePattern::Bin {
                lhs: Cow::owned(lhs.to_string()),
                key: key.into_static(),
                rhs: rhs.into_static(),
                kind,
            },
            PredicatePattern::RecordPatternEq { lhs, key, pattern } => {
                PredicatePattern::RecordPatternEq {
                    lhs: Cow::owned(lhs.to_string()),
                    key: key.into_static(),
                    pattern: pattern.into_static(),
                }
            }
            PredicatePattern::ArrayPatternEq { lhs, key, pattern } => {
                PredicatePattern::ArrayPatternEq {
                    lhs: Cow::owned(lhs.to_string()),
                    key: key.into_static(),
                    pattern: pattern.into_static(),
                }
            }
            PredicatePattern::FieldPresent { lhs, key } => PredicatePattern::FieldPresent {
                lhs: Cow::owned(lhs.to_string()),
                key: key.into_static(),
            },
            PredicatePattern::FieldAbsent { lhs, key } => PredicatePattern::FieldAbsent {
                lhs: Cow::owned(lhs.to_string()),
                key: key.into_static(),
            },
        }
    }
}

impl<'script> ArrayPredicatePattern<'script> {
    pub(crate) fn into_static(self) -> ArrayPredicatePattern<'static> {
        match self {
            ArrayPredicatePattern::Expr(e) => ArrayPredicatePattern::Expr(e.into_static()),
            ArrayPredicatePattern::Tilde(e) => ArrayPredicatePattern::Tilde(e),
            ArrayPredicatePattern::Record(e) => ArrayPredicatePattern::Record(e.into_static()),
            ArrayPredicatePattern::Ignore => ArrayPredicatePattern::Ignore,
        }
    }
}

impl<'script> InvokeAggrFn<'script> {
    pub(crate) fn into_static(self) -> InvokeAggrFn<'static> {
        let InvokeAggrFn {
            mid,
            invocable,
            module,
            fun,
            args,
        } = self;
        InvokeAggrFn {
            mid,
            invocable,
            module,
            fun,
            args: args.into_iter().map(ImutExpr::into_static).collect(),
        }
    }
}

impl<'script> Script<'script> {
    pub(crate) fn into_static(self) -> Script<'static> {
        let Script {
            imports,
            exprs,
            consts,
            aggregates,
            windows,
            functions,
            locals,
            node_meta,
            docs,
        } = self;
        Script {
            imports,
            exprs: exprs.into_iter().map(Expr::into_static).collect(),
            consts: consts.into_static(),
            aggregates: aggregates
                .into_iter()
                .map(InvokeAggrFn::into_static)
                .collect(),
            windows: windows
                .into_iter()
                .map(|(k, v)| (k, v.into_static()))
                .collect(),
            functions: functions.into_iter().map(CustomFn::into_static).collect(),
            locals,
            node_meta,
            docs,
        }
    }
}

impl<'script> Consts<'script> {
    pub(crate) fn into_static(self) -> Consts<'static> {
        let Consts {
            names,
            values,
            args,
            group,
            window,
        } = self;
        Consts {
            names,
            values: values.into_iter().map(Value::into_static).collect(),
            args: args.into_static(),
            group: group.into_static(),
            window: window.into_static(),
        }
    }
}

impl<'script> WindowDecl<'script> {
    /// Removes lifetime dependencies from a `WindowDecl`
    #[must_use]
    pub fn into_static(self) -> WindowDecl<'static> {
        let WindowDecl {
            mid,
            module,
            id,
            kind,
            params,
            script,
        } = self;
        WindowDecl {
            mid,
            module,
            id,
            kind,
            params: params
                .into_iter()
                .map(|(k, v)| (k, v.into_static()))
                .collect(),

            script: script.map(Script::into_static),
        }
    }
}

impl<'script> OperatorDecl<'script> {
    /// Removes lifetime dependencies from a `WindowDecl`
    #[must_use]
    pub fn into_static(self) -> OperatorDecl<'static> {
        let OperatorDecl {
            mid,
            kind,
            module,
            id,
            params,
        } = self;
        OperatorDecl {
            mid,
            kind,
            module,
            id,
            params: params.map(|params| {
                params
                    .into_iter()
                    .map(|(k, v)| (k, v.into_static()))
                    .collect()
            }),
        }
    }
}
