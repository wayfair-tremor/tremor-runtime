// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crate::{Event, Operator};
use halfbrown::HashMap;
use serde::Serialize;
use simd_json::borrowed::Value;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::sync::Arc;
//use simd_json::value::ValueTrait;
//use tremor_script::ast::ImutExpr;
use tremor_script::{
    self,
    ast::InvokeAggrFn,
    interpreter::{AggrType, ExecOpts},
    script::rentals::Stmt,
    Context, EventContext,
}; //, FN_REGISTRY};

pub type Aggrs<'script> = Vec<InvokeAggrFn<'script, EventContext>>;

rental! {
    pub mod rentals {
        use tremor_script::script::rentals::Query;
        use std::sync::Arc;
        use halfbrown::HashMap;
        use serde::Serialize;
        use super::*;

        #[rental(covariant,debug)]
        pub struct Dims<Ctx>
        where
            Ctx: Context + Serialize +'static {
            query: Arc<Stmt<Ctx>>,
            groups: HashMap<String, Window<'query>>,
        }
    }
}
pub use rentals::Dims as SelectDims;

impl<Ctx> SelectDims<Ctx>
where
    Ctx: Context + Serialize + 'static,
{
    pub fn from_query(stmt: Arc<Stmt<Ctx>>) -> Self {
        Self::new(stmt, |_| HashMap::new())
    }
}

#[derive(Debug)]
pub struct TrickleSelect {
    pub id: String,
    pub stmt: tremor_script::StmtRentalWrapper,
    pub groups: SelectDims<EventContext>,
    pub window: Option<WindowImpl>,
}

pub trait WindowTrait: std::fmt::Debug + Clone {
    fn on_event(&mut self, event: &Event) -> WindowEvent;
}

#[derive(Debug, Clone)]
pub struct Window<'query> {
    window_impl: WindowImpl,
    aggregates: Aggrs<'query>,
}

impl<'query> Window<'query> {
    fn from_aggregates(aggregates: Aggrs<'query>, window_impl: WindowImpl) -> Window<'query> {
        Window {
            aggregates,
            window_impl,
        }
    }
}

impl<'query> WindowTrait for Window<'query> {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        self.window_impl.on_event(event)
    }
}

#[derive(Debug, Clone)]
pub enum WindowImpl {
    Tumbling(TumblingWindowOnEventTime),
    No(NoWindow),
}

impl std::default::Default for WindowImpl {
    fn default() -> Self {
        TumblingWindowOnEventTime {
            size: 15_000_000_000,
            next_window: None,
        }
        .into()
    }
}

impl WindowTrait for WindowImpl {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        match self {
            WindowImpl::Tumbling(w) => w.on_event(event),
            WindowImpl::No(w) => w.on_event(event),
        }
    }
}

impl From<NoWindow> for WindowImpl {
    fn from(w: NoWindow) -> Self {
        WindowImpl::No(w)
    }
}

impl From<TumblingWindowOnEventTime> for WindowImpl {
    fn from(w: TumblingWindowOnEventTime) -> Self {
        WindowImpl::Tumbling(w)
    }
}

#[derive(Debug, PartialEq)]
pub enum Accumulate {
    Not,
    Before,
    After,
}

#[derive(Debug, PartialEq)]
pub struct WindowEvent {
    /// New window is opened,
    pub open: bool,
    /// Close the window before this event and opeen the next one
    pub emit: bool,
}

#[derive(Default, Debug, Clone)]
pub struct NoWindow {
    open: bool,
}

impl WindowTrait for NoWindow {
    fn on_event(&mut self, _event: &Event) -> WindowEvent {
        if self.open {
            WindowEvent {
                open: false,
                emit: true,
            }
        } else {
            self.open = true;
            WindowEvent {
                open: true,
                emit: true,
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TumblingWindowOnEventTime {
    pub next_window: Option<u64>,
    pub size: u64,
}

impl WindowTrait for TumblingWindowOnEventTime {
    fn on_event(&mut self, event: &Event) -> WindowEvent {
        match self.next_window {
            None => {
                self.next_window = Some(event.ingest_ns + self.size);
                WindowEvent {
                    open: true,
                    emit: false,
                }
            }
            Some(next_window) if next_window <= event.ingest_ns => {
                self.next_window = Some(event.ingest_ns + self.size);
                WindowEvent {
                    open: false,
                    emit: true,
                }
            }
            Some(_) => WindowEvent {
                open: false,
                emit: false,
            },
        }
    }
}

const NO_AGGRS: [InvokeAggrFn<'static, EventContext>; 0] = [];

impl TrickleSelect {
    fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

impl Operator for TrickleSelect {
    #[allow(clippy::transmute_ptr_to_ptr)]
    #[allow(mutable_transmutes)]
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let opts = Self::opts();
        let unwind_event: &mut Value<'_> = unsafe { std::mem::transmute(event.value.suffix()) };

        let local_stack = tremor_script::interpreter::LocalStack::with_size(0);

        // NOTE We are unwrapping our rental wrapped stmt
        let stmt: &mut tremor_script::ast::Stmt<EventContext> =
            unsafe { std::mem::transmute(self.stmt.stmt.suffix()) };

        let ctx = EventContext::from_ingest_ns(event.ingest_ns);
        match stmt {
            tremor_script::ast::Stmt::SelectStmt {
                stmt,
                aggregates,
                consts,
            } => {
                //
                // Before any select processing, we filter by where clause
                //
                // FIXME: ?
                let event_meta: simd_json::borrowed::Value =
                    simd_json::owned::Value::Object(event.meta.clone()).into();
                if let Some(guard) = &stmt.maybe_where {
                    let test = guard.run(
                        opts,
                        &ctx,
                        &NO_AGGRS,
                        unwind_event,
                        &event_meta,
                        &local_stack,
                        &consts,
                    )?;
                    match test.borrow() {
                        Value::Bool(true) => (),
                        Value::Bool(false) => {
                            return Ok(vec![]);
                        }
                        other => {
                            return tremor_script::errors::query_guard_not_bool(
                                &stmt, guard, &other,
                            )?;
                        }
                    };
                }

                if let Some(window) = &self.window {
                    let group = if let Some(group_by) = &stmt.maybe_group_by {
                        let group = group_by.run(
                            opts,
                            &ctx,
                            &NO_AGGRS,
                            unwind_event,
                            &event_meta,
                            &local_stack,
                            &consts,
                        )?;
                        group.to_string()
                    } else {
                        String::new()
                    };
                    let groups: &mut HashMap<String, Window> =
                        unsafe { std::mem::transmute(self.groups.suffix()) };
                    let w = groups.entry(group).or_insert_with(|| {
                        Window::from_aggregates(aggregates.clone(), window.clone())
                    });
                    let window_event = w.on_event(&event);
                    if window_event.open {
                        for aggr in w.aggregates.iter_mut() {
                            let invocable = &mut aggr.invocable;
                            invocable.init();
                        }
                    }

                    let maybe_value = if window_event.emit {
                        // After having has been applied to any emissions causal on this
                        // event, we prepare the target expression synthetic event and
                        // return it for downstream processing
                        //
                        // FIXME: This can be nicer, got to look at run for tremor script

                        let value = stmt.target.run(
                            opts,
                            &ctx,
                            &w.aggregates,
                            unwind_event,
                            &event_meta,
                            &local_stack,
                            &consts,
                        )?;

                        Some(value.into_owned())
                    } else {
                        None
                    };

                    for aggr in w.aggregates.iter_mut() {
                        let invocable = &mut aggr.invocable;
                        let mut argv: Vec<Cow<Value>> = Vec::with_capacity(aggr.args.len());
                        let mut argv1: Vec<&Value> = Vec::with_capacity(aggr.args.len());
                        for arg in aggr.args.iter() {
                            let result = arg.run(
                                opts,
                                &ctx,
                                &NO_AGGRS,
                                unwind_event,
                                &event_meta,
                                &local_stack,
                                &consts,
                            )?;
                            argv.push(result);
                        }
                        unsafe {
                            for i in 0..argv.len() {
                                argv1.push(argv.get_unchecked(i));
                            }
                        }
                        invocable.accumulate(argv1.as_slice()).map_err(|e| {
                            use tremor_script::Registry;
                            // FIXME nice error
                            let r: Option<&Registry<EventContext>> = None;
                            e.into_err(aggr, aggr, r)
                        })?;
                    }

                    if let Some(value) = maybe_value {
                        *unwind_event = value;
                        if let Some(guard) = &stmt.maybe_having {
                            let test = guard.run(
                                opts,
                                &ctx,
                                &w.aggregates,
                                unwind_event,
                                &Value::Null,
                                &local_stack,
                                &consts,
                            )?;
                            let _ = match test.into_owned() {
                                Value::Bool(true) => (),
                                Value::Bool(false) => {
                                    return Ok(vec![]);
                                }
                                other => {
                                    return tremor_script::errors::query_guard_not_bool(
                                        &stmt, guard, &other,
                                    )?;
                                }
                            };
                        }

                        Ok(vec![("out".to_string(), event)])
                    } else {
                        Ok(vec![])
                    }
                } else {
                    // After having has been applied to any emissions causal on this
                    // event, we prepare the target expression synthetic event and
                    // return it for downstream processing
                    //
                    // FIXME: This can be nicer, got to look at run for tremor script

                    let value = stmt.target.run(
                        opts,
                        &ctx,
                        &NO_AGGRS,
                        unwind_event,
                        &event_meta,
                        &local_stack,
                        &consts,
                    )?;

                    *unwind_event = value.into_owned();
                    if let Some(guard) = &stmt.maybe_having {
                        let test = guard.run(
                            opts,
                            &ctx,
                            &NO_AGGRS,
                            unwind_event,
                            &Value::Null,
                            &local_stack,
                            &consts,
                        )?;
                        let _ = match test.into_owned() {
                            Value::Bool(true) => (),
                            Value::Bool(false) => {
                                return Ok(vec![]);
                            }
                            other => {
                                return tremor_script::errors::query_guard_not_bool(
                                    &stmt, guard, &other,
                                )?;
                            }
                        };
                    }

                    Ok(vec![("out".to_string(), event)])
                }
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MetaMap; //, FN_REGISTRY};
    use serde_json::json;
    use simd_json::borrowed::Value;
    use simd_json::OwnedValue;
    use std::convert::TryInto;
    use tremor_script::ast;
    use tremor_script::pos::Location;

    fn test_target<'test>() -> ast::ImutExpr<'test, EventContext> {
        let target: ast::ImutExpr<'test, EventContext> =
            ast::ImutExpr::<EventContext>::Literal(ast::Literal {
                start: Location::default(),
                end: Location::default(),
                value: Value::I64(42),
            });
        target
    }

    fn test_stmt<'test>(
        target: ast::ImutExpr<'test, EventContext>,
    ) -> ast::MutSelect<'test, EventContext> {
        tremor_script::ast::MutSelect {
            start: Location::default(),
            end: Location::default(),
            from: ast::Ident {
                start: Location::default(),
                end: Location::default(),
                id: "in".into(),
            },
            into: ast::Ident {
                start: Location::default(),
                end: Location::default(),
                id: "out".into(),
            },
            target,
            maybe_where: Some(ast::ImutExpr::Literal(ast::Literal {
                start: Location::default(),
                end: Location::default(),
                value: Value::Bool(true),
            })),
            maybe_window: None,
            maybe_group_by: None,
            maybe_having: None,
        }
    }

    fn test_query<'test>(stmt: ast::Stmt<'test, EventContext>) -> ast::Query<'test, EventContext> {
        ast::Query {
            stmts: vec![stmt.clone()],
        }
    }

    fn test_event(s: u64) -> Event {
        Event {
            is_batch: false,
            id: s,
            ingest_ns: s * 1_000_000_000,
            meta: MetaMap::new(),
            value: sjv!(json!({
               "h2g2" : 42,
            })
            .try_into()
            .expect("failed to create test event")),
            kind: None,
        }
    }

    use std::sync::Arc;

    fn test_select<'test>(stmt: tremor_script::StmtRentalWrapper) -> TrickleSelect {
        let groups = SelectDims::from_query(stmt.stmt.clone());
        TrickleSelect {
            id: "select".to_string(),
            stmt,
            groups,
            window: Some(WindowImpl::default()),
        }
    }

    fn try_enqueue<'test>(
        op: &'test mut TrickleSelect,
        event: Event,
    ) -> Result<Option<(String, Event)>> {
        let mut action = op.on_event("in", event)?;
        Ok(action.pop())
    }

    fn parse_query(query: &str) -> Result<crate::op::trickle::select::TrickleSelect> {
        let reg = tremor_script::registry();
        let aggr_reg = tremor_script::aggr_registry();
        let query = tremor_script::script::QueryRentalWrapper::parse(query, &reg, &aggr_reg)?;

        let stmt_rental = tremor_script::script::rentals::Stmt::new(query.query.clone(), |q| {
            q.suffix().stmts[0].clone()
        });
        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };
        Ok(test_select(stmt))
    }

    #[test]
    fn test_sum() -> Result<()> {
        let mut op = parse_query("select stats::sum(event.h2g2) from in into out;")?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(j, 84.0);
        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        let mut op = parse_query("select stats::count() from in into out;")?;
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(j, 2);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_nowhr_nohav() -> Result<()> {
        let target = test_target();
        let stmt_ast = test_stmt(target);

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);
        assert!(try_enqueue(&mut op, test_event(0))?.is_none());
        assert!(try_enqueue(&mut op, test_event(1))?.is_none());
        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        //        let jj: OwnedValue = json!({"snot": "badger", "a": 1}).try_into().expect("");
        assert_eq!(OwnedValue::I64(42), j);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);

        assert!(try_enqueue(&mut op, test_event(0))?.is_none());

        let (out, event) = try_enqueue(&mut op, test_event(15))?.expect("no event");
        assert_eq!("out", out);

        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrf_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);

        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));
        let stmt_ast = test_select_stmt(stmt_ast);

        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);
        let next = try_enqueue(&mut op, test_event(0))?;
        assert_eq!(None, next);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrbad_nohav() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::String("snot".into()),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);

        assert!(try_enqueue(&mut op, test_event(0)).is_err());

        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_havt() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);

        let event = test_event(0);
        assert!(try_enqueue(&mut op, event)?.is_none());

        let event = test_event(15);
        let (out, event) = try_enqueue(&mut op, event)?.expect("no event");
        assert_eq!("out", out);
        let j: OwnedValue = event.value.rent(|j| j.clone().into());
        assert_eq!(OwnedValue::I64(42), j);
        Ok(())
    }

    #[test]
    fn select_nowin_nogrp_whrt_havf() -> Result<()> {
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(false),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        assert_eq!(None, next);
        Ok(())
    }

    fn test_select_stmt<'snot>(
        stmt: tremor_script::ast::MutSelect<'snot, EventContext>,
    ) -> tremor_script::ast::Stmt<'snot, EventContext> {
        ast::Stmt::SelectStmt {
            stmt: Box::new(stmt),
            aggregates: vec![],
            consts: vec![],
        }
    }
    #[test]
    fn select_nowin_nogrp_whrt_havbad() -> Result<()> {
        use halfbrown::hashmap;
        let target = test_target();
        let mut stmt_ast = test_stmt(target);
        stmt_ast.maybe_where = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Bool(true),
        }));
        stmt_ast.maybe_having = Some(ast::ImutExpr::Literal(ast::Literal {
            start: Location::default(),
            end: Location::default(),
            value: Value::Object(hashmap! {
                "snot".into() => "badger".into(),
            }),
        }));

        let stmt_ast = test_select_stmt(stmt_ast);
        let script = "fake".to_string();
        let script_box = Box::new(script.clone());
        let query_rental = Arc::new(tremor_script::script::rentals::Query::new(
            script_box,
            |_| test_query(stmt_ast.clone()),
        ));

        let query = tremor_script::QueryRentalWrapper {
            query: query_rental,
            locals: 0,
            source: script,
            warnings: vec![],
        };

        let stmt_rental =
            tremor_script::script::rentals::Stmt::new(query.query.clone(), |_| stmt_ast);

        let stmt = tremor_script::StmtRentalWrapper {
            stmt: Arc::new(stmt_rental),
        };

        let mut op = test_select(stmt);
        let event = test_event(0);

        let next = try_enqueue(&mut op, event)?;

        // FIXME TODO - would be nicer to get error output in tests
        // syntax highlighted in capturable form for assertions
        assert_eq!(None, next);
        Ok(())
    }
}
