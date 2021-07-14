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

// [x] PERF0001: handle select without grouping or windows easier.

use crate::{errors::Result, op::prelude::*, Event, Operator};
use tremor_script::{
    self,
    ast::{InvokeAggrFn, Select, SelectStmt},
    interpreter::Env,
    prelude::*,
    srs,
};

/// optimized variant for a simple select of the form:
///
/// select event from in [where ...] into out [having ...]
#[derive(Debug)]
pub struct SimpleSelect {
    pub id: String,
    pub(crate) select: srs::Select,
}

const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

impl SimpleSelect {
    pub fn with_stmt(id: String, stmt: &srs::Stmt) -> Result<Self> {
        let select = srs::Select::try_new_from_stmt(stmt)?;
        Ok(Self { id, select })
    }
    fn opts() -> ExecOpts {
        ExecOpts {
            result_needed: true,
            aggr: AggrType::Emit,
        }
    }
}

impl Operator for SimpleSelect {
    fn on_event(
        &mut self,
        _uid: u64,
        _port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        let opts = Self::opts();
        // We guarantee at compile time that select in itself can't have locals, so this is safe

        // NOTE We are unwrapping our rental wrapped stmt
        // TODO: add soundness reasoning

        self.select.rent(
            |SelectStmt {
                 stmt,
                 locals,
                 consts,
                 node_meta,
                 ..
             }| {
                let local_stack = tremor_script::interpreter::LocalStack::with_size(*locals);

                // TODO avoid origin_uri clone here
                let ctx = EventContext::new(event.ingest_ns, event.origin_uri.clone());

                //
                // Before any select processing, we filter by where clause
                //
                if let Some(guard) = &stmt.maybe_where {
                    let (unwind_event, event_meta) = event.data.parts();
                    let env = Env {
                        context: &ctx,
                        consts: consts.run(),
                        aggrs: &NO_AGGRS,
                        meta: &node_meta,
                        recursion_limit: tremor_script::recursion_limit(),
                    };
                    let test =
                        guard.run(opts, &env, unwind_event, state, event_meta, &local_stack)?;
                    if let Some(test) = test.as_bool() {
                        if !test {
                            return Ok(EventAndInsights::default());
                        };
                    } else {
                        let s: &Select = &stmt;
                        return tremor_script::errors::query_guard_not_bool(
                            s, guard, &test, &node_meta,
                        )?;
                    };
                }

                if let Some(guard) = &stmt.maybe_having {
                    let (unwind_event, event_meta) = event.data.parts();
                    let env = Env {
                        context: &ctx,
                        consts: consts.run(),
                        aggrs: &NO_AGGRS,
                        meta: &node_meta,
                        recursion_limit: tremor_script::recursion_limit(),
                    };
                    let test =
                        guard.run(opts, &env, unwind_event, state, event_meta, &local_stack)?;
                    if let Some(test) = test.as_bool() {
                        if !test {
                            return Ok(EventAndInsights::default());
                        };
                    } else {
                        let s: &Select = &stmt;
                        return tremor_script::errors::query_guard_not_bool(
                            s, guard, &test, &node_meta,
                        )?;
                    };
                }

                Ok(event.into())
            },
        )
    }
}
