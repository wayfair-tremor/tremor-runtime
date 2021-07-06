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

use super::{FResult, FunctionError, Result};
use crate::ast::{Consts, Expr, Exprs, FnDecl, ImutExpr, ImutExprInt, ImutExprs, InvokeAggrFn};
use crate::interpreter::{AggrType, Cont, Env, ExecOpts, LocalStack};
use crate::prelude::*;
use crate::Value;
use beef::Cow;
//use std::mem;
const RECUR_STR: &str = "recur";
pub(crate) const RECUR_PTR: Option<*const u8> = Some(RECUR_STR.as_ptr());
pub(crate) const RECUR: Value<'static> = Value::String(Cow::const_str(RECUR_STR));
pub(crate) const RECUR_REF: &Value<'static> = &RECUR;

#[derive(Debug, Clone, PartialEq, Serialize)]
/// Encapsulates a user defined or standard library function
pub struct CustomFn<'script> {
    /// Name
    pub name: Cow<'script, str>,
    /// Function body
    pub body: Exprs<'script>,
    /// Nominal arguments
    pub args: Vec<String>,
    /// True, if the function supports variable arguments
    pub open: bool,
    /// Locals
    pub locals: usize,
    /// Is it a const function
    pub is_const: bool,
    /// Should the function be inlined?
    pub inline: bool,
}

impl<'script> From<FnDecl<'script>> for CustomFn<'script> {
    fn from(f: FnDecl<'script>) -> Self {
        CustomFn {
            name: f.name.id,
            args: f.args.iter().map(|i| i.id.to_string()).collect(),
            locals: f.locals,
            body: f.body,
            is_const: false, // TODO we should find a way to examine this!
            open: f.open,
            inline: f.inline,
        }
    }
}

impl<'script> CustomFn<'script> {
    pub(crate) fn is_const(&self) -> bool {
        self.is_const
    }
    pub(crate) fn can_inline(&self) -> bool {
        if self.body.len() != 1 {
            return false;
        }
        if self.inline {
            return true;
        }
        let i = match self.body.first() {
            Some(Expr::Imut(
                ImutExprInt::Invoke1(i)
                | ImutExprInt::Invoke2(i)
                | ImutExprInt::Invoke3(i)
                | ImutExprInt::Invoke(i),
            )) => i,
            _ => return false,
        };
        let mut a_idx = 0;
        for a in &i.args {
            if let ImutExpr(ImutExprInt::Local { idx, .. }) = a {
                if *idx != a_idx {
                    return false;
                }
                a_idx += 1;
            } else {
                return false;
            }
        }

        true
    }
    pub(crate) fn inline(
        &self,
        args: ImutExprs<'script>,
        mid: usize,
    ) -> Result<ImutExprInt<'script>> {
        if self.body.len() != 1 {
            return Err(format!("can't inline {}: too large body", self.name).into());
        }

        let i = match self.body.first() {
            Some(Expr::Imut(
                ImutExprInt::Invoke1(i)
                | ImutExprInt::Invoke2(i)
                | ImutExprInt::Invoke3(i)
                | ImutExprInt::Invoke(i),
            )) => i,
            Some(e) => {
                return Err(format!("can't inline {}: bad expression: {:?}", self.name, e).into())
            }
            None => return Err(format!("can't inline {}: no body", self.name).into()),
        };

        if i.args.len() != self.args.len() {
            return Err(format!("can't inline {}: different argc", self.name).into());
        }

        let mut i = i.clone();
        i.mid = mid;
        i.args = args;

        Ok(match i.args.len() {
            1 => ImutExprInt::Invoke1(i),
            2 => ImutExprInt::Invoke2(i),
            3 => ImutExprInt::Invoke3(i),
            _ => ImutExprInt::Invoke(i),
        })
    }

    // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1028
    #[allow(mutable_transmutes, clippy::transmute_ptr_to_ptr)]
    pub(crate) fn invoke<'event>(
        &self,
        env: &Env<'_, 'event>,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>>
    where
        'script: 'event,
    {
        use std::mem;
        const NO_AGGRS: [InvokeAggrFn<'static>; 0] = [];

        let mut args_const = Value::from(
            args.iter()
                .skip(self.locals)
                .map(|v| v.clone_static())
                .collect::<Vec<Value<'static>>>(),
        );

        // We are swapping out the var args for constants
        // ALLOW: https://github.com/tremor-rs/tremor-runtime/issues/1028
        let consts: &mut Consts<'event> = unsafe { mem::transmute(env.consts) };

        mem::swap(&mut consts.args, &mut args_const);

        let mut this_local = LocalStack::with_size(self.locals);
        for (arg, local) in args.iter().zip(this_local.values.iter_mut()) {
            *local = Some((*arg).clone_static());
        }
        let opts = ExecOpts {
            result_needed: false,
            aggr: AggrType::Tick,
        };
        let env = Env {
            context: env.context,
            consts: env.consts,
            aggrs: &NO_AGGRS,
            meta: env.meta,
            recursion_limit: env.recursion_limit,
        };
        let mut recursion_depth = 0;
        'recur: loop {
            let mut exprs = self.body.iter().peekable();
            let mut no_event = Value::null();
            // we cannot access metadata from within custom functions, so we dont need an object here
            let mut no_meta = Value::null();
            let mut state = Value::null().into_static();
            while let Some(expr) = exprs.next() {
                if exprs.peek().is_none() {
                    let r = expr.run(
                        opts.with_result(),
                        &env,
                        &mut no_event,
                        &mut state,
                        &mut no_meta,
                        &mut this_local,
                    );
                    if r.is_err() {
                        mem::swap(&mut consts.args, &mut args_const);
                    };
                    match r? {
                        Cont::Cont(v) => {
                            return Ok(v.into_owned());
                        }
                        Cont::Drop => {
                            recursion_depth += 1;
                            if recursion_depth == env.recursion_limit {
                                mem::swap(&mut consts.args, &mut args_const);
                                return Err(FunctionError::RecursionLimit);
                            }
                            // clear the local variables (that are not the
                            // arguments)
                            for local in this_local.values.iter_mut().skip(args.len()) {
                                *local = None;
                            }
                            //We are abusing this as a recursion hint
                            continue 'recur;
                        }
                        _ => {
                            mem::swap(&mut consts.args, &mut args_const);
                            return Err(FunctionError::Error(Box::new("can't emit here".into())));
                        }
                    };
                }
                let r = expr.run(
                    opts,
                    &env,
                    &mut no_event,
                    &mut state,
                    &mut no_meta,
                    &mut this_local,
                );
                if r.is_err() {
                    mem::swap(&mut consts.args, &mut args_const);
                };
                r?;
            }
        }
    }
}
