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

use crate::op::prelude::*;
use beef::Cow;
use tremor_script::{ast, prelude::*, srs};
#[derive(Debug)]
pub(crate) struct TrickleOperator {
    pub id: String,
    pub op: Box<dyn Operator>,
}

fn mk_node_config(
    id: String,
    op_type: String,
    config: Option<HashMap<String, Value>>,
) -> NodeConfig {
    NodeConfig {
        id,
        kind: crate::NodeKind::Operator,
        op_type,
        config: config.map(|v| {
            serde_yaml::Value::from(
                v.iter()
                    .filter_map(|(k, v)| {
                        let mut v = v.encode();
                        Some((
                            serde_yaml::Value::from(k.as_str()),
                            simd_json::serde::from_str::<serde_yaml::Value>(&mut v).ok()?,
                        ))
                    })
                    .collect::<serde_yaml::Mapping>(),
            )
        }),
        ..NodeConfig::default()
    }
}

impl TrickleOperator {
    pub fn with_stmt(operator_uid: u64, id: String, decl: &srs::Stmt) -> Result<Self> {
        use crate::operator;
        let stmt = decl.suffix();
        let op: Box<dyn Operator> = match stmt {
            ast::Stmt::OperatorDecl(ref op) => {
                let op = op.clone().into_static();
                let config = mk_node_config(
                    op.id,
                    format!("{}::{}", op.kind.module, op.kind.operation),
                    op.params,
                );
                operator(operator_uid, &config)?
            }
            _ => {
                return Err(ErrorKind::PipelineError(
                    "Trying to turn a non operator into a operator".into(),
                )
                .into())
            }
        };

        Ok(Self { id, op })
    }
}

impl Operator for TrickleOperator {
    fn on_event(
        &mut self,
        uid: u64,
        port: &str,
        state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        self.op.on_event(uid, port, state, event)
    }

    fn handles_signal(&self) -> bool {
        self.op.handles_signal()
    }
    fn on_signal(
        &mut self,
        uid: u64,
        state: &Value<'static>,
        signal: &mut Event,
    ) -> Result<EventAndInsights> {
        self.op.on_signal(uid, state, signal)
    }

    fn handles_contraflow(&self) -> bool {
        self.op.handles_contraflow()
    }
    fn on_contraflow(&mut self, uid: u64, contraevent: &mut Event) {
        self.op.on_contraflow(uid, contraevent)
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
