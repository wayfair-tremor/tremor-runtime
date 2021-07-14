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

use crate::env;
use crate::errors::{Error, Result};
use crate::report;
use crate::test;
use crate::test::stats;
use crate::test::status;
use report::TestSuite;
use std::io::Read;
use std::{collections::HashMap, path::Path};
use test::tag;
use tremor_common::time::nanotime;
use tremor_script::ast::base_expr::BaseExpr;
use tremor_script::ast::{Expr, ImutExpr, ImutExprInt, Invoke, List, NodeMetas, Record};
use tremor_script::ctx::{EventContext, EventOriginUri};
use tremor_script::highlighter::{Dumb as DumbHighlighter, Highlighter, Term as TermHighlighter};
use tremor_script::interpreter::{AggrType, Env, ExecOpts, LocalStack};
use tremor_script::prelude::*;
use tremor_script::Value;
const EXEC_OPTS: ExecOpts = ExecOpts {
    result_needed: true,
    aggr: AggrType::Tick,
};

#[allow(clippy::too_many_arguments)]
fn eval_suite_entrypoint(
    env: &Env,
    local: &LocalStack,
    script: &str,
    meta: &NodeMetas,
    suite_spec: &Record<'_>,
    suite_result: &[Value<'_>],
    tags: &tag::TagFilter,
    sys_filter: &[&str],
    includes: &[String],
    excludes: &[String],
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();

    let o = suite_result
        .first()
        .and_then(ValueAccess::as_object)
        .ok_or_else(|| Error::from("bad suite results"))?;
    if o.contains_key("name") {
        // let name = suite_spec
        //     .get_literal("name")
        //     .ok_or_else(|| Error::from("Missing suite name"))?;
        let spec = suite_spec
            .get_field_expr("tests")
            .ok_or_else(|| Error::from("Missing suite tests"))?;

        if let ImutExprInt::List(l) = spec {
            if let Some(tests) = o.get("suite").get("tests") {
                if let Ok((s, mut e)) = eval_suite_tests(
                    &env, &local, script, meta, l, tests, &tags, sys_filter, includes, excludes,
                ) {
                    elements.append(&mut e);
                    stats.merge(&s);
                } else {
                    stats.fail();
                }
            }
        } // TODO error/warning handling when no tests found
    }

    Ok((stats, elements))
}

fn eval(expr: &ImutExprInt, env: &Env, local: &LocalStack) -> Result<Value<'static>> {
    let state = Value::object();
    let meta = Value::object();
    let event = Value::object();
    Ok(expr
        .run(EXEC_OPTS, &env, &event, &state, &meta, local)?
        .into_owned()
        .into_static())
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn eval_suite_tests(
    env: &Env,
    local: &LocalStack,
    script: &str,
    node_metas: &NodeMetas,
    suite_spec: &List,
    suite_result: &Value,
    suite_tags: &test::TagFilter,
    sys_filter: &[&str],
    includes: &[String],
    excludes: &[String],
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stats = stats::Stats::new();
    if let Some(a) = suite_result.as_array() {
        let al = a.len();
        let ll = suite_spec.exprs.len();
        for (idx, item) in suite_spec.exprs.iter().enumerate() {
            if let ImutExpr(ImutExprInt::Invoke1(Invoke {
                module, fun, args, ..
            })) = item
            {
                let m = module.join("").to_string();
                if m == "test" && fun == "test" {
                    if let ImutExpr(ImutExprInt::Record(spec)) = &args
                        .first()
                        .ok_or_else(|| Error::from("Invalid test specification"))?
                    {
                        let mut found_tags = Vec::new();

                        if let Some(tags) = spec.get_field_expr("tags") {
                            let tag_value = eval(tags, env, local)?;
                            if let Some(tags) = tag_value.as_array() {
                                let inner_tags = tags.iter().map(|x| (*x).to_string());
                                found_tags.extend(inner_tags);
                            }
                        } else if let Some(tags) =
                            spec.get_literal("tags").and_then(Value::as_array)
                        {
                            let inner_tags = tags.iter().map(|x| (*x).to_string());
                            found_tags.extend(inner_tags);
                        }

                        let case_tags = suite_tags.join(Some(found_tags));

                        if let Some(item) = spec.get_field_expr("test") {
                            let start = nanotime();
                            let value = eval(item, env, local)?;
                            let elapsed = nanotime() - start;

                            if let Some(status) = value.as_bool() {
                                // Non colorized test source extent for json report capture
                                let extent = item.extent(node_metas);
                                let mut hh = DumbHighlighter::new();
                                tremor_script::Script::highlight_script_with_range(
                                    script, extent, &mut hh,
                                )?;

                                if let (matched, false) =
                                    case_tags.matches(sys_filter, includes, excludes)
                                {
                                    status::h1("    Test ( Skipping )", "")?;
                                    status::tagsx(
                                        "        ",
                                        &case_tags,
                                        Some(&matched),
                                        Some(excludes),
                                    )?;
                                    stats.skip();
                                    continue;
                                }
                                status::h1("    Test", "")?;
                                status::tagsx(
                                    "        ",
                                    &case_tags,
                                    Some(includes),
                                    Some(excludes),
                                )?;

                                let prefix = if status { "(+)" } else { "(-)" };
                                // Test record
                                elements.push(report::TestElement {
                                    description: format!(
                                        "{} Executing test {} of {}",
                                        prefix,
                                        idx + 1,
                                        ll
                                    ),
                                    keyword: report::KeywordKind::Test,
                                    result: report::ResultKind {
                                        status: stats.report(status),
                                        duration: elapsed,
                                    },
                                    info: Some(hh.to_string()),
                                    hidden: false,
                                });
                                drop(hh);
                                stats.assert();

                                // Interactive console report
                                status::executing_unit_testcase(idx, ll, status)?;
                                let mut hh: TermHighlighter = TermHighlighter::default();
                                tremor_script::Script::highlight_script_with_range_indent(
                                    "       ", script, extent, &mut hh,
                                )?;
                                hh.finalize()?;
                                drop(hh);
                                println!();
                            }
                            continue;
                        }
                    };
                }
            }

            if let Some(status) = suite_result
                .get_idx(idx)
                .ok_or_else(|| Error::from("Invalid test result"))?
                .as_bool()
            {
                // Predicate tests do not have tags support

                // Non colorized test source extent for json report capture
                let extent = item.extent(node_metas);
                let mut hh = DumbHighlighter::new();
                tremor_script::Script::highlight_script_with_range(script, extent, &mut hh)?;
                let prefix = if status { "(+)" } else { "(-)" };

                let case_tags = suite_tags;

                let (matched, is_match) = case_tags.matches(sys_filter, includes, excludes);
                if is_match {
                    status::h1("    Test", "")?;
                    status::tagsx("        ", &case_tags, Some(&matched), Some(excludes))?;
                } else {
                    status::h1("    Test ( Skipping )", "")?;
                    status::tagsx("        ", &case_tags, Some(&matched), Some(excludes))?;
                    continue;
                }

                // Test record
                elements.push(report::TestElement {
                    description: format!("    {} Executing test {} of {}", prefix, idx + 1, al),
                    keyword: report::KeywordKind::Predicate,
                    result: report::ResultKind {
                        status: stats.report(status),
                        duration: 0, // Compile time evaluation
                    },
                    info: Some(hh.to_string()),
                    hidden: false,
                });
                drop(hh);

                stats.assert();

                // Interactive console report
                status::executing_unit_testcase(idx, ll, status)?;
                let mut h = TermHighlighter::default();
                tremor_script::Script::highlight_script_with_range_indent(
                    "      ", script, extent, &mut h,
                )?;
                h.finalize()?;
                drop(h);
            }
        }
    }

    Ok((stats, elements))
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_suite(
    path: &Path,
    scenario_tags: &tag::TagFilter,
    sys_filter: &[&str],
    includes: &[String],
    excludes: &[String],
) -> Result<report::TestReport> {
    println!();

    let mut suites: HashMap<String, TestSuite> = HashMap::new();
    let script = path.to_string_lossy().to_string();

    let mut raw = String::new();
    let mut input = crate::open_file(&path, None)?;
    input.read_to_string(&mut raw)?;

    let env = env::setup()?;
    let report_start = nanotime();
    let mut stat_x = stats::Stats::new();
    match tremor_script::Script::parse(&env.module_path, &script, raw.clone(), &env.fun) {
        Ok(runnable) => {
            let local = LocalStack::default();

            let mut h = TermHighlighter::default();
            runnable.format_warnings_with(&mut h)?;

            let script = runnable.script.suffix();

            let context = &EventContext::new(nanotime(), Some(EventOriginUri::default()));
            let env = Env {
                context,
                consts: script.consts.run(),
                aggrs: &script.aggregates,
                meta: &script.node_meta,
                recursion_limit: tremor_script::recursion_limit(),
            };

            let mut stat_s = stats::Stats::new();
            for expr in &script.exprs {
                let state = Value::object();
                let event = Value::object();
                let meta = Value::object();
                let mut elements = Vec::new();
                let mut suite_name = "".to_string();

                if let Expr::Imut(ImutExprInt::Invoke1(Invoke {
                    module, fun, args, ..
                })) = expr
                {
                    let m = module.join("").to_string();
                    if m == "test" && fun == "suite" {
                        // A Test suite
                        let mut specs: Vec<Value> = vec![];
                        for arg in args {
                            let value = arg.run(EXEC_OPTS, &env, &event, &state, &meta, &local)?;
                            specs.push(value.into_owned());
                        }
                        if let ImutExpr(ImutExprInt::Record(spec)) = args
                            .first()
                            .ok_or_else(|| Error::from("Invalid test specification"))?
                        {
                            let mut found_tags = Vec::new();
                            if let Some(tags) = spec.get_field_expr("tags") {
                                let tag_value = eval(tags, &env, &local)?;
                                if let Some(tags) = tag_value.as_array() {
                                    let inner_tags = tags.iter().map(|x| (*x).to_string());
                                    found_tags.extend(inner_tags);
                                }
                            } else if let Some(tags) =
                                spec.get_literal("tags").and_then(Value::as_array)
                            {
                                let inner_tags = tags.iter().map(|x| (*x).to_string());
                                found_tags.extend(inner_tags);
                            }

                            let suite_tags = scenario_tags.join(Some(found_tags));
                            if let Some(ImutExprInt::Record(r)) = spec.get_field_expr("suite") {
                                if let Some(value) = spec.get_literal("name") {
                                    suite_name = value.to_string()
                                };

                                // TODO revisit tags in unit tests
                                if let (_matched, true) =
                                    suite_tags.matches(sys_filter, includes, excludes)
                                {
                                    status::h1("  Suite", &suite_name.to_string())?;
                                    status::tagsx(
                                        "      ",
                                        &suite_tags,
                                        Some(includes),
                                        Some(excludes),
                                    )?;

                                    let (test_stats, mut test_reports) = eval_suite_entrypoint(
                                        &env,
                                        &local,
                                        &runnable.source,
                                        &script.node_meta,
                                        r,
                                        &specs,
                                        &suite_tags,
                                        sys_filter,
                                        includes,
                                        excludes,
                                    )?;
                                    stat_s.merge(&test_stats);
                                    elements.append(&mut test_reports);
                                }
                            } else {
                                stat_s.skip();

                                status::h1("  Suite", &suite_name.to_string())?;
                                status::tagsx(
                                    "      ",
                                    &suite_tags,
                                    Some(includes),
                                    Some(excludes),
                                )?;
                            }

                            suites.insert(
                                suite_name.clone(),
                                TestSuite {
                                    name: suite_name.to_string(),
                                    description: suite_name,
                                    elements,
                                    evidence: None,
                                    stats: stat_s.clone(),
                                    duration: 0,
                                },
                            );
                        };
                    }
                };
            }
        }
        Err(e) => {
            stat_x.fail();
            let mut h = TermHighlighter::default();
            if let Err(e) = tremor_script::Script::format_error_from_script(&raw, &mut h, &e) {
                eprintln!("Error: {}", e);
            };
        }
    }

    for v in suites.values() {
        stat_x.merge(&v.stats)
    }

    Ok(report::TestReport {
        description: "unit test suites".into(),
        elements: suites,
        stats: stat_x,
        duration: nanotime() - report_start,
    })
}
