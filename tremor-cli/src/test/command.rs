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

use crate::errors::{Error, Result};
use crate::job;
use crate::status;
use crate::test::after;
use crate::test::assert;
use crate::test::before;
use crate::test::report;
use crate::test::stats;
use crate::test::tag::{self, Tags};
use crate::test::Meta;
use crate::util::slurp_string;
use globwalk::{FileType, GlobWalkerBuilder};
use std::path::Path;
use std::{collections::HashMap, thread};
use tremor_common::file;
use tremor_common::time::nanotime;

#[derive(Deserialize, Debug)]
pub(crate) struct CommandRun {
    pub(crate) name: String,
    pub(crate) tags: Option<Tags>,
    pub(crate) suites: Vec<CommandSuite>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct CommandSuite {
    pub(crate) name: String,
    pub(crate) tags: Option<Tags>,
    pub(crate) cases: Vec<CommandTest>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct CommandTest {
    pub(crate) name: String,
    pub(crate) command: String,
    #[serde(default = "Default::default")]
    pub(crate) env: HashMap<String, String>,
    pub(crate) tags: Option<Tags>,
    pub(crate) status: i32,
    pub(crate) expects: assert::Asserts,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn suite_command(
    base: &Path,
    root: &Path,
    _meta: &Meta,
    quiet: bool,
    sys_filter: &[&str],
    includes: &[String],
    excludes: &[String],
) -> Result<(stats::Stats, Vec<report::TestReport>)> {
    let api_suites = GlobWalkerBuilder::new(root, "**/command.yml")
        .case_insensitive(true)
        .file_type(FileType::FILE)
        .build()
        .map_err(|e| {
            Error::from(format!(
                "Unable to walk test path (`{}`) for command-driven tests: {:?}",
                root.to_str().unwrap_or_default(),
                e
            ))
        })?;

    status::h0("Framework", "Finding command-driven test scenarios")?;

    let mut evidence = HashMap::new();

    let mut suites: HashMap<String, report::TestSuite> = HashMap::new();
    let mut counter = 0;
    let mut api_stats = stats::Stats::new();
    let report_start = nanotime();
    let api_suites = api_suites.filter_map(std::result::Result::ok);
    for suite in api_suites {
        if let Some(suite_root) = suite.path().parent() {
            let base_tags = tag::resolve(base, suite_root)?;

            // Set cwd to test root
            let cwd = std::env::current_dir()?;
            file::set_current_dir(&suite_root)?;

            let mut before = before::BeforeController::new(suite_root);
            let before_process = before.spawn()?;
            thread::spawn(move || {
                if let Err(e) = before.capture(before_process) {
                    eprint!("Can't capture results from 'before' process: {}", e)
                };
            });

            let suite_start = nanotime();
            let command_str = slurp_string(&suite.path())?;
            let suite = serde_yaml::from_str::<CommandRun>(&command_str)?;
            let mut header_printed = false;
            for suite in suite.suites {
                let suite_tags = base_tags.join(suite.tags);
                let mut casex = stats::Stats::new();
                for case in suite.cases {
                    let current_tags = suite_tags.join(case.tags.clone());
                    if let (_, false) = current_tags.matches(sys_filter, includes, excludes) {
                        if !quiet {
                            status::h1("Command Test ( Skipping )", &case.name)?;
                            status::tags(&current_tags, Some(includes), Some(excludes))?;
                        }
                        continue; // SKIP
                    }
                    if !header_printed {
                        status::h0("Command Suite: ", &suite.name)?;
                        status::hr();
                        header_printed = true;
                    }
                    status::h1("Command Test", &case.name)?;
                    status::tags(&current_tags, Some(includes), Some(excludes))?;

                    let args = shell_words::split(&case.command).unwrap_or_default();

                    if let Some((cmd, args)) = args.split_first() {
                        let resolved_cmd = job::which(cmd)?;

                        // TODO wintel
                        let mut fg_process =
                            job::TargetProcess::new_with_stderr(resolved_cmd, &args, &case.env)?;
                        let exit_status = fg_process.wait_with_output();

                        let fg_out_file = suite_root.join(&format!("fg.{}.out.log", counter));
                        let fg_err_file = suite_root.join(&format!("fg.{}.err.log", counter));
                        let start = nanotime();
                        fg_process.tail(&fg_out_file, &fg_err_file)?;
                        let elapsed = nanotime() - start;

                        counter += 1;

                        let (case_stats, elements) = process_testcase(
                            &fg_out_file,
                            &fg_err_file,
                            exit_status?.code(),
                            elapsed,
                            &case,
                        )?;
                        casex.merge(&case_stats);

                        status::stats(&case_stats, "    Test")?;
                        status::hr();
                        let suite = report::TestSuite {
                            name: case.name.trim().into(),
                            description: "Command-driven test".to_string(),
                            elements,
                            evidence: None,
                            stats: case_stats,
                            duration: nanotime() - suite_start,
                        };
                        suites.insert(case.name, suite);
                    } else {
                        eprintln!(
                            "Failed {} / {} since the case command could not be parsed",
                            suite.name, case.name
                        );
                        casex.fail();
                        casex.assert += 1;
                    }
                }
                api_stats.merge(&casex); // BEEP BOOP
                status::stats(&casex, "Suite")?;
                status::hr();
            }

            before::update_evidence(suite_root, &mut evidence)?;

            let mut after = after::AfterController::new(suite_root);
            after.spawn()?;
            after::update_evidence(suite_root, &mut evidence)?;

            // Reset cwd
            file::set_current_dir(&cwd)?;
        } else {
            return Err("Could not get parent of base path in command driven test walker".into());
        }
    }

    status::rollups("Command", &api_stats)?;

    let elapsed = nanotime() - report_start;
    status::duration(elapsed, "")?;
    status::hr();

    Ok((
        api_stats.clone(),
        vec![report::TestReport {
            description: "Command-based test suite".into(),
            elements: suites,
            stats: api_stats,
            duration: elapsed,
        }],
    ))
}

fn process_testcase(
    stdout_path: &Path,
    stderr_path: &Path,
    process_status: Option<i32>,
    duration: u64,
    spec: &CommandTest,
) -> Result<(stats::Stats, Vec<report::TestElement>)> {
    let mut elements = Vec::new();
    let mut stat_s = stats::Stats::new();
    if let Some(code) = process_status {
        let success = code == spec.status;
        stat_s.assert();
        status::assert_has(
            "  ",
            "Assert 0",
            &format!("Status {}", &spec.name.trim()),
            Some(&spec.status.to_string()),
            success,
        )?;
        elements.push(report::TestElement {
            description: format!("Process expected to exit with status code {}", spec.status),
            info: Some(code.to_string()),
            hidden: false,
            keyword: report::KeywordKind::Predicate,
            result: report::ResultKind {
                status: stat_s.report(success),
                duration,
            },
        });
    };

    let (stat_assert, mut filebased_assert_elements) =
        assert::process_filebased_asserts("  ", stdout_path, stderr_path, &spec.expects, &None)?;
    stat_s.merge(&stat_assert);
    elements.append(&mut filebased_assert_elements);

    Ok((stat_s, elements))
}
