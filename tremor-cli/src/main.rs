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

// #![deny(warnings)]
// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]

#[macro_use]
extern crate serde_derive;
// This is silly but serde is forcing you to import serde if you want serde_derive
#[allow(unused_extern_crates)]
extern crate serde;

#[macro_use]
extern crate log;

use crate::errors::{Error, Result};
use crate::util::{load_config, FormatKind, TremorApp};
use async_std::task;
use clap::App;
use clap::{load_yaml, AppSettings, ArgMatches};
use std::fs::File;
use std::path::{Path, PathBuf};
use tremor_common::file;
// use tremor_runtime::errors;

mod alloc;
mod api;
mod completions;
mod debug;
mod doc;
mod env;
mod errors;
// mod explain;
mod job;
mod report;
mod run;
mod server;
pub(crate) mod status;
mod test;
mod util;

pub(crate) fn open_file<S>(path: &S, base: Option<&String>) -> Result<File>
where
    S: AsRef<Path> + ?Sized,
{
    match file::open(&path) {
        Ok(f) => Ok(f),
        Err(tremor_common::Error::FileOpen(io_error, msg)) => {
            let msg = if let Some(base) = base {
                let mut p = PathBuf::from(base);
                p.push(path);
                if let Ok(f) = file::open(&p) {
                    return Ok(f);
                }
                format!(
                    "Failed to open `{}` (or  `{}`)",
                    path.as_ref().display(),
                    p.to_str().unwrap_or("<unknown>")
                )
            } else {
                msg
            };
            Err(tremor_common::Error::FileOpen(io_error, msg).into())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(tarpaulin_include))]
fn main() -> Result<()> {
    let yaml = load_yaml!("./cli.yaml");
    let long_version = tremor_runtime::version::long_ver();
    let app = App::from(yaml);
    let app = app.version(long_version.as_str());
    let app = app.global_setting(AppSettings::ColoredHelp);
    let app = app.global_setting(AppSettings::ColorAlways);

    tremor_runtime::functions::load()?;
    let matches = app.clone().get_matches();
    unsafe {
        // We know that instance will only get set once at
        // the very beginning nothing can access it yet,
        // this makes it allowable to use unsafe here.
        let s = matches
            .value_of("instance")
            .ok_or_else(|| Error::from("instance argument missing"))?;
        // ALLOW: We do this on startup and forget the memory once we drop it, that's on purpose
        let forget_s = std::mem::transmute(&s as &str);
        // This means we're going to LEAK this memory, however
        // it is fine since as we do actually need it for the
        // rest of the program execution.
        tremor_runtime::metrics::INSTANCE = forget_s;
    }
    if let Err(e) = run(app, &matches) {
        eprintln!("{}", e);
        // ALLOW: this is supposed to exit
        std::process::exit(1);
    }
    Ok(())
}

fn run(mut app: App, cmd: &ArgMatches) -> Result<()> {
    let format = match &cmd.value_of("format") {
        Some("json") => FormatKind::Json,
        _ => FormatKind::Yaml,
    };

    match cmd
        .subcommand_name()
        .map(|name| (name, cmd.subcommand_matches(name)))
    {
        Some(("explain", Some(_matches))) => Err("Not yet implemented".into()),
        Some(("completions", Some(matches))) => completions::run_cmd(app, matches),
        Some(("server", Some(matches))) => server::run_cmd(app, matches),
        Some(("run", Some(matches))) => run::run_cmd(&matches),
        Some(("doc", Some(matches))) => doc::run_cmd(&matches),
        Some(("api", Some(matches))) => task::block_on(api::run_cmd(
            TremorApp {
                format,
                config: load_config()?,
            },
            &matches,
        )),
        Some(("dbg", Some(matches))) => debug::run_cmd(&matches),
        Some(("test", Some(matches))) => test::run_cmd(&matches),
        _ => app
            .print_long_help()
            .map_err(|e| Error::from(format!("failed to print help: {}", e))),
    }
}
