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
use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Read, Write};
use tremor_common::file;

use std::collections::HashMap;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::TryRecvError;

use std::sync::mpsc;
use std::{env, process, thread};

pub(crate) fn which<P>(exe_name: P) -> Result<PathBuf>
where
    P: AsRef<Path>,
{
    let name = exe_name.as_ref().to_string_lossy();

    // use the current binary if `tremor` is used as an executable
    // this can be overwritten by giving it a path
    if name == "tremor" {
        env::current_exe()
            .map_err(|e| Error::from(format!("Unable to execute current tremor binary: {}", e)))
    } else {
        env::var_os("PATH")
            .and_then(|paths| {
                env::split_paths(&paths).find_map(|dir| {
                    let path = dir.join(&exe_name);
                    if path.is_file() {
                        Some(path)
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| {
                Error::from(format!("Unable to find suitable `{}` binary on path", name))
            })
    }
}

/// Read until EOF.
pub(crate) fn readlines_until_eof<R: Read, F: FnMut(String) -> Result<()>>(
    reader: R,
    mut handler: F,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    loop {
        let mut buffy = String::new();
        let nread = reader.read_line(&mut buffy)?;
        handler(buffy)?;
        if nread == 0 {
            break; // EOS
        }
    }

    Ok(())
}

/// Manage target process and communications with it
#[derive(Debug)]
pub(crate) struct TargetProcess {
    stdout_thread: Option<thread::JoinHandle<Result<()>>>,
    stderr_thread: Option<thread::JoinHandle<Result<()>>>,
    pub(crate) process: process::Child,
    pub(crate) stderr_receiver: mpsc::Receiver<String>,
    pub(crate) stdout_receiver: mpsc::Receiver<String>,
}

impl TargetProcess {
    pub fn new_with_stderr<S>(
        cmd: S,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<Self>
    where
        S: AsRef<OsStr>,
    {
        TargetProcess::new(cmd, args, env)
    }

    /// Spawn target process and pipe IO
    fn new<S>(cmd: S, args: &[String], env: &HashMap<String, String>) -> Result<Self>
    where
        S: AsRef<OsStr>,
    {
        let cmd: &OsStr = cmd.as_ref();
        let mut target_cmd = Command::new(cmd)
            .args(args)
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = target_cmd.stdout.take();
        let stderr = target_cmd.stderr.take();
        match stdout.zip(stderr) {
            Some((stdout, stderr)) => {
                let (stdout_tx, stdout_rx) = mpsc::channel();
                let (stderr_tx, stderr_rx) = mpsc::channel();

                let cmd_name = cmd.to_string_lossy().to_string();
                let stdout_thread = Some(thread::spawn(move || -> Result<()> {
                    // Redirect target process stdout
                    readlines_until_eof(stdout, |resp| {
                        trace!("[{} OUT] {}", cmd_name, &resp);
                        stdout_tx.send(resp).map_err(|e| e.into())
                    })
                }));

                let cmd_name = cmd.to_string_lossy().to_string();
                let stderr_thread = Some(thread::spawn(move || -> Result<()> {
                    // Redirect target process stderr
                    readlines_until_eof(stderr, |resp| {
                        trace!("[{} ERR] {}", cmd_name, &resp);
                        stderr_tx.send(resp).map_err(|e| e.into())
                    })
                }));

                Ok(Self {
                    process: target_cmd,
                    stdout_thread,
                    stderr_thread,
                    stdout_receiver: stdout_rx,
                    stderr_receiver: stderr_rx,
                })
            }
            None => Err("Unable to create stdout and stderr streams from target process".into()),
        }
    }

    pub fn wait_with_output(&mut self) -> Result<ExitStatus> {
        Ok(self.process.wait()?)
    }

    pub(crate) fn tail(&mut self, stdout_path: &Path, stderr_path: &Path) -> Result<()> {
        let mut tailout = file::create(stdout_path)?;
        let mut tailerr = file::create(stderr_path)?;

        self.wait_with_output()?;

        loop {
            match self.stdout_receiver.try_recv() {
                Ok(line) => {
                    tailout.write_all(line.as_bytes())?;
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        loop {
            match self.stderr_receiver.try_recv() {
                Ok(line) => {
                    tailerr.write_all(line.as_bytes())?;
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        tailout.sync_all()?;
        tailerr.sync_all()?;
        if self.process.kill().is_err() {
            // Do nothing
        };
        Ok(())
    }
}

impl Drop for TargetProcess {
    fn drop(&mut self) {
        if let Some(handle) = self.stdout_thread.take() {
            if let Err(e) = handle.join() {
                eprintln!("target process drop error: {:?}", e);
            }
        }

        if let Some(handle) = self.stderr_thread.take() {
            if let Err(e) = handle.join().unwrap_or(Ok(())) {
                eprintln!("target process drop error: {:?}", e);
            }
        }

        if let Err(e) = self.process.wait() {
            eprintln!("target process drop error: {:?}", e);
        }
    }
}
