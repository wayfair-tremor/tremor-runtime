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

// TODO: Add correlation of reads and replies.

#![cfg(not(tarpaulin_include))]
use crate::sink::{prelude::*, Reply};
use crate::source::prelude::*;
use async_channel::Sender;
use halfbrown::HashMap;
use serde::Deserialize;
use sled::{CompareAndSwapError, IVec};
use std::boxed::Box;
use tremor_pipeline::EventIdGenerator;
use tremor_value::literal;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    dir: String,
}

impl ConfigImpl for Config {}
pub struct Kv {
    sink_url: TremorUrl,
    event_origin_uri: EventOriginUri,
    idgen: EventIdGenerator,
    reply_tx: Sender<Reply>,
    db: sled::Db,
}

fn decode(mut v: Option<IVec>, codec: &mut dyn Codec, ingest_ns: u64) -> Result<Value<'static>> {
    if let Some(v) = v.as_mut() {
        let data: &mut [u8] = v;
        // TODO: We could optimize this
        Ok(codec
            .decode(data, ingest_ns)?
            .unwrap_or_default()
            .into_static())
    } else {
        Ok(Value::null())
    }
}

fn ok(k: Vec<u8>, v: Value<'static>) -> Value<'static> {
    literal!({
        "ok": {
            "key": Value::Bytes(k.into()),
            "value": v
        }
    })
}

impl Kv {
    fn execute(
        &self,
        cmd: Command,
        codec: &mut dyn Codec,
        ingest_ns: u64,
    ) -> Result<Value<'static>> {
        match cmd {
            Command::Get { key } => {
                decode(self.db.get(&key)?, codec, ingest_ns).map(|v| ok(key, v))
            }
            Command::Put { key, value } => decode(
                self.db.insert(&key, codec.encode(value)?)?,
                codec,
                ingest_ns,
            )
            .map(|v| ok(key, v)),
            Command::Delete { key } => {
                decode(self.db.remove(&key)?, codec, ingest_ns).map(|v| ok(key, v))
            }
            Command::Cas { key, old, new } => {
                if let Err(CompareAndSwapError { current, proposed }) = self.db.compare_and_swap(
                    &key,
                    old.map(|v| codec.encode(v)).transpose()?,
                    new.map(|v| codec.encode(v)).transpose()?,
                )? {
                    Ok(literal!({
                        "key": Value::Bytes(key.into()),
                        "error": {
                            "current": decode(current, codec, ingest_ns)?,
                            "proposed": decode(proposed, codec, ingest_ns)?                        }
                    }))
                } else {
                    Ok(ok(key, Value::null()))
                }
            }
            Command::Scan { start, end } => {
                let i = match (start, end) {
                    (None, None) => self.db.range::<Vec<u8>, _>(..),
                    (Some(start), None) => self.db.range(start..),
                    (None, Some(end)) => self.db.range(..end),
                    (Some(start), Some(end)) => self.db.range(start..end),
                };
                let mut res = Vec::with_capacity(32);
                for e in i {
                    let (key, e) = e?;
                    let key: &[u8] = &key;
                    let value = literal!({
                        "key": Value::Bytes(key.to_vec().into()),
                        "value": decode(Some(e), codec, ingest_ns)?
                    });
                    res.push(value)
                }
                Ok(literal!({ "ok": res }))
            }
        }
    }
}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            let db = sled::open(&config.dir)?;
            let event_origin_uri = EventOriginUri {
                uid: 0,
                scheme: "tremor-kv".to_string(),
                host: "localhost".to_string(),
                port: None,
                path: config.dir.split('/').map(ToString::to_string).collect(),
            };
            // dummy
            let (dummy_tx, _) = async_channel::bounded(1);

            Ok(SinkManager::new_box(Kv {
                sink_url: TremorUrl::from_onramp_id("kv")?, // dummy value
                idgen: EventIdGenerator::new(0),
                reply_tx: dummy_tx, // dummy, will be replaced in init
                event_origin_uri,
                db,
            }))
        } else {
            Err("[KV Offramp] Offramp requires a config".into())
        }
    }
}

#[derive(Debug)]
enum Command<'v> {
    /// ```json
    /// {"get": {"key": "the-key"}}
    /// ```
    Get { key: Vec<u8> },
    /// ```json
    /// {"put": {"key": "the-key", "value": "the-value"}}
    /// ```
    Put { key: Vec<u8>, value: &'v Value<'v> },
    /// ```json
    /// {"delete": {"key": "the-key"}}
    /// ```
    Delete { key: Vec<u8> },
    /// ```json
    /// {"scan": {
    ///    "start": "key1",
    ///    "end": "key2",
    /// }
    /// ```
    Scan {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    /// ```json
    /// {"cas": {
    ///    "key": "key"
    ///    "old": "<value|null|not-set>",
    ///    "new": "<value|null|not-set>",
    /// }
    /// ```
    Cas {
        key: Vec<u8>,
        old: Option<&'v Value<'v>>,
        new: Option<&'v Value<'v>>,
    },
}

impl<'v> Command<'v> {
    fn op_name(&self) -> &'static str {
        match self {
            Command::Get { .. } => "get",
            Command::Put { .. } => "put",
            Command::Delete { .. } => "delete",
            Command::Scan { .. } => "scan",
            Command::Cas { .. } => "cas",
        }
    }

    fn key(&self) -> Option<Vec<u8>> {
        match self {
            Command::Get { key, .. }
            | Command::Put { key, .. }
            | Command::Delete { key }
            | Command::Cas { key, .. } => Some(key.clone()),
            Command::Scan { .. } => None,
        }
    }
}

#[async_trait::async_trait]
impl Sink for Kv {
    #[allow(clippy::too_many_lines, clippy::option_if_let_else)]
    async fn on_event(
        &mut self,
        _input: &str,
        codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let mut r = Vec::with_capacity(10);
        let ingest_ns = tremor_common::time::nanotime();

        let cmds = event.value_meta_iter().map(|(v, m)| {
            let command_res = if let Some(g) = v.get("get") {
                g.get_bytes("key")
                    .ok_or_else(|| "Missing or invalid `key` field".into())
                    .map(|key| Command::Get { key: key.to_vec() })
            } else if let Some(p) = v.get("put") {
                p.get_bytes("key")
                    .ok_or_else(|| "Missing or invalid `key` field".into())
                    .and_then(|key| {
                        p.get("value")
                            .map(|value| Command::Put {
                                key: key.to_vec(),
                                value,
                            })
                            .ok_or_else(|| "Missing `value` field".into())
                    })
            } else if let Some(c) = v.get("cas") {
                c.get_bytes("key")
                    .ok_or_else(|| "Missing or invalid `key` field".into())
                    .map(|key| Command::Cas {
                        key: key.to_vec(),
                        old: c.get("old"),
                        new: c.get("new"),
                    })
            } else if let Some(d) = v.get("delete") {
                d.get_bytes("key")
                    .ok_or_else(|| "Missing or invalid `key` field".into())
                    .map(|key| Command::Delete { key: key.to_vec() })
            } else if let Some(s) = v.get("scan") {
                Ok(Command::Scan {
                    start: s.get_bytes("start").map(|v| v.to_vec()),
                    end: s.get_bytes("end").map(|v| v.to_vec()),
                })
            } else {
                Err(format!("Invalid KV command: {}", v))
            };
            (
                command_res.map_err(|e| ErrorKind::KvError(e).into()),
                m.get("correlation"),
            )
        });
        let mut first_error = None;
        // note: we always try to execute all commands / handle all errors.
        //       we might want to exit early in some cases though
        for (cmd_or_err, correlation) in cmds {
            let executed = match cmd_or_err {
                Ok(cmd) => {
                    let name = cmd.op_name();
                    let key = cmd.key();
                    self.execute(cmd, codec, ingest_ns)
                        .map(|res| (name, res))
                        .map_err(|e| (Some(name), key, e))
                }
                Err(e) => Err((None, None, e)),
            };
            match executed {
                Ok((op, data)) => {
                    let mut id = self.idgen.next_id();
                    id.track(&event.id);

                    let mut meta = Value::object_with_capacity(2);
                    meta.try_insert("kv", literal!({ "op": op }));
                    if let Some(correlation) = correlation {
                        meta.try_insert("correlation", correlation.clone_static());
                    }
                    let e = Event {
                        id,
                        ingest_ns,
                        data: (data, meta).into(),
                        origin_uri: Some(self.event_origin_uri.clone()),
                        ..Event::default()
                    };
                    r.push(Reply::Response(OUT, e))
                }
                Err((op, key, e)) => {
                    // send ERR response and log err
                    let mut id = self.idgen.next_id();
                    id.track(&event.id);
                    let data = literal!({
                        "key": key.map_or_else(Value::null, |v| Value::Bytes(v.into())),
                        "error": e.to_string(),
                    });
                    let mut meta = Value::object_with_capacity(3);
                    meta.try_insert("kv", literal!({ "op": op }));
                    meta.try_insert("error", e.to_string());
                    if let Some(correlation) = correlation {
                        meta.try_insert("correlation", correlation.clone_static());
                    }
                    let err_event = Event {
                        id,
                        ingest_ns,
                        data: (data, meta).into(),
                        origin_uri: Some(self.event_origin_uri.clone()),
                        ..Event::default()
                    };
                    r.push(Reply::Response(ERR, err_event));
                    first_error.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_error {
            // send away all response events asynchronously before
            for reply in r {
                if let Err(e) = self.reply_tx.send(reply).await {
                    error!("[Sink::{}] Error sending error reply: {}", self.sink_url, e);
                }
            }
            // trigger CB fail
            Err(e)
        } else {
            Ok(Some(r))
        }
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        self.event_origin_uri.uid = sink_uid;
        self.sink_url = sink_url.clone();
        self.idgen.set_source(sink_uid);
        self.reply_tx = reply_channel;
        Ok(())
    }

    fn is_active(&self) -> bool {
        true
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    async fn terminate(&mut self) {}
}
