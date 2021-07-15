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

#![cfg(not(tarpaulin_include))]

//! # `PostgreSQL` Offramp
//!
//! Writes events to a `PostgreSQL` and `TimescaleDB` database
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::common::postgres::{json_to_record, Record};
use crate::sink::prelude::*;
use halfbrown::HashMap;
use postgres::{Client, NoTls};

pub struct Postgres {
    pub config: Config,
    client: Option<postgres::Client>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub table: String,
}

impl ConfigImpl for Config {}

pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(SinkManager::new_box(Postgres {
                config,
                client: None,
            }))
        } else {
            Err("Missing config for postgres offramp".into())
        }
    }
}

fn init_cli(config: &Config) -> std::result::Result<postgres::Client, postgres::Error> {
    let conn_str = format!(
        "host={} user={} password={} port={} dbname={}",
        config.host, config.user, config.password, config.port, config.dbname
    );
    let cli = Client::connect(&conn_str, NoTls)?;

    Ok(cli)
}

#[async_trait::async_trait]
impl Sink for Postgres {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        for val in event.value_iter() {
            let obj = val.as_object();
            if let Some(kv) = obj {
                let mut fields: Vec<String> = Vec::with_capacity(kv.len());
                let mut params: Vec<String> = Vec::with_capacity(kv.len());
                let mut records: Vec<Record> = Vec::with_capacity(kv.len());

                let mut ct: usize = 1;
                for (field, value) in kv {
                    fields.push(field.to_string());
                    params.push(format!("${}", ct));
                    ct += 1;
                    let record = match json_to_record(value) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(format!("Could not convert json to record: {}", e).into())
                        }
                    };
                    records.push(record);
                }

                let fields = fields.join(",");
                let params = params.join(",");

                let q = format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    self.config.table, fields, params
                );

                if self.client.is_none() {
                    self.client = match init_cli(&self.config) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            return Err(
                                format!("Could not initialize a Postgres client: {}", e).into()
                            )
                        }
                    }
                }

                let client = match self.client.as_mut() {
                    Some(v) => v,
                    None => return Err("could not move client value".into()),
                };

                match client.query_raw(
                    q.as_str(),
                    records
                        .iter()
                        .map(|p| p as &dyn postgres::types::ToSql)
                        .collect::<Vec<&dyn postgres::types::ToSql>>(),
                ) {
                    Ok(_) => return Ok(None),
                    Err(e) => return Err(format!("Failure while querying: {}", e).into()),
                }
            }
        }

        Ok(None)
    }
    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        Ok(())
    }
    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        true
    }
}
