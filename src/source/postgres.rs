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

//! # Postgres Onramp
//!
//! See [Config](struct.Config.html) for details.

use crate::common::mmap;
use crate::common::mmap::{Config as CacheConfig, Kv};
use crate::common::postgres::row_to_json;
use crate::errors::Result;
use crate::source::prelude::*;
use async_compat::Compat;
use chrono::prelude::*;
use simd_json::OwnedValue;
use std::fmt;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, Row, Statement};
const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S%.6f %:z";

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub query: String,
    pub interval_ms: u32,
    pub consume_from: String,
    pub cache: CacheConfig,
}

impl ConfigImpl for Config {}

pub struct Postgres {
    onramp_id: TremorUrl,
    pub config: Config,
}

pub struct Int {
    pub config: Config,
    // onramp_uid: u64,
    onramp_id: TremorUrl,
    origin_uri: EventOriginUri,
    cache: Box<dyn Kv + Send>,
    cli: Option<Client>,
    stmt: Option<Statement>,
    rows: Vec<Row>,
}
impl fmt::Debug for Int {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Postgres")
    }
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;

            Ok(Box::new(Postgres {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for postgres onramp".into())
        }
    }
}

impl Int {
    async fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Result<Self> {
        let origin_uri = EventOriginUri {
            uid,
            scheme: "tremor-file".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.host.clone()],
        };
        let consume_from = DateTime::parse_from_str(&config.consume_from, TIME_FMT)?;
        let consume_from = consume_from.format(TIME_FMT).to_string();

        let consume_until: DateTime<Utc> = chrono::offset::Utc::now();
        let consume_until = consume_until.format(TIME_FMT).to_string();

        let mut obj = OwnedValue::object();

        obj.try_insert("consume_from", consume_from);
        obj.try_insert("consume_until", consume_until);

        let cache = match mmap::lookup("mmap_file", Some(config.cache.clone()), &obj) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };

        Ok(Int {
            // onramp_uid,
            onramp_id,
            config: config.clone(),
            origin_uri,
            cache,
            cli: None,
            stmt: None,
            rows: Vec::new(),
        })
    }

    async fn init_cli(&mut self) -> Result<()> {
        let conn_str = format!(
            "host={} user={} password={} port={} dbname={}",
            self.config.host,
            self.config.user,
            self.config.password,
            self.config.port,
            self.config.dbname
        );
        let (client, connection) = Compat::new(tokio_postgres::connect(&conn_str, NoTls)).await?;
        task::spawn(async move {
            if let Err(e) = Compat::new(connection).await {
                error!("connection error: {}", e);
            }
        });
        self.cli = Some(client);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Source for Int {
    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        if let Some(row) = self.rows.pop() {
            return Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                data: row_to_json(&row)?.into(),
            });
        };

        if self.cli.is_none() {
            self.init_cli().await?;
        };

        let client = self
            .cli
            .as_mut()
            .ok_or_else(|| Error::from("No CLI connection"))?;

        if self.stmt.is_none() {
            let q = &self.config.query;
            self.stmt = Some(Compat::new(client.prepare(q)).await?);
        };

        let statement = self
            .stmt
            .as_mut()
            .ok_or_else(|| Error::from("No Statement connection"))?;

        let mut obj = self.cache.get()?;

        let consume_from = obj
            .get_str("consume_from")
            .ok_or_else(|| Error::from("Failed to fetching consume_from"))?;

        let consume_until = obj
            .get_str("consume_until")
            .ok_or_else(|| Error::from("Failed to fetching consume_until"))?;

        let cf = DateTime::parse_from_str(consume_from, TIME_FMT)?;
        let ct = DateTime::parse_from_str(consume_until, TIME_FMT)?;

        self.rows = match Compat::new(client.query(statement, &[&cf, &ct])).await {
            Ok(v) => v,
            Err(e) => {
                let code = e.code().unwrap_or(&SqlState::CONNECTION_EXCEPTION);
                if code == &SqlState::CONNECTION_EXCEPTION {
                    self.cli = None;
                    self.stmt = None;
                }
                return Ok(SourceReply::Empty(10));
            }
        };
        self.rows.reverse();

        // prepare interval for the next query
        let cf = DateTime::parse_from_str(&consume_until.to_string(), TIME_FMT)?;
        let ct = cf + chrono::Duration::milliseconds(i64::from(self.config.interval_ms));
        let cf = cf.format(TIME_FMT);
        let ct = ct.format(TIME_FMT);
        obj.insert("consume_from", cf.to_string())?;
        obj.insert("consume_until", ct.to_string())?;

        self.cache.set(obj)?;

        if let Some(row) = self.rows.pop() {
            Ok(SourceReply::Structured {
                origin_uri: self.origin_uri.clone(),
                data: row_to_json(&row)?.into(),
            })
        } else {
            Ok(SourceReply::Empty(100))
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        Ok(SourceState::Connected)
    }

    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Postgres {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source =
            Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config).await?;
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
