// Copyright 2021, The Tremor Team
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

//! # `PostgreSQL` Ramp
//!
//! Implements an intermediate representation
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use crate::errors::{Error, Result};
use bytes::buf::BufMut;
use bytes::BytesMut;
use chrono::prelude::*;
use postgres::types::to_sql_checked;
use std::time::SystemTime;
use tremor_script::prelude::*;

#[derive(Debug)]
pub struct Record<'a> {
    pub t: postgres::types::Type,
    pub value: &'a Value<'a>,
    pub name: &'a str,
}

impl postgres::types::ToSql for Record<'_> {
    fn to_sql(
        &self,
        type_: &postgres::types::Type,
        w: &mut BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        if self.value.is_null() {
            return Ok(postgres::types::IsNull::Yes);
        }

        match self.t {
            postgres::types::Type::BOOL => {
                match self.value.as_bool() {
                    Some(v) => postgres_protocol::types::bool_to_sql(v, w),
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "bool").into()
                        )
                    }
                };
            }
            postgres::types::Type::BPCHAR
            | postgres::types::Type::NAME
            | postgres::types::Type::TEXT
            | postgres::types::Type::UNKNOWN
            | postgres::types::Type::VARCHAR => {
                match self.value.as_str() {
                    Some(v) => postgres_protocol::types::text_to_sql(v, w),
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "text").into()
                        )
                    }
                };
            }
            postgres::types::Type::INT2 => {
                let val = match self.value.as_i16() {
                    Some(v) => v,
                    None => {
                        return Err(
                            format!("Could not serialize {} into {}", self.name, "i16").into()
                        )
                    }
                };

                postgres_protocol::types::int2_to_sql(val, w)
            }
            postgres::types::Type::INT4 => {
                let val = match self.value.as_i32() {
                    Some(v) => v,
                    None => return Err("Could not serialize".into()),
                };

                postgres_protocol::types::int4_to_sql(val, w)
            }
            postgres::types::Type::INT8 => {
                let val = match self.value.as_i64() {
                    Some(v) => v,
                    None => return Err("Could not serialize".into()),
                };

                postgres_protocol::types::int8_to_sql(val, w)
            }
            postgres::types::Type::JSON => {
                let val = self.value.as_str().unwrap_or_default();
                simd_json::to_writer(w.writer(), &val)?;
            }
            postgres::types::Type::JSONB => {
                let val = self.value.as_str().unwrap_or_default();
                w.put_u8(1);

                simd_json::to_writer(w.writer(), &val)?;
            }
            postgres::types::Type::TIMESTAMPTZ => {
                let val = self.value.as_str().unwrap_or_default();

                let dt = match DateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.6f %:z") {
                    Ok(v) => v,
                    Err(e) => return Err(format!("error parsing timestamptz: {}", e).into()),
                };
                dt.with_timezone(&Utc).to_sql(type_, w)?;
            }
            postgres::types::Type::TIMESTAMP => {
                let val = self.value.as_str().unwrap_or_default();

                let base: NaiveDateTime = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
                let dt = match NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.6f %:z") {
                    Ok(v) => v,
                    Err(e) => return Err(format!("error parsing timestamptz: {}", e).into()),
                };
                let time = match dt.signed_duration_since(base).num_microseconds() {
                    Some(time) => time,
                    None => return Err("value too large".into()),
                };

                postgres_protocol::types::timestamp_to_sql(time, w)
            }
            _ => return Err(format!("Not supported type: {}", self.t).into()),
        };

        Ok(postgres::types::IsNull::No)
    }

    // NOTE: cannot easily tell which types are accepted due to trait not accepting self
    // which holds t context. This renders the function not as useful
    fn accepts(ty: &postgres::types::Type) -> bool {
        matches!(
            *ty,
            postgres::types::Type::BOOL
                | postgres::types::Type::CHAR
                | postgres::types::Type::TEXT
                | postgres::types::Type::NAME
                | postgres::types::Type::INT2
                | postgres::types::Type::INT4
                | postgres::types::Type::INT8
                | postgres::types::Type::JSON
                | postgres::types::Type::JSONB
                | postgres::types::Type::TIMESTAMP
                | postgres::types::Type::TIMESTAMPTZ
                | postgres::types::Type::UNKNOWN
                | postgres::types::Type::VARCHAR
        )
    }

    to_sql_checked!();
}

pub fn json_to_record<'a>(json: &'a Value<'a>) -> Result<Record> {
    let field_type = match json.get_str("fieldType") {
        Some(v) => v,
        None => return Err("error getting fieldType".into()),
    };

    let t: postgres::types::Type = match field_type {
        "VARCHAR" => postgres::types::Type::VARCHAR,
        "UNKNOWN" => postgres::types::Type::UNKNOWN,
        "BOOL" => postgres::types::Type::BOOL,
        "CHAR" => postgres::types::Type::CHAR,
        "TEXT" => postgres::types::Type::TEXT,
        "NAME" => postgres::types::Type::NAME,
        "INT8" => postgres::types::Type::INT8,
        "INT2" => postgres::types::Type::INT2,
        "INT4" => postgres::types::Type::INT4,
        "JSON" => postgres::types::Type::JSON,
        "JSONB" => postgres::types::Type::JSONB,
        "TIMESTAMPTZ" => postgres::types::Type::TIMESTAMPTZ,
        "TIMESTAMP" => postgres::types::Type::TIMESTAMP,
        _ => return Err("intermediate representation does not support field type".into()),
    };

    let name = json
        .get_str("name")
        .ok_or_else(|| Error::from("Missing field `name`"))?;

    let value = json
        .get("value")
        .ok_or_else(|| Error::from("Missing field `value`"))?;

    Ok(Record { t, value, name })
}
pub fn row_to_json(
    row: &postgres::row::Row,
) -> std::result::Result<Value<'static>, Box<dyn std::error::Error + Sync + Send>> {
    let mut json: Value<'static> = Value::object();

    for (cid, col) in row.columns().iter().enumerate() {
        let mut obj: Value<'static> = Value::object();

        let (t, v) = match *col.type_() {
            postgres::types::Type::BOOL => ("BOOL", Value::from(row.get::<_, bool>(cid))),
            postgres::types::Type::CHAR => ("CHAR", Value::from(row.get::<_, String>(cid))),
            postgres::types::Type::INT8 => ("INT8", Value::from(row.get::<_, i64>(cid))),
            postgres::types::Type::INT2 => ("INT2", Value::from(row.get::<_, i64>(cid))),
            postgres::types::Type::INT4 => ("INT4", Value::from(row.get::<_, i64>(cid))),
            postgres::types::Type::JSON => ("JSON", Value::from(row.get::<_, String>(cid))),
            postgres::types::Type::JSONB => ("JSONB", Value::from(row.get::<_, String>(cid))),
            postgres::types::Type::NAME => ("NAME", Value::from(row.get::<_, String>(cid))),
            postgres::types::Type::TEXT => ("TEXT", Value::from(row.get::<_, String>(cid))),
            // TODO: We should specify timezone offset for data in config
            postgres::types::Type::TIMESTAMPTZ => {
                let ts: DateTime<Utc> = row.get::<_, SystemTime>(cid).into();
                (
                    "TIMESTAMPTZ",
                    Value::from(ts.with_timezone(&FixedOffset::east(0)).to_string()),
                )
            }
            postgres::types::Type::TIMESTAMP => {
                let ts: DateTime<Utc> = row.get::<_, SystemTime>(cid).into();
                ("TIMESTAMP", Value::from(ts.to_string()))
            }
            postgres::types::Type::UNKNOWN => ("UNKNOWN", Value::from(row.get::<_, String>(cid))),
            // TODO: Encoding, see UTF-8
            postgres::types::Type::VARCHAR => ("VARCHAR", Value::from(row.get::<_, String>(cid))),
            _ => return Err(format!("type not supported: {}", col.type_()).into()),
        };
        obj.try_insert("fieldType", Value::from(t));
        obj.try_insert("value", v);
        obj.try_insert("name", Value::from(String::from(col.name())));

        json.try_insert(col.name().to_string(), obj);
    }

    Ok(json)
}
