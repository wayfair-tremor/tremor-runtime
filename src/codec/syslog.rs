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

use super::prelude::*;
use chrono::{Datelike, Offset, TimeZone, Utc};
use syslog_loose::{IncompleteDate, ProcId, Protocol, SyslogFacility, SyslogSeverity};
use tremor_script::{Object, Value};

#[derive(Clone)]
pub struct Syslog {}

impl Codec for Syslog {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "syslog"
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        let line: &str = std::str::from_utf8(data)?;
        let parsed = syslog_loose::parse_message_with_year_tz(
            line,
            resolve_year,
            Some(chrono::offset::Utc.fix()),
        );

        let mut es_msg = Object::with_capacity(11);
        es_msg.insert("hostname".into(), Value::from(parsed.hostname));
        es_msg.insert(
            "severity".into(),
            Value::from(parsed.severity.map(syslog_loose::SyslogSeverity::as_str)),
        );
        es_msg.insert(
            "facility".into(),
            Value::from(parsed.facility.map(syslog_loose::SyslogFacility::as_str)),
        );
        es_msg.insert(
            "timestamp".into(),
            Value::from(parsed.timestamp.map(|x| x.timestamp_nanos())),
        );
        es_msg.insert(
            "protocol".into(),
            Value::from(match parsed.protocol {
                Protocol::RFC3164 => "RFC3164",
                Protocol::RFC5424(_) => "RFC5424",
            }),
        );
        es_msg.insert(
            "protocol_version".into(),
            match parsed.protocol {
                Protocol::RFC5424(version) => Value::from(version),
                Protocol::RFC3164 => Value::null(),
            },
        );
        es_msg.insert("appname".into(), Value::from(parsed.appname));
        es_msg.insert("msgid".into(), Value::from(parsed.msgid));

        let mut temp = Object::with_capacity(parsed.structured_data.len());
        for element in parsed.structured_data {
            let mut e = Vec::with_capacity(element.params.len());
            for (name, value) in element.params {
                let mut param = Value::object_with_capacity(1);
                param.insert(name.to_owned(), Value::from(value))?;
                e.push(param);
            }
            temp.insert(element.id.into(), Value::from(e));
        }
        if temp.is_empty() {
            es_msg.insert("structured_data".into(), Value::null());
        } else {
            es_msg.insert("structured_data".into(), Value::from(temp));
        }

        if let Some(procid) = parsed.procid {
            let value: Value = match procid {
                ProcId::PID(pid) => pid.to_string().into(),
                ProcId::Name(name) => name.to_string().into(),
            };
            es_msg.insert("procid".into(), value);
        } else {
            es_msg.insert("procid".into(), Value::null());
        }

        es_msg.insert("msg".into(), Value::from(parsed.msg));

        Ok(Some(Value::from(es_msg)))
    }

    #[allow(clippy::too_many_lines)]
    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        let mut result: Vec<String> = Vec::new();

        let mut v = String::new();
        let protocol = match (data.get_str("protocol"), data.get_u32("protocol_version")) {
            (Some("RFC3164"), _) => Protocol::RFC3164,
            (Some("RFC5424"), Some(version)) => {
                v = version.to_owned().to_string();
                Protocol::RFC5424(version)
            }
            (Some("RFC5424"), None) => {
                return Err(ErrorKind::InvalidSyslogData("Missing protocol version").into())
            }
            (None, Some(_)) => {
                return Err(ErrorKind::InvalidSyslogData("Missing protocol type").into())
            }
            (Some(&_), _) => {
                return Err(ErrorKind::InvalidSyslogData("invalid protocol type").into())
            }
            (None, None) => {
                v = "1".to_owned();
                Protocol::RFC5424(1_u32)
            }
        };

        let f = data
            .get_str("facility")
            .ok_or_else(|| Error::from(ErrorKind::InvalidSyslogData("facility missing")))?;
        let s = data
            .get_str("severity")
            .ok_or_else(|| Error::from(ErrorKind::InvalidSyslogData("severity missing")))?;
        result.push(format!(
            "<{}>{}",
            compose_pri(to_facility(f)?, to_severity(s)?),
            v
        ));

        if let Some(t) = data.get_i64("timestamp") {
            let datetime = Utc.timestamp_nanos(t);
            let timestamp_str = datetime.to_rfc3339();
            result.push(timestamp_str);
        } else {
            result.push(String::from("-"));
        }

        if let Some(h) = data.get_str("hostname") {
            result.push(h.to_owned());
        } else {
            result.push(String::from("-"));
        }

        match protocol {
            Protocol::RFC3164 => {
                if let Some(appname) = data.get_str("appname") {
                    if let Some(procid) = data.get_str("procid") {
                        result.push(format!("{}[{}]:", appname, procid));
                    } else {
                        result.push(format!("{}:", appname));
                    }
                } else {
                    result.push(String::from(":"));
                }
            }
            Protocol::RFC5424(_) => {
                if let Some(appname) = data.get_str("appname") {
                    result.push(appname.to_owned());
                } else {
                    result.push(String::from("-"));
                }
                if let Some(procid) = data.get_str("procid") {
                    result.push(procid.to_owned());
                } else {
                    result.push(String::from("-"));
                }
                if let Some(msgid) = data.get_str("msgid") {
                    result.push(msgid.to_owned());
                } else {
                    result.push(String::from("-"));
                }
            }
        }

        if let Some(o) = data.get("structured_data") {
            if let Some(sd) = o.as_object() {
                let mut elem = String::new();
                for (id, params) in sd.iter() {
                    elem.push('[');
                    elem.push_str(&id.to_string());
                    if let Some(v) = params.as_array() {
                        for key_value in v.iter() {
                            if let Some(o) = key_value.as_object() {
                                for (k, v) in o {
                                    if let Some(v) = v.as_str() {
                                        elem.push(' ');
                                        elem.push_str(&k.to_string());
                                        elem.push('=');
                                        elem.push('"');
                                        elem.push_str(v);
                                        elem.push('"');
                                    } else {
                                        return Err(ErrorKind::InvalidSyslogData(
                                            "Invalid structured data: param value not a string",
                                        )
                                        .into());
                                    }
                                }
                            } else {
                                return Err(ErrorKind::InvalidSyslogData(
                                    "Invalid structured data: param's key value pair not an object",
                                )
                                .into());
                            }
                        }
                    } else {
                        return Err(ErrorKind::InvalidSyslogData(
                            "Invalid structured data: params not an array of objects",
                        )
                        .into());
                    }
                    elem.push(']');
                }
                result.push(elem);
            } else {
                return Err(ErrorKind::InvalidSyslogData(
                    "Invalid structured data: structured data not an object",
                )
                .into());
            }
        } else if let Protocol::RFC5424(_) = protocol {
            result.push(String::from("-"));
        }

        if let Some(msg) = data.get_str("msg") {
            result.push(msg.to_owned());
        }

        Ok(result.join(" ").as_bytes().to_vec())
    }

    #[cfg(not(tarpaulin_include))]
    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

// Function used to resolve the year for syslog messages that don't include the year.
// If the current month is January, and the syslog message is for December, it will take the previous year.
// Otherwise, take the current year.
fn resolve_year((month, _date, _hour, _min, _sec): IncompleteDate) -> i32 {
    let now = Utc::now();
    if now.month() == 1 && month == 12 {
        now.year() - 1
    } else {
        now.year()
    }
}

// Convert string to its respective syslog severity representation.
fn to_severity(s: &str) -> Result<SyslogSeverity> {
    match s {
        "emerg" => Ok(SyslogSeverity::SEV_EMERG),
        "alert" => Ok(SyslogSeverity::SEV_ALERT),
        "crit" => Ok(SyslogSeverity::SEV_CRIT),
        "err" => Ok(SyslogSeverity::SEV_ERR),
        "warning" => Ok(SyslogSeverity::SEV_WARNING),
        "notice" => Ok(SyslogSeverity::SEV_NOTICE),
        "info" => Ok(SyslogSeverity::SEV_INFO),
        "debug" => Ok(SyslogSeverity::SEV_DEBUG),
        _ => Err(ErrorKind::InvalidSyslogData("invalid severity").into()),
    }
}

// Convert string to its respective syslog facility representation.
fn to_facility(s: &str) -> Result<SyslogFacility> {
    match s {
        "kern" => Ok(SyslogFacility::LOG_KERN),
        "user" => Ok(SyslogFacility::LOG_USER),
        "mail" => Ok(SyslogFacility::LOG_MAIL),
        "daemon" => Ok(SyslogFacility::LOG_DAEMON),
        "auth" => Ok(SyslogFacility::LOG_AUTH),
        "syslog" => Ok(SyslogFacility::LOG_SYSLOG),
        "lpr" => Ok(SyslogFacility::LOG_LPR),
        "news" => Ok(SyslogFacility::LOG_NEWS),
        "uucp" => Ok(SyslogFacility::LOG_UUCP),
        "cron" => Ok(SyslogFacility::LOG_CRON),
        "authpriv" => Ok(SyslogFacility::LOG_AUTHPRIV),
        "ftp" => Ok(SyslogFacility::LOG_FTP),
        "ntp" => Ok(SyslogFacility::LOG_NTP),
        "audit" => Ok(SyslogFacility::LOG_AUDIT),
        "alert" => Ok(SyslogFacility::LOG_ALERT),
        "clockd" => Ok(SyslogFacility::LOG_CLOCKD),
        "local0" => Ok(SyslogFacility::LOG_LOCAL0),
        "local1" => Ok(SyslogFacility::LOG_LOCAL1),
        "local2" => Ok(SyslogFacility::LOG_LOCAL2),
        "local3" => Ok(SyslogFacility::LOG_LOCAL3),
        "local4" => Ok(SyslogFacility::LOG_LOCAL4),
        "local5" => Ok(SyslogFacility::LOG_LOCAL5),
        "local6" => Ok(SyslogFacility::LOG_LOCAL6),
        "local7" => Ok(SyslogFacility::LOG_LOCAL7),
        _ => Err(ErrorKind::InvalidSyslogData("invalid facility").into()),
    }
}

// Compose the facility and severity as a single integer.
fn compose_pri(facility: SyslogFacility, severity: SyslogSeverity) -> i32 {
    ((facility as i32) << 3) + (severity as i32)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_syslog_codec() -> Result<()> {
        let mut s = b"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource= \"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"] BOMAn application event log entry...".to_vec();

        let mut codec = Syslog {};
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let mut a = codec.encode(&decoded)?;
        let b = codec.decode(a.as_mut_slice(), 0)?.unwrap();
        assert_eq!(decoded, b);
        Ok(())
    }

    #[test]
    fn test_decode_empty() -> Result<()> {
        let mut s = b"<191>1 2021-03-18T20:30:00.123Z - - - - - message".to_vec();
        let mut codec = Syslog {};
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "hostname": null,
            "severity": "debug",
            "facility": "local7",
            "appname": null,
            "msg": "message",
            "msgid": null,
            "procid": null,
            "protocol": "RFC5424",
            "protocol_version": 1,
            "structured_data": null,
            "timestamp": 1_616_099_400_123_000_000_u64
        }));
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn decode_invalid_message() -> Result<()> {
        let mut msg = b"an invalid message".to_vec();
        let mut codec = Syslog {};
        let decoded = codec.decode(msg.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "hostname": null,
            "severity": null,
            "facility": null,
            "appname": null,
            "msg": "an invalid message",
            "msgid": null,
            "procid": null,
            "protocol": "RFC3164",
            "protocol_version": null,
            "structured_data": null,
            "timestamp": null
        }));
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn encode_empty_rfc5424() -> Result<()> {
        let codec = Syslog {};
        let msg = Value::from(simd_json::json!({
            "severity": "notice",
            "facility": "local4",
            "msg": "test message",
            "protocol": "RFC5424",
            "protocol_version": 1
        }));
        let encoded = codec.encode(&msg)?;
        let expected = "<165>1 - - - - - - test message";
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), expected);
        Ok(())
    }

    #[test]
    fn encode_empty_rfc3164() -> Result<()> {
        let codec = Syslog {};
        let msg = Value::from(simd_json::json!({
            "severity": "notice",
            "facility": "local4",
            "msg": "test message",
            "protocol": "RFC3164"
        }));
        let encoded = codec.encode(&msg)?;
        let expected = "<165> - - : test message";
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), expected);
        Ok(())
    }

    #[test]
    fn test_incorrect_sd() -> Result<()> {
        let mut msg =
            b"<13>1 2021-03-18T20:30:00.123Z 74794bfb6795 root 8449 - [incorrect x] message"
                .to_vec();
        let mut codec = Syslog {};
        let decoded = codec.decode(msg.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "hostname": "74794bfb6795",
            "severity": "notice",
            "facility": "user",
            "appname": "root",
            "msg": "message",
            "msgid": null,
            "procid": "8449",
            "protocol": "RFC5424",
            "protocol_version": 1,
            "structured_data": null,
            "timestamp": 1_616_099_400_123_000_000_u64
        }));
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn test_invalid_sd_3164() -> Result<()> {
        let mut s = b"<46>Jan  5 15:33:03 plertrood-ThinkPad-X220 rsyslogd:  [software=\"rsyslogd\" swVersion=\"8.32.0\"] message".to_vec();
        let mut codec = Syslog {};
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "hostname": "plertrood-ThinkPad-X220",
            "severity": "info",
            "facility": "syslog",
            "appname": "rsyslogd",
            "msg": "message",
            "msgid": null,
            "procid": null,
            "protocol": "RFC3164",
            "protocol_version": null,
            "structured_data": null,
            "timestamp": 1_609_860_783_000_000_000_u64
        }));
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn errors() {
        let mut o = Value::object();
        let s = Syslog {};
        assert_eq!(
            s.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: facility missing"
        );

        o.insert("facility", "cron").unwrap();
        assert_eq!(
            s.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: severity missing"
        );

        o.insert("severity", "info").unwrap();
        o.insert("structured_data", "sd").unwrap();
        assert_eq!(
            s.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: Invalid structured data: structured data not an object"
        );
    }
}
