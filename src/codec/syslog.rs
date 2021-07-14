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
use chrono::{DateTime, Datelike, Offset, TimeZone, Utc};
use syslog_loose::{IncompleteDate, ProcId, Protocol, SyslogFacility, SyslogSeverity};
use tremor_value::Value;

const DEFAULT_PRI: i32 = 13;

pub trait Now: Send + Sync + Clone {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone)]
pub struct UtcNow {}
impl Now for UtcNow {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct Syslog<N>
where
    N: Now,
{
    now: N,
}

impl Syslog<UtcNow> {
    /// construct a Syslog codec
    /// that adds the current time in UTC during encoding if none was provided in the event payload
    pub fn utcnow() -> Self {
        Self { now: UtcNow {} }
    }
}

impl<N> Syslog<N>
where
    N: Now,
{
    /// encode structured data `sd` into `result`
    fn encode_sd(sd: &Value, result: &mut Vec<String>) -> Result<()> {
        let sd = sd.as_object().ok_or_else(|| {
            Error::from(ErrorKind::InvalidSyslogData(
                "Invalid structured data: structured data not an object",
            ))
        })?;
        let mut elem = String::with_capacity(16);
        for (id, params) in sd.iter() {
            elem.push('[');
            elem.push_str(&id.to_string());
            let params = params.as_array().ok_or_else(|| {
                Error::from(ErrorKind::InvalidSyslogData(
                    "Invalid structured data: params not an array of objects",
                ))
            })?;
            for key_value in params.iter() {
                let kv_map = key_value.as_object().ok_or_else(|| {
                    Error::from(ErrorKind::InvalidSyslogData(
                        "Invalid structured data: param's key value pair not an object",
                    ))
                })?;
                for (k, v) in kv_map {
                    let value = v.as_str().ok_or_else(|| {
                        Error::from(ErrorKind::InvalidSyslogData(
                            "Invalid structured data: param's key value pair not an object",
                        ))
                    })?;
                    elem.push(' ');
                    elem.push_str(&k.to_string());
                    elem.push('=');
                    elem.push('"');
                    elem.push_str(value);
                    elem.push('"');
                }
            }
            elem.push(']');
        }
        result.push(elem);

        Ok(())
    }

    fn encode_rfc3164(&self, data: &Value) -> Result<Vec<String>> {
        let mut result = Vec::with_capacity(4);
        let f = data.get_str("facility");
        let s = data.get_str("severity");

        let pri = match f.zip(s) {
            Some((f, s)) => compose_pri(to_facility(f)?, to_severity(s)?),
            None => DEFAULT_PRI, // https://datatracker.ietf.org/doc/html/rfc3164#section-4.3.3
        };
        let datetime = data
            .get_i64("timestamp")
            .map_or_else(|| self.now.now(), |t| Utc.timestamp_nanos(t));
        result.push(format!("<{}>{}", pri, datetime.format("%b %e %H:%M:%S")));

        result.push(
            data.get_str("hostname")
                .map_or_else(Self::nil, ToOwned::to_owned),
        );
        result.push(data.get_str("appname").map_or_else(
            || String::from(":"),
            |appname| {
                data.get_str("procid").map_or_else(
                    || format!("{}:", appname),
                    |procid| format!("{}[{}]:", appname, procid),
                )
            },
        ));

        // structured data shouldnt pop up in this format, but syslog_loose parses it anyways
        if let Some(sd) = data.get("structured_data") {
            Self::encode_sd(sd, &mut result)?;
        }
        if let Some(msg) = data.get_str("msg") {
            result.push(msg.to_owned());
        }
        Ok(result)
    }

    fn nil() -> String {
        String::from("-")
    }

    fn encode_rfc5424(data: &Value, version: u32) -> Result<Vec<String>> {
        let mut result = Vec::with_capacity(10); // reserving 3 slots for structured data
        let f = data
            .get_str("facility")
            .ok_or_else(|| Error::from(ErrorKind::InvalidSyslogData("facility missing")))?;
        let s = data
            .get_str("severity")
            .ok_or_else(|| Error::from(ErrorKind::InvalidSyslogData("severity missing")))?;
        result.push(format!(
            "<{}>{}",
            compose_pri(to_facility(f)?, to_severity(s)?),
            version
        ));

        result.push(data.get_i64("timestamp").map_or_else(Self::nil, |t| {
            let datetime = Utc.timestamp_nanos(t);
            datetime.to_rfc3339()
        }));

        result.push(
            data.get_str("hostname")
                .map_or_else(Self::nil, ToOwned::to_owned),
        );
        result.push(
            data.get_str("appname")
                .map_or_else(Self::nil, ToOwned::to_owned),
        );
        result.push(
            data.get_str("procid")
                .map_or_else(Self::nil, ToOwned::to_owned),
        );
        result.push(
            data.get_str("msgid")
                .map_or_else(Self::nil, ToOwned::to_owned),
        );
        if let Some(sd) = data.get("structured_data") {
            Self::encode_sd(sd, &mut result)?;
        } else {
            result.push(Self::nil());
        }

        if let Some(msg) = data.get_str("msg") {
            result.push(msg.to_owned());
        }
        Ok(result)
    }
}

impl<N> Codec for Syslog<N>
where
    N: Now + 'static,
{
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

        let mut decoded = Value::object_with_capacity(11);
        if let Some(hostname) = parsed.hostname {
            decoded.try_insert("hostname", hostname);
        }
        if let Some(severity) = parsed.severity {
            decoded.try_insert("severity", severity.as_str());
        }
        if let Some(facility) = parsed.facility {
            decoded.try_insert("facility", facility.as_str());
        }
        if let Some(timestamp) = parsed.timestamp {
            decoded.try_insert("timestamp", timestamp.timestamp_nanos());
        }
        decoded.try_insert(
            "protocol",
            match parsed.protocol {
                Protocol::RFC3164 => "RFC3164",
                Protocol::RFC5424(_) => "RFC5424",
            },
        );
        if let Protocol::RFC5424(version) = parsed.protocol {
            decoded.try_insert("protocol_version", version);
        }
        if let Some(appname) = parsed.appname {
            decoded.try_insert("appname", appname);
        }
        if let Some(msgid) = parsed.msgid {
            decoded.try_insert("msgid", msgid);
        }

        if !parsed.structured_data.is_empty() {
            let mut temp = Value::object_with_capacity(parsed.structured_data.len());
            for element in parsed.structured_data {
                let mut e = Vec::with_capacity(element.params.len());
                for (name, value) in element.params {
                    let mut param = Value::object_with_capacity(1);
                    param.try_insert(name, value);
                    e.push(param);
                }
                temp.try_insert(element.id, Value::from(e));
            }
            decoded.try_insert("structured_data", temp);
        }

        if let Some(procid) = parsed.procid {
            let value: Value = match procid {
                ProcId::PID(pid) => pid.to_string().into(),
                ProcId::Name(name) => name.to_string().into(),
            };
            decoded.try_insert("procid", value);
        }
        if !parsed.msg.is_empty() {
            decoded.try_insert("msg", Value::from(parsed.msg));
        }

        Ok(Some(decoded))
    }

    #[allow(clippy::too_many_lines)]
    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        let protocol = match (data.get_str("protocol"), data.get_u32("protocol_version")) {
            (Some("RFC3164"), _) => Protocol::RFC3164,
            (Some("RFC5424"), Some(version)) => Protocol::RFC5424(version),
            (Some("RFC5424"), None) => {
                return Err(ErrorKind::InvalidSyslogData("Missing protocol version").into())
            }
            (None, Some(_)) => {
                return Err(ErrorKind::InvalidSyslogData("Missing protocol type").into())
            }
            (Some(&_), _) => {
                return Err(ErrorKind::InvalidSyslogData("invalid protocol type").into())
            }
            (None, None) => Protocol::RFC5424(1_u32),
        };
        let result = match protocol {
            Protocol::RFC3164 => self.encode_rfc3164(data)?,
            Protocol::RFC5424(version) => Self::encode_rfc5424(data, version)?,
        };

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
    use test_case::test_case;
    use tremor_value::literal;

    #[derive(Clone)]
    struct TestNow {}
    impl Now for TestNow {
        fn now(&self) -> DateTime<Utc> {
            Utc.ymd(1970, 01, 01).and_hms(0, 0, 0)
        }
    }

    fn test_codec() -> Syslog<TestNow> {
        Syslog { now: TestNow {} }
    }

    #[test]
    fn test_syslog_codec() -> Result<()> {
        let mut s = b"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource= \"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"] BOMAn application event log entry...".to_vec();

        let mut codec = test_codec();
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let mut a = codec.encode(&decoded)?;
        let b = codec.decode(a.as_mut_slice(), 0)?.unwrap();
        assert_eq!(decoded, b);
        Ok(())
    }

    #[test]
    fn test_decode_empty() -> Result<()> {
        let mut s = b"<191>1 2021-03-18T20:30:00.123Z - - - - - message".to_vec();
        let mut codec = test_codec();
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let expected = literal!({
            "severity": "debug",
            "facility": "local7",
            "msg": "message",
            "protocol": "RFC5424",
            "protocol_version": 1,
            "timestamp": 1_616_099_400_123_000_000_u64
        });
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn decode_invalid_message() -> Result<()> {
        let mut msg = b"an invalid message".to_vec();
        let mut codec = test_codec();
        let decoded = codec.decode(msg.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "msg": "an invalid message",
            "protocol": "RFC3164",
        }));
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn encode_empty_rfc5424() -> Result<()> {
        let mut codec = test_codec();
        let msg = literal!({
            "severity": "notice",
            "facility": "local4",
            "msg": "test message",
            "protocol": "RFC5424",
            "protocol_version": 1,
            "timestamp": 0_u64
        });
        let mut encoded = codec.encode(&msg)?;
        let expected = "<165>1 1970-01-01T00:00:00+00:00 - - - - - test message";
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), expected);
        let decoded = codec.decode(&mut encoded, 0)?.unwrap();
        assert_eq!(msg, decoded);

        Ok(())
    }

    #[test]
    fn encode_empty_rfc3164() -> Result<()> {
        let codec = test_codec();
        let msg = Value::from(simd_json::json!({
            "severity": "notice",
            "facility": "local4",
            "msg": "test message",
            "protocol": "RFC3164"
        }));
        let encoded = codec.encode(&msg)?;
        let expected = "<165>Jan  1 00:00:00 - : test message";
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), expected);
        Ok(())
    }

    #[test]
    fn test_incorrect_sd() -> Result<()> {
        let mut msg =
            b"<13>1 2021-03-18T20:30:00.123Z 74794bfb6795 root 8449 - [incorrect x] message"
                .to_vec();
        let mut codec = test_codec();
        let decoded = codec.decode(msg.as_mut_slice(), 0)?.unwrap();
        let expected = literal!({
            "hostname": "74794bfb6795",
            "severity": "notice",
            "facility": "user",
            "appname": "root",
            "msg": "message",
            "procid": "8449",
            "protocol": "RFC5424",
            "protocol_version": 1,
            "timestamp": 1_616_099_400_123_000_000_u64
        });
        assert_eq!(
            tremor_script::utils::sorted_serialize(&expected)?,
            tremor_script::utils::sorted_serialize(&decoded)?
        );
        Ok(())
    }

    #[test]
    fn test_invalid_sd_3164() -> Result<()> {
        let mut s: Vec<u8> = r#"<46>Jan  5 15:33:03 plertrood-ThinkPad-X220 rsyslogd:  [software="rsyslogd" swVersion="8.32.0"] message"#.as_bytes().to_vec();
        let mut codec = test_codec();
        let decoded = codec.decode(s.as_mut_slice(), 0)?.unwrap();
        let expected = Value::from(simd_json::json!({
            "hostname": "plertrood-ThinkPad-X220",
            "severity": "info",
            "facility": "syslog",
            "appname": "rsyslogd",
            "msg": "[software=\"rsyslogd\" swVersion=\"8.32.0\"] message",
            "protocol": "RFC3164",
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
        let codec = test_codec();
        assert_eq!(
            codec.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: facility missing"
        );

        o.insert("facility", "cron").unwrap();
        assert_eq!(
            codec.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: severity missing"
        );

        o.insert("severity", "info").unwrap();
        o.insert("structured_data", "sd").unwrap();
        assert_eq!(
            codec.encode(&o).err().unwrap().to_string(),
            "Invalid Syslog Protocol data: Invalid structured data: structured data not an object"
        );
    }

    #[test_case(r#"<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8"#, None => Ok(()); "Example 1")]
    // error during encoding step, as important data is missing
    #[test_case(r#"Use the BFG!"#, Some(r#"<13>Jan  1 00:00:00 - : Use the BFG!"#) => Ok(()); "Example 2")]
    //
    #[test_case(r#"<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport:Conveyer1=OK, Conveyer2=OK # %%"#, Some(r#"<165>Aug 24 05:34:00 CST 1987: mymachine myproc[10]: %% It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport:Conveyer1=OK, Conveyer2=OK # %%"#) => Ok(()); "Example 3")]
    #[test_case(r#"<0>1990 Oct 22 10:52:01 TZ-6 scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!"#, Some(r#"<13>Jan  1 00:00:00 - : <0>1990 Oct 22 10:52:01 TZ-6 scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!"#) => Ok(()); "Example 4")]
    fn rfc3164_examples(sample: &'static str, expected: Option<&'static str>) -> Result<()> {
        let mut codec = test_codec();
        let mut vec = sample.as_bytes().to_vec();
        let decoded = codec.decode(&mut vec, 0)?.unwrap();
        let a = codec.encode(&decoded)?;
        if let Some(expected) = expected {
            // compare against expected output
            assert_eq!(expected, std::str::from_utf8(&a)?);
        } else {
            // compare against sample
            assert_eq!(sample, std::str::from_utf8(&a)?);
        }
        Ok(())
    }

    // date formatting is a bit different
    #[test_case("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - \u{FEFF}'su root' failed for lonvick on /dev/pts/8",
           Some("<34>1 2003-10-11T22:14:15.003+00:00 mymachine.example.com su - ID47 - \u{feff}'su root' failed for lonvick on /dev/pts/8") => Ok(()); "Example 1")]
    // we always parse to UTC date - so encoding a decoded msg will always give UTC
    #[test_case(r#"<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts."#,
           Some(r#"<165>1 2003-08-24T12:14:15.000003+00:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts."#) => Ok(()); "Example 2")]
    #[test_case("<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] \u{FEFF}An application event log entry...",
           Some("<165>1 2003-10-11T22:14:15.003+00:00 mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] \u{feff}An application event log entry...") => Ok(()); "Example 3")]
    #[test_case(r#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]"#,
           Some(r#"<165>1 2003-10-11T22:14:15.003+00:00 mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]"#) => Ok(()); "Example 4")]
    fn rfc5424_examples(sample: &'static str, expected: Option<&'static str>) -> Result<()> {
        let mut codec = test_codec();
        let mut vec = sample.as_bytes().to_vec();
        let decoded = codec.decode(&mut vec, 0)?.unwrap();
        let a = codec.encode(&decoded)?;
        if let Some(expected) = expected {
            // compare against expected output
            assert_eq!(expected, std::str::from_utf8(&a)?);
        } else {
            // compare against sample
            assert_eq!(sample, std::str::from_utf8(&a)?);
        }
        Ok(())
    }
}
