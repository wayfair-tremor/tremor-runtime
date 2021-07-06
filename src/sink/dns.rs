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
use async_std_resolver::{
    lookup::Lookup,
    proto::{
        rr::{RData, RecordType},
        xfer::DnsRequestOptions,
    },
    resolver_from_system_conf, AsyncStdResolver,
};
use halfbrown::HashMap;
use std::boxed::Box;
use tremor_value::literal;

pub struct Dns {
    // sink_url: TremorUrl,
    event_origin_uri: EventOriginUri,
    resolver: Option<AsyncStdResolver>,
    // reply: Option<Sender<Reply>>,
}

impl offramp::Impl for Dns {
    fn from_config(_config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        let event_origin_uri = EventOriginUri {
            uid: 0,
            scheme: "tremor-dns".to_string(),
            host: "localhost".to_string(),
            port: None,
            path: Vec::new(),
        };

        Ok(SinkManager::new_box(Dns {
            event_origin_uri,
            resolver: None,
        }))
    }
}

fn str_to_record_type(s: &str) -> Result<RecordType> {
    match s {
        "A" => Ok(RecordType::A),
        "AAAA" => Ok(RecordType::AAAA),
        "ANAME" => Ok(RecordType::ANAME),
        "ANY" => Ok(RecordType::ANY),
        "AXFR" => Ok(RecordType::AXFR),
        "CAA" => Ok(RecordType::CAA),
        "CNAME" => Ok(RecordType::CNAME),
        "HINFO" => Ok(RecordType::HINFO),
        "HTTPS" => Ok(RecordType::HTTPS),
        "IXFR" => Ok(RecordType::IXFR),
        "MX" => Ok(RecordType::MX),
        "NAPTR" => Ok(RecordType::NAPTR),
        "NS" => Ok(RecordType::NS),
        "NULL" => Ok(RecordType::NULL),
        "OPENPGPKEY" => Ok(RecordType::OPENPGPKEY),
        "OPT" => Ok(RecordType::OPT),
        "PTR" => Ok(RecordType::PTR),
        "SOA" => Ok(RecordType::SOA),
        "SRV" => Ok(RecordType::SRV),
        "SSHFP" => Ok(RecordType::SSHFP),
        "SVCB" => Ok(RecordType::SVCB),
        "TLSA" => Ok(RecordType::TLSA),
        "TXT" => Ok(RecordType::TXT),
        "ZERO" => Ok(RecordType::ZERO),
        other => Err(format!("Invalid or unsupported record type: {}", other).into()),
    }
}

fn rdata_to_value(r: &RData) -> Option<Value<'static>> {
    Some(match r {
        RData::A(v) => literal!({ "A": v.to_string() }),
        RData::AAAA(v) => literal!({ "AAAA": v.to_string() }),
        RData::ANAME(v) => literal!({ "ANAME": v.to_string() }),
        RData::CNAME(v) => literal!({ "CNAME": v.to_string() }),
        RData::TXT(v) => literal!({ "TXT": v.to_string() }),
        RData::PTR(v) => literal!({ "PTR": v.to_string() }),
        RData::CAA(v) => literal!({ "CAA": v.to_string() }),
        RData::HINFO(v) => literal!({ "HINFO": v.to_string() }),
        RData::HTTPS(v) => literal!({ "HTTPS": v.to_string() }),
        RData::MX(v) => literal!({ "MX": v.to_string() }),
        RData::NAPTR(v) => literal!({ "NAPTR": v.to_string() }),
        RData::NULL(v) => literal!({ "NULL": v.to_string() }),
        RData::NS(v) => literal!({ "NS": v.to_string() }),
        RData::OPENPGPKEY(v) => literal!({ "OPENPGPKEY": v.to_string() }),
        RData::SOA(v) => literal!({ "SOA": v.to_string() }),
        RData::SRV(v) => literal!({ "SRV": v.to_string() }),
        RData::SSHFP(v) => literal!({ "SSHFP": v.to_string() }),
        RData::SVCB(v) => literal!({ "SVCB": v.to_string() }),
        RData::TLSA(v) => literal!({ "TLSA": v.to_string() }),
        RData::OPT(_) | RData::Unknown { .. } | RData::ZERO => return None,
    })
}
fn lookup_to_value(l: &Lookup) -> Value<'static> {
    l.record_iter()
        .filter_map(|r| {
            let mut v = rdata_to_value(r.rdata())?;
            v.try_insert("ttl", r.ttl());
            Some(v)
        })
        .collect()
}

impl Dns {
    async fn query<'event>(
        &self,
        e: &Value<'event>,
        correlation: Option<&Value<'event>>,
    ) -> Result<Event> {
        let resolver = self.resolver.as_ref().ok_or("No resolver set")?;
        let lookup = e.get("lookup").ok_or("Invalid DNS request")?;
        let name = lookup
            .as_str()
            .or_else(|| lookup.get_str("name"))
            .ok_or("Invaliud DNS request")?;

        let data = if let Some(record_type) =
            lookup.get_str("type").map(str_to_record_type).transpose()?
        {
            // type lookup
            lookup_to_value(
                &resolver
                    .lookup(name, record_type, DnsRequestOptions::default())
                    .await?,
            )
        } else {
            // generic lookup
            lookup_to_value(resolver.lookup_ip(name).await?.as_lookup())
        };
        let meta = correlation
            .map(|c| literal!({ "correlation": c.clone_static() }))
            .unwrap_or_default();
        let e = Event {
            data: (data, meta).into(),
            origin_uri: Some(self.event_origin_uri.clone()),
            ..Event::default()
        };

        Ok(e)
    }
}

#[async_trait::async_trait]
impl Sink for Dns {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let mut res = Vec::with_capacity(event.len());

        for (e, m) in event.value_meta_iter() {
            match self.query(e, m.get("correlation")).await {
                Ok(out) => res.push(Reply::Response(OUT, out)),
                Err(err) => {
                    let data = literal!({
                        "event": e.clone_static(),
                        "error": format!("{}", err),
                    });
                    let meta = if let Some(c) = m.get("correlation") {
                        literal!({ "correlation": c.clone_static() })
                    } else {
                        Value::object()
                    };

                    let error_e = Event {
                        data: (data, meta).into(),
                        origin_uri: Some(self.event_origin_uri.clone()),
                        ..Event::default()
                    };

                    res.push(Reply::Response(ERR, error_e))
                }
            }
        }
        Ok(Some(res))
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }

    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<Reply>,
    ) -> Result<()> {
        // self.reply = Some(reply_channel);
        self.resolver = Some(resolver_from_system_conf().await?);
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.resolver.is_some()
    }

    fn auto_ack(&self) -> bool {
        true
    }

    fn default_codec(&self) -> &str {
        "null"
    }
}
