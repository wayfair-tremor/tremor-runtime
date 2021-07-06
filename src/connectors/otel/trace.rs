// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache trace_id: (), span_id: (), trace_state: () License, Version 2.0 (the "License");
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

use super::super::pb::{
    maybe_int_to_pbi32, maybe_int_to_pbu32, maybe_int_to_pbu64, maybe_string_to_pb,
};
use super::{
    common::{self, EMPTY},
    id,
    resource::{self, resource_to_pb},
};
use crate::errors::Result;
use simd_json::Mutable;
use tremor_value::literal;

use tremor_otelapis::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{
        span::{Event, Link},
        InstrumentationLibrarySpans, ResourceSpans, Span, Status,
    },
};

use tremor_value::Value;
use value_trait::ValueAccess;

#[allow(deprecated)]
pub(crate) fn status_to_json<'event>(data: Option<Status>) -> Value<'event> {
    data.map_or_else(
        || literal!({ "code": 0, "deprecated_code": 0, "message": "status code unset" }),
        |data| {
            literal!({
                "code": data.code,
                "deprecated_code": data.deprecated_code,
                "message": data.message
            })
        },
    )
}

pub(crate) fn span_events_to_json(pb: Vec<Event>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            literal!({
                "time_unix_nano" : data.time_unix_nano,
                "name" : data.name.to_string(),
                "attributes" : common::key_value_list_to_json(data.attributes),
                "dropped_attributes_count" : data.dropped_attributes_count
            })
        })
        .collect()
}

pub(crate) fn span_events_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Event>> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Event {
                name: maybe_string_to_pb(json.get("name"))?,
                time_unix_nano: maybe_int_to_pbu64(json.get("time_unix_nano"))?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))?,
                dropped_attributes_count: maybe_int_to_pbu32(json.get("dropped_attributes_count"))?,
            })
        })
        .collect()
}

pub(crate) fn span_links_to_json(pb: Vec<Link>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            literal!({
                "trace_id": id::hex_trace_id_to_json(&data.trace_id),
                "span_id": id::hex_span_id_to_json(&data.span_id),
                "trace_state": data.trace_state,
                "attributes": common::key_value_list_to_json(data.attributes),
                "dropped_attributes_count" : data.dropped_attributes_count
            })
        })
        .collect()
}

pub(crate) fn span_links_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Link>> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Link {
                span_id: id::hex_span_id_to_pb(json.get("span_id"))?,
                trace_id: id::hex_trace_id_to_pb(json.get("trace_id"))?,
                trace_state: maybe_string_to_pb(json.get("trace_state"))?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))?,
                dropped_attributes_count: maybe_int_to_pbu32(json.get("dropped_attributes_count"))?,
            })
        })
        .collect()
}

pub(crate) fn status_to_pb(json: Option<&Value<'_>>) -> Result<Option<Status>> {
    if json.is_none() {
        return Ok(None);
    }
    let json = json
        .as_object()
        .ok_or("Unable to map json value to pb trace status")?;

    // This is generated code in the pb stub code deriving from otel proto files
    #[allow(deprecated)]
    Ok(Some(Status {
        code: maybe_int_to_pbi32(json.get("code"))?,
        deprecated_code: maybe_int_to_pbi32(json.get("deprecated_code"))?,
        message: maybe_string_to_pb(json.get("message"))?,
    }))
}

fn span_to_json(span: Span) -> Value<'static> {
    literal!({
        "attributes": common::key_value_list_to_json(span.attributes),
        "events": span_events_to_json(span.events),
        "links": span_links_to_json(span.links),
        "span_id": id::hex_span_id_to_json(&span.span_id),
        "parent_span_id": id::hex_span_id_to_json(&span.parent_span_id),
        "trace_id": id::hex_trace_id_to_json(&span.trace_id),
        "start_time_unix_nano": span.start_time_unix_nano,
        "end_time_unix_nano": span.end_time_unix_nano,
        "trace_state": span.trace_state,
        "dropped_attributes_count": span.dropped_attributes_count,
        "dropped_events_count": span.dropped_events_count,
        "dropped_links_count": span.dropped_links_count,
        "status": status_to_json(span.status),
        "kind": span.kind,
        "name": span.name
    })
}

pub(crate) fn instrumentation_library_spans_to_json(
    data: Vec<InstrumentationLibrarySpans>,
) -> Value<'static> {
    let mut json: Vec<Value> = Vec::with_capacity(data.len());
    for data in data {
        let spans: Value = data.spans.into_iter().map(span_to_json).collect();

        let mut e = literal!({ "spans": spans });
        if let Some(il) = data.instrumentation_library {
            let il = common::maybe_instrumentation_library_to_json(il);
            e.try_insert("instrumentation_library", il);
        }
        json.push(e);
    }

    Value::from(json)
}

pub(crate) fn instrumentation_library_spans_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibrarySpans>> {
    data.as_array()
        .ok_or("Invalid json mapping for InstrumentationLibrarySpans")?
        .iter()
        .filter_map(Value::as_object)
        .map(|data| {
            let spans = data
                .get("spans")
                .and_then(Value::as_array)
                .unwrap_or(&EMPTY)
                .iter()
                .map(|span| {
                    Ok(Span {
                        name: maybe_string_to_pb(span.get("name"))?,
                        start_time_unix_nano: maybe_int_to_pbu64(span.get("start_time_unix_nano"))?,
                        end_time_unix_nano: maybe_int_to_pbu64(span.get("end_time_unix_nano"))?,
                        status: status_to_pb(span.get("status"))?,
                        kind: maybe_int_to_pbi32(span.get("kind"))?,
                        parent_span_id: id::hex_parent_span_id_to_pb(span.get("parent_span_id"))?,
                        span_id: id::hex_span_id_to_pb(span.get("span_id"))?,
                        trace_id: id::hex_trace_id_to_pb(span.get("trace_id"))?,
                        trace_state: maybe_string_to_pb(span.get("trace_state"))?,
                        attributes: common::maybe_key_value_list_to_pb(span.get("attributes"))?,
                        dropped_attributes_count: maybe_int_to_pbu32(
                            span.get("dropped_attributes_count"),
                        )?,
                        dropped_events_count: maybe_int_to_pbu32(span.get("dropped_events_count"))?,
                        dropped_links_count: maybe_int_to_pbu32(span.get("dropped_links_count"))?,
                        events: span_events_to_pb(span.get("events"))?,
                        links: span_links_to_pb(span.get("links"))?,
                    })
                })
                .collect::<Result<_>>()?;

            Ok(InstrumentationLibrarySpans {
                instrumentation_library: data
                    .get("instrumentation_library")
                    .map(common::instrumentation_library_to_pb)
                    .transpose()?,
                spans,
            })
        })
        .collect()
}

pub(crate) fn resource_spans_to_json(request: ExportTraceServiceRequest) -> Value<'static> {
    let json: Value = request
        .resource_spans
        .into_iter()
        .map(|span| {
            let ill = instrumentation_library_spans_to_json(span.instrumentation_library_spans);
            let mut base = literal!({ "instrumentation_library_spans": ill });
            if let Some(r) = span.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            base
        })
        .collect();

    literal!({ "trace": json })
}

pub(crate) fn resource_spans_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ResourceSpans>> {
    json.get_array("trace")
        .ok_or("Invalid json mapping for otel trace message - cannot convert to pb")?
        .iter()
        .filter_map(Value::as_object)
        .map(|json| {
            Ok(ResourceSpans {
                instrumentation_library_spans: instrumentation_library_spans_to_pb(
                    json.get("instrumentation_library_spans"),
                )?,
                resource: json.get("resource").map(resource_to_pb).transpose()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::InstrumentationLibrary, resource::v1::Resource,
    };

    use super::*;

    #[test]
    #[allow(deprecated)]
    fn status() -> Result<()> {
        let pb = Status {
            deprecated_code: 0,
            message: "everything is snot".into(),
            code: 1,
        };
        let json = status_to_json(Some(pb.clone()));
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value =
            literal!({"deprecated_code": 0, "message": "everything is snot", "code": 1});

        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);

        // None
        let json = status_to_json(None);
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value =
            literal!({"deprecated_code": 0, "message": "status code unset", "code": 0});
        let pb = Status {
            deprecated_code: 0,
            message: "status code unset".into(),
            code: 0,
        };
        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);

        // Invalid
        let invalid = status_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

        Ok(())
    }

    #[test]
    fn span_event() -> Result<()> {
        let pb = vec![Event {
            time_unix_nano: 0,
            name: "badger".into(),
            attributes: vec![],
            dropped_attributes_count: 44,
        }];
        let json = span_events_to_json(pb.clone());
        let back_again = span_events_to_pb(Some(&json))?;
        let expected: Value = literal!([
            {
                "time_unix_nano": 0,
                "name": "badger",
                "attributes": {},
                "dropped_attributes_count": 44,
            }
        ]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty span events
        let pb: Vec<Event> = vec![];
        let json = span_events_to_json(vec![]);
        let back_again = span_events_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn span_link() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![Link {
            attributes: vec![],
            dropped_attributes_count: 42,
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            trace_state: "snot:badger".into(),
        }];
        let json = span_links_to_json(pb.clone());
        let back_again = span_links_to_pb(Some(&json))?;
        let expected: Value = literal!([
            {
                "span_id": span_id_json,
                "trace_id": trace_id_json,
                "trace_state": "snot:badger",
                "attributes": {},
                "dropped_attributes_count": 42,
            }
        ]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        // Empty span events
        let json = span_links_to_json(vec![]);
        let back_again = span_links_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn instrument_library_spans() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::test::json_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![InstrumentationLibrarySpans {
            instrumentation_library: Some(InstrumentationLibrary {
                name: "name".into(),
                version: "v0.1.2".into(),
            }), // TODO For now its an error for this to be None - may need to revisit
            spans: vec![Span {
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                name: "test".into(),
                attributes: vec![],
                dropped_attributes_count: 100,
                trace_state: "snot:badger".into(),
                parent_span_id: parent_span_id_pb.clone(),
                span_id: span_id_pb.clone(),
                trace_id: trace_id_pb,
                kind: 0,
                status: Some(Status {
                    code: 0,
                    deprecated_code: 0,
                    message: "woot".into(),
                }),
                events: vec![],
                dropped_events_count: 11,
                links: vec![],
                dropped_links_count: 13,
            }],
        }];
        let json = instrumentation_library_spans_to_json(pb.clone());
        let back_again = instrumentation_library_spans_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "spans": [{
                  "start_time_unix_nano": 0,
                  "end_time_unix_nano": 0,
                  "name": "test",
                  "dropped_attributes_count": 100,
                  "attributes": {},
                  "trace_state": "snot:badger",
                  "parent_span_id": parent_span_id_json,
                  "span_id": span_id_json,
                  "trace_id": trace_id_json,
                  "kind": 0,
                  "status": {
                      "code": 0,
                      "deprecated_code": 0,
                      "message": "woot"
                  },
                  "events": [],
                  "links": [],
                  "dropped_events_count": 11,
                  "dropped_links_count": 13,
                }
            ]
        }]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let invalid = instrumentation_library_spans_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

        Ok(())
    }

    #[test]
    fn resource_spans() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::test::json_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        #[allow(deprecated)]
        let pb = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_spans: vec![InstrumentationLibrarySpans {
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    spans: vec![Span {
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 0,
                        name: "test".into(),
                        attributes: vec![],
                        dropped_attributes_count: 100,
                        trace_state: "snot:badger".into(),
                        parent_span_id: parent_span_id_pb.clone(),
                        span_id: span_id_pb.clone(),
                        trace_id: trace_id_pb,
                        kind: 0,
                        status: Some(Status {
                            code: 0,
                            deprecated_code: 0,
                            message: "woot".into(),
                        }),
                        events: vec![],
                        dropped_events_count: 11,
                        links: vec![],
                        dropped_links_count: 13,
                    }],
                }],
            }],
        };
        let json = resource_spans_to_json(pb.clone());
        let back_again = resource_spans_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "trace": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "instrumentation_library_spans": [{
                        "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                        "spans": [{
                            "start_time_unix_nano": 0,
                            "end_time_unix_nano": 0,
                            "name": "test",
                            "dropped_attributes_count": 100,
                            "attributes": {},
                            "trace_state": "snot:badger",
                            "parent_span_id": parent_span_id_json,
                            "span_id": span_id_json,
                            "trace_id": trace_id_json,
                            "kind": 0,
                            "status": {
                                "code": 0,
                                "deprecated_code": 0,
                                "message": "woot"
                            },
                            "events": [],
                            "links": [],
                            "dropped_events_count": 11,
                            "dropped_links_count": 13,
                            }
                        ]
                    }]
                }
            ]
        });

        assert_eq!(expected, json);
        assert_eq!(pb.resource_spans, back_again);

        let invalid = resource_spans_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

        Ok(())
    }
}
