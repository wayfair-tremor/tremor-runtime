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

use crate::connectors::pb;
use crate::errors::Result;
use simd_json::Builder;
use tremor_otelapis::opentelemetry::proto::common::v1::{
    any_value, AnyValue, ArrayValue, InstrumentationLibrary, KeyValue, KeyValueList, StringKeyValue,
};
use tremor_value::StaticNode;

use tremor_value::{literal, Value};
use value_trait::ValueAccess;
pub(crate) const EMPTY: Vec<Value> = Vec::new();

pub(crate) fn any_value_to_json(pb: AnyValue) -> Value<'static> {
    use any_value::Value as Inner;
    let v: Value = match pb.value {
        Some(Inner::StringValue(v)) => v.into(),
        Some(Inner::BoolValue(v)) => v.into(),
        Some(Inner::IntValue(v)) => v.into(),
        Some(Inner::DoubleValue(v)) => v.into(),
        Some(Inner::ArrayValue(v)) => v.values.into_iter().map(any_value_to_json).collect(),
        Some(Inner::KvlistValue(v)) => {
            // let mut record = HashMap::with_capacity(v.values.len());
            v.values
                .into_iter()
                .map(|e| {
                    (
                        e.key,
                        e.value.map(any_value_to_json).unwrap_or_default(), // TODO check conformance - not sure this is correct at all
                    )
                })
                .collect()

            // Value::from(record)
        }
        None => Value::null(),
    };
    v
}

pub(crate) fn any_value_to_pb(data: &Value<'_>) -> AnyValue {
    use any_value::Value as PbAnyValue;
    match data {
        Value::Static(StaticNode::Null) => AnyValue { value: None },
        Value::Static(StaticNode::Bool(v)) => AnyValue {
            value: Some(PbAnyValue::BoolValue(*v)),
        },
        Value::Static(StaticNode::I64(v)) => AnyValue {
            value: Some(PbAnyValue::IntValue(*v)),
        },
        Value::Static(StaticNode::U64(v)) => {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
            let v = *v as i64;

            AnyValue {
                value: Some(PbAnyValue::IntValue(v)),
            }
        }
        Value::Static(StaticNode::F64(v)) => AnyValue {
            value: Some(PbAnyValue::DoubleValue(*v)),
        },
        Value::String(v) => AnyValue {
            value: Some(PbAnyValue::StringValue(v.to_string())),
        },
        Value::Array(va) => {
            let a: Vec<AnyValue> = va.iter().map(|v| any_value_to_pb(v)).collect();
            let x = PbAnyValue::ArrayValue(ArrayValue { values: a });
            AnyValue { value: Some(x) }
        }
        Value::Object(vo) => {
            let mut kvl = Vec::with_capacity(vo.len());
            for (k, v) in vo.iter() {
                kvl.push(KeyValue {
                    key: k.to_string(),
                    value: Some(any_value_to_pb(v)),
                });
            }
            let pb = KeyValueList { values: kvl };
            AnyValue {
                value: Some(PbAnyValue::KvlistValue(pb)),
            }
        }
        Value::Bytes(b) => {
            // TODO find a better binary mapping - the below is clearly not right!
            let b: Vec<AnyValue> = b
                .iter()
                .map(|b| AnyValue {
                    value: Some(PbAnyValue::IntValue(i64::from(*b))),
                })
                .collect();
            let x = PbAnyValue::ArrayValue(ArrayValue { values: b });
            AnyValue { value: Some(x) }
        }
    }
}

pub(crate) fn maybe_any_value_to_json(pb: Option<AnyValue>) -> Option<Value<'static>> {
    pb.map(any_value_to_json)
}

pub(crate) fn string_key_value_to_json(pb: Vec<StringKeyValue>) -> Value<'static> {
    pb.into_iter()
        .map(|StringKeyValue { key, value }| (key, value))
        .collect()
}

pub(crate) fn string_key_value_to_pb(data: Option<&Value<'_>>) -> Result<Vec<StringKeyValue>> {
    data.as_object()
        .ok_or("Unable to map json to Vec<StringKeyValue> pb")?
        .iter()
        .map(|(key, value)| {
            let key = key.to_string();
            let value = pb::maybe_string_to_pb(Some(value))?;
            Ok(StringKeyValue { key, value })
        })
        .collect()
}

pub(crate) fn key_value_list_to_json(pb: Vec<KeyValue>) -> Value<'static> {
    pb.into_iter()
        .map(|KeyValue { key, value }| (key, maybe_any_value_to_json(value)))
        .collect()
}

pub(crate) fn maybe_key_value_list_to_pb(data: Option<&Value<'_>>) -> Result<Vec<KeyValue>> {
    data.as_object()
        .ok_or("Expected a json object, found otherwise - cannot map to pb")?
        .iter()
        .map(|(key, value)| {
            let key = key.to_string();
            let value = Some(any_value_to_pb(value));
            Ok(KeyValue { key, value })
        })
        .collect()
}

pub(crate) fn maybe_instrumentation_library_to_json(il: InstrumentationLibrary) -> Value<'static> {
    literal!({
        "name": il.name,
        "version": il.version,
    })
}

pub(crate) fn instrumentation_library_to_pb(data: &Value<'_>) -> Result<InstrumentationLibrary> {
    Ok(InstrumentationLibrary {
        name: pb::maybe_string_to_pb((*data).get("name"))?,
        version: pb::maybe_string_to_pb((*data).get("version"))?,
    })
}

#[cfg(test)]
mod tests {
    use simd_json::prelude::*;
    use tremor_otelapis::opentelemetry::proto::common::v1::{ArrayValue, KeyValueList};

    use super::*;

    #[test]
    fn any_value_none() -> Result<()> {
        let pb = AnyValue { value: None };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert!(json.is_null());
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_string() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, "snot");
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::StringValue("".into())),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, "");
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_bool() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, true);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(false)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, false);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_int() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(0i64)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, 0);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MAX)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::i64::MAX);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MIN)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::i64::MIN);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_double() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(0f64)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, 0f64);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MAX)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::f64::MAX);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MIN)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::f64::MIN);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_list() -> Result<()> {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![snot],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!(["snot"]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue { values: vec![] })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn any_value_record() -> Result<()> {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "badger".into(),
                    value: Some(snot),
                }],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({"badger": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn key_value_list() -> Result<()> {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = vec![KeyValue {
            key: "snot".into(),
            value: Some(snot),
        }];
        let json = key_value_list_to_json(pb.clone());
        let back_again = maybe_key_value_list_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn string_key_value_list() -> Result<()> {
        let pb = vec![StringKeyValue {
            key: "snot".into(),
            value: "badger".into(),
        }];
        let json = string_key_value_to_json(pb.clone());
        let back_again = string_key_value_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "badger"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn instrumentation_library() -> Result<()> {
        let pb = InstrumentationLibrary {
            name: "name".into(),
            version: "v0.1.2".into(),
        };
        let json = maybe_instrumentation_library_to_json(pb.clone());
        let back_again = instrumentation_library_to_pb(&json)?;
        let expected: Value = literal!({"name": "name", "version": "v0.1.2"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }
}
