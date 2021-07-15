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

use crate::connectors::qos;
use crate::errors::Result;
use crate::sink::prelude::*;
use simd_json_derive::Serialize;
pub struct Cb {}

///
/// CB event provoking offramp for testing sources and operators for their handling of CB events
///
/// Put a single string or an array of strings under the `cb` key into the meta or value to trigger the corresponding events.
///
/// Examples: `{"cb": "ack"}` or `{"cb": ["fail", "close"]}`
///
pub(crate) struct Builder {}
impl offramp::Builder for Builder {
    fn from_config(&self, _config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        Ok(SinkManager::new_box(Cb {}))
    }
}

#[async_trait::async_trait]
impl Sink for Cb {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        let mut res = Vec::with_capacity(event.len());
        for (value, meta) in event.value_meta_iter() {
            //            let event = event.clone();
            debug!(
                "[Sink::CB] {} {}",
                &event.id.event_id(),
                value.json_string()?
            );
            if let Some(cb) = meta.get("cb").or_else(|| value.get("cb")) {
                let cb_cmds = if let Some(array) = cb.as_array() {
                    array
                        .iter()
                        .filter_map(|v| v.as_str().map(ToString::to_string))
                        .collect()
                } else if let Some(str) = cb.as_str() {
                    vec![str.to_string()]
                } else {
                    vec![]
                };

                // Acknowledgement tracking
                let mut insight = event.clone();
                if cb_cmds.contains(&"ack".to_string()) {
                    res.push(qos::ack(&mut insight));
                } else if cb_cmds.contains(&"fail".to_string()) {
                    res.push(qos::fail(&mut insight));
                }

                // Circuit breaker tracking
                let mut insight = event.clone();
                if cb_cmds.contains(&"close".to_string())
                    || cb_cmds.contains(&"trigger".to_string())
                {
                    res.push(qos::close(&mut insight));
                } else if cb_cmds.contains(&"open".to_string())
                    || cb_cmds.contains(&"restore".to_string())
                {
                    res.push(qos::open(&mut insight));
                }
            }
        }
        Ok(Some(res))
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        // TODO: add signal reaction via config
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        _sink_uid: u64,
        _sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &halfbrown::HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        _is_linked: bool,
        _reply_channel: Sender<Reply>,
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(not(tarpaulin_include))]
    fn is_active(&self) -> bool {
        true
    }

    #[cfg(not(tarpaulin_include))]
    fn auto_ack(&self) -> bool {
        false
    }

    #[cfg(not(tarpaulin_include))]
    fn default_codec(&self) -> &str {
        "json"
    }

    async fn terminate(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::BorrowMut;
    use tremor_pipeline::{EventId, OpMeta};

    #[async_std::test]
    async fn cb_meta() -> Result<()> {
        let mut codec = crate::codec::lookup("json")?;
        let mut cb = Cb {};
        let url = TremorUrl::parse("/offramp/cb/instance")?;
        let codec_map = halfbrown::HashMap::new();
        let (tx, _rx) = async_channel::bounded(1);
        let in_ = "IN";
        cb.init(
            0x00,
            &url,
            codec.as_ref(),
            &codec_map,
            Processors::default(),
            false,
            tx,
        )
        .await?;
        let mut data = Value::object_with_capacity(1);
        data.insert("cb", "ack")?;
        let id = EventId::new(1, 2, 3);
        let origin_uri = Some(EventOriginUri {
            uid: 1,
            scheme: "test".to_string(),
            host: "localhost".to_string(),
            port: Some(1),
            path: vec![],
        });
        let mut op_meta = OpMeta::default();
        op_meta.insert(1, "foo");

        let event = Event {
            id: id.clone(),
            data: (data, Value::null()).into(),
            op_meta: op_meta.clone(),
            origin_uri: origin_uri.clone(),
            ..Event::default()
        };

        let c: &mut dyn Codec = codec.borrow_mut();
        let res = cb.on_event(in_, c, &codec_map, event).await?;

        assert!(res.is_some(), "got nothing back");
        if let Some(replies) = res {
            assert_eq!(1, replies.len());
            if let Some(Reply::Insight(insight)) = replies.first() {
                assert_eq!(CbAction::Ack, insight.cb);
                assert_eq!(id, insight.id);
                assert_eq!(op_meta, insight.op_meta);
                assert_eq!(origin_uri, insight.origin_uri);
            } else {
                assert!(
                    false,
                    "expected to get anm insight back. Got {:?}",
                    replies.first()
                );
            }
        }

        // meta takes precedence
        let mut meta = Value::object_with_capacity(1);
        meta.insert("cb", "fail")?;
        let mut data = Value::object_with_capacity(1);
        data.insert("cb", "ack")?;
        let event = Event {
            id: id.clone(),
            data: (data, meta).into(),
            op_meta: op_meta.clone(),
            origin_uri: origin_uri.clone(),
            ..Event::default()
        };
        let c: &mut dyn Codec = codec.borrow_mut();
        let res = cb.on_event(in_, c, &codec_map, event).await?;
        assert!(res.is_some(), "got nothing back");
        if let Some(replies) = res {
            assert_eq!(1, replies.len());
            if let Some(Reply::Insight(insight)) = replies.first() {
                assert_eq!(CbAction::Fail, insight.cb);
                assert_eq!(id, insight.id);
                assert_eq!(op_meta, insight.op_meta);
                assert_eq!(origin_uri, insight.origin_uri);
            } else {
                assert!(
                    false,
                    "expected to get anm insight back. Got {:?}",
                    replies.first()
                );
            }
        }

        // array data - second ack/fail will be ignored, just one from ack/fail or open/close (trigger/restore) is returned
        let meta = literal!(
        {"cb": ["ack", "open", "fail"]}
        );
        let event = Event {
            id: id.clone(),
            data: (Value::null(), meta).into(),
            op_meta: op_meta.clone(),
            origin_uri: origin_uri.clone(),
            ..Event::default()
        };

        let c: &mut dyn Codec = codec.borrow_mut();
        let res = cb.on_event(in_, c, &codec_map, event).await?;
        assert!(res.is_some(), "got nothing back");
        if let Some(replies) = res {
            assert_eq!(2, replies.len());
            match replies.as_slice() {
                [Reply::Insight(insight1), Reply::Insight(insight2)] => {
                    assert_eq!(CbAction::Ack, insight1.cb);
                    assert_eq!(id, insight1.id);
                    assert_eq!(op_meta, insight1.op_meta);
                    assert_eq!(origin_uri, insight1.origin_uri);

                    assert_eq!(CbAction::Open, insight2.cb);
                    assert_eq!(op_meta, insight2.op_meta);
                    assert_eq!(origin_uri, insight2.origin_uri);
                }
                _ => assert!(
                    false,
                    "expected to get two insights back. Got {:?}",
                    replies
                ),
            }
        }

        // array data - second ack/fail will be ignored, just one from ack/fail or open/close (trigger/restore) is returned
        let data = literal!({
            "cb": ["trigger", "fail"]
        });
        let event = Event {
            id: id.clone(),
            data: (data, Value::null()).into(),
            op_meta: op_meta.clone(),
            origin_uri: origin_uri.clone(),
            ..Event::default()
        };
        let c: &mut dyn Codec = codec.borrow_mut();
        let res = cb.on_event(in_, c, &codec_map, event).await?;
        assert!(res.is_some(), "got nothing back");
        if let Some(replies) = res {
            assert_eq!(2, replies.len());
            match replies.as_slice() {
                [Reply::Insight(insight1), Reply::Insight(insight2)] => {
                    assert_eq!(CbAction::Fail, insight1.cb);
                    assert_eq!(id, insight1.id);
                    assert_eq!(op_meta, insight1.op_meta);
                    assert_eq!(origin_uri, insight1.origin_uri);

                    assert_eq!(CbAction::Close, insight2.cb);
                    assert_eq!(op_meta, insight2.op_meta);
                    assert_eq!(origin_uri, insight2.origin_uri);
                }
                _ => assert!(
                    false,
                    "expected to get two insights back. Got {:?}",
                    replies
                ),
            }
        }

        cb.terminate().await;
        Ok(())
    }
}
