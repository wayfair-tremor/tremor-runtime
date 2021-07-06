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

//! # Elastic Search Offramp
//!
//! The Elastic Search Offramp uses batch writes to send data to elastic search
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Input Metadata Variables
//!   * `index` - index to write to (required)
//!   * `doc-type` - document type for the event (required)
//!   * `pipeline` - pipeline to use
//!
//! ## Outputs
//!
//! The 1st additional output is used to send divert messages that can not be
//! enqueued due to overload

#![cfg(not(tarpaulin_include))]

use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use async_std::task::JoinHandle;
use elastic::{
    client::responses::bulk::{ErrorItem, OkItem},
    prelude::*,
};
use halfbrown::HashMap;
use simd_json::json;
use std::time::Instant;
use std::{iter, str};
use tremor_pipeline::{EventId, EventIdGenerator};
use tremor_script::prelude::*;
use tremor_script::Object;
use tremor_value::literal;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// list of elasticsearch cluster nodes
    pub nodes: Vec<String>,
    /// maximum number of paralel in flight batches (default: 4)
    #[serde(default = "concurrency")]
    pub concurrency: usize,
}
fn concurrency() -> usize {
    4
}
impl ConfigImpl for Config {}

pub struct Elastic {
    sink_url: TremorUrl,
    client: SyncClient,
    queue: AsyncSink<u64>,
    insight_tx: Sender<sink::Reply>,
    is_linked: bool,
    response_sender: Sender<(EventPayload, Cow<'static, str>)>,
    response_task_handle: Option<JoinHandle<Result<()>>>,
    origin_uri: EventOriginUri,
}

impl offramp::Impl for Elastic {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let client = SyncClientBuilder::new()
                .static_nodes(config.nodes.into_iter())
                .build()?;

            let queue = AsyncSink::new(config.concurrency);
            let (tx, _rx) = bounded(1); // dummy value
            let (res_tx, _res_rx) = bounded(1); // dummy value

            Ok(SinkManager::new_box(Self {
                sink_url: TremorUrl::from_offramp_id("elastic")?, // just a dummy value, gonna be overwritten on init
                client,
                queue,
                insight_tx: tx,
                is_linked: false,
                response_sender: res_tx,
                response_task_handle: None,
                origin_uri: EventOriginUri::default(),
            }))
        } else {
            Err("Elastic offramp requires a configuration.".into())
        }
    }
}

fn build_source(id: &EventId, origin_uri: Option<&EventOriginUri>) -> Value<'static> {
    let mut source = Object::with_capacity(2);
    source.insert("event_id".into(), Value::from(id.to_string()));
    source.insert(
        "origin".into(),
        origin_uri.map_or_else(Value::null, |uri| Value::from(uri.to_string())),
    );
    Value::from(source)
}

fn build_bulk_error_data(
    item: &ErrorItem<String, String, String>,
    id: &EventId,
    payload: Value<'static>,
    origin_uri: Option<&EventOriginUri>,
    maybe_correlation: Option<Value<'static>>,
) -> Result<EventPayload> {
    let mut meta = literal!({
        "elastic": {
            "_id": item.id().to_string(),
            "_index": item.index().to_string(),
            "_type": item.ty().to_string(),
            // TODO: deprecated remove with removing top level es keys
            "id": item.id().to_string(),
            "index": item.index().to_string(),
            "doc_type": item.ty().to_string(),
        }
    });
    if let Some(correlation) = maybe_correlation {
        meta.try_insert("correlation", correlation);
    }

    let value = literal!({
        "source": build_source(id, origin_uri),
        "payload": payload,
        "error": tremor_value::to_value(item.err())?,
        "success": false
    });
    Ok((value, meta).into())
}

fn build_bulk_success_data(
    item: &OkItem<String, String, String>,
    id: &EventId,
    payload: Value<'static>,
    origin_uri: Option<&EventOriginUri>,
    maybe_correlation: Option<Value<'static>>,
) -> EventPayload {
    let mut meta = literal!({
        "elastic": {
            "_id": item.id().to_string(),
            "_index": item.index().to_string(),
            "_type": item.ty().to_string(),
            // TODO: deprecated remove with removing top level es keys
            "id": item.id().to_string(),
            "index": item.index().to_string(),
            "doc_type": item.ty().to_string(),

            "version": item.version().map_or_else(Value::null, Value::from)
        }
    });
    if let Some(correlation) = maybe_correlation {
        meta.try_insert("correlation", correlation);
    }

    let value = literal!({
        "source": build_source(id, origin_uri),
        "payload": payload,
        "success": true
    });
    (value, meta).into()
}

/// Build event payload for elasticsearch _bulk request
fn build_event_payload(event: &Event) -> Result<Vec<u8>> {
    // We estimate a single message is 512 byte on everage, might be off but it's
    // a guess
    let vec_size = 512 * event.len();
    let mut payload = Vec::with_capacity(vec_size);

    for (value, meta) in event.value_meta_iter() {
        let elastic = meta.get("elastic");
        let index = if let Some(idx) = meta.get_str("index") {
            warn!("[Sink::ES] $index is deprecated please use `$elastic._index` instead");
            idx
        } else if let Some(idx) = elastic.get_str("_index") {
            idx
        } else {
            return Err(Error::from("'index' not set for elastic offramp!"));
        };
        let mut index_meta = json!({
            "_index": index,
        });
        // _type is deprecated in ES 7.10, thus it is no longer required
        if let Some(doc_type) = meta.get_str("doc_type") {
            warn!("[Sink::ES] $doc_type is deprecated please use `$elastic._type` instead");
            index_meta.insert("_type", doc_type)?;
        } else if let Some(doc_type) = elastic.get_str("_type") {
            index_meta.insert("_type", doc_type)?;
        }
        if let Some(id) = meta.get_str("doc_id") {
            warn!("[Sink::ES] $doc_id is deprecated please use `$elastic._id` instead");
            index_meta.insert("_id", id)?;
        } else if let Some(id) = elastic.get_str("_id") {
            index_meta.insert("_id", id)?;
        }
        if let Some(pipeline) = meta.get_str("pipeline") {
            warn!("[Sink::ES] $pipeline is deprecated please use `$elastic.pipeline` instead");
            index_meta.insert("pipeline", pipeline)?;
        } else if let Some(pipeline) = elastic.get_str("pipeline") {
            index_meta.insert("pipeline", pipeline)?;
        };
        let action = if meta.get_str("action").is_some() {
            warn!("[Sink::ES] $action is deprecated please use `$elastic.action` instead");
            meta.get_str("action")
        } else {
            elastic.get_str("action")
        };
        let key = match action {
            Some("delete") => "delete",
            Some("create") => "create",
            Some("update") => "update",
            Some("index") | None => "index",
            Some(other) => {
                return Err(format!(
                    "invalid ES operation, use one of `delete`, `create`, `update`, `index`: {}",
                    other
                )
                .into())
            }
        };
        let value_meta = json!({ key: index_meta });
        value_meta.write(&mut payload)?;
        match key {
            "delete" => (),
            "update" => {
                payload.push(b'\n');
                let value = json!({ "doc": value });
                value.write(&mut payload)?;
            }
            "create" | "index" => {
                payload.push(b'\n');
                value.write(&mut payload)?;
            }
            other => error!("[ES::Sink] Unsupported action: {}", other),
        }
        payload.push(b'\n');
    }
    Ok(payload)
}

impl Elastic {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::too_many_lines)]
    async fn enqueue_send_future(&mut self, event: Event) {
        let (tx, rx) = bounded(1);
        let insight_tx = self.insight_tx.clone();
        let response_tx = self.response_sender.clone();
        let is_linked = self.is_linked;

        let transactional = event.transactional;
        let id = event.id.clone();
        let op_meta = event.op_meta.clone();

        let ingest_ns = event.ingest_ns;
        let response_origin_uri = if is_linked {
            event.origin_uri.clone()
        } else {
            None
        };

        let mut responses = Vec::with_capacity(if is_linked { 8 } else { 0 });

        // build payload and request
        let payload = match build_event_payload(&event) {
            Ok(payload) => payload,
            Err(e) => {
                // send fail
                self.send_insight(event.to_fail()).await;

                // send error response about event not being able to be serialized into ES payload
                let error_msg = format!("Invalid ES Payload: {}", e);
                let mut data = Value::object_with_capacity(2);
                let payload = event.data.suffix().value().clone_static();
                data.try_insert("success", Value::from(false));
                data.try_insert("error", Value::from(error_msg));
                data.try_insert("payload", payload);
                let source = build_source(&event.id, event.origin_uri.as_ref());
                data.try_insert("source", source);

                // if we have more than one event (batched), correlation will be an array with `null` or an actual value
                // for the event at the batch position
                let correlation_value = event.correlation_meta();
                let meta = correlation_value.map_or_else(Value::object, |correlation| {
                    literal!({ "correlation": correlation })
                });

                // send error response
                if let Err(e) = response_tx.send(((data, meta).into(), ERR)).await {
                    error!(
                        "[Sink::{}] Failed to send build_event_payload error response: {}",
                        self.sink_url, e
                    );
                }
                return;
            }
        };
        let req = self.client.request(BulkRequest::new(payload));

        let mut correlation_values = if is_linked {
            event.correlation_metas()
        } else {
            vec![]
        };
        // go async
        task::spawn_blocking(move || {
            let ress = &mut responses;

            let start = Instant::now();
            let r: Result<BulkResponse> = (move || {
                // send event to elastic, blockingly
                Ok(req.send()?.into_response::<BulkResponse>()?)
            })();

            // The truncation we do is sensible since we're only looking at a short timeframe
            let time = start.elapsed().as_millis() as u64;
            let mut insight_meta = Value::object_with_capacity(1);
            let cb = match &r {
                Ok(bulk_response) => {
                    insight_meta.try_insert("time", Value::from(time));
                    if is_linked {
                        // send out response events for each item
                        // success events via OUT port
                        // error   events via ERR port
                        for ((item, value), correlation) in bulk_response
                            .iter()
                            .zip(event.value_iter())
                            .zip(correlation_values.into_iter().chain(iter::repeat(None)))
                        {
                            let item_res = match item {
                                Ok(ok_item) => (
                                    OUT,
                                    Ok(build_bulk_success_data(
                                        ok_item,
                                        &id,
                                        value.clone_static(), // uaarrghhh
                                        event.origin_uri.as_ref(),
                                        correlation,
                                    )),
                                ),
                                Err(err_item) => (
                                    ERR,
                                    build_bulk_error_data(
                                        err_item,
                                        &id,
                                        value.clone_static(), // uaarrrghhh
                                        event.origin_uri.as_ref(),
                                        correlation,
                                    ),
                                ),
                            };
                            if let (port, Ok(item_body)) = item_res {
                                ress.push((item_body, port));
                            } else {
                                error!(
                                    "Error extracting bulk item response for document id {}",
                                    item.map_or_else(ErrorItem::id, OkItem::id)
                                );
                            }
                        }
                    };
                    CbAction::Ack
                }
                Err(e) => {
                    // request failed
                    // TODO how to update error metric here?
                    insight_meta.try_insert("error", Value::from(e.to_string()));
                    if is_linked {
                        // if we have more than one event (batched), correlation will be an array with `null` or an actual value
                        // for the event at the batch position
                        let mut meta = Value::object_with_capacity(1);
                        match correlation_values.len() {
                            1 => {
                                if let Some(cv) = correlation_values.pop().flatten() {
                                    meta.try_insert("correlation", cv);
                                }
                            }
                            l if l > 1 => {
                                meta.try_insert("correlation", Value::from(correlation_values));
                            }
                            _ => {}
                        };
                        // send error event via ERR port
                        let error_data = literal!({
                            "source": {
                                "event_id": id.to_string(),
                                "origin": response_origin_uri.map(|uri| uri.to_string()),
                            },
                            "success": false,
                            "error": e.to_string()

                        });
                        responses.push(((error_data, meta).into(), ERR));
                    };
                    CbAction::Fail
                }
            };
            task::block_on(async move {
                // send response events
                for response in responses {
                    if let Err(e) = response_tx.send(response).await {
                        error!("[Sink::ES] Failed to send bulk item response: {}", e);
                    }
                }

                // send insight - if required
                if transactional {
                    let insight = Event {
                        id,
                        data: (Value::null(), insight_meta).into(),
                        ingest_ns,
                        op_meta,
                        cb,
                        ..Event::default()
                    };
                    if let Err(e) = insight_tx.send(sink::Reply::Insight(insight)).await {
                        error!("[Sink::ES] Failed to send insight: {}", e)
                    }
                }

                // mark this task as done in order to free a slot
                if let Err(e) = tx.send(r.map(|_| time)).await {
                    error!("[Sink::ES] Failed to send AsyncSink done message: {}", e)
                }
            });
        });
        // this should actually never fail, given how we call this from maybe_enqueue
        if let Err(e) = self.queue.enqueue(rx) {
            // no need to send insight or response here, this should not affect event handling
            // this might just mess with the concurrency limitation
            error!(
                "[Sink::{}] Error enqueuing the ready receiver for the Es request execution: {}",
                &self.sink_url, e
            );
        }
    }

    async fn handle_error(&mut self, event: Event, error_msg: &'static str) {
        self.send_insight(event.to_fail()).await;

        let data = literal!({
            "success": false,
            "error": error_msg,
            "payload": event.data.suffix().value().clone_static(),
        });
        let mut meta = literal!({ "error": error_msg });
        if let Some(correlation) = event.correlation_meta() {
            meta.try_insert("correlation", correlation);
        }

        if let Err(e) = self.response_sender.send(((data, meta).into(), ERR)).await {
            error!(
                "[Sink::{}] Failed to send error response on overflow: {}.",
                self.sink_url, e
            );
        }

        error!("[Sink::{}] {}", &self.sink_url, error_msg);
    }

    async fn maybe_enque(&mut self, event: Event) {
        match self.queue.dequeue() {
            Err(SinkDequeueError::NotReady) if !self.queue.has_capacity() => {
                let error_msg = "Dropped data due to es overload";
                self.handle_error(event, error_msg).await;
            }
            _ => {
                self.enqueue_send_future(event).await;
            }
        }
    }

    // we swallow send errors here, we only log them
    async fn send_insight(&mut self, insight: Event) {
        if let Err(e) = self.insight_tx.send(sink::Reply::Insight(insight)).await {
            error!("[Sink::{}] Failed to send insight: {}", &self.sink_url, e)
        }
    }
}

/// task responsible to receive raw response data, build response event and send it off.
/// keeps ownership of `EventIdGenerator` and more
async fn response_task(
    mut id_gen: EventIdGenerator,
    origin_uri: EventOriginUri,
    rx: Receiver<(EventPayload, Cow<'static, str>)>,
    insight_tx: Sender<sink::Reply>,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok((data, port)) => {
                let response_event = Event {
                    id: id_gen.next_id(),
                    data,
                    ingest_ns: nanotime(),
                    origin_uri: Some(origin_uri.clone()),
                    ..Event::default()
                };
                if let Err(e) = insight_tx
                    .send(sink::Reply::Response(port, response_event))
                    .await
                {
                    return Err(e.into());
                }
            }
            Err(e) => {
                error!("[Sink::elastic] Response task channel closed: {}", e);
                return Err(e.into());
            }
        }
    }
}

#[async_trait::async_trait]
impl Sink for Elastic {
    // We enforce json here!
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        mut event: Event,
    ) -> ResultVec {
        if event.is_empty() {
            // nothing to send to ES, an empty event would result in
            debug!(
                "[Sink::{}] Received empty event. Won't send it to ES",
                self.sink_url
            );
            Ok(Some(if event.transactional {
                vec![Reply::Insight(event.insight_ack())]
            } else {
                vec![]
            }))
        } else {
            // we have either one event or a batched one with > 1 event
            self.maybe_enque(event).await;
            Ok(None) // insights are sent via reply_channel directly
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorUrl,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        _processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        // try to connect to check provided config and extract the cluster name
        let cluster_name = self.client.ping().send()?.cluster_name().to_string();
        info!(
            "[Sink::{}] Connected to ES cluster {}.",
            &sink_url, &cluster_name
        );

        self.sink_url = sink_url.clone();
        self.insight_tx = reply_channel;
        self.is_linked = is_linked;
        if is_linked {
            let event_id_gen = EventIdGenerator::new(sink_uid);
            self.origin_uri = EventOriginUri {
                uid: sink_uid, // placeholder,
                scheme: "elastic".to_string(),
                host: cluster_name,
                port: None,
                path: vec![],
            };
            // channels that events go through that end up here are bounded, so we should not grow out of memory
            let (tx, rx) = async_channel::unbounded();
            self.response_sender = tx;
            self.response_task_handle = Some(task::spawn(response_task(
                event_id_gen,
                self.origin_uri.clone(),
                rx,
                self.insight_tx.clone(),
            )));
        }
        Ok(())
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None) // insights are sent via reply_channel directly
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
    fn default_codec(&self) -> &str {
        "json"
    }
    async fn terminate(&mut self) {
        let mut swap: Option<JoinHandle<Result<()>>> = None;
        std::mem::swap(&mut swap, &mut self.response_task_handle);
        if let Some(handle) = swap {
            info!("[Sink::{}] Terminating response task...", self.sink_url);
            handle.cancel().await;
            info!("[Sink::{}] Done.", self.sink_url);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn build_event_payload_test() -> Result<()> {
        let mut numbers = Value::array_with_capacity(3);
        for x in 1..=3 {
            numbers.push(Value::from(x))?;
        }
        numbers.push(Value::from("snot"))?;

        let data = json!({
            "numbers": numbers,
        });
        let meta = json!({
            "index": "my_index",
            "doc_type": "my_type",
            "doc_id": "my_id",
            "pipeline": "my_pipeline"
        });

        let event = Event {
            data: (data.clone(), meta).into(),
            ..Event::default()
        };
        let payload = build_event_payload(&event)?;

        let mut expected = Vec::new();
        let es_meta = json!({
            "index": {
                "_index": "my_index",
                "_type": "my_type",
                "_id": "my_id",
                "pipeline": "my_pipeline"
            }
        });
        es_meta.write(&mut expected)?;
        expected.push(b'\n');
        data.write(&mut expected)?; // this clone is here so both are the same structure internally
        expected.push(b'\n');

        assert_eq!(
            String::from_utf8_lossy(&expected),
            String::from_utf8_lossy(&payload)
        );
        Ok(())
    }

    #[test]
    fn build_event_payload_missing_index() -> Result<()> {
        let meta = json!({
            "doc_type": "my_type",
            "doc_id": "my_id"
        });

        let event = Event {
            data: (Value::object(), meta).into(),
            ..Event::default()
        };

        let p = build_event_payload(&event);
        assert!(p.is_err(), "Didnt fail with missing index.");
        Ok(())
    }
}
