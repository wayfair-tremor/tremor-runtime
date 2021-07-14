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

use crate::codec::Codec;
use crate::connectors::gcp::auth;
use crate::errors::ErrorKind;
use crate::sink::prelude::*;
use async_channel::{bounded, Receiver, Sender};
use halfbrown::HashMap;
use http_types::mime::Mime;
use http_types::{headers::HeaderValue, Method};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use surf::{Body, Client, Request, Response};
use tremor_pipeline::{EventId, EventIdGenerator, OpMeta};
use tremor_script::Object;

/// custom Url struct for parsing a url
#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
pub struct Endpoint {
    scheme: Option<String>,
    username: Option<String>,
    password: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    path: Option<String>,
    query: Option<String>,
    fragment: Option<String>,
}

/// Authorization methods
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Auth {
    GCP,
}

fn none_if_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn err(s: &str) -> ErrorKind {
    s.into()
}

impl Endpoint {
    fn as_url(&self) -> Result<url::Url> {
        // scheme is the default, and host will be overwritten anyways, as relative http uris are not supported anyways
        self.as_url_with_base("http://localhost")
    }

    #[allow(clippy::map_err_ignore)] // we have only () as err here
                                     // expensive but seemingly correct way of populating a url from this struct
    fn as_url_with_base(&self, base_url: &str) -> Result<url::Url> {
        let mut res = url::Url::parse(base_url)?; //stupid placeholder, no other way to create a url
        if let Some(scheme) = &self.scheme {
            res.set_scheme(scheme.as_str())
                .map_err(|_| err("Invalid URI scheme"))?;
        };
        if let Some(username) = &self.username {
            res.set_username(username.as_str())
                .map_err(|_| err("Invalid URI username"))?;
        };
        if let Some(password) = &self.password {
            res.set_password(Some(password.as_str()))
                .map_err(|_| err("Invalid URI password"))?;
        };

        if let Some(host) = &self.host {
            res.set_host(Some(host.as_str()))?;
        } else {
            res.set_host(None)?;
        }
        res.set_port(self.port)
            .map_err(|_| err("Invalid URI port"))?;
        if let Some(path) = &self.path {
            res.set_path(path.as_str());
        };
        if let Some(query) = &self.query {
            res.set_query(Some(query.as_str()));
        }
        if let Some(fragment) = &self.fragment {
            res.set_fragment(Some(fragment.as_str()));
        }
        Ok(res)
    }

    /// overwrite fields in self with existing properties in `endpoint_value`
    fn merge(&mut self, endpoint_value: &Value) {
        if let Some(scheme) = endpoint_value.get_str("scheme") {
            self.scheme = Some(scheme.to_string());
        }
        if let Some(host) = endpoint_value.get_str("host") {
            self.host = Some(host.to_string());
        }
        if let Some(port) = endpoint_value.get_u16("port") {
            self.port = Some(port);
        }
        if let Some(username) = endpoint_value.get_str("username") {
            self.username = Some(username.to_string());
        }
        if let Some(password) = endpoint_value.get_str("password") {
            self.password = Some(password.to_string());
        }
        if let Some(path) = endpoint_value.get_str("path") {
            self.path = Some(path.to_string());
        }
        if let Some(query) = endpoint_value.get_str("query") {
            self.query = Some(query.to_string());
        }
        if let Some(fragment) = endpoint_value.get_str("fragment") {
            self.fragment = Some(fragment.to_string());
        }
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let url = url::Url::parse(s)?;
        Ok(Self {
            scheme: none_if_empty(url.scheme()),
            username: none_if_empty(url.username()),
            password: url.password().map(ToString::to_string),
            host: url.host_str().map(ToString::to_string),
            port: url.port(),
            path: none_if_empty(url.path()),
            query: url.query().map(ToString::to_string),
            fragment: url.fragment().map(ToString::to_string),
        })
    }
}

/// serialize with `FromStr` if given a `String`, or with `Deserialize` impl given a map/struct thing.
/// See <https://serde.rs/string-or-struct.html> for reference.
fn string_or_struct<'de, T, D>(deserializer: D) -> core::result::Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Error>,
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = crate::errors::Error>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> core::result::Result<T, E>
        where
            E: de::Error,
        {
            FromStr::from_str(value).map_err(de::Error::custom)
        }

        fn visit_map<M>(self, map: M) -> core::result::Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

/// Machinery for deserializing the HTTP method
struct MethodStrVisitor;

impl<'de> Visitor<'de> for MethodStrVisitor {
    type Value = SerdeMethod;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an HTTP method string.")
    }

    fn visit_str<E>(self, v: &str) -> core::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Method::from_str(v)
            .map(SerdeMethod)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SerdeMethod(Method);

impl<'de> Deserialize<'de> for SerdeMethod {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(MethodStrVisitor)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// authorization method, if any.
    pub auth: Option<Auth>,

    /// endpoint url - given as String or struct
    #[serde(deserialize_with = "string_or_struct", default)]
    pub endpoint: Endpoint,

    /// maximum number of parallel in flight batches (default: 4)
    /// this avoids blocking further events from progressing while waiting for upstream responses.
    #[serde(default = "dflt_concurrency")]
    pub concurrency: usize,

    // HTTP method to use (default: POST)
    #[serde(default = "dflt_method")]
    pub method: SerdeMethod,

    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn dflt_concurrency() -> usize {
    4
}

fn dflt_method() -> SerdeMethod {
    SerdeMethod(Method::Post)
}

impl ConfigImpl for Config {}

#[allow(clippy::large_enum_variant)]
enum CodecTaskInMsg {
    ToRequest(Event, Sender<SendTaskInMsg>),
    ToEvent {
        id: EventId,
        origin_uri: Box<EventOriginUri>, // box to avoid becoming the struct too big
        op_meta: Box<Option<OpMeta>>,
        request_meta: Value<'static>,
        correlation: Option<Value<'static>>,
        response: Response,
        duration: u64,
    },
    ReportFailure {
        id: EventId,
        op_meta: Option<OpMeta>,
        correlation: Option<Value<'static>>,
        e: Error,
        status: u16,
    },
}

enum SendTaskInMsg {
    Request(Request),
    Failed,
}

pub struct Rest {
    uid: u64,
    sink_url: TremorUrl,
    config: Config,
    num_inflight_requests: Arc<AtomicMaxCounter>,
    is_linked: bool,
    codec_task_tx: Option<Sender<CodecTaskInMsg>>,
    client: Client,
}

impl offramp::Impl for Rest {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let num_inflight_requests = Arc::new(AtomicMaxCounter::new(config.concurrency));
            let client = surf::client();
            Ok(SinkManager::new_box(Self {
                uid: 0,
                sink_url: TremorUrl::from_offramp_id("rest")?, // dummy
                config,
                num_inflight_requests,
                is_linked: false,
                codec_task_tx: None,
                client,
            }))
        } else {
            Err("Rest offramp requires a configuration.".into())
        }
    }
}

#[async_trait::async_trait]
impl Sink for Rest {
    async fn on_event(
        &mut self,
        _input: &str,
        _codec: &mut dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec {
        if self.is_linked && event.is_batch {
            error!(
                "[Sink::{}] Batched events are not supported for linked rest offramps.",
                &self.sink_url
            );
            return Ok(if event.transactional {
                Some(vec![sink::Reply::Insight(event.to_fail())])
            } else {
                None
            });
        }
        let sink_uid = self.uid;
        let id = event.id.clone();
        let op_meta = if event.transactional {
            Some(event.op_meta.clone())
        } else {
            None
        };

        let codec_task_channel = match &self.codec_task_tx {
            Some(codec_task_channel) => codec_task_channel.clone(),
            None => return Err("Offramp in invalid state: No codec task channel available.".into()),
        };
        // limit concurrency
        if let Ok(current_inflights) = self.num_inflight_requests.inc() {
            let (tx, rx) = bounded::<SendTaskInMsg>(1);
            let max_counter = self.num_inflight_requests.clone();
            let http_client = self.client.clone(); // should be quite cheap, just some Arcs

            // spawn send task
            task::spawn(async move {
                let start = Instant::now();
                let correlation = event.correlation_meta();
                // send command to codec task
                codec_task_channel
                    .send(CodecTaskInMsg::ToRequest(event, tx))
                    .await?;
                // wait for encoded request to come in
                match rx.recv().await? {
                    SendTaskInMsg::Request(request) => {
                        let url = request.url();
                        let event_origin_uri = EventOriginUri {
                            uid: sink_uid,
                            scheme: "tremor-rest".to_string(),
                            host: url.host_str().map_or(String::new(), ToString::to_string),
                            port: url.port(),
                            path: url.path_segments().map_or_else(Vec::new, |segments| {
                                segments.map(String::from).collect()
                            }),
                        };
                        let request_meta = build_request_metadata(&request)?;
                        // send request
                        match http_client.send(request).await {
                            Ok(response) => {
                                #[allow(clippy::cast_possible_truncation)]
                                // we don't care about the upper 64 bit
                                let duration = start.elapsed().as_millis() as u64; // measure response duration
                                codec_task_channel
                                    .send(CodecTaskInMsg::ToEvent {
                                        id,
                                        origin_uri: Box::new(event_origin_uri),
                                        op_meta: Box::new(op_meta),
                                        request_meta,
                                        correlation,
                                        response,
                                        duration,
                                    })
                                    .await?
                            }
                            Err(e) => {
                                error!("[Sink::Rest] Error sending HTTP request: {}", e);
                                codec_task_channel
                                    .send(CodecTaskInMsg::ReportFailure {
                                        id,
                                        op_meta,
                                        correlation,
                                        e: e.into(),
                                        status: 503,
                                    })
                                    .await?;
                            }
                        }
                    }
                    SendTaskInMsg::Failed => {} // just stop the task here, error already handled and reported in codec_task
                }

                max_counter.dec_from(current_inflights); // be fair to others and free our spot
                Ok::<(), Error>(())
            });
        } else {
            error!("[Sink::{}] Dropped data due to overload", self.sink_url);
            codec_task_channel
                .send(CodecTaskInMsg::ReportFailure {
                    id,
                    op_meta,
                    correlation: event.correlation_meta(),
                    e: Error::from(String::from("Dropped data due to overload")),
                    status: 429,
                })
                .await?;
        }
        Ok(None)
    }

    fn default_codec(&self) -> &str {
        "json"
    }

    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorUrl,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<sink::Reply>,
    ) -> Result<()> {
        // clone the hell out of all the shit
        let postprocessors = make_postprocessors(processors.post)?;
        let preprocessors = make_preprocessors(processors.pre)?;
        let my_codec = codec.boxed_clone();
        let my_codec_map = codec_map
            .iter()
            .map(|(k, v)| (k.clone(), v.boxed_clone()))
            .collect::<HashMap<String, Box<dyn Codec>>>();
        let default_method = self.config.method.0;
        let endpoint = self.config.endpoint.clone();
        let config_headers = self.config.headers.clone();
        let cloned_sink_url = sink_url.clone();
        self.sink_url = sink_url.clone();
        let auth = self.config.auth.clone();

        // inbound channel towards codec task
        // sending events to be turned into requests
        // and responses to be turned into events
        let (in_tx, in_rx) = bounded::<CodecTaskInMsg>(self.config.concurrency);
        // channel for sending shit back to the connected pipelines
        let reply_tx = reply_channel;
        // start the codec task
        task::spawn(async move {
            codec_task(
                sink_uid,
                cloned_sink_url,
                postprocessors,
                preprocessors,
                my_codec,
                my_codec_map,
                endpoint,
                default_method,
                config_headers,
                reply_tx,
                in_rx,
                is_linked,
                auth,
            )
            .await
        });
        self.is_linked = is_linked;
        self.codec_task_tx = Some(in_tx);
        self.uid = sink_uid;
        Ok(())
    }

    async fn on_signal(&mut self, _signal: Event) -> ResultVec {
        Ok(None)
    }
    fn is_active(&self) -> bool {
        true
    }
    fn auto_ack(&self) -> bool {
        false
    }
}

// TODO: use headers from config
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn codec_task(
    sink_uid: u64,
    sink_url: TremorUrl,
    mut postprocessors: Postprocessors,
    mut preprocessors: Preprocessors,
    mut codec: Box<dyn Codec>,
    mut codec_map: HashMap<String, Box<dyn Codec>>,
    endpoint: Endpoint,
    default_method: Method,
    default_headers: HashMap<String, String>,
    reply_tx: Sender<sink::Reply>,
    in_rx: Receiver<CodecTaskInMsg>,
    is_linked: bool,
    auth: Option<Auth>,
) -> Result<()> {
    debug!("[Sink::{}] Codec task started.", &sink_url);
    let mut response_ids = EventIdGenerator::new(sink_uid);
    let response_origin_uri = EventOriginUri {
        uid: sink_uid,
        scheme: "tremor-rest".to_string(),
        host: hostname(),
        ..EventOriginUri::default()
    };
    // create token in this lifetime (ambiguous to compiler), instead of bound to the sink.
    let token = match auth {
        Some(Auth::GCP) => {
            let (t, _) = auth::authenticate_bearer().await?;
            Some(t)
        }
        None => None,
    };
    let codec: &mut dyn Codec = codec.as_mut();
    while let Ok(msg) = in_rx.recv().await {
        match msg {
            CodecTaskInMsg::ToRequest(event, tx) => {
                let mut request_headers = default_headers.clone();
                if let Some(ref t) = token {
                    request_headers
                        .insert("authorization".to_string(), t.header_value()?.to_string());
                }
                match build_request(
                    &event,
                    codec,
                    &codec_map,
                    postprocessors.as_mut_slice(),
                    default_method,
                    &request_headers,
                    &endpoint,
                ) {
                    Ok(request) => {
                        if let Err(e) = tx.send(SendTaskInMsg::Request(request)).await {
                            error!(
                                "[Sink::{}] Error sending out encoded request {}",
                                &sink_url, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("[Sink::{}] Error encoding the request: {}", &sink_url, &e);
                        // send error result to send task
                        if let Err(e) = tx.send(SendTaskInMsg::Failed).await {
                            error!(
                                "[Sink::{}] Error sending Failed back to send task: {}",
                                &sink_url, e
                            );
                        }
                        if event.transactional {
                            // send CB_fail
                            if let Err(e) =
                                reply_tx.send(sink::Reply::Insight(event.to_fail())).await
                            {
                                error!("[Sink::{}] Error sending CB fail event {}", &sink_url, e);
                            }
                        }

                        // send response through error port
                        let error_event = create_error_response(
                            // TODO: add proper stream handling
                            response_ids.next_id(),
                            &event.id,
                            event.correlation_meta(),
                            400,
                            &response_origin_uri,
                            &e,
                        );
                        if let Err(e) = reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                        {
                            error!(
                                "[Sink::{}] Error sending error response event {}",
                                &sink_url, e
                            );
                        }
                    }
                }
            }
            CodecTaskInMsg::ToEvent {
                id,
                origin_uri,
                op_meta,
                request_meta,
                correlation,
                mut response,
                duration,
            } => {
                // send CB insight -> handle status >= 400
                let status = response.status();

                let meta = literal!({ "time": duration });
                let mut cb = if status.is_client_error() || status.is_server_error() {
                    // when the offramp is linked to pipeline, we want to send
                    // the response back and not consume it yet (or log about it)
                    if !is_linked {
                        if log::log_enabled!(log::Level::Debug) {
                            if let Ok(body) = response.body_string().await {
                                error!(
                                    "[Sink::{}] HTTP request failed: {} => {}",
                                    &sink_url, status, body
                                )
                            }
                        } else {
                            error!("[Sink::{}] HTTP request failed: {}", &sink_url, status)
                        }
                    }
                    CbAction::Fail
                } else {
                    CbAction::Ack
                };

                if is_linked {
                    match build_response_events(
                        &sink_url,
                        &id,
                        &mut response_ids,
                        origin_uri.as_ref(),
                        request_meta,
                        correlation.as_ref(),
                        response,
                        codec,
                        &mut codec_map,
                        preprocessors.as_mut_slice(),
                    )
                    .await
                    {
                        Ok(response_events) => {
                            for response_event in response_events {
                                // TODO: stream handling
                                if let Err(e) = reply_tx
                                    .send(sink::Reply::Response(OUT, response_event))
                                    .await
                                {
                                    error!(
                                        "[Sink::{}] Error sending response event: {}",
                                        &sink_url, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            cb = CbAction::Fail;
                            error!("[Sink::{}] Error encoding the request: {}", &sink_url, &e);
                            let error_event = create_error_response(
                                // TODO: stream handling
                                response_ids.next_id(),
                                &id,
                                correlation,
                                500,
                                origin_uri.as_ref(),
                                &e,
                            );

                            if let Err(e) =
                                reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                            {
                                error!(
                                    "[Sink::{}] Error sending error event on response decoding error: {}",
                                    &sink_url,
                                    e
                                );
                            }
                        }
                    }
                }
                // wait with sending CB Ack until response has been handled in linked case, might still fail
                // if op_meta is None, we dont need to send an insight
                if let Some(op_meta) = *op_meta {
                    if let Err(e) = reply_tx
                        .send(sink::Reply::Insight(Event {
                            id: id.clone(),
                            op_meta,
                            data: (Value::null(), meta).into(),
                            cb,
                            ingest_ns: nanotime(),
                            ..Event::default()
                        }))
                        .await
                    {
                        error!("[Sink::{}] Error sending CB event {}", &sink_url, e);
                    };
                }
            }
            CodecTaskInMsg::ReportFailure {
                id,
                op_meta,
                correlation,
                e,
                status,
            } => {
                // report send error as CB fail
                // sending a CB close would mean we need to take measures to reopen - introduce a healthcheck
                if let Some(op_meta) = op_meta {
                    let mut insight = Event::cb_fail(nanotime(), id.clone());
                    insight.op_meta = op_meta;
                    if let Err(send_err) = reply_tx.send(sink::Reply::Insight(insight)).await {
                        error!(
                            "[Sink::{}] Error sending CB trigger event for event {}: {}",
                            &sink_url, id, send_err
                        );
                    }
                }

                // send response via ERR port
                let error_event = create_error_response(
                    // TODO: stream handling
                    response_ids.next_id(),
                    &id,
                    correlation,
                    status,
                    &response_origin_uri,
                    &e,
                );
                if let Err(send_err) = reply_tx.send(sink::Reply::Response(ERR, error_event)).await
                {
                    error!(
                        "[Sink::{}] Error sending error response for failed request send: {}",
                        &sink_url, send_err
                    );
                }
            }
        }
    }
    info!("[Sink::{}] Codec task stopped. Channel closed.", &sink_url);
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_request(
    event: &Event,
    codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    postprocessors: &mut [Box<dyn Postprocessor>],
    default_method: Method,
    default_headers: &HashMap<String, String>,
    config_endpoint: &Endpoint,
) -> Result<surf::Request> {
    let mut body: Vec<u8> = vec![];
    let mut method = None;
    let mut endpoint = None;
    let mut headers: Vec<(&beef::Cow<str>, Vec<HeaderValue>)> = Vec::with_capacity(8);
    let mut codec_in_use = None;
    for (data, meta) in event.value_meta_iter() {
        if let Some(request_meta) = meta.get("request") {
            // use method from first event
            if method.is_none() {
                method = Some(
                    match request_meta
                        .get_str("method")
                        .map(|m| Method::from_str(&m.trim().to_uppercase()))
                    {
                        Some(Ok(method)) => method,
                        Some(Err(e)) => return Err(e.into()),
                        None => default_method,
                    },
                );
            }
            // use headers from first event
            if headers.is_empty() {
                if let Some(map) = request_meta.get_object("headers") {
                    for (header_name, v) in map {
                        // filter out content-length (might be carried over from received request), likely to have changed
                        if "content-length".eq_ignore_ascii_case(header_name) {
                            continue;
                        }
                        if let Some(value) = v.as_str() {
                            let values = vec![HeaderValue::from_str(value)?];
                            headers.push((header_name, values));
                            // TODO: deduplicate
                            // try to determine codec
                            if "content-type".eq_ignore_ascii_case(header_name)
                                && codec_in_use.is_none()
                            {
                                codec_in_use = Some(
                                    Mime::from_str(value)
                                        .ok()
                                        .and_then(|m| codec_map.get(m.essence()))
                                        .map_or(codec, |c| c.borrow()),
                                );
                            }
                        } else if let Some(array) = v.as_array() {
                            let mut values = Vec::with_capacity(array.len());
                            for header in array {
                                if let Some(value) = header.as_str() {
                                    values.push(HeaderValue::from_str(value)?);
                                    // try to determine codec
                                    if "content-type".eq_ignore_ascii_case(header_name)
                                        && codec_in_use.is_none()
                                    {
                                        codec_in_use = Some(
                                            Mime::from_str(value)
                                                .ok()
                                                .and_then(|m| codec_map.get(m.essence()))
                                                .map_or(codec, |c| c.borrow()),
                                        );
                                    }
                                }
                            }
                            headers.push((header_name, values));
                        }
                    }
                }
            }
        }
        // use url from first event
        if endpoint.is_none() {
            endpoint = match meta.get("endpoint") {
                Some(v) if v.is_str() => {
                    // overwrite the configured endpoint with that is provided as String
                    if let Some(s) = v.as_str() {
                        Some(Endpoint::from_str(s)?)
                    } else {
                        None // shouldn't happen
                    }
                }
                Some(v) if v.is_object() => {
                    let mut ep = config_endpoint.clone();
                    ep.merge(v);
                    Some(ep)
                }
                // invalid type
                Some(_) => return Err("Invalid $endpoint type. Use String or Map.".into()),
                None => None, // we use the config endpoint
            }
        }

        // apply the given codec, fall back to the configured codec if none is found
        let codec = codec_in_use.unwrap_or(codec);
        let encoded = codec.encode(data)?;
        let mut processed = postprocess(postprocessors, event.ingest_ns, encoded)?;
        for processed_elem in &mut processed {
            body.append(processed_elem);
        }
    }
    let endpoint = endpoint.map_or_else(|| config_endpoint.as_url(), |ep| ep.as_url())?;
    trace!("endpoint [{}] chosen", &endpoint);
    let host = match (endpoint.host(), endpoint.port()) {
        (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
        (Some(host), _) => Some(host.to_string()),
        _ => None,
    };

    let mut request_builder = surf::RequestBuilder::new(method.unwrap_or(default_method), endpoint);

    // build headers from config
    for (k, v) in default_headers {
        request_builder = request_builder.header(k.as_str(), v.as_str());
    }

    // build headers from meta - effectively overwrite config headers in case of conflict
    for (k, v) in headers {
        if "content-type".eq_ignore_ascii_case(k) {
            'inner: for hv in &v {
                if let Ok(mime) = Mime::from_str(hv.as_str()) {
                    request_builder = request_builder.content_type(mime);
                    break 'inner;
                }
            }
        }
        request_builder = request_builder.header(k.as_ref(), v.as_slice());
    }
    // overwrite Host header
    // otherwise we might run into trouble when overwriting the endpoint.
    if let Some(host) = host {
        request_builder = request_builder.header("Host", host);
    }
    request_builder = request_builder.body(Body::from_bytes(body));
    Ok(request_builder.build())
}

fn build_request_metadata(request: &Request) -> Result<Value<'static>> {
    let mut request_meta = Value::object_with_capacity(3);
    let method = request.method().to_string();
    request_meta.insert("method", Value::from(method))?;

    let mut request_headers = Value::object_with_capacity(request.header_values().count());
    {
        for (name, values) in request.iter() {
            let header_values: Value = values
                .iter()
                .map(ToString::to_string)
                .map(Value::from)
                .collect();
            request_headers.insert(name.to_string(), header_values)?;
        }
    }
    request_meta.insert("headers", request_headers)?;

    let mut endpoint = Value::object_with_capacity(8);
    let scheme = none_if_empty(request.url().scheme());
    endpoint.insert("scheme", Value::from(scheme))?;
    let username = none_if_empty(request.url().username());
    endpoint.insert("username", Value::from(username))?;
    let password = request.url().password().map(ToString::to_string);
    endpoint.insert("password", Value::from(password))?;
    let host = request.url().host_str().map(ToString::to_string);
    endpoint.insert("host", Value::from(host))?;
    let port = request.url().port();
    endpoint.insert("port", Value::from(port))?;
    let path = none_if_empty(request.url().path());
    endpoint.insert("path", Value::from(path))?;
    let query = request.url().query().map(ToString::to_string);
    endpoint.insert("query", Value::from(query))?;
    let fragment = request.url().fragment().map(ToString::to_string);
    endpoint.insert("fragment", Value::from(fragment))?;
    request_meta.insert("endpoint", endpoint)?; // temp variable for debugging aid
    Ok(request_meta)
}

#[allow(clippy::too_many_arguments)]
async fn build_response_events<'response>(
    sink_url: &'response TremorUrl,
    event_id: &'response EventId,
    response_ids: &'response mut EventIdGenerator,
    event_origin_uri: &'response EventOriginUri,
    request_meta: Value<'static>,
    correlation: Option<&Value<'static>>,
    mut response: Response,
    codec: &'response mut dyn Codec,
    codec_map: &'response mut HashMap<String, Box<dyn Codec>>,
    preprocessors: &'response mut [Box<dyn Preprocessor>],
) -> Result<Vec<Event>> {
    let mut meta = Value::object_with_capacity(2);
    if let Some(correlation) = correlation {
        meta.insert("correlation", correlation.clone_static())?;
    }
    let mut response_meta = Value::object_with_capacity(2);

    let numeric_status: u16 = response.status().into();
    response_meta.insert("status", numeric_status)?;

    let mut response_headers = Value::object_with_capacity(8);
    {
        for (name, values) in response.iter() {
            let header_values: Value = values
                .iter()
                .map(ToString::to_string)
                .map(Value::from)
                .collect();
            response_headers.insert(name.to_string(), header_values)?;
        }
    }

    response_meta.insert("headers", response_headers)?;

    response_meta.insert("request", request_meta)?;

    meta.insert("response", response_meta)?;
    // chose a codec
    let the_chosen_one = response
        .content_type()
        .and_then(|mime| codec_map.get_mut(mime.essence()))
        .map_or(codec, |c| -> &mut dyn Codec { c.as_mut() });

    let response_bytes = response.body_bytes().await?;
    let mut ingest_ns = nanotime();
    let preprocessed = preprocess(preprocessors, &mut ingest_ns, response_bytes, sink_url)?;

    let mut events = Vec::with_capacity(preprocessed.len());
    for pp in preprocessed {
        events.push(
            EventPayload::try_new::<crate::Error, _>(pp, |mut_data| {
                let body = the_chosen_one
                    .decode(mut_data, nanotime())?
                    .unwrap_or_else(Value::object);

                Ok(ValueAndMeta::from_parts(body, meta.clone())) // TODO: no need to clone the last element?
            })
            .map(|data| {
                let mut id = response_ids.next_id();
                id.track(event_id);
                Event {
                    id,
                    origin_uri: Some(event_origin_uri.clone()),
                    data,
                    ..Event::default()
                }
            })?,
        );
    }

    Ok(events)
}

/// build an error event bearing error information and metadata to be handled as http response
fn create_error_response(
    mut error_id: EventId,
    event_id: &EventId,
    correlation: Option<Value<'static>>,
    status: u16,
    origin_uri: &EventOriginUri,
    e: &Error,
) -> Event {
    let mut error_data = Object::with_capacity(2);
    let mut meta = Object::with_capacity(3);
    let mut response_meta = Object::with_capacity(2);
    response_meta.insert_nocheck("status".into(), Value::from(status));
    let mut headers = Object::with_capacity(2);
    headers.insert_nocheck("Content-Type".into(), Value::from("application/json"));
    headers.insert_nocheck("Server".into(), Value::from("Tremor"));
    response_meta.insert_nocheck("headers".into(), Value::from(headers));

    meta.insert_nocheck("response".into(), Value::from(response_meta));
    meta.insert_nocheck("error".into(), Value::from(e.to_string()));
    if let Some(correlation) = correlation {
        meta.insert_nocheck("correlation".into(), correlation);
    }

    error_data.insert_nocheck("error".into(), Value::from(e.to_string()));
    error_data.insert_nocheck("event_id".into(), Value::from(event_id.to_string()));
    error_id.track(event_id); // make sure we carry over the old events ids
    Event {
        id: error_id,
        data: (error_data, meta).into(),
        origin_uri: Some(origin_uri.clone()),
        ingest_ns: nanotime(),
        ..Event::default()
    }
}

/// atomically count up from 0 to a given max
/// and fail incrementing further if that max is reached.
struct AtomicMaxCounter {
    counter: AtomicUsize,
    max: usize,
}

impl AtomicMaxCounter {
    fn new(max: usize) -> Self {
        Self {
            counter: AtomicUsize::new(0),
            max,
        }
    }

    fn load(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }

    fn inc_from(&self, cur: usize) -> Result<usize> {
        let mut real_cur = cur;
        if (real_cur + 1) > self.max {
            return Err("max value reached".into());
        }
        while self
            .counter
            .compare_exchange(real_cur, real_cur + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            real_cur = self.load();
            if (real_cur + 1) > self.max {
                return Err("max value reached".into());
            }
        }
        Ok(real_cur + 1)
    }

    fn inc(&self) -> Result<usize> {
        self.inc_from(self.load())
    }

    fn dec_from(&self, cur: usize) -> usize {
        let mut real_cur = cur;
        if real_cur == 0 {
            // avoid underflow
            return real_cur;
        }
        while self
            .counter
            .compare_exchange(real_cur, real_cur - 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            real_cur = self.load();
            if real_cur == 0 {
                // avoid underflow
                return real_cur;
            }
        }
        real_cur - 1
    }
}

#[cfg(test)]
mod test {

    use http_types::{headers::HeaderValues, StatusCode};
    use simd_json::StaticNode;

    use super::*;

    #[test]
    fn deserialize_from_string() -> Result<()> {
        let config_s = r#"
            endpoint: "http://localhost:8080/path?query=value#fragment"
            concurrency: 4
            method: GET
            headers:
                Forwarded: "by=tremor(0.9);"
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        let _ = config.endpoint.as_url_with_base("http://localhost");
        assert_eq!(
            config.endpoint,
            Endpoint {
                scheme: Some("http".to_string()),
                host: Some("localhost".to_string()),
                port: Some(8080),
                path: Some("/path".to_string()),
                query: Some("query=value".to_string()),
                fragment: Some("fragment".to_string()),
                ..Endpoint::default()
            }
        );
        assert_eq!(config.method.0, Method::Get);
        Ok(())
    }

    #[test]
    fn deserialize_from_object() -> Result<()> {
        let config_s = r#"
            endpoint:
                host: example.org
                query: via=tremor
            concurrency: 4
            method: OPTIONS
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        assert_eq!(
            config.endpoint,
            Endpoint {
                host: Some("example.org".to_string()),
                query: Some("via=tremor".to_string()),
                ..Endpoint::default()
            }
        );
        assert_eq!(config.method.0, Method::Options);
        Ok(())
    }

    #[test]
    fn deserialize_method() -> Result<()> {
        let config_s = r#"
          endpoint: http://localhost:8080/
          method: PATCH
        "#;
        let v: serde_yaml::Value = serde_yaml::from_str(config_s)?;
        let config = Config::new(&v)?;
        assert_eq!(config.method.0, Method::Patch);
        Ok(())
    }

    #[test]
    fn endpoint_merge() -> Result<()> {
        let mut ep = Endpoint {
            scheme: Some("http".to_string()),
            host: Some("example.org".to_string()),
            port: Some(80),
            path: Some("/".to_string()),
            ..Endpoint::default()
        };
        let mut value = Value::object_with_capacity(2);
        value.insert("host", "tremor.rs")?;
        value.insert("query", "version=0.9")?;
        ep.merge(&value);
        assert_eq!(
            Endpoint {
                scheme: Some("http".to_string()),
                host: Some("tremor.rs".to_string()),
                port: Some(80),
                path: Some("/".to_string()),
                query: Some("version=0.9".to_string()),
                ..Endpoint::default()
            },
            ep
        );
        Ok(())
    }

    #[test]
    fn endpoint_as_url() -> Result<()> {
        let ep = Endpoint {
            scheme: Some("http".to_string()),
            host: Some("tremor.rs".to_string()),
            path: Some("/getting-started/".to_string()),
            ..Endpoint::default()
        };
        let url = ep.as_url()?;
        assert_eq!(
            url::Url::from_str("http://tremor.rs/getting-started/")?,
            url
        );
        Ok(())
    }

    #[test]
    fn endpoint_as_url_fail() -> Result<()> {
        // endpoint without host
        let ep = Endpoint {
            scheme: Some("http".to_string()),
            path: Some("/getting-started/".to_string()),
            ..Endpoint::default()
        };
        let res = ep.as_url();
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn build_request_multiple_headers() -> Result<()> {
        let mut event = Event::default();
        let mut meta = Value::object_with_capacity(2);
        let mut request_meta = Value::object_with_capacity(2);
        let mut request_headers_meta = Value::object_with_capacity(2);
        request_headers_meta.insert("Content-Type", "text/plain")?;
        request_headers_meta.insert("Content-Length", "3")?;
        request_headers_meta.insert("Overwrite-Me", "indeed!")?;
        let mut multiple = Value::array_with_capacity(2);
        multiple.push("first")?;
        multiple.push("second")?;
        request_headers_meta.insert("Multiple", multiple)?;
        request_meta.insert("headers", request_headers_meta)?;
        meta.insert("request", request_meta)?;

        let data = Value::String("foo".into());
        event.data = (data, meta).into();
        let codec = crate::codec::lookup("string")?;
        let codec_map = crate::codec::builtin_codec_map();
        let mut pp = vec![];
        let mut default_headers = halfbrown::HashMap::with_capacity(2);
        default_headers.insert_nocheck("Server".to_string(), "Tremor".to_string());
        default_headers.insert_nocheck("Overwrite-Me".to_string(), "oh noes!".to_string());
        let endpoint = Endpoint::from_str("http://localhost:65535/path?query=value#fragment")?;
        let request = build_request(
            &event,
            codec.as_ref(),
            &codec_map,
            pp.as_mut_slice(),
            Method::Get,
            &default_headers,
            &endpoint,
        )?;
        let expected = HeaderValues::from(HeaderValue::from_str("indeed!")?);
        assert_eq!(
            Some(expected.to_string()),
            request.header("Overwrite-Me").map(ToString::to_string)
        );
        let mut expected_multiple = HeaderValues::from(HeaderValue::from_str("first")?);
        expected_multiple.append(&mut HeaderValues::from(HeaderValue::from_str("second")?));
        assert_eq!(
            Some(expected_multiple.to_string()),
            request.header("Multiple").map(ToString::to_string)
        );
        assert_eq!(request.url(), &endpoint.as_url()?);
        Ok(())
    }

    // we can't use async_std::tst here as it causes lifetime issues with codec
    #[async_std::test]
    async fn build_response() -> Result<()> {
        let sink_url = TremorUrl::from_offramp_id("rest")?;
        let sink_uid = 0_u64;
        let mut response_id_gen = EventIdGenerator::new(sink_uid);
        let id = EventId::default();
        let event_origin_uri = EventOriginUri::default();
        let mut request = http_types::Request::new(
            Method::Get,
            http_types::Url::parse("http://localhost:3000/path?query=value#fragment")?,
        );
        request.insert_header("Content-Type", "application/json");
        let metadata = build_request_metadata(&Request::from(request))?;
        println!("{:?}", metadata); // this is None
        let mut body = Body::from_string(r#"{"foo": true}"#.to_string());
        body.set_mime(http_types::mime::JSON);
        let mut response = http_types::Response::new(StatusCode::Ok);
        response.append_header("Server", "Upstream");
        response.append_header("Multiple", "first");
        response.append_header("Multiple", "second");
        response.set_body(body);
        let mut codec = crate::codec::lookup("string")?;
        let mut codec_map = crate::codec::builtin_codec_map();
        let mut pp = vec![];

        let res = build_response_events(
            &sink_url,
            &id,
            &mut response_id_gen,
            &event_origin_uri,
            metadata,
            Some(&Value::Static(StaticNode::I64(-1))),
            Response::from(response),
            codec.as_mut(),
            &mut codec_map,
            pp.as_mut_slice(),
        )
        .await?;
        assert_eq!(1, res.len());
        if let Some(event) = res.first() {
            let mut expected_data = Value::object_with_capacity(1);
            expected_data.insert("foo", true)?;
            let data_parts = event.data.parts();
            assert_eq!(&mut expected_data, data_parts.0);
            let mut expected_meta = literal!({
                "correlation": -1,
                "response": {
                    "headers": {
                        "multiple": ["first", "second"],
                        "server": ["Upstream"],
                        "content-type": ["application/json"]
                    },
                    "request": {
                        "method": "GET",
                        "headers": {
                            "content-type": ["application/json"]
                        },
                        "endpoint": {
                            "scheme": "http",
                            "username": null,
                            "password": null,
                            "host": "localhost",
                            "port": 3000_u64,
                            "path": "/path",
                            "query": "query=value",
                            "fragment": "fragment"
                        }
                    }
                }
            });
            if let Some(response) = expected_meta.get_mut("response") {
                response.insert("status", 200_u64)?;
            }
            assert_eq!(expected_meta, data_parts.1.to_owned());
        }
        Ok(())
    }
}
