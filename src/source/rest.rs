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

// TODO add tests

use crate::codec::Codec;
use crate::postprocessor::{make_postprocessors, postprocess, Postprocessors};
use crate::source::prelude::*;
use async_channel::{unbounded, Sender, TryRecvError};
use halfbrown::HashMap;
use http_types::Mime;
use std::str::FromStr;
use tide::http::headers::HeaderValue;
use tide::{Body, Request, Response};
use tremor_script::Value;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// host to listen to, defaults to "0.0.0.0"
    #[serde(default = "dflt_host")]
    pub host: String,
    /// port to listen to, defaults to 8000
    #[serde(default = "dflt_port")]
    pub port: u16,
}

// TODO possible to do this in source trait?
impl ConfigImpl for Config {}

fn dflt_host() -> String {
    String::from("0.0.0.0")
}

fn dflt_port() -> u16 {
    8000
}

pub struct Rest {
    pub config: Config,
    onramp_id: TremorUrl,
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Rest {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for REST onramp".into())
        }
    }
}

struct RestSourceReply(Option<Sender<Response>>, SourceReply);

impl From<SourceReply> for RestSourceReply {
    fn from(reply: SourceReply) -> Self {
        Self(None, reply)
    }
}

// TODO possible to do this in source trait?
pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<RestSourceReply>>,
    post_processors: Postprocessors,
    onramp_id: TremorUrl,
    is_linked: bool,
    // TODO better way to manage this?
    response_txes: HashMap<u64, Sender<Response>>,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "REST")
    }
}

impl Int {
    fn from_config(
        uid: u64,
        onramp_id: TremorUrl,
        config: &Config,
        post_processors: &[String],
        is_linked: bool,
    ) -> Result<Self> {
        let config = config.clone();
        let post_processors = make_postprocessors(post_processors)?;
        Ok(Self {
            uid,
            config,
            listener: None,
            post_processors,
            onramp_id,
            is_linked,
            response_txes: HashMap::new(),
        })
    }
}

struct BoxedCodec(Box<dyn Codec>);

impl Clone for BoxedCodec {
    fn clone(&self) -> Self {
        Self(self.0.boxed_clone())
    }
}

impl From<Box<dyn Codec>> for BoxedCodec {
    fn from(bc: Box<dyn Codec>) -> Self {
        Self(bc)
    }
}

impl AsRef<Box<dyn Codec>> for BoxedCodec {
    fn as_ref(&self) -> &Box<dyn Codec> {
        &self.0
    }
}

#[derive(Clone)]
struct ServerState {
    tx: Sender<RestSourceReply>,
    uid: u64,
    link: bool,
}

async fn handle_request(mut req: Request<ServerState>) -> tide::Result<Response> {
    // TODO cache parts of this and update host only on new request
    let origin_uri = EventOriginUri {
        uid: req.state().uid,
        scheme: "tremor-rest".to_string(),
        host: req
            .host()
            .unwrap_or("tremor-rest-client-host.remote")
            .to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp)
        path: vec![String::default()],
    };

    let headers = req
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                // tide does not seem to guarantee the order of values though --
                // look into it later
                req.header(name)
                    .iter()
                    .map(|value| value.as_str().to_string())
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    let ct: Option<Mime> = req.content_type();
    let codec_override = ct.map(|ct| ct.essence().to_string());

    // request metadata
    let mut meta = Value::object_with_capacity(1);
    let mut request_meta = Value::object_with_capacity(3);
    let mut url_meta = Value::object_with_capacity(7);
    let url = req.url();
    url_meta.insert("scheme", url.scheme().to_string())?;
    if !url.username().is_empty() {
        url_meta.insert("username", url.username().to_string())?;
    }
    url.password()
        .and_then(|p| url_meta.insert("password", p.to_string()).ok());
    url.host_str()
        .and_then(|h| url_meta.insert("host", h.to_string()).ok());
    url.port().and_then(|p| url_meta.insert("port", p).ok());
    url_meta.insert("path", url.path().to_string())?;
    url.query()
        .and_then(|q| url_meta.insert("query", q.to_string()).ok());
    url.fragment()
        .and_then(|f| url_meta.insert("fragment", f.to_string()).ok());

    request_meta.insert("method", req.method().to_string())?;
    request_meta.insert("headers", headers)?;
    request_meta.insert("url", url_meta)?;
    meta.insert("request", request_meta)?;

    let data = req.body_bytes().await?;
    if req.state().link {
        let (response_tx, response_rx) = unbounded();

        // TODO check how tide handles request timeouts here
        //
        // also figure out best way to bubble up errors (to the end user)
        // during processing of this data, that may occur before a valid
        // event is sent back from the pipeline.
        // eg: in case of invalid json input with json content-type/codec,
        // event processing fails at the codec decoding stage, even before
        // this event reaches pipeline, but the rest source request just
        // hangs (until it times out).
        req.state()
            .tx
            .send(RestSourceReply(
                Some(response_tx),
                SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(meta),
                    codec_override,
                    stream: 0,
                },
            ))
            .await?;
        // TODO honor accept header
        Ok(response_rx.recv().await?)
    } else {
        req.state()
            .tx
            .send(
                SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(meta),
                    codec_override,
                    stream: 0,
                }
                .into(),
            )
            .await?;

        // TODO set proper content-type
        Ok(Response::builder(202).body(Body::empty()).build())
    }
}

fn make_response(
    default_codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    post_processors: &mut Postprocessors,
    event: &tremor_pipeline::Event,
) -> Result<Response> {
    let err: Error = "Empty event.".into();
    let ingest_ns = event.ingest_ns;
    let (response_data, meta) = event.value_meta_iter().next().ok_or(err)?;

    if let Some(response_meta) = meta.get("response") {
        let status = response_meta.get_u16("status").unwrap_or(200);

        // hallo heinz! :)
        let mut builder = Response::builder(status);
        // extract headers
        let mut header_content_type: Option<&str> = None;
        if let Some(headers) = response_meta.get_object("headers") {
            for (name, values) in headers {
                if let Some(header_values) = values.as_array() {
                    if name.eq_ignore_ascii_case("content-type") {
                        // pick first value in case of multiple content-type headers
                        header_content_type = header_values.first().and_then(Value::as_str);
                    }
                    let mut v = Vec::with_capacity(header_values.len());
                    for value in header_values {
                        if let Some(header_value) = value.as_str() {
                            v.push(HeaderValue::from_str(header_value)?);
                        }
                    }
                    builder = builder.header(name.as_ref(), v.as_slice());
                } else if let Some(header_value) = values.as_str() {
                    if name.eq_ignore_ascii_case("content-type") {
                        header_content_type = Some(header_value);
                    }
                    builder = builder.header(name.as_ref(), header_value);
                }
            }
        }

        let maybe_content_type = match header_content_type {
            None => None,
            Some(ct) => Some(Mime::from_str(ct)?),
        };

        if let Some((mime, dynamic_codec)) = maybe_content_type
            .and_then(|mime| codec_map.get(mime.essence()).map(|c| (mime, c.as_ref())))
        {
            let processed = postprocess(
                post_processors.as_mut_slice(),
                ingest_ns,
                dynamic_codec.encode(response_data)?,
            )?;

            // TODO: see if we can use a reader instead of creating a new vector
            let v: Vec<u8> = processed.into_iter().flatten().collect();
            let mut body = Body::from_bytes(v);

            body.set_mime(mime);
            builder = builder.body(body);
        } else {
            // fallback to default codec
            let processed = postprocess(
                post_processors.as_mut_slice(),
                ingest_ns,
                default_codec.encode(response_data)?,
            )?;
            // TODO: see if we can use a reader instead of creating a new vector
            let v: Vec<u8> = processed.into_iter().flatten().collect();
            let mut body = Body::from_bytes(v);

            // set mime type for default codec
            // TODO: cache mime type for default codec
            if let Some(mime) = default_codec
                .mime_types()
                .drain(..)
                .find_map(|mstr| Mime::from_str(mstr).ok())
            {
                body.set_mime(mime);
            }
            builder = builder.body(body);
        }
        Ok(builder.build())
    } else {
        Err("No response metadata available.".into())
    }
}

#[async_trait::async_trait()]
impl Source for Int {
    // TODO possible to do this in source trait?
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        let response_txes = &mut self.response_txes;
        self.listener.as_ref().map_or_else(
            // listener channel dropped, server stopped
            || Ok(SourceReply::StateChange(SourceState::Disconnected)),
            // get some request from the server channel
            |listener| {
                match listener.try_recv() {
                    Ok(RestSourceReply(Some(response_tx), source_reply)) => {
                        // store a sender here to be able to send the response later
                        response_txes.insert(id, response_tx);
                        Ok(source_reply)
                    }
                    Ok(r) => Ok(r.1),
                    Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                    Err(TryRecvError::Closed) => {
                        Ok(SourceReply::StateChange(SourceState::Disconnected))
                    }
                }
            },
        )
    }

    async fn reply_event(
        &mut self,
        event: Event,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        if let Some((_stream_id, event_id)) = event.id.get_max_by_source(self.uid) {
            if let Some(response_tx) = self.response_txes.remove(&event_id) {
                if event.is_batch && self.is_linked {
                    return Err("Batched events not supported in linked REST source.".into());
                }
                let res = match make_response(codec, codec_map, &mut self.post_processors, &event) {
                    Ok(response) => response,
                    Err(e) => {
                        error!(
                            "[Source::{}] Error encoding reply event: {}",
                            &self.onramp_id,
                            e.to_string()
                        );
                        // build error response
                        let mut error_data = Value::object_with_capacity(1);
                        error_data.insert("error", e.to_string())?;
                        let mut builder = Response::builder(500).header("Server", "Tremor");
                        if let Ok(data) = codec.encode(&error_data) {
                            let mut body = Body::from_bytes(data);
                            for mime in codec.mime_types() {
                                if let Ok(mime) = Mime::from_str(mime) {
                                    body.set_mime(mime);
                                    break;
                                }
                            }
                        } else {
                            builder = builder.body(Body::from_string(e.to_string()));
                        }
                        builder.build()
                    }
                };
                response_tx.send(res).await?
            } else {
                debug!("No outstanding HTTP session for event-id {}", event_id);
            }
        }
        Ok(())
    }

    async fn on_empty_event(&mut self, id: u64, stream: usize) -> Result<()> {
        debug!(
            "[Source::{}] on_empty_event(id={}, stream={})",
            self.onramp_id, id, stream
        );
        if let Some(response_tx) = self.response_txes.remove(&id) {
            // send no-content HTTP response
            let res = Response::builder(400)
                .header("Content-Length", "0")
                .header("Server", "Tremor")
                .build();
            response_tx.send(res).await?;
        }
        Ok(())
    }

    async fn init(&mut self) -> Result<SourceState> {
        // override the builtin map with onramp-instance specific config
        let (tx, rx) = bounded(crate::QSIZE);

        let mut server = tide::Server::with_state(ServerState {
            tx: tx.clone(),
            uid: self.uid,
            link: self.is_linked,
        });

        // TODO add override for path and method from config (defaulting to
        // all paths and methods like below if not provided)
        server.at("/").all(handle_request);
        server.at("/*").all(handle_request);
        // alt method without relying on server state
        //server.at("/*").all(|r| handle_request(r, self.uid, self.is_linked));

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let source_id = self.onramp_id.to_string();

        task::spawn::<_, Result<()>>(async move {
            info!("[Source::{}] Listening at {}", source_id, addr);
            if let Err(e) = server.listen(addr).await {
                error!(
                    "[Source::{}] Error while listening from the rest server: {}",
                    e, source_id
                )
            }
            warn!("[Source::{}] Server stopped", source_id);

            // TODO better state change here?
            tx.send(SourceReply::StateChange(SourceState::Disconnected).into())
                .await?;

            Ok(())
        });

        self.listener = Some(rx);

        // TODO ideally should happen only on successful server listen?
        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Rest {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(
            config.onramp_uid,
            self.onramp_id.clone(),
            &self.config,
            &config.processors.post,
            config.is_linked,
        )?;
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
