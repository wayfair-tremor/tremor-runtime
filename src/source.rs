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

use crate::errors::Error;
use crate::metrics::RampReporter;
use crate::onramp;
use crate::pipeline;
use crate::preprocessor::{make_preprocessors, preprocess, Preprocessors};
use crate::url::ports::{ERR, METRICS, OUT};
use crate::url::TremorUrl;
use crate::{
    codec::{self, Codec},
    pipeline::ConnectTarget,
};

use crate::Result;
use async_channel::{self, unbounded, Receiver, Sender};
use async_std::task;
use beef::Cow;
use halfbrown::HashMap;
use std::collections::BTreeMap;
use std::time::Duration;
use tremor_common::time::nanotime;
use tremor_pipeline::{CbAction, Event, EventId, EventOriginUri, DEFAULT_STREAM_ID};
use tremor_script::prelude::*;

use self::prelude::OnrampConfig;

pub(crate) mod amqp;
pub(crate) mod blaster;
pub(crate) mod cb;
pub(crate) mod crononome;
pub(crate) mod discord;
pub(crate) mod file;
pub(crate) mod gsub;
pub(crate) mod kafka;
pub(crate) mod metronome;
pub(crate) mod nats;
pub(crate) mod otel;
pub(crate) mod postgres;
pub(crate) mod prelude;
pub(crate) mod rest;
pub(crate) mod sse;
pub(crate) mod stdin;
pub(crate) mod tcp;
pub(crate) mod udp;
pub(crate) mod ws;

struct StaticValue(Value<'static>);

#[derive(Default)]
/// Set of pre and postprocessors
pub struct Processors<'processor> {
    /// preprocessors
    pub pre: &'processor [String],
    /// postprocessors
    pub post: &'processor [String],
}

#[derive(Debug)]
pub(crate) enum SourceState {
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub(crate) enum SourceReply {
    /// A normal batch_data event with a `Vec<Vec<u8>>` for data
    BatchData {
        origin_uri: EventOriginUri,
        batch_data: Vec<(Vec<u8>, Option<Value<'static>>)>,
        /// allow source to override codec when pulling event
        /// the given string must be configured in the `config-map` as part of the source config
        codec_override: Option<String>,
        stream: usize,
    },
    /// A normal data event with a `Vec<u8>` for data
    Data {
        origin_uri: EventOriginUri,
        data: Vec<u8>,
        meta: Option<Value<'static>>,
        /// allow source to override codec when pulling event
        /// the given string must be configured in the `config-map` as part of the source config
        codec_override: Option<String>,
        stream: usize,
    },
    /// Allow for passthrough of already structured events
    Structured {
        origin_uri: EventOriginUri,
        data: EventPayload,
    },
    /// A stream is opened
    StartStream(usize),
    /// A stream is closed
    EndStream(usize),
    /// We change the connection state of the source
    StateChange(SourceState),
    /// There is no event currently ready and we're asked to wait an amount of ms
    Empty(u64),
}

#[async_trait::async_trait]
pub(crate) trait Source {
    /// Pulls an event from the source if one exists
    /// determine the codec to be used
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply>;

    /// This callback is called when the data provided from
    /// pull_event did not create any events, this is needed for
    /// linked sources that require a 1:1 mapping between requests
    /// and responses, we're looking at you REST
    async fn on_empty_event(&mut self, _id: u64, _stream: usize) -> Result<()> {
        Ok(())
    }

    /// Send event back from source (for linked onramps)
    async fn reply_event(
        &mut self,
        _event: Event,
        _codec: &dyn Codec,
        _codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        Ok(())
    }

    /// Pulls metrics from the source
    fn metrics(&mut self, _t: u64) -> Vec<Event> {
        vec![]
    }

    /// Initializes the onramp (ideally this should be idempotent)
    async fn init(&mut self) -> Result<SourceState>;
    /// Graceful shutdown
    async fn terminate(&mut self) {}

    /// Trigger the circuit breaker on the source
    fn trigger_breaker(&mut self) {}
    /// Restore the circuit breaker on the source
    fn restore_breaker(&mut self) {}

    /// Acknowledge an event
    fn ack(&mut self, _id: u64) {}
    /// Fail an event
    fn fail(&mut self, _id: u64) {}

    /// Gives a human readable ID for the source
    fn id(&self) -> &TremorUrl;
    /// Is this source transactional or can acks/fails be ignored
    fn is_transactional(&self) -> bool {
        false
    }
}

fn make_error(source_id: String, e: &Error, original_id: u64) -> tremor_script::EventPayload {
    error!("[Source::{}] Error decoding event data: {}", source_id, e);
    let mut meta = Object::with_capacity(1);
    meta.insert_nocheck("error".into(), e.to_string().into());

    let mut data = Object::with_capacity(3);
    data.insert_nocheck("error".into(), e.to_string().into());
    data.insert_nocheck("event_id".into(), original_id.into());
    data.insert_nocheck("source_id".into(), source_id.into());
    (Value::from(data), Value::from(meta)).into()
}

pub(crate) struct SourceManager<T>
where
    T: Source,
{
    source_id: TremorUrl,
    source: T,
    rx: Receiver<onramp::Msg>,
    tx: Sender<onramp::Msg>,
    pp_template: Vec<String>,
    preprocessors: BTreeMap<usize, Preprocessors>,
    codec: Box<dyn Codec>,
    codec_map: HashMap<String, Box<dyn Codec>>,
    metrics_reporter: RampReporter,
    triggered: bool,
    pipelines_out: Vec<(TremorUrl, pipeline::Addr)>,
    pipelines_err: Vec<(TremorUrl, pipeline::Addr)>,
    err_required: bool,
    id: u64,
    is_transactional: bool,
    /// Unique Id for the source
    uid: u64,
}

impl<T> SourceManager<T>
where
    T: Source + Send + 'static + std::fmt::Debug,
{
    fn handle_pp(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        data: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>> {
        if let Some(preprocessors) = self.preprocessors.get_mut(&stream) {
            preprocess(
                preprocessors.as_mut_slice(),
                ingest_ns,
                data,
                &self.source_id,
            )
        } else {
            Err(format!(
                "[Source:{}] Failed to fetch preprocessors for stream {}",
                self.source_id, stream,
            )
            .into())
        }
    }

    async fn make_event_data(
        &mut self,
        stream: usize,
        ingest_ns: &mut u64,
        codec_override: Option<String>,
        data: Vec<u8>,
        meta: Option<StaticValue>, // See: https://github.com/rust-lang/rust/issues/63033
    ) -> Vec<Result<EventPayload>> {
        let mut results = vec![];
        match self.handle_pp(stream, ingest_ns, data) {
            Ok(data) => {
                let meta_value = meta.map_or_else(Value::object, |m| m.0);
                for d in data {
                    let line_value = EventPayload::try_new::<Option<Error>, _>(d, |mut_data| {
                        let codec_map = &mut self.codec_map;
                        let codec = codec_override
                            .as_ref()
                            .and_then(|codec_name| codec_map.get_mut(codec_name))
                            .unwrap_or(&mut self.codec);
                        let decoded = codec.decode(mut_data, *ingest_ns);
                        match decoded {
                            Ok(None) => Err(None),
                            Err(e) => Err(Some(e)),
                            Ok(Some(decoded)) => {
                                Ok(ValueAndMeta::from_parts(decoded, meta_value.clone()))
                            }
                        }
                    });
                    match line_value {
                        Ok(decoded) => results.push(Ok(decoded)),
                        Err(None) => (),
                        Err(Some(e)) => {
                            // TODO: add error context (with error handling update)
                            results.push(Err(e));
                        }
                    }
                }
            }
            Err(e) => {
                // record preprocessor failures too
                // TODO: add error context (with error handling update)
                results.push(Err(e));
            }
        }
        results
    }

    fn needs_pipeline_msg(&self) -> bool {
        self.pipelines_out.is_empty()
            || self.triggered
            || !self.rx.is_empty()
            || (self.err_required && self.pipelines_err.is_empty())
    }
    async fn handle_pipelines(&mut self) -> Result<bool> {
        loop {
            let msg = if self.needs_pipeline_msg() {
                self.rx.recv().await?
            } else {
                return Ok(false);
            };

            match msg {
                onramp::Msg::Connect(port, ps) => {
                    if port.eq_ignore_ascii_case(METRICS.as_ref()) {
                        if ps.len() > 1 {
                            warn!("[Source::{}] Connecting more than 1 metrics pipelines will only connect the latest.", self.source_id);
                        }
                        for p in ps {
                            info!(
                                "[Source::{}] Connecting {} as metrics pipeline.",
                                self.source_id, p.0
                            );
                            self.metrics_reporter.set_metrics_pipeline(p);
                        }
                    } else {
                        for p in ps {
                            let pipelines = if port == OUT {
                                &mut self.pipelines_out
                            } else if port == ERR {
                                &mut self.pipelines_err
                            } else {
                                return Err(format!(
                                    "Invalid Onramp Port: {}. Cannot connect.",
                                    port
                                )
                                .into());
                            };
                            let msg = pipeline::MgmtMsg::ConnectInput {
                                input_url: self.source_id.clone(),
                                target: ConnectTarget::Onramp(self.tx.clone()),
                                transactional: self.is_transactional,
                            };
                            p.1.send_mgmt(msg).await?;
                            pipelines.push(p);
                        }
                    }
                }
                onramp::Msg::Disconnect { id, tx } => {
                    for (_, p) in self
                        .pipelines_out
                        .iter()
                        .chain(self.pipelines_err.iter())
                        .filter(|(pid, _)| pid == &id)
                    {
                        p.send_mgmt(pipeline::MgmtMsg::DisconnectInput(id.clone()))
                            .await?;
                    }

                    let mut empty_pipelines = true;
                    self.pipelines_out.retain(|(pipeline, _)| pipeline != &id);
                    empty_pipelines &= self.pipelines_out.is_empty();
                    self.pipelines_err.retain(|(pipeline, _)| pipeline != &id);
                    empty_pipelines &= self.pipelines_err.is_empty();

                    tx.send(empty_pipelines).await?;
                    if empty_pipelines {
                        self.source.terminate().await;
                        return Ok(true);
                    }
                }
                onramp::Msg::Cb(CbAction::Fail, ids) => {
                    // TODO: stream handling
                    // when failing, we use the earliest/min event within the tracked set
                    if let Some((_stream_id, id)) = ids.get_min_by_source(self.uid) {
                        self.source.fail(id);
                    }
                }
                // Circuit breaker explicit acknowledgement of an event
                onramp::Msg::Cb(CbAction::Ack, ids) => {
                    // TODO: stream handling
                    // when acknowledging, we use the latest/max event within the tracked set
                    if let Some((_stream_id, id)) = ids.get_max_by_source(self.uid) {
                        self.source.ack(id);
                    }
                }
                // Circuit breaker source failure - triggers close
                onramp::Msg::Cb(CbAction::Close, _ids) => {
                    self.source.trigger_breaker();
                    self.triggered = true;
                }
                //Circuit breaker source recovers - triggers open
                onramp::Msg::Cb(CbAction::Open, _ids) => {
                    self.source.restore_breaker();
                    self.triggered = false;
                }
                onramp::Msg::Cb(CbAction::None, _ids) => {}

                onramp::Msg::Response(event) => {
                    if let Err(e) = self
                        .source
                        .reply_event(event, self.codec.as_ref(), &self.codec_map)
                        .await
                    {
                        error!(
                            "[Source::{}] [Onramp] failed to reply event from source: {}",
                            self.source_id, e
                        );
                    }
                }
            }
        }
    }

    pub(crate) async fn transmit_event(
        &mut self,
        data: EventPayload,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
        port: Cow<'static, str>,
    ) -> bool {
        let event = Event {
            // TODO: use EventIdGen and stream handling
            id: EventId::new(self.uid, DEFAULT_STREAM_ID, self.id),
            data,
            ingest_ns,
            // TODO make origin_uri non-optional here too?
            origin_uri: Some(origin_uri),
            transactional: self.is_transactional,
            ..Event::default()
        };
        let mut error = false;
        self.id += 1;
        let pipelines = if OUT == port {
            &mut self.pipelines_out
        } else if ERR == port {
            &mut self.pipelines_err
        } else {
            return false;
        };
        if let Some((last, pipelines)) = pipelines.split_last_mut() {
            if let Some(t) = self.metrics_reporter.periodic_flush(ingest_ns) {
                self.metrics_reporter.send(self.source.metrics(t))
            }

            // TODO refactor metrics_reporter to do this by port now
            if ERR == port {
                self.metrics_reporter.increment_err();
            } else {
                self.metrics_reporter.increment_out();
            }

            for (input, addr) in pipelines {
                if let Some(input) = input.instance_port() {
                    if let Err(e) = addr
                        .send(pipeline::Msg::Event {
                            input: input.to_string().into(),
                            event: event.clone(),
                        })
                        .await
                    {
                        error!(
                            "[Source::{}] [Onramp] failed to send to pipeline: {}",
                            self.source_id, e
                        );
                        error = true;
                    }
                }
            }
            if let Some(input) = last.0.instance_port() {
                if let Err(e) = last
                    .1
                    .send(pipeline::Msg::Event {
                        input: input.to_string().into(),
                        event,
                    })
                    .await
                {
                    error!(
                        "[Source::{}] [Onramp] failed to send to pipeline: {}",
                        self.source_id, e
                    );
                    error = true;
                }
            }
        }
        error
    }

    async fn new(mut source: T, config: OnrampConfig<'_>) -> Result<(Self, Sender<onramp::Msg>)> {
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress past
        // the sending.
        // This prevents a livelock where the pipeline is waiting for a full channel to send data to
        // the source and the source is waiting for a full channel to send data to the pipeline.
        // We prevent unbounded growth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forward flow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        let (tx, rx) = unbounded();
        let codec = codec::lookup(&config.codec)?;
        let mut resolved_codec_map = codec::builtin_codec_map();
        // override the builtin map
        for (k, v) in config.codec_map {
            resolved_codec_map.insert(k, codec::lookup(&v)?);
        }
        let pp_template = config.processors.pre.to_vec();
        let mut preprocessors = BTreeMap::new();
        preprocessors.insert(0, make_preprocessors(&&pp_template)?);

        source.init().await?;
        let is_transactional = source.is_transactional();
        Ok((
            Self {
                source_id: source.id().clone(),
                pp_template,
                source,
                rx,
                tx: tx.clone(),
                preprocessors,
                //postprocessors,
                codec,
                codec_map: resolved_codec_map,
                metrics_reporter: config.metrics_reporter,
                triggered: false,
                id: 0,
                pipelines_out: Vec::new(),
                pipelines_err: Vec::new(),
                uid: config.onramp_uid,
                is_transactional,
                err_required: config.err_required,
            },
            tx,
        ))
    }

    async fn start(source: T, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let name = source.id().short_id("src");
        let (manager, tx) = SourceManager::new(source, config).await?;
        task::Builder::new().name(name).spawn(manager.run())?;
        Ok(tx)
    }

    async fn route_result(
        &mut self,
        results: Vec<Result<tremor_script::EventPayload>>,
        original_id: u64,
        ingest_ns: u64,
        origin_uri: EventOriginUri,
    ) -> bool {
        let mut error = false;
        for result in results {
            let (port, data) = result.map_or_else(
                |e| (ERR, make_error(self.source_id.to_string(), &e, original_id)),
                |data| (OUT, data),
            );
            error |= self
                .transmit_event(data, ingest_ns, origin_uri.clone(), port)
                .await;
        }
        error
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<()> {
        loop {
            if self.handle_pipelines().await? {
                return Ok(());
            }

            let pipelines_out_empty = self.pipelines_out.is_empty();

            if !self.triggered && !pipelines_out_empty {
                match self.source.pull_event(self.id).await {
                    Ok(SourceReply::StartStream(id)) => {
                        self.preprocessors
                            .insert(id, make_preprocessors(&self.pp_template)?);
                    }
                    Ok(SourceReply::EndStream(id)) => {
                        self.preprocessors.remove(&id);
                    }
                    Ok(SourceReply::Structured { origin_uri, data }) => {
                        let ingest_ns = nanotime();

                        self.transmit_event(data, ingest_ns, origin_uri, OUT).await;
                    }
                    Ok(SourceReply::BatchData {
                        mut origin_uri,
                        batch_data,
                        codec_override,
                        stream,
                    }) => {
                        for (data, meta_data) in batch_data {
                            origin_uri.maybe_set_uid(self.uid);
                            let mut ingest_ns = nanotime();

                            let original_id = self.id;
                            let results = self
                                .make_event_data(
                                    stream,
                                    &mut ingest_ns,
                                    codec_override.clone(),
                                    data,
                                    meta_data.map(StaticValue),
                                )
                                .await;
                            if results.is_empty() {
                                self.source.on_empty_event(original_id, stream).await?;
                            }

                            let error = self
                                .route_result(results, original_id, ingest_ns, origin_uri.clone())
                                .await;
                            // We ONLY fail on transmit errors as preprocessor errors might be
                            // problematic
                            if error {
                                self.source.fail(original_id);
                            }
                        }
                    }
                    Ok(SourceReply::Data {
                        mut origin_uri,
                        data,
                        meta,
                        codec_override,
                        stream,
                    }) => {
                        origin_uri.maybe_set_uid(self.uid);
                        let mut ingest_ns = nanotime();

                        let original_id = self.id;
                        let results = self
                            .make_event_data(
                                stream,
                                &mut ingest_ns,
                                codec_override,
                                data,
                                meta.map(StaticValue),
                            )
                            .await;
                        if results.is_empty() {
                            self.source.on_empty_event(original_id, stream).await?;
                        }

                        let error = self
                            .route_result(results, original_id, ingest_ns, origin_uri)
                            .await;

                        // We ONLY fail on transmit errors as preprocessor errors might be
                        // problematic
                        if error {
                            self.source.fail(original_id);
                        }
                    }
                    Ok(SourceReply::StateChange(SourceState::Disconnected)) => return Ok(()),
                    Ok(SourceReply::StateChange(SourceState::Connected)) => (),
                    Ok(SourceReply::Empty(sleep_ms)) => {
                        task::sleep(Duration::from_millis(sleep_ms)).await
                    }
                    Err(e) => {
                        warn!("[Source::{}] Error: {}", self.source_id, e);
                        self.metrics_reporter.increment_err();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FakeSource {
        url: TremorUrl,
    }

    #[async_trait::async_trait]
    impl Source for FakeSource {
        async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
            Ok(SourceReply::Data {
                origin_uri: EventOriginUri::default(),
                codec_override: None,
                stream: 0,
                data: "data".as_bytes().to_vec(),
                meta: None,
            })
        }

        async fn init(&mut self) -> Result<SourceState> {
            Ok(SourceState::Connected)
        }

        fn id(&self) -> &TremorUrl {
            &self.url
        }
    }

    #[async_std::test]
    async fn fake_source_manager_connect_cb() -> Result<()> {
        let onramp_url = TremorUrl::from_onramp_id("fake")?;
        let s = FakeSource {
            url: onramp_url.clone(),
        };
        let o_config = OnrampConfig {
            onramp_uid: 1,
            codec: "string",
            codec_map: HashMap::new(),
            processors: Processors::default(),
            metrics_reporter: RampReporter::new(onramp_url.clone(), None),
            is_linked: false,
            err_required: false,
        };
        let (sm, sender) = SourceManager::new(s, o_config).await?;
        let handle = task::spawn(sm.run());

        let pipeline_url = TremorUrl::parse("/pipeline/bla/01/in")?;
        let (tx1, rx1) = async_channel::unbounded();
        let (tx2, _rx2) = async_channel::unbounded();
        let (tx3, rx3) = async_channel::unbounded();
        let addr = pipeline::Addr::new(tx1, tx2, tx3, pipeline_url.clone());

        // trigger the source to ensure it is not being pulled from
        sender
            .send(onramp::Msg::Cb(CbAction::Close, EventId::default()))
            .await?;

        // connect our fake pipeline
        sender
            .send(onramp::Msg::Connect(
                OUT,
                vec![(pipeline_url.clone(), addr)],
            ))
            .await?;
        let answer = rx3.recv().await?;
        match answer {
            pipeline::MgmtMsg::ConnectInput {
                input_url,
                transactional,
                ..
            } => {
                assert_eq!(input_url, onramp_url);
                assert_eq!(transactional, false);
            }
            _ => return Err("Invalid Pipeline connect answer.".into()),
        }
        // ensure no events are pulled as long as we are not opened yet
        task::sleep(Duration::from_millis(200)).await;
        assert!(rx1.try_recv().is_err()); // nothing put into connected pipeline yet

        // send the initial open event
        sender
            .send(onramp::Msg::Cb(CbAction::Open, EventId::default()))
            .await?;

        // wait some time
        task::sleep(Duration::from_millis(200)).await;
        assert!(rx1.len() > 0);

        let (tx4, rx4) = async_channel::unbounded();
        // disconnect to break the busy loop
        sender
            .send(onramp::Msg::Disconnect {
                id: pipeline_url,
                tx: tx4,
            })
            .await?;
        assert_eq!(rx4.recv().await?, true);
        handle.cancel().await;
        Ok(())
    }

    #[test]
    fn make_error() {
        let source_id = "snot".to_string();
        let e = Error::from("oh no!");
        let original_id = 5;
        let error_result = super::make_error(source_id.clone(), &e, original_id);
        let mut expec_meta = Object::with_capacity(1);
        expec_meta.insert_nocheck("error".into(), e.to_string().into());

        let mut expec_data = Object::with_capacity(3);
        expec_data.insert_nocheck("error".into(), e.to_string().into());
        expec_data.insert_nocheck("event_id".into(), original_id.into());
        expec_data.insert_nocheck("source_id".into(), source_id.into());

        assert_eq!(error_result.suffix().value(), &Value::from(expec_data));
        assert_eq!(error_result.suffix().meta(), &Value::from(expec_meta));

        // testing with a second set of inputs
        let source_id = "tremor-source-testing".to_string();
        let e = Error::from("error oh no!!!");
        let original_id = 7;
        let error_result = super::make_error(source_id.clone(), &e, original_id);
        let mut expec_meta = Object::with_capacity(1);
        expec_meta.insert_nocheck("error".into(), e.to_string().into());

        let mut expec_data = Object::with_capacity(3);
        expec_data.insert_nocheck("error".into(), e.to_string().into());
        expec_data.insert_nocheck("event_id".into(), original_id.into());
        expec_data.insert_nocheck("source_id".into(), source_id.into());

        assert_eq!(error_result.suffix().value(), &Value::from(expec_data));
        assert_eq!(error_result.suffix().meta(), &Value::from(expec_meta));
    }
}
