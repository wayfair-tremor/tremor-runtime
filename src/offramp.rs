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

use crate::codec::Codec;
use crate::errors::Result;
use crate::metrics::RampReporter;
use crate::permge::PriorityMerge;
use crate::pipeline;
use crate::registry::ServantId;
use crate::sink::{
    self, amqp, blackhole, cb, debug, dns, elastic, exit, file, gcs, gpub, handle_response, kafka,
    kv, nats, newrelic, otel, postgres, rest, stderr, stdout, tcp, udp, ws,
};
use crate::source::Processors;
use crate::url::ports::{IN, METRICS};
use crate::url::TremorUrl;
use crate::{Event, OpConfig};
use async_channel::{self, bounded, unbounded};
use async_std::stream::StreamExt; // for .next() on PriorityMerge
use async_std::task::{self, JoinHandle};
use beef::Cow;
use halfbrown::HashMap;
use pipeline::ConnectTarget;
use std::borrow::{Borrow, BorrowMut};
use std::fmt;
use tremor_common::ids::OfframpIdGen;
use tremor_common::time::nanotime;

#[derive(Debug)]
pub enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Signal(Event),
    Connect {
        port: Cow<'static, str>,
        id: TremorUrl,
        addr: Box<pipeline::Addr>,
    },
    Disconnect {
        port: Cow<'static, str>,
        id: TremorUrl,
        tx: async_channel::Sender<bool>,
    },
    Terminate,
}

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;
pub type Addr = async_channel::Sender<Msg>;

#[async_trait::async_trait]
pub trait Offramp: Send {
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        offramp_uid: u64,
        offramp_url: &TremorUrl,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: async_channel::Sender<sink::Reply>,
    ) -> Result<()>;
    async fn on_event(
        &mut self,
        codec: &mut dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        input: &str,
        event: Event,
    ) -> Result<()>;
    #[cfg(not(tarpaulin_include))]
    async fn on_signal(&mut self, _signal: Event) -> Option<Event> {
        None
    }
    async fn terminate(&mut self) {}
    fn default_codec(&self) -> &str;
    fn add_pipeline(&mut self, id: TremorUrl, addr: pipeline::Addr);
    fn remove_pipeline(&mut self, id: TremorUrl) -> bool;
    fn add_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorUrl, addr: pipeline::Addr);
    fn remove_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorUrl) -> bool;
    #[cfg(not(tarpaulin_include))]
    fn is_active(&self) -> bool {
        true
    }
    #[cfg(not(tarpaulin_include))]
    fn auto_ack(&self) -> bool {
        true
    }
}

pub trait Impl {
    fn from_config(config: &Option<OpConfig>) -> Result<Box<dyn Offramp>>;
}

// just a lookup
#[cfg(not(tarpaulin_include))]
pub fn lookup(name: &str, config: &Option<OpConfig>) -> Result<Box<dyn Offramp>> {
    match name {
        "amqp" => amqp::Amqp::from_config(config),
        "blackhole" => blackhole::Blackhole::from_config(config),
        "cb" => cb::Cb::from_config(config),
        "debug" => debug::Debug::from_config(config),
        "dns" => dns::Dns::from_config(config),
        "elastic" => elastic::Elastic::from_config(config),
        "exit" => exit::Exit::from_config(config),
        "file" => file::File::from_config(config),
        "kafka" => kafka::Kafka::from_config(config),
        "kv" => kv::Kv::from_config(config),
        "nats" => nats::Nats::from_config(config),
        "newrelic" => newrelic::NewRelic::from_config(config),
        "otel" => otel::OpenTelemetry::from_config(config),
        "postgres" => postgres::Postgres::from_config(config),
        "rest" => rest::Rest::from_config(config),
        "stderr" => stderr::StdErr::from_config(config),
        "stdout" => stdout::StdOut::from_config(config),
        "tcp" => tcp::Tcp::from_config(config),
        "udp" => udp::Udp::from_config(config),
        "ws" => ws::Ws::from_config(config),
        "gcs" => gcs::GoogleCloudStorage::from_config(config),
        "gpub" => gpub::GoogleCloudPubSub::from_config(config),
        _ => Err(format!("Offramp {} not known", name).into()),
    }
}

pub(crate) struct Create {
    pub id: ServantId,
    pub offramp: Box<dyn Offramp>,
    pub codec: Box<dyn Codec>,
    pub codec_map: halfbrown::HashMap<String, Box<dyn Codec>>,
    pub preprocessors: Vec<String>,
    pub postprocessors: Vec<String>,
    pub metrics_reporter: RampReporter,
    pub is_linked: bool,
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOfframp({})", self.id)
    }
}

/// This is control plane
pub(crate) enum ManagerMsg {
    Create(async_channel::Sender<Result<Addr>>, Box<Create>),
    Stop,
}

#[derive(Debug, Default)]
pub(crate) struct Manager {
    qsize: usize,
}

async fn send_to_pipelines(
    offramp_id: &TremorUrl,
    pipelines: &mut HashMap<TremorUrl, pipeline::Addr>,
    e: Event,
) {
    let mut i = pipelines.values_mut();
    if let Some(first) = i.next() {
        for p in i {
            if let Err(e) = p.send_insight(e.clone()).await {
                error!("[Offramp::{}] Counterflow error: {}", offramp_id, e);
            };
        }
        if let Err(e) = first.send_insight(e).await {
            error!("[Offramp::{}] Counterflow error: {}", offramp_id, e);
        };
    }
}

pub(crate) enum OfframpMsg {
    Msg(Msg),
    Reply(sink::Reply),
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }

    #[allow(clippy::too_many_lines)]
    async fn offramp_task(
        &self,
        r: async_channel::Sender<Result<Addr>>,
        Create {
            mut codec,
            codec_map,
            mut offramp,
            preprocessors,
            postprocessors,
            mut metrics_reporter,
            is_linked,
            id,
        }: Create,
        offramp_uid: u64,
    ) -> Result<()> {
        let (msg_tx, msg_rx) = bounded::<Msg>(self.qsize);
        let (cf_tx, cf_rx) = unbounded::<sink::Reply>(); // we might need to wrap that somehow, but *shrug*

        if let Err(e) = offramp
            .start(
                offramp_uid,
                &id,
                codec.borrow(),
                &codec_map,
                Processors {
                    pre: &preprocessors,
                    post: &postprocessors,
                },
                is_linked,
                cf_tx.clone(),
            )
            .await
        {
            error!("Failed to create offramp {}: {}", id, e);
            return Err(e);
        }
        // merge channels and prioritize contraflow/insight events
        let m_rx = msg_rx.map(OfframpMsg::Msg);
        let c_rx = cf_rx.map(OfframpMsg::Reply);
        let mut to_and_from_offramp_rx = PriorityMerge::new(c_rx, m_rx);

        let offramp_url = id.clone();
        let offramp_addr = msg_tx.clone();

        task::spawn::<_, Result<()>>(async move {
            let mut pipelines: HashMap<TremorUrl, pipeline::Addr> = HashMap::new();

            // for linked offramp output (port to pipeline(s) mapping)
            let mut dest_pipelines: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>> =
                HashMap::new();

            info!("[Offramp::{}] started", offramp_url);

            while let Some(offramp_msg) = to_and_from_offramp_rx.next().await {
                match offramp_msg {
                    OfframpMsg::Msg(m) => {
                        match m {
                            Msg::Signal(signal) => {
                                if let Some(insight) = offramp.on_signal(signal).await {
                                    send_to_pipelines(&offramp_url, &mut pipelines, insight).await;
                                }
                            }
                            Msg::Event { event, input } => {
                                let ingest_ns = event.ingest_ns;
                                let transactional = event.transactional;
                                let ids = event.id.clone();

                                metrics_reporter.periodic_flush(ingest_ns);
                                metrics_reporter.increment_in();

                                let c: &mut dyn Codec = codec.borrow_mut();
                                let fail = if let Err(err) =
                                    offramp.on_event(c, &codec_map, input.borrow(), event).await
                                {
                                    error!("[Offramp::{}] On Event error: {}", offramp_url, err);
                                    metrics_reporter.increment_err();
                                    true
                                } else {
                                    metrics_reporter.increment_out();
                                    false
                                };
                                // always send a fail, if on_event errored and the event is transactional
                                // assuming if a sink fails, it didnt already sent a fail insight via reply_channel
                                // even if it did, double fails or double acks should not lead to trouble
                                // this will prevent fail insights being swallowed here
                                // sinks need to take care of sending acks themselves. Deal with it.
                                if (fail || offramp.auto_ack()) && transactional {
                                    let e = Event::ack_or_fail(!fail, ingest_ns, ids);
                                    send_to_pipelines(&offramp_url, &mut pipelines, e).await;
                                }
                            }
                            Msg::Connect { port, id, addr } => {
                                if port.eq_ignore_ascii_case(IN.as_ref()) {
                                    // connect incoming pipeline
                                    info!(
                                        "[Offramp::{}] Connecting pipeline {} to incoming port {}",
                                        offramp_url, id, port
                                    );
                                    // if we are linked we send CB when an output is connected
                                    // TODO: if we are linked we should only send CB restore/break events if we receive one from the dest_pipelines
                                    if !is_linked || !dest_pipelines.is_empty() {
                                        let insight = Event::restore_or_break(
                                            offramp.is_active(),
                                            nanotime(),
                                        );
                                        if let Err(e) = addr.send_insight(insight).await {
                                            error!(
                                                "[Offramp::{}] Could not send initial insight to {}: {}",
                                                offramp_url, id, e
                                            );
                                        };
                                    }

                                    pipelines.insert(id.clone(), (*addr).clone());
                                    offramp.add_pipeline(id, *addr);
                                } else if port.eq_ignore_ascii_case(METRICS.as_ref()) {
                                    info!(
                                        "[Offramp::{}] Connecting system metrics pipeline {}",
                                        offramp_url, id
                                    );
                                    metrics_reporter.set_metrics_pipeline((id, *addr));
                                } else {
                                    // connect pipeline to outgoing port
                                    info!(
                                        "[Offramp::{}] Connecting pipeline {} via outgoing port {}",
                                        offramp_url, id, port
                                    );
                                    let p = (id.clone(), (*addr).clone());
                                    if let Some(port_ps) = dest_pipelines.get_mut(&port) {
                                        port_ps.push(p);
                                    } else {
                                        dest_pipelines.insert(port.clone(), vec![p]);
                                    }
                                    offramp.add_dest_pipeline(port, id.clone(), (*addr).clone());

                                    // send connectInput msg to pipeline
                                    if let Err(e) = addr
                                        .send_mgmt(pipeline::MgmtMsg::ConnectInput {
                                            input_url: offramp_url.clone(),
                                            target: ConnectTarget::Offramp(offramp_addr.clone()),
                                            transactional: false, // TODO: Linked Offramps do not support insights yet
                                        })
                                        .await
                                    {
                                        error!("[Offramp::{}] Error connecting this offramp as input to pipeline {}: {}", offramp_url, &id, e);
                                    }

                                    // send a CB restore/break if we have both an input an an output connected
                                    // to all the connected inputs
                                    if is_linked && !pipelines.is_empty() {
                                        let insight = Event::restore_or_break(
                                            offramp.is_active(),
                                            nanotime(),
                                        );
                                        let mut iter = pipelines.iter();
                                        if let Some((input_id, input_addr)) = iter.next() {
                                            for (input_id, input_addr) in iter {
                                                if let Err(e) =
                                                    input_addr.send_insight(insight.clone()).await
                                                {
                                                    error!(
                                                        "[Offramp::{}] Could not send initial insight to {}: {}",
                                                        offramp_url, input_id, e
                                                    );
                                                };
                                            }
                                            if let Err(e) = input_addr.send_insight(insight).await {
                                                error!(
                                                    "[Offramp::{}] Could not send initial insight to {}: {}",
                                                    offramp_url, input_id, e
                                                );
                                            };
                                        }
                                    }
                                }
                            }
                            Msg::Disconnect { port, id, tx } => {
                                info!(
                                    "[Offramp::{}] Disconnecting pipeline {} on port {}",
                                    offramp_url, id, port
                                );
                                let marked_done = if port.eq_ignore_ascii_case(IN.as_ref()) {
                                    pipelines.remove(&id);
                                    offramp.remove_pipeline(id.clone())
                                } else if port.eq_ignore_ascii_case(METRICS.as_ref()) {
                                    warn!(
                                        "[Offramp::{}] Cannot unlink pipeline {} from port {}",
                                        offramp_url, &id, &port
                                    );
                                    false
                                } else {
                                    if let Some(port_ps) = dest_pipelines.get_mut(&port) {
                                        port_ps.retain(|(url, _)| url != &id)
                                    }
                                    offramp.remove_dest_pipeline(port.clone(), id.clone())
                                };

                                tx.send(marked_done).await?;
                                info!(
                                    "[Offramp::{}] Pipeline {} disconnected from port {}",
                                    offramp_url, &id, &port
                                );
                                if marked_done {
                                    info!("[Offramp::{}] Marked as done ", offramp_url);
                                    offramp.terminate().await;
                                    break;
                                }
                            }
                            Msg::Terminate => {
                                info!("[Offramp::{}] Terminating...", offramp_url);
                                offramp.terminate().await;
                                break;
                            }
                        }
                    }
                    OfframpMsg::Reply(sink::Reply::Insight(event)) => {
                        send_to_pipelines(&offramp_url, &mut pipelines, event).await
                    }
                    OfframpMsg::Reply(sink::Reply::Response(port, event)) => {
                        if let Some(pipelines) = dest_pipelines.get_mut(&port) {
                            if let Err(e) = handle_response(event, pipelines.iter()).await {
                                error!("[Offramp::{}] Response error: {}", offramp_url, e)
                            }
                        }
                    }
                }
            }
            info!("[Offramp::{}] stopped", offramp_url);
            Ok(())
        });
        r.send(Ok(msg_tx)).await?;
        Ok(())
    }

    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(self.qsize);
        let h = task::spawn(async move {
            info!("Offramp manager started");
            let mut offramp_id_gen = OfframpIdGen::new();
            while let Ok(msg) = rx.recv().await {
                match msg {
                    ManagerMsg::Stop => {
                        info!("Stopping onramps...");
                        break;
                    }
                    ManagerMsg::Create(r, c) => {
                        self.offramp_task(r, *c, offramp_id_gen.next_id()).await?
                    }
                };
                info!("Stopping offramps...");
            }
            info!("Offramp manager stopped.");
            Ok(())
        });

        (h, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QSIZE;

    #[derive(Debug)]
    enum FakeOfframpMsg {
        Event(Event),
        Signal(Event),
        AddPipeline(TremorUrl, pipeline::Addr),
        RemovePipeline(TremorUrl),
        AddDestPipeline(Cow<'static, str>, TremorUrl, pipeline::Addr),
        RemoveDestPipeline(Cow<'static, str>, TremorUrl),
        Start(u64),
        Terminate,
    }
    struct FakeOfframp {
        pipelines: usize,
        sender: async_channel::Sender<FakeOfframpMsg>,
    }

    impl FakeOfframp {
        fn new(sender: async_channel::Sender<FakeOfframpMsg>) -> Self {
            Self {
                pipelines: 0,
                sender,
            }
        }
    }

    #[async_trait::async_trait]
    impl Offramp for FakeOfframp {
        async fn start(
            &mut self,
            offramp_uid: u64,
            _offramp_url: &TremorUrl,
            _codec: &dyn Codec,
            _codec_map: &HashMap<String, Box<dyn Codec>>,
            _processors: Processors<'_>,
            _is_linked: bool,
            _reply_channel: async_channel::Sender<sink::Reply>,
        ) -> Result<()> {
            self.sender.send(FakeOfframpMsg::Start(offramp_uid)).await?;
            Ok(())
        }

        async fn on_event(
            &mut self,
            _codec: &mut dyn Codec,
            _codec_map: &HashMap<String, Box<dyn Codec>>,
            _input: &str,
            event: Event,
        ) -> Result<()> {
            self.sender.send(FakeOfframpMsg::Event(event)).await?;
            Ok(())
        }

        async fn on_signal(&mut self, signal: Event) -> Option<Event> {
            self.sender
                .send(FakeOfframpMsg::Signal(signal))
                .await
                .unwrap();
            None
        }

        fn default_codec(&self) -> &str {
            "json"
        }

        fn add_pipeline(&mut self, id: TremorUrl, addr: pipeline::Addr) {
            self.pipelines += 1;
            let sender = self.sender.clone();
            task::block_on(async {
                sender
                    .send(FakeOfframpMsg::AddPipeline(id, addr))
                    .await
                    .unwrap();
            })
        }

        fn remove_pipeline(&mut self, id: TremorUrl) -> bool {
            self.pipelines -= 1;
            let sender = self.sender.clone();
            task::block_on(async {
                sender
                    .send(FakeOfframpMsg::RemovePipeline(id))
                    .await
                    .unwrap();
            });
            self.pipelines == 0
        }

        fn add_dest_pipeline(
            &mut self,
            port: Cow<'static, str>,
            id: TremorUrl,
            addr: pipeline::Addr,
        ) {
            self.pipelines += 1;
            let sender = self.sender.clone();
            task::block_on(async {
                sender
                    .send(FakeOfframpMsg::AddDestPipeline(port, id, addr))
                    .await
                    .unwrap();
            })
        }

        fn remove_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorUrl) -> bool {
            self.pipelines -= 1;
            let sender = self.sender.clone();
            task::block_on(async {
                sender
                    .send(FakeOfframpMsg::RemoveDestPipeline(port, id))
                    .await
                    .unwrap();
            });
            self.pipelines == 0
        }

        async fn terminate(&mut self) {
            self.sender.send(FakeOfframpMsg::Terminate).await.unwrap()
        }
    }

    #[async_std::test]
    async fn offramp_lifecycle_test() -> Result<()> {
        let mngr = Manager::new(QSIZE);
        let (handle, sender) = mngr.start();
        let (tx, rx) = async_channel::bounded(1);
        let codec = crate::codec::lookup("json")?;
        let id = TremorUrl::parse("/offramp/fake/instance")?;
        let ramp_reporter = RampReporter::new(id.clone(), Some(1_000_000_000));
        let (offramp_tx, offramp_rx) = async_channel::unbounded();
        let offramp = FakeOfframp::new(offramp_tx);
        let create = ManagerMsg::Create(
            tx,
            Box::new(Create {
                id,
                codec,
                codec_map: HashMap::new(),
                preprocessors: vec!["lines".into()],
                postprocessors: vec!["lines".into()],
                metrics_reporter: ramp_reporter,
                offramp: Box::new(offramp),
                is_linked: true,
            }),
        );
        sender.send(create).await?;
        let offramp_sender = rx.recv().await??;
        match offramp_rx.recv().await? {
            FakeOfframpMsg::Start(id) => {
                println!("started with id: {}", id);
            }
            e => assert!(false, "Expected start msg, got {:?}", e),
        }

        let fake_pipeline_id = TremorUrl::parse("/pipeline/fake/instance/out")?;
        let (tx, _rx) = async_channel::unbounded();
        let (cf_tx, _cf_rx) = async_channel::unbounded();
        let (mgmt_tx, _mgmt_rx) = async_channel::unbounded();

        let fake_pipeline = Box::new(pipeline::Addr::new(
            tx,
            cf_tx,
            mgmt_tx,
            fake_pipeline_id.clone(),
        ));
        // connect incoming pipeline
        offramp_sender
            .send(Msg::Connect {
                port: IN,
                id: fake_pipeline_id.clone(),
                addr: fake_pipeline.clone(),
            })
            .await?;
        match offramp_rx.recv().await? {
            FakeOfframpMsg::AddPipeline(id, _addr) => {
                assert_eq!(fake_pipeline_id, id);
            }
            e => {
                assert!(false, "Expected add pipeline msg, got {:?}", e)
            }
        }

        // send event
        offramp_sender
            .send(Msg::Event {
                input: IN,
                event: Event::default(),
            })
            .await?;
        match offramp_rx.recv().await? {
            FakeOfframpMsg::Event(_event) => {}
            e => assert!(false, "Expected event msg, got {:?}", e),
        }

        let (disc_tx, disc_rx) = async_channel::bounded(1);
        offramp_sender
            .send(Msg::Disconnect {
                port: IN,
                id: fake_pipeline_id.clone(),
                tx: disc_tx,
            })
            .await?;
        assert!(
            disc_rx.recv().await?,
            "expected true, as nothing is connected anymore"
        );
        match offramp_rx.recv().await? {
            FakeOfframpMsg::RemovePipeline(id) => {
                assert_eq!(fake_pipeline_id, id);
            }
            e => {
                assert!(false, "Expected terminate msg, got {:?}", e)
            }
        }
        // stop shit
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }
}
