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
use crate::errors::{Error, Result};
use crate::permge::{PriorityMerge, M};
use crate::registry::ServantId;
use crate::repository::PipelineArtefact;
use crate::url::TremorUrl;
use crate::{offramp, onramp};
use async_channel::{bounded, unbounded};
use async_std::stream::StreamExt;
use async_std::task::{self, JoinHandle};
use beef::Cow;
use std::fmt;
use std::time::Duration;
use tremor_common::ids::OperatorIdGen;
use tremor_common::time::nanotime;
use tremor_pipeline::errors::ErrorKind as PipelineErrorKind;
use tremor_pipeline::{CbAction, Event, ExecutableGraph, SignalKind};

const TICK_MS: u64 = 100;
pub(crate) type Sender = async_channel::Sender<ManagerMsg>;
type Inputs = halfbrown::HashMap<TremorUrl, (bool, Input)>;
type Dests = halfbrown::HashMap<Cow<'static, str>, Vec<(TremorUrl, Dest)>>;
type Eventset = Vec<(Cow<'static, str>, Event)>;
/// Address for a pipeline
#[derive(Clone)]
pub struct Addr {
    addr: async_channel::Sender<Msg>,
    cf_addr: async_channel::Sender<CfMsg>,
    mgmt_addr: async_channel::Sender<MgmtMsg>,
    id: ServantId,
}

impl Addr {
    /// creates a new address
    pub(crate) fn new(
        addr: async_channel::Sender<Msg>,
        cf_addr: async_channel::Sender<CfMsg>,
        mgmt_addr: async_channel::Sender<MgmtMsg>,
        id: ServantId,
    ) -> Self {
        Self {
            addr,
            cf_addr,
            mgmt_addr,
            id,
        }
    }
    #[cfg(not(tarpaulin_include))]
    pub fn len(&self) -> usize {
        self.addr.len()
    }
    #[cfg(not(tarpaulin_include))]
    pub fn id(&self) -> &ServantId {
        &self.id
    }

    pub(crate) async fn send_insight(&self, event: Event) -> Result<()> {
        Ok(self.cf_addr.send(CfMsg::Insight(event)).await?)
    }

    pub(crate) async fn send(&self, msg: Msg) -> Result<()> {
        Ok(self.addr.send(msg).await?)
    }

    #[cfg(not(tarpaulin_include))]
    pub(crate) fn try_send(&self, msg: Msg) -> Result<()> {
        Ok(self.addr.try_send(msg)?)
    }

    pub(crate) async fn send_mgmt(&self, msg: MgmtMsg) -> Result<()> {
        Ok(self.mgmt_addr.send(msg).await?)
    }
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Addr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pipeline({})", self.id)
    }
}
#[derive(Debug)]
pub(crate) enum CfMsg {
    Insight(Event),
}

#[derive(Debug)]
pub(crate) enum ConnectTarget {
    Onramp(onramp::Addr),
    Offramp(offramp::Addr),
    Pipeline(Box<Addr>),
}

#[derive(Debug)]
pub(crate) enum MgmtMsg {
    /// input can only ever be connected to the `in` port, so no need to include it here
    ConnectInput {
        input_url: TremorUrl,
        target: ConnectTarget,
        /// should we send insights to this input
        transactional: bool,
    },
    ConnectOutput {
        port: Cow<'static, str>,
        output_url: TremorUrl,
        target: ConnectTarget,
    },
    DisconnectOutput(Cow<'static, str>, TremorUrl),
    DisconnectInput(TremorUrl),
    // only for testing
    Echo(async_channel::Sender<()>),
}

#[derive(Debug)]
pub(crate) enum Msg {
    Event {
        event: Event,
        input: Cow<'static, str>,
    },
    Signal(Event),
}

#[derive(Debug)]
pub enum Dest {
    Offramp(offramp::Addr),
    Pipeline(Addr),
    LinkedOnramp(onramp::Addr),
}

impl Dest {
    pub async fn send_event(&mut self, input: Cow<'static, str>, event: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Event { input, event }).await?,
            Self::Pipeline(addr) => addr.send(Msg::Event { input, event }).await?,
            Self::LinkedOnramp(addr) => addr.send(onramp::Msg::Response(event)).await?,
        }
        Ok(())
    }
    pub async fn send_signal(&mut self, signal: Event) -> Result<()> {
        match self {
            Self::Offramp(addr) => addr.send(offramp::Msg::Signal(signal)).await?,
            Self::Pipeline(addr) => {
                // Each pipeline has their own ticks, we don't
                // want to propagate them
                if signal.kind != Some(SignalKind::Tick) {
                    addr.send(Msg::Signal(signal)).await?
                }
            }
            Self::LinkedOnramp(_addr) => {
                // TODO implement!
                //addr.send(onramp::Msg::Signal(signal)).await?
            }
        }
        Ok(())
    }
}

impl From<ConnectTarget> for Dest {
    #[cfg(not(tarpaulin_include))]
    fn from(ct: ConnectTarget) -> Self {
        match ct {
            ConnectTarget::Offramp(off) => Self::Offramp(off),
            ConnectTarget::Onramp(on) => Self::LinkedOnramp(on),
            ConnectTarget::Pipeline(pipe) => Self::Pipeline(*pipe),
        }
    }
}

/// possible pipeline event inputs, receiving insights
/// These are the same as Dest, but kept separately for clarity
#[derive(Debug)]
pub enum Input {
    LinkedOfframp(offramp::Addr),
    Pipeline(Addr),
    Onramp(onramp::Addr),
}

impl From<ConnectTarget> for Input {
    #[cfg(not(tarpaulin_include))]
    fn from(ct: ConnectTarget) -> Self {
        match ct {
            ConnectTarget::Offramp(off) => Self::LinkedOfframp(off),
            ConnectTarget::Onramp(on) => Self::Onramp(on),
            ConnectTarget::Pipeline(pipe) => Self::Pipeline(*pipe),
        }
    }
}

pub struct Create {
    pub config: PipelineArtefact,
    pub id: ServantId,
}

pub(crate) enum ManagerMsg {
    Stop,
    Create(async_channel::Sender<Result<Addr>>, Box<Create>),
}

#[derive(Default, Debug)]
pub(crate) struct Manager {
    qsize: usize,
    operator_id_gen: OperatorIdGen,
}

#[inline]
async fn send_events(eventset: &mut Eventset, dests: &mut Dests) -> Result<()> {
    for (output, event) in eventset.drain(..) {
        if let Some(dest) = dests.get_mut(&output) {
            if let Some((last, rest)) = dest.split_last_mut() {
                for (id, offramp) in rest {
                    let port = id.instance_port_required()?.to_string().into();
                    offramp.send_event(port, event.clone()).await?;
                }
                let last_port = last.0.instance_port_required()?.to_string().into();
                last.1.send_event(last_port, event).await?;
            }
        };
    }
    Ok(())
}

#[inline]
async fn send_signal(own_id: &TremorUrl, signal: Event, dests: &mut Dests) -> Result<()> {
    let mut offramps = dests.values_mut().flatten();
    let first = offramps.next();
    for (id, offramp) in offramps {
        if id != own_id {
            offramp.send_signal(signal.clone()).await?;
        }
    }
    if let Some((id, offramp)) = first {
        if id != own_id {
            offramp.send_signal(signal).await?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_insight(
    skip_to: Option<usize>,
    insight: Event,
    pipeline: &mut ExecutableGraph,
    inputs: &Inputs,
) {
    let insight = pipeline.contraflow(skip_to, insight);
    if insight.cb != CbAction::None {
        let mut input_iter = inputs.iter();
        let first = input_iter.next();
        for (url, (send, input)) in input_iter {
            let is_cb = insight.cb.is_cb();
            if *send || is_cb {
                if let Err(e) = match input {
                    Input::Onramp(addr) => addr
                        .send(onramp::Msg::Cb(insight.cb, insight.id.clone()))
                        .await
                        .map_err(Error::from),
                    Input::Pipeline(addr) => addr.send_insight(insight.clone()).await,
                    Input::LinkedOfframp(_addr) =>
                    /*
                        =========
                        IMPORTANT
                        =========
                        We do not forward Contraflow events back to linked offramps
                        as otherwise we might get into cycles if we dont have a cycle detector preventing this kind of setup
                    */
                    {
                        Ok(())
                    }
                } {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        &pipeline.id, e, url
                    );
                }
            }
        }
        if let Some((url, (send, input))) = first {
            if *send || insight.cb.is_cb() {
                if let Err(e) = match input {
                    Input::Onramp(addr) => addr
                        .send(onramp::Msg::Cb(insight.cb, insight.id))
                        .await
                        .map_err(Error::from),
                    Input::Pipeline(addr) => addr.send_insight(insight).await,
                    Input::LinkedOfframp(_addr) =>
                    /*
                        =========
                        IMPORTANT
                        =========
                        We do not forward Contraflow events back to linked offramps
                        as otherwise we might get into cycles if we dont have a cycle detector preventing this kind of setup
                    */
                    {
                        Ok(())
                    }
                } {
                    error!(
                        "[Pipeline::{}] failed to send insight to input: {} {}",
                        &pipeline.id, e, &url
                    );
                }
            }
        }
    }
}

#[inline]
async fn handle_insights(pipeline: &mut ExecutableGraph, onramps: &Inputs) {
    if !pipeline.insights.is_empty() {
        let mut insights = Vec::with_capacity(pipeline.insights.len());
        std::mem::swap(&mut insights, &mut pipeline.insights);
        for (skip_to, insight) in insights.drain(..) {
            handle_insight(Some(skip_to), insight, pipeline, onramps).await
        }
    }
}

async fn tick(tick_tx: async_channel::Sender<Msg>) {
    let mut e = Event {
        ingest_ns: nanotime(),
        kind: Some(SignalKind::Tick),
        ..Event::default()
    };

    while tick_tx.send(Msg::Signal(e.clone())).await.is_ok() {
        task::sleep(Duration::from_millis(TICK_MS)).await;
        e.ingest_ns = nanotime();
    }
}

async fn handle_cf_msg(msg: CfMsg, pipeline: &mut ExecutableGraph, inputs: &Inputs) -> Result<()> {
    match msg {
        CfMsg::Insight(insight) => handle_insight(None, insight, pipeline, inputs).await,
    }
    Ok(())
}

#[cfg(not(tarpaulin_include))]
fn maybe_send(r: Result<()>) {
    if let Err(e) = r {
        error!("Failed to send : {}", e)
    }
}

#[allow(dead_code)]
async fn echo(addr: &Addr) -> Result<()> {
    let (tx, rx) = async_channel::bounded(1);
    addr.send_mgmt(MgmtMsg::Echo(tx)).await?;
    rx.recv().await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn pipeline_task(
    id: TremorUrl,
    mut pipeline: ExecutableGraph,
    addr: Addr,
    rx: async_channel::Receiver<Msg>,
    cf_rx: async_channel::Receiver<CfMsg>,
    mgmt_rx: async_channel::Receiver<MgmtMsg>,
) -> Result<()> {
    let mut pid = id.clone();
    pid.trim_to_instance();
    pipeline.id = pid.to_string();

    let mut dests: Dests = halfbrown::HashMap::new();
    let mut inputs: Inputs = halfbrown::HashMap::new();
    let mut eventset: Eventset = Vec::new();

    info!("[Pipeline:{}] starting task.", id);

    let ff = rx.map(M::F);
    let cf = cf_rx.map(M::C);
    let mf = mgmt_rx.map(M::M);

    // prioritize management flow over contra flow over forward event flow
    let mut s = PriorityMerge::new(mf, PriorityMerge::new(cf, ff));
    while let Some(msg) = s.next().await {
        match msg {
            M::C(msg) => {
                handle_cf_msg(msg, &mut pipeline, &inputs).await?;
            }
            M::F(Msg::Event { input, event }) => {
                match pipeline.enqueue(&input, event, &mut eventset) {
                    Ok(()) => {
                        handle_insights(&mut pipeline, &inputs).await;
                        maybe_send(send_events(&mut eventset, &mut dests).await);
                    }
                    Err(e) => {
                        let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                            let script_error = tremor_script::errors::Error(script_kind, e.1);
                            // possibly a hygienic error
                            pipeline
                                .source
                                .as_ref()
                                .and_then(|s| script_error.locate_in_source(s))
                                .map_or_else(
                                    || format!(" {:?}", script_error),
                                    |located| format!("\n{}", located),
                                ) // add a newline to have the error nicely formatted in the log
                        } else {
                            format!(" {}", e)
                        };
                        error!("Error handling event:{}", err_str);
                    }
                }
            }
            M::F(Msg::Signal(signal)) => {
                if let Err(e) = pipeline.enqueue_signal(signal.clone(), &mut eventset) {
                    let err_str = if let PipelineErrorKind::Script(script_kind) = e.0 {
                        let script_error = tremor_script::errors::Error(script_kind, e.1);
                        // possibly a hygienic error
                        pipeline
                            .source
                            .as_ref()
                            .and_then(|s| script_error.locate_in_source(s))
                            .map_or_else(
                                || format!(" {:?}", script_error),
                                |located| format!("\n{}", located),
                            ) // add a newline to have the error nicely formatted in the log
                    } else {
                        format!(" {:?}", e)
                    };
                    error!("[Pipeline::{}] Error handling signal:{}", pid, err_str);
                } else {
                    maybe_send(send_signal(&id, signal, &mut dests).await);
                    handle_insights(&mut pipeline, &inputs).await;
                    maybe_send(send_events(&mut eventset, &mut dests).await);
                }
            }
            M::M(MgmtMsg::ConnectInput {
                input_url,
                target,
                transactional,
            }) => {
                info!("[Pipeline::{}] Connecting {} to 'in'", pid, input_url);
                inputs.insert(input_url, (transactional, target.into()));
            }
            M::M(MgmtMsg::ConnectOutput {
                port,
                output_url,
                target,
            }) => {
                info!(
                    "[Pipeline::{}] Connecting '{}' to {}",
                    pid, &port, &output_url
                );
                // notify other pipeline about a new input
                if let ConnectTarget::Pipeline(pipe) = &target {
                    // avoid linking the same pipeline as input to itself
                    // as this will create a nasty circle filling up queues.
                    // In general this does not avoid cycles via more complex constructs.
                    //
                    // Also don't connect anything as input to the metrics pipeline, as we dont want any contraflow to flow via the METRICS_PIPELINE
                    if !pid.same_instance_as(&output_url)
                        && !crate::system::METRICS_PIPELINE.same_instance_as(&output_url)
                    {
                        if let Err(e) = pipe
                            .send_mgmt(MgmtMsg::ConnectInput {
                                input_url: pid.clone(),
                                target: ConnectTarget::Pipeline(Box::new(addr.clone())),
                                transactional: true,
                            })
                            .await
                        {
                            error!(
                                "[Pipeline::{}] Error connecting input pipeline {}: {}",
                                pid, &output_url, e
                            );
                        }
                    }
                }

                if let Some(output_dests) = dests.get_mut(&port) {
                    output_dests.push((output_url, target.into()));
                } else {
                    dests.insert(port, vec![(output_url, target.into())]);
                }
            }
            M::M(MgmtMsg::DisconnectOutput(port, to_delete)) => {
                info!(
                    "[Pipeline::{}] Disconnecting {} from '{}'",
                    pid, &to_delete, &port
                );

                let mut remove = false;
                if let Some(output_vec) = dests.get_mut(&port) {
                    while let Some(index) = output_vec.iter().position(|(k, _)| k == &to_delete) {
                        if let (delete_url, Dest::Pipeline(pipe)) = output_vec.swap_remove(index) {
                            if let Err(e) =
                                pipe.send_mgmt(MgmtMsg::DisconnectInput(id.clone())).await
                            {
                                error!(
                                    "[Pipeline::{}] Error disconnecting input pipeline {}: {}",
                                    pid, &delete_url, e
                                );
                            }
                        }
                    }
                    remove = output_vec.is_empty();
                }
                if remove {
                    dests.remove(&port);
                }
            }
            M::M(MgmtMsg::DisconnectInput(input_url)) => {
                info!("[Pipeline::{}] Disconnecting {} from 'in'", pid, &input_url);
                inputs.remove(&input_url);
            }
            M::M(MgmtMsg::Echo(sender)) => {
                if let Err(e) = sender.send(()).await {
                    error!(
                        "[Pipeline::{}] Error responding to echo message: {}",
                        pid, e
                    );
                }
            }
        }
    }

    info!("[Pipeline:{}] stopping task.", id);
    Ok(())
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self {
            qsize,
            operator_id_gen: OperatorIdGen::new(),
        }
    }
    pub fn start(mut self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(crate::QSIZE);
        let h = task::spawn(async move {
            info!("Pipeline manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, create)) => {
                        r.send(self.start_pipeline(*create)).await?
                    }
                    Err(e) => {
                        info!("Stopping Pipeline manager... {}", e);
                        break;
                    }
                }
            }
            info!("Pipeline manager stopped");
            Ok(())
        });
        (h, tx)
    }

    fn start_pipeline(&mut self, req: Create) -> Result<Addr> {
        let config = req.config;
        let pipeline = config.to_pipe(&mut self.operator_id_gen)?;

        let id = req.id.clone();

        let (tx, rx) = bounded::<Msg>(self.qsize);
        // We use a unbounded channel for counterflow, while an unbounded channel seems dangerous
        // there is soundness to this.
        // The unbounded channel ensures that on counterflow we never have to block, or in other
        // words that sinks or pipelines sending data backwards always can progress passt
        // the sending.
        // This prevents a livelock where the sink is waiting for a full channel to send data to
        // the pipeline and the pipeline is waiting for a full channel to send data to the sink.
        // We prevent unbounded groth by two mechanisms:
        // 1) counterflow is ALWAYS and ONLY created in response to a message
        // 2) we always process counterflow prior to forward flow
        //
        // As long as we have counterflow messages to process, and channel size is growing we do
        // not process any forward flow. Without forwardflow we stave the counterflow ensuring that
        // the counterflow channel is always bounded by the forward flow in a 1:N relationship where
        // N is the maximum number of counterflow events a single event can trigger.
        // N is normally < 1.
        let (cf_tx, cf_rx) = unbounded::<CfMsg>();
        let (mgmt_tx, mgmt_rx) = bounded::<MgmtMsg>(self.qsize);

        task::spawn(tick(tx.clone()));

        let addr = Addr::new(tx, cf_tx, mgmt_tx, req.id);
        task::Builder::new()
            .name(format!("pipeline-{}", id))
            .spawn(pipeline_task(
                id,
                pipeline,
                addr.clone(),
                rx,
                cf_rx,
                mgmt_rx,
            ))?;
        Ok(addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::url::ports::OUT;
    use async_std::future::timeout;
    use tremor_pipeline::EventId;
    use tremor_pipeline::FN_REGISTRY;

    use tremor_script::prelude::*;
    use tremor_script::{path::ModulePath, query::Query};

    // used when we expect something
    const POSITIVE_RECV_TIMEOUT: Duration = Duration::from_millis(10000);
    // used when we expect nothing, to not spend too much time in this test
    const NEGATIVE_RECV_TIMEOUT: Duration = Duration::from_millis(200);

    /// ensure the last message has been processed by waiting for the manager to answer
    /// leveraging the sequential execution at the manager task
    /// this only works reliably for MgmtMsgs, not for insights or events/signals
    async fn manager_fence(addr: &Addr) -> Result<()> {
        echo(&addr).await
    }
    async fn wait_for_event(
        offramp_rx: &async_channel::Receiver<offramp::Msg>,
        wait_for: Option<Duration>,
    ) -> Result<Event> {
        loop {
            match timeout(wait_for.unwrap_or(POSITIVE_RECV_TIMEOUT), offramp_rx.recv()).await {
                Ok(Ok(offramp::Msg::Event { event, .. })) => return Ok(event),
                Ok(_) => continue, // ignore anything else, like signals
                Err(_) => return Err("timeout waiting for event at offramp".into()),
            }
        }
    }

    #[async_std::test]
    async fn test_pipeline_connect_disconnect() -> Result<()> {
        let module_path = ModulePath { mounts: vec![] };
        let query = r#"
            select event
            from in
            into out;
        "#;
        let aggr_reg: tremor_script::registry::Aggr = tremor_script::aggr_registry();
        let q = Query::parse(
            &module_path,
            "test_pipeline_connect.trickle",
            query,
            vec![],
            &*FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        let config = tremor_pipeline::query::Query(q);
        let id = TremorUrl::parse("/pipeline/test_pipeline_connect/instance")?;
        let manager = Manager::new(12);
        let (handle, sender) = manager.start();

        let (tx, rx) = async_channel::bounded(1);
        let create = Create { config, id };
        let create_msg = ManagerMsg::Create(tx, Box::new(create));
        sender.send(create_msg).await?;
        let addr = rx.recv().await??;

        // connect a fake onramp
        let (onramp_tx, onramp_rx) = async_channel::unbounded();
        let onramp_url = TremorUrl::parse("/onramp/fake_onramp/instance/out")?;
        addr.send_mgmt(MgmtMsg::ConnectInput {
            input_url: onramp_url.clone(),
            target: ConnectTarget::Onramp(onramp_tx.clone()), // clone avoids the channel to be closed on disconnect below
            transactional: true,
        })
        .await?;

        // connect another fake onramp, not transactional
        let (onramp2_tx, onramp2_rx) = async_channel::unbounded();
        let onramp2_url = TremorUrl::parse("/onramp/fake_onramp2/instance/out")?;
        addr.send_mgmt(MgmtMsg::ConnectInput {
            input_url: onramp2_url.clone(),
            target: ConnectTarget::Onramp(onramp2_tx.clone()),
            transactional: false,
        })
        .await?;
        manager_fence(&addr).await?;

        // send a non-cb insight
        let event_id = EventId::from((0, 0, 1));
        addr.send_insight(Event {
            id: event_id.clone(),
            cb: CbAction::Ack,
            ..Event::default()
        })
        .await?;

        // transactional onramp received it
        // chose exceptionally big timeout, which we will only hit in the bad case that stuff isnt working, normally this should return fine
        match timeout(POSITIVE_RECV_TIMEOUT, onramp_rx.recv()).await {
            Ok(Ok(onramp::Msg::Cb(CbAction::Ack, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),
            m => assert!(false, "received unexpected msg: {:?}", m),
        }

        // non-transactional did not
        assert!(onramp2_rx.is_empty());

        // send a cb insight
        let event_id = EventId::from((0, 0, 1));
        addr.send_insight(Event {
            id: event_id.clone(),
            cb: CbAction::Close,
            ..Event::default()
        })
        .await?;

        // transactional onramp received it
        // chose exceptionally big timeout, which we will only hit in the bad case that stuff isnt working, normally this should return fine
        match timeout(POSITIVE_RECV_TIMEOUT, onramp_rx.recv()).await {
            Ok(Ok(onramp::Msg::Cb(CbAction::Close, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),
            m => assert!(false, "received unexpected msg: {:?}", m),
        }

        // non-transactional did also receive it
        match timeout(POSITIVE_RECV_TIMEOUT, onramp2_rx.recv()).await {
            Ok(Ok(onramp::Msg::Cb(CbAction::Close, cb_id))) => assert_eq!(cb_id, event_id),
            Err(_) => assert!(false, "No msg received."),

            m => assert!(false, "received unexpected msh: {:?}", m),
        }

        // disconnect our fake offramp
        addr.send_mgmt(MgmtMsg::DisconnectInput(onramp_url)).await?;
        manager_fence(&addr).await?; // ensure the last message has been processed

        // probe it with an insight
        let event_id2 = EventId::from((0, 0, 2));
        addr.send_insight(Event {
            id: event_id2.clone(),
            cb: CbAction::Close,
            ..Event::default()
        })
        .await?;
        // we expect nothing after disconnect, so we run into a timeout
        match timeout(NEGATIVE_RECV_TIMEOUT, onramp_rx.recv()).await {
            Ok(m) => assert!(false, "Didnt expect a message. Got: {:?}", m),
            Err(_e) => {}
        };

        // second onramp received it, as it is a CB insight
        match onramp2_rx.recv().await {
            Ok(onramp::Msg::Cb(CbAction::Close, cb_id)) => assert_eq!(cb_id, event_id2),
            m => assert!(false, "received unexpected msg: {:?}", m),
        };

        addr.send_mgmt(MgmtMsg::DisconnectInput(onramp2_url))
            .await?;
        manager_fence(&addr).await?; // ensure the last message has been processed

        // probe it with an insight
        let event_id3 = EventId::from((0, 0, 3));
        addr.send_insight(Event {
            id: event_id3.clone(),
            cb: CbAction::Open,
            ..Event::default()
        })
        .await?;

        // we expect nothing after disconnect, so we run into a timeout
        match timeout(NEGATIVE_RECV_TIMEOUT, onramp2_rx.recv()).await {
            Ok(m) => assert!(false, "Didnt expect a message. Got: {:?}", m),
            Err(_e) => {}
        };

        // stopping the manager
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }

    #[async_std::test]
    async fn test_pipeline_event_error() -> Result<()> {
        let module_path = ModulePath { mounts: vec![] };
        let query = r#"
            select event.non_existent
            from in
            into out;
        "#;
        let aggr_reg: tremor_script::registry::Aggr = tremor_script::aggr_registry();
        let q = Query::parse(
            &module_path,
            "manager_error_test.trickle",
            query,
            vec![],
            &*FN_REGISTRY.lock()?,
            &aggr_reg,
        )?;
        let config = tremor_pipeline::query::Query(q);
        let id = TremorUrl::parse("/pipeline/manager_error_test/instance")?;
        let manager = Manager::new(12);
        let (handle, sender) = manager.start();
        let (tx, rx) = async_channel::bounded(1);
        let create = Create { config, id };
        let create_msg = ManagerMsg::Create(tx, Box::new(create));
        sender.send(create_msg).await?;
        let addr = rx.recv().await??;
        let (offramp_tx, offramp_rx) = async_channel::unbounded();
        let offramp_url = TremorUrl::parse("/offramp/fake_offramp/instance/in")?;
        // connect a channel so we can receive events from the back of the pipeline :)
        addr.send_mgmt(MgmtMsg::ConnectOutput {
            port: OUT,
            output_url: offramp_url.clone(),
            target: ConnectTarget::Offramp(offramp_tx.clone()),
        })
        .await?;
        manager_fence(&addr).await?;

        // sending an event, that triggers an error
        addr.send(Msg::Event {
            event: Event::default(),
            input: "in".into(),
        })
        .await?;

        // no event at offramp
        match timeout(NEGATIVE_RECV_TIMEOUT, offramp_rx.recv()).await {
            Ok(Ok(m @ offramp::Msg::Event { .. })) => {
                assert!(false, "Did not expect an event, but got: {:?}", m)
            }
            Ok(Err(e)) => return Err(e.into()),
            _ => {}
        };

        // send a second event, that gets through
        let mut second_event = Event::default();
        second_event.data = literal!({
            "non_existent": true
        })
        .into();
        addr.send(Msg::Event {
            event: second_event,
            input: "in".into(),
        })
        .await?;

        let event = wait_for_event(&offramp_rx, None).await?;
        let (value, _meta) = event.data.suffix().clone().into_parts();
        assert_eq!(Value::from(true), value); // check that we received what we sent in, not the faulty event

        // disconnect the output
        addr.send_mgmt(MgmtMsg::DisconnectOutput(OUT, offramp_url))
            .await?;
        manager_fence(&addr).await?;

        // probe it with an Event
        addr.send(Msg::Event {
            event: Event::default(),
            input: "in".into(),
        })
        .await?;

        // we expect nothing to arrive, so we run into a timeout
        match timeout(NEGATIVE_RECV_TIMEOUT, offramp_rx.recv()).await {
            Ok(Ok(m @ offramp::Msg::Event { .. })) => {
                assert!(false, "Didnt expect to receive something, got: {:?}", m)
            }
            Ok(Err(e)) => return Err(e.into()),
            _ => {}
        };

        // stopping the manager
        sender.send(ManagerMsg::Stop).await?;
        handle.cancel().await;
        Ok(())
    }
}
