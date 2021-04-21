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
use crate::pipeline;
use crate::sink::prelude::*;
use crate::url::TremorUrl;
use async_channel::Sender;
use halfbrown::HashMap;

pub(crate) mod amqp;
pub(crate) mod blackhole;
pub(crate) mod cb;
pub(crate) mod debug;
pub(crate) mod dns;
pub(crate) mod elastic;
pub(crate) mod exit;
pub(crate) mod file;
pub(crate) mod gcs;
pub(crate) mod kafka;
pub(crate) mod kv;
pub(crate) mod nats;
pub(crate) mod newrelic;
pub(crate) mod otel;
pub(crate) mod postgres;
pub(crate) mod prelude;
pub(crate) mod rest;
pub(crate) mod stderr;
pub(crate) mod stdout;
pub(crate) mod tcp;
pub(crate) mod udp;
pub(crate) mod ws;

#[derive(Debug)]
pub enum Reply {
    Insight(Event),
    // out port and the event
    // TODO better name for this?
    Response(Cow<'static, str>, Event),
}

/// Result for a sink function that may provide insights or response.
///
/// It can return None or Some(vec![]) if no insights/response were generated.
///
/// An insight is a contraflowevent containing control information for the runtime like
/// circuit breaker events, guaranteed delivery events, etc.
///
/// A response is an event generated from the sink delivery.
pub(crate) type ResultVec = Result<Option<Vec<Reply>>>;

#[async_trait::async_trait]
pub(crate) trait Sink {
    /// Handles an incoming event.
    ///
    /// ## Error handling
    ///
    /// The circuit-breaker (CB) and guaranteed deliver (GD) mechanics require this function to exhibit certain behaviour:
    /// if `auto_ack()` returns `false`:
    ///   * This function should catch __ALL__ errors and send appropriate insights in the returned `ResultVec` (or via the `reply_channel` it received in `init`).
    ///   * For returned `Err()`s, no insight will be sent, this violates the GD requirements for some upstream onramps/operators and will lead to sneaky bugs. Do not do that!
    ///
    /// if `auto_ack()` returns `true`:
    ///   * Errors can be bubbled up from this function using `?`, if the event requires GD events (`event.transactional == true`) the `OfframpManager` will take care of this.
    ///   * CB events like `trigger` or `restore` need to be sent via the `ResultVec` or `reply_channel`.
    async fn on_event(
        &mut self,
        input: &str,
        codec: &mut dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        event: Event,
    ) -> ResultVec;
    async fn on_signal(&mut self, signal: Event) -> ResultVec;

    /// This function should be implemented to be idempotent
    ///
    /// The passed reply_channel is for fast-tracking sink-replies going back to the connected pipelines.
    /// It is an additional way to returning them in a ResultVec via on_event, on_signal.
    #[allow(clippy::too_many_arguments)]
    async fn init(
        &mut self,
        sink_uid: u64,
        sink_url: &TremorUrl,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<Reply>,
    ) -> Result<()>;

    // this empty function passed manual inspect, it is bug free
    #[cfg(not(tarpaulin_include))]
    /// Callback for graceful shutdown (default behaviour: do nothing)
    async fn terminate(&mut self) {}

    /// Is the sink active and ready to process events
    fn is_active(&self) -> bool;

    /// Is the sink automatically acknowledging events or engaged in some form of delivery
    /// guarantee
    ///
    /// If this sink returns `false` here, it needs to adhere to the following protocol for CB ack/fail insights:
    /// send one `ack` or `fail` CB insight for each incoming event if `event.transaction == true`.
    /// Otherwise dont send any.
    /// For `ack` insights include a `time` field in the insight metadata with duration it took for handling the event in milliseconds, if it makes sense.
    fn auto_ack(&self) -> bool;

    fn default_codec(&self) -> &str;
}

pub(crate) struct SinkManager<T>
where
    T: Sink,
{
    sink_url: Option<TremorUrl>,
    sink: T,
    pipelines: HashMap<TremorUrl, pipeline::Addr>,
    // for linked offramps
    dest_pipelines: HashMap<Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>>,
}

impl<T> SinkManager<T>
where
    T: Sink + Send,
{
    fn new(sink: T) -> Self {
        Self {
            sink_url: None,
            sink,
            pipelines: HashMap::new(),
            dest_pipelines: HashMap::new(),
        }
    }

    fn new_box(sink: T) -> Box<Self> {
        Box::new(Self::new(sink))
    }

    fn has_dest_pipelines(&self) -> bool {
        self.dest_pipelines.values().any(|xs| !xs.is_empty())
    }
}

#[async_trait::async_trait]
impl<T> Offramp for SinkManager<T>
where
    T: Sink + Send,
{
    async fn terminate(&mut self) {
        self.sink.terminate().await
    }
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        offramp_uid: u64,
        offramp_url: &TremorUrl,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        processors: Processors<'_>,
        is_linked: bool,
        reply_channel: Sender<Reply>,
    ) -> Result<()> {
        self.sink_url = Some(offramp_url.clone());
        self.sink
            .init(
                offramp_uid, // we treat offramp_uid and sink_uid as the same thing
                offramp_url,
                codec,
                codec_map,
                processors,
                is_linked,
                reply_channel,
            )
            .await
    }

    async fn on_event(
        &mut self,
        codec: &mut dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
        input: &str,
        event: Event,
    ) -> Result<()> {
        if let Some(mut replies) = self.sink.on_event(input, codec, codec_map, event).await? {
            for reply in replies.drain(..) {
                match reply {
                    Reply::Insight(e) => handle_insight(e, self.pipelines.values()).await?,
                    Reply::Response(port, event) => {
                        if let Some(pipelines) = self.dest_pipelines.get_mut(&port) {
                            handle_response(event, pipelines.iter()).await?
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn default_codec(&self) -> &str {
        self.sink.default_codec()
    }

    fn add_pipeline(&mut self, id: TremorUrl, addr: pipeline::Addr) {
        self.pipelines.insert(id, addr);
    }

    fn add_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorUrl, addr: pipeline::Addr) {
        let p = (id, addr);
        if let Some(port_ps) = self.dest_pipelines.get_mut(&port) {
            port_ps.push(p);
        } else {
            self.dest_pipelines.insert(port, vec![p]);
        }
    }

    fn remove_pipeline(&mut self, id: TremorUrl) -> bool {
        self.pipelines.remove(&id);
        self.pipelines.is_empty() && !self.has_dest_pipelines()
    }
    fn remove_dest_pipeline(&mut self, port: Cow<'static, str>, id: TremorUrl) -> bool {
        if let Some(port_ps) = self.dest_pipelines.get_mut(&port) {
            port_ps.retain(|(url, _)| url != &id)
        }
        self.pipelines.is_empty() && !self.has_dest_pipelines()
    }

    async fn on_signal(&mut self, signal: Event) -> Option<Event> {
        let replies = match self.sink.on_signal(signal).await {
            Ok(results) => results?,
            Err(e) => {
                if let Some(sink_url) = &self.sink_url {
                    error!("[Sink::{}] Error processing signal: {}", sink_url, e);
                }
                return None;
            }
        };
        for reply in replies {
            match reply {
                Reply::Insight(e) => {
                    if let Err(e) = handle_insight(e, self.pipelines.values()).await {
                        if let Some(sink_url) = &self.sink_url {
                            error!("[Sink::{}] Error handling insight in sink: {}", sink_url, e);
                        }
                    }
                }
                Reply::Response(port, event) => {
                    if let Some(pipelines) = self.dest_pipelines.get_mut(&port) {
                        if let Err(e) = handle_response(event, pipelines.iter()).await {
                            if let Some(sink_url) = &self.sink_url {
                                error!(
                                    "[Sink::{}] Error handling response in sink: {}",
                                    sink_url, e
                                );
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn is_active(&self) -> bool {
        self.sink.is_active()
    }

    fn auto_ack(&self) -> bool {
        self.sink.auto_ack()
    }
}

/// we explicitly do not fail upon send errors, just log errors
pub(crate) async fn handle_insight<'iter, T>(insight: Event, mut pipelines: T) -> Result<()>
where
    T: Iterator<Item = &'iter pipeline::Addr>,
{
    if let Some(first) = pipelines.next() {
        for p in pipelines {
            if let Err(e) = p.send_insight(insight.clone()).await {
                // TODO: is this wanted to not raise the error here?
                error!("Error: {}", e)
            };
        }
        if let Err(e) = first.send_insight(insight).await {
            error!("Error: {}", e)
        };
    }
    Ok(())
}

/// handle response back from sink e.g. in linked transport case
///
/// we explicitly do not fail upon send errors, just log errors
pub(crate) async fn handle_response<'iter, T>(response: Event, mut pipelines: T) -> Result<()>
where
    T: Iterator<Item = &'iter (TremorUrl, pipeline::Addr)>,
{
    if let Some((first_id, first_addr)) = pipelines.next() {
        for (id, addr) in pipelines {
            // TODO alt way here?
            // pre-save this already in dest_pipelines?
            let port = id.instance_port_required()?.to_owned();
            if let Err(e) = addr
                .send(pipeline::Msg::Event {
                    event: response.clone(),
                    input: port.into(),
                })
                .await
            {
                error!("Error: {}", e)
            };
        }
        let first_port = first_id.instance_port_required()?.to_owned();

        if let Err(e) = first_addr
            .send(pipeline::Msg::Event {
                event: response,
                input: first_port.into(),
            })
            .await
        {
            error!("Error: {}", e)
        };
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pipeline::Msg;

    #[async_std::test]
    async fn test_send() -> Result<()> {
        let e = Event::default();

        let (t11, r11) = async_channel::unbounded();
        let (t12, r12) = async_channel::unbounded();
        let (t13, r13) = async_channel::unbounded();
        let p1 = pipeline::Addr::new(
            t11,
            t12,
            t13,
            TremorUrl::parse("tremor://host/pipeline/name1/instance1/port1")?,
        );

        let (t21, r21) = async_channel::unbounded();
        let (t22, r22) = async_channel::unbounded();
        let (t23, r23) = async_channel::unbounded();
        let p2 = pipeline::Addr::new(
            t21,
            t22,
            t23,
            TremorUrl::parse("tremor://host/pipeline/name2/instance2/port2")?,
        );

        let p = vec![(p1.id().clone(), p1), (p2.id().clone(), p2)];

        handle_response(e.clone(), p.iter()).await?;

        if let Msg::Event { event, input } = r11.recv().await? {
            assert_eq!(event, e);
            assert_eq!(input, "port1");
        } else {
            panic!("not an event");
        }

        assert!(r11.is_empty());
        assert!(r12.is_empty());
        assert!(r13.is_empty());

        if let Msg::Event { event, input } = r21.recv().await? {
            assert_eq!(event, e);
            assert_eq!(input, "port2");
        } else {
            panic!("not an event");
        }

        assert!(r21.is_empty());
        assert!(r22.is_empty());
        assert!(r23.is_empty());

        Ok(())
    }
}
