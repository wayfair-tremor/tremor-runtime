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
use crate::errors::Result;
use crate::metrics::RampReporter;
use crate::pipeline;
use crate::repository::ServantId;
use crate::source::prelude::*;
use crate::url::TremorUrl;
use crate::OpConfig;
use async_std::task::{self, JoinHandle};
use halfbrown::{Entry, HashMap};
use std::fmt;
use tremor_common::ids::OnrampIdGen;
use tremor_pipeline::EventId;

pub(crate) type Sender = async_channel::Sender<ManagerMsg>;

/// builder for onramps
pub trait Builder: Send {
    /// build an onramp instance from the given `id` and `config`
    fn from_config(&self, id: &TremorUrl, config: &Option<OpConfig>) -> Result<Box<dyn Onramp>>;
}

/// messages an onramp manager can receive
#[derive(Clone, Debug)]
pub enum Msg {
    /// connect a pipeline to a given port
    Connect(Cow<'static, str>, Vec<(TremorUrl, pipeline::Addr)>),
    /// disconnect a connected pipeline
    Disconnect {
        /// url of the pipeline to disconnect
        id: TremorUrl,
        /// receives true if the onramp is not connected anymore
        tx: async_channel::Sender<bool>,
    },
    /// circuit breaker event
    Cb(CbAction, EventId),
    // TODO pick good naming here: LinkedEvent / Response / Result?
    /// response from a linked onramp
    Response(tremor_pipeline::Event),
}

/// onramp address
pub type Addr = async_channel::Sender<Msg>;

/// config for onramps
pub struct OnrampConfig<'cfg> {
    /// unique numeric identifier
    pub onramp_uid: u64,
    /// configured codec
    pub codec: &'cfg str,
    /// map of dynamic codecs
    pub codec_map: halfbrown::HashMap<String, String>,
    /// pre- and postprocessors
    pub processors: Processors<'cfg>,
    /// thingy that reports metrics
    pub metrics_reporter: RampReporter,
    /// flag: is this a linked transport?
    pub is_linked: bool,
    /// if true only start event processing when the `err` port is connected
    pub err_required: bool,
}

/// onramp - legacy source of events
#[async_trait::async_trait]
pub trait Onramp: Send {
    /// start the onramp
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<Addr>;
    /// default codec
    fn default_codec(&self) -> &str;
}

// just a lookup
pub(crate) struct Create {
    pub id: ServantId,
    pub stream: Box<dyn Onramp>,
    pub codec: String,
    pub codec_map: halfbrown::HashMap<String, String>,
    pub preprocessors: Vec<String>,
    pub postprocessors: Vec<String>,
    pub metrics_reporter: RampReporter,
    pub is_linked: bool,
    pub err_required: bool,
}

impl fmt::Debug for Create {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StartOnramp({})", self.id)
    }
}

/// This is control plane
pub(crate) enum ManagerMsg {
    Register {
        onramp_type: String,
        builder: Box<dyn Builder>,
        builtin: bool,
    },
    Unregister(String),
    TypeExists(String, async_channel::Sender<bool>),
    /// create an onramp instance from the given type and config and send it back
    Instantiate {
        onramp_type: String,
        url: TremorUrl,
        config: Option<OpConfig>,
        sender: async_channel::Sender<Result<Box<dyn Onramp>>>,
    },
    // start an onramp, start polling for data if connected
    Create(async_channel::Sender<Result<Addr>>, Box<Create>),
    Stop,
}

#[derive(Debug, Default)]
pub(crate) struct Manager {
    qsize: usize,
}

impl Manager {
    pub fn new(qsize: usize) -> Self {
        Self { qsize }
    }
    pub fn start(self) -> (JoinHandle<Result<()>>, Sender) {
        let (tx, rx) = bounded(self.qsize);

        let h = task::spawn::<_, Result<()>>(async move {
            let mut onramp_id_gen = OnrampIdGen::new();
            let mut types = HashMap::with_capacity(8);
            info!("Onramp manager started");
            loop {
                match rx.recv().await {
                    Ok(ManagerMsg::Stop) => {
                        info!("Stopping onramps...");
                        break;
                    }
                    Ok(ManagerMsg::Create(r, c)) => {
                        let Create {
                            codec,
                            codec_map,
                            mut stream,
                            preprocessors,
                            postprocessors,
                            metrics_reporter,
                            is_linked,
                            id,
                            err_required,
                        } = *c;

                        match stream
                            .start(OnrampConfig {
                                onramp_uid: onramp_id_gen.next_id(),
                                codec: &codec,
                                codec_map,
                                processors: Processors {
                                    pre: &preprocessors,
                                    post: &postprocessors,
                                },
                                metrics_reporter,
                                is_linked,
                                err_required,
                            })
                            .await
                        {
                            Ok(addr) => {
                                info!("Onramp {} started.", id);
                                r.send(Ok(addr)).await?;
                            }
                            Err(e) => error!("Creating an onramp failed: {}", e),
                        }
                    }
                    Ok(ManagerMsg::Register {
                        onramp_type,
                        builder,
                        builtin,
                    }) => match types.entry(onramp_type) {
                        Entry::Occupied(e) => {
                            error!("Onramp type '{}' already registered.", e.key());
                        }
                        Entry::Vacant(e) => {
                            info!("Onramp type '{}' successfully registered.", e.key());
                            e.insert((builder, builtin));
                        }
                    },
                    Ok(ManagerMsg::Unregister(onramp_type)) => {
                        if let Entry::Occupied(e) = types.entry(onramp_type) {
                            let (_builder, builtin) = e.get();
                            if *builtin {
                                error!("Cannot unregister builtin onramp type '{}'", e.key());
                            } else {
                                let (k, _) = e.remove_entry();
                                info!("Unregistered onramp type '{}'", k);
                            }
                        }
                    }
                    Ok(ManagerMsg::TypeExists(onramp_type, sender)) => {
                        let r = types.contains_key(&onramp_type);
                        if let Err(e) = sender.send(r).await {
                            error!("Error sending Offramp TypeExists result: {}", e);
                        }
                    }
                    Ok(ManagerMsg::Instantiate {
                        onramp_type,
                        url,
                        config,
                        sender,
                    }) => {
                        let res = types
                            .get(&onramp_type)
                            .ok_or_else(|| {
                                Error::from(ErrorKind::UnknownOnrampType(onramp_type.clone()))
                            })
                            .and_then(|(builder, _)| builder.from_config(&url, &config));
                        if let Err(e) = sender.send(res).await {
                            error!("Error sending Onramp Instantiate result: {}", e);
                        }
                    }
                    Err(e) => {
                        info!("Stopping onramps... {}", e);
                        break;
                    }
                }
            }
            info!("Onramp manager stopped.");
            Ok(())
        });

        (h, tx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::Binding;
    use crate::config::MappingMap;
    use crate::repository::BindingArtefact;
    use crate::system;
    use crate::url::TremorUrl;
    use simd_json::json;
    use std::io::Write;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::net::UdpSocket;
    use std::ops::Range;

    fn port_is_free(port: u16) -> bool {
        match TcpListener::bind(format!("127.0.0.1:{}", port)) {
            Ok(_x) => true,
            _otherwise => false,
        }
    }

    fn find_free_port(mut range: Range<u16>) -> Option<u16> {
        range.find(|port| port_is_free(*port))
    }

    struct TcpInjector {
        stream: TcpStream,
    }

    impl TcpInjector {
        fn with_port(port: u16) -> Self {
            let hostport = format!("localhost:{}", port);
            let stream = TcpStream::connect(hostport).expect("could not connect");
            stream.set_nodelay(true).expect("could not disable nagle");
            stream
                .set_nonblocking(true)
                .expect("could not set non-blocking");
            TcpInjector { stream }
        }
    }

    struct UdpInjector {
        datagram: UdpSocket,
    }

    impl UdpInjector {
        fn with_port(port: u16) -> Self {
            let ephemeral_port = format!(
                "localhost:{}",
                find_free_port(30000..31000).expect("no free ports in range")
            );
            let hostport = format!("localhost:{}", port);
            let datagram = UdpSocket::bind(ephemeral_port).expect("could not bind");
            datagram.connect(hostport).expect("could not connect");
            datagram
                .set_nonblocking(true)
                .expect("could not set non-blocking");
            UdpInjector { datagram }
        }
    }

    macro_rules! rampercize {
        ($onramp_config:expr, $offramp_config:expr, $test:tt) => {
            let storage_directory = Some("./storage".to_string());
            let (world, _handle) = system::World::start(50, storage_directory).await?;
            let config = serde_yaml::to_value($onramp_config).expect("json to yaml not ok");

            let onramp: crate::config::OnRamp = serde_yaml::from_value(config)?;
            let onramp_url = TremorUrl::from_onramp_id("test").expect("bad url");
            world
                .repo
                .publish_onramp(&onramp_url, false, onramp)
                .await?;

            let config2 = serde_yaml::to_value($offramp_config).expect("json to yaml not ok");

            let offramp: crate::config::OffRamp = serde_yaml::from_value(config2)?;
            let offramp_url = TremorUrl::from_offramp_id("test").expect("bad url");
            world
                .repo
                .publish_offramp(&offramp_url, false, offramp)
                .await?;

            let id = TremorUrl::parse(&format!("/pipeline/{}", "test"))?;

            let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
            let aggr_reg = tremor_script::aggr_registry();
            let artefact = tremor_pipeline::query::Query::parse(
                &module_path,
                "select event from in into out;",
                "<test>",
                Vec::new(),
                &*tremor_pipeline::FN_REGISTRY.lock()?,
                &aggr_reg,
            )?;
            world.repo.publish_pipeline(&id, false, artefact).await?;

            let binding: Binding = serde_yaml::from_str(
                r#"
id: test
links:
  '/onramp/test/{instance}/out': [ '/pipeline/test/{instance}/in' ]
  '/pipeline/test/{instance}/out': [ '/offramp/test/{instance}/in' ]
"#,
            )?;

            world
                .repo
                .publish_binding(
                    &TremorUrl::parse(&format!("/binding/{}", "test"))?,
                    false,
                    BindingArtefact {
                        binding,
                        mapping: None,
                    },
                )
                .await?;

            let mapping: MappingMap = serde_yaml::from_str(
                r#"
/binding/test/01:
  instance: "01"
"#,
            )?;

            let id = TremorUrl::parse(&format!("/binding/{}/01", "test"))?;
            world.link_binding(&id, mapping[&id].clone()).await?;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            $test;

            std::thread::sleep(std::time::Duration::from_millis(1000));

            world.stop().await?;
        };
    }

    // macro_rules! rampercize_with_logs {
    //     ($onramp_config:expr, $offramp_config:expr, $test:tt) => {
    //         env_logger::init();
    //         rampercize!($onramp_config, $offramp_config, $test)
    //     };
    // }

    #[async_std::test]
    async fn tcp_onramp() -> Result<()> {
        let port = find_free_port(9000..9099).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "tcp",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let mut inject = TcpInjector::with_port(port);
                    inject
                        .stream
                        .write_all(r#"{"snot": "badger"}\n"#.as_bytes())
                        .expect("something bad happened in cli injector");
                    inject.stream.flush().expect("");
                    drop(inject);
                }
            }
        );
        Ok(())
    }

    #[async_std::test]
    async fn udp_onramp() -> Result<()> {
        let port = find_free_port(9100..9199).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "udp",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let inject = UdpInjector::with_port(port);
                    inject
                        .datagram
                        .send(r#"{"snot": "badger"}\n"#.as_bytes())
                        .expect("something bad happened in cli injector");
                    drop(inject);
                }
            }
        );
        Ok(())
    }

    #[ignore]
    #[async_std::test]
    async fn rest_onramp() -> Result<()> {
        let port = find_free_port(9200..9299).expect("no free port");
        rampercize!(
            // onramp config
            json!({
                "id": "test",
                "type": "rest",
                "codec": "json",
                "preprocessors": [ "lines" ],
                "config": {
                  "host": "127.0.0.1",
                  "port": port,
                  "is_non_blocking": true,
                  "ttl": 32,
                  "resources": [

                  ],
                }
            }),
            // offramp config
            json!({
                "id": "test",
                "type": "stdout",
                "codec": "json",
            }),
            {
                for _ in 0..3 {
                    let mut inject = TcpInjector::with_port(port);
                    inject
                        .stream
                        .write_all(
                            r#"{"HTTP/1.1\nContent-type: application\nContent-Length: 3\n\n{}\n"#
                                .as_bytes(),
                        )
                        .expect("something bad happened in cli injector");
                    inject.stream.flush().expect("");
                    drop(inject);
                }
            }
        );
        Ok(())
    }
}
