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

use crate::source::prelude::*;
use async_channel::{Sender, TryRecvError};
use halfbrown::HashMap;
use surf::middleware::{Middleware, Next};
use surf::{Client, Request, Response};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// URL for the sse endpoint
    pub url: String,
    /// Header modifications to the request.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}
impl ConfigImpl for Config {}

struct MiddlewareHeader {
    header: HashMap<String, String>,
}

#[surf::utils::async_trait]
impl Middleware for MiddlewareHeader {
    async fn handle(
        &self,
        req: Request,
        client: Client,
        next: Next<'_>,
    ) -> std::result::Result<Response, http_types::Error> {
        let mut req = req;
        // set the headers in the request.
        for (name, value) in self.header.iter() {
            req.append_header(name.as_str(), value.as_str());
        }
        let resp = next.run(req, client).await?;
        Ok(resp)
    }
}

pub struct Sse {
    pub config: Config,
    onramp_id: TremorUrl,
}

pub(crate) struct Builder {}
impl onramp::Builder for Builder {
    fn from_config(&self, id: &TremorUrl, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Sse {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for SSE onramp".into())
        }
    }
}

#[async_trait::async_trait()]
impl Onramp for Sse {
    async fn start(&mut self, config: OnrampConfig<'_>) -> Result<onramp::Addr> {
        let source = Int::from_config(config.onramp_uid, self.onramp_id.clone(), &self.config);
        SourceManager::start(source, config).await
    }

    fn default_codec(&self) -> &str {
        // What is the default use case? maybe json?
        "string"
    }
}

pub struct Int {
    uid: u64,
    config: Config,
    onramp_id: TremorUrl,
    event_source: Option<Receiver<surf_sse::Event>>,
}
impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSE")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorUrl, config: &Config) -> Self {
        Int {
            uid,
            config: config.clone(),
            onramp_id,
            event_source: None,
        }
    }
}

async fn handle_init(
    url: surf_sse::Url,
    headers: HashMap<String, String>,
    tx: Sender<surf_sse::Event>,
) -> Result<()> {
    // Create the specific client.
    let client = surf::client().with(MiddlewareHeader { header: headers });
    // Ok to block here. This is an indirection from the main thread.
    let mut event_source = surf_sse::EventSource::with_client(client, url);

    while let Some(event) = event_source.next().await {
        match event {
            Ok(event) => {
                tx.send(event).await?;
            }
            Err(e) => {
                error!("SSE Source Error: {}", e);
            }
        };
    }
    tx.close();
    Ok(())
}

#[async_trait::async_trait()]
impl Source for Int {
    fn id(&self) -> &TremorUrl {
        &self.onramp_id
    }

    async fn init(&mut self) -> Result<SourceState> {
        info!(
            "[Source::{}] subscribing to {}",
            self.onramp_id.to_string(),
            &self.config.url
        );

        // Neccessary check as Isahc(Curl) client panics if the url isn't complete. Seems like an edge case.
        // Though Heinz might have a better solution which is Endpoint struct in rest sink.
        let url_check = self.config.url.parse::<http::Uri>();
        if let Err(err) = url_check {
            return Err(err.to_string().into());
        }

        let (tx, rx) = bounded(crate::QSIZE);

        let url: surf_sse::Url = self.config.url.parse()?;
        let headers = self.config.headers.clone();

        // The client runs with default configuration from crate
        task::spawn(handle_init(url, headers, tx));
        self.event_source = Some(rx);

        Ok(SourceState::Connected)
    }

    async fn pull_event(&mut self, _id: u64) -> Result<SourceReply> {
        let origin_uri = EventOriginUri {
            uid: self.uid,
            scheme: "tremor-sse".to_string(),
            // What even is the host here?
            host: hostname(),
            port: None,
            path: vec![String::default()],
        };

        self.event_source.as_ref().map_or_else(
            // There is not recv.
            || Ok(SourceReply::StateChange(SourceState::Disconnected)),
            |event_source| match event_source.try_recv() {
                Ok(event) => {
                    let src_rply = SourceReply::Data {
                        origin_uri: origin_uri.clone(),
                        data: event.data.into_bytes(),
                        meta: None,
                        codec_override: None,
                        stream: 0,
                    };
                    Ok(src_rply)
                }
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            },
        )
    }
}
