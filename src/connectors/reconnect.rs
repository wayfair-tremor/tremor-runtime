// Copyright 2021, The Tremor Team
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

/// reconnect logic and execution for connectors
use crate::config::ReconnectConfig;
use crate::connectors::{Addr, Connectivity, Connector, ConnectorContext, Msg};
use crate::errors::{ErrorKind, Result};
use crate::url::TremorUrl;
use async_channel::Sender;
use async_std::task;
use std::time::Duration;

/// Entity that executes the reconnect logic based upon the given `ReconnectConfig`
pub(crate) struct Reconnect {
    attempt: u64,
    interval_ms: u64,
    config: ReconnectConfig,
}

impl Reconnect {
    /// constructor
    fn new(config: ReconnectConfig) -> Self {
        let interval_ms = config.interval_ms;
        Self {
            config,
            interval_ms,
            attempt: 0,
        }
    }

    /// Use the given connector to attempt to establish a connection.
    ///
    /// Will issue a retry after the configured interval (based upon the number of the attempt)
    /// asynchronously and send a `connector::Msg::Reconnect` to the connector identified by `addr`.
    pub(crate) async fn attempt(
        &mut self,
        connector: &mut dyn Connector,
        ctx: &ConnectorContext,
        addr: &Addr,
    ) -> Result<Connectivity> {
        match connector.connect(&ctx).await {
            Ok(true) => {
                self.reset();
                Ok(Connectivity::Connected)
            }
            Ok(false) => {
                // TODO: avoid some of those clones
                self.update_and_retry(addr.sender.clone(), addr.url.clone())?;

                Ok(Connectivity::Disconnected)
            }

            Err(e) => {
                error!(
                    "[Connector::{}] Reconnect Error (Attempt {}): {}",
                    addr.url, self.attempt, e
                );
                // TODO: avoid some of those clones
                self.update_and_retry(addr.sender.clone(), addr.url.clone())?;

                Ok(Connectivity::Disconnected)
            }
        }
    }

    /// update internal state for the current failed connect attempt
    /// and spawn a retry task
    fn update_and_retry(&mut self, sender: Sender<Msg>, url: TremorUrl) -> Result<()> {
        // update internal state
        if let Some(max_retries) = self.config.max_retry {
            if self.attempt >= max_retries {
                return Err(ErrorKind::MaxRetriesExceeded(max_retries).into());
            }
        }
        self.attempt += 1;
        self.interval_ms = (self.interval_ms as f64 * self.config.growth_rate) as u64;

        // spawn retry
        let duration = Duration::from_millis(self.interval_ms);
        task::spawn(async move {
            task::sleep(duration).await;
            if let Err(_) = sender.send(Msg::Reconnect).await {
                error!(
                    "[Connector::{}] Error sending reconnect msg to connector.",
                    &url
                )
            }
        });

        Ok(())
    }

    /// reset internal state after successful connect attempt
    fn reset(&mut self) {
        self.attempt = 0;
        self.interval_ms = self.config.interval_ms;
    }
}

impl From<ReconnectConfig> for Reconnect {
    fn from(config: ReconnectConfig) -> Self {
        Self::new(config)
    }
}
