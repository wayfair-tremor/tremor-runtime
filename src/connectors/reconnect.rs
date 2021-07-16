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
use async_channel::Sender;
use async_std::task;
use std::time::Duration;

/// Entity that executes the reconnect logic based upon the given `ReconnectConfig`
pub(crate) struct Reconnect {
    attempt: u64,
    interval_ms: u64,
    config: ReconnectConfig,
    addr: Addr,
}

/// Notifier that connector implementations
/// can use to asynchronously notify the runtime whenever their connection is lost
///
/// This will change the connector state properly and trigger a new reconnect attempt (according to the configured logic)
pub struct ConnectionLostNotifier(Sender<Msg>);

impl ConnectionLostNotifier {
    /// notify the runtime that this connector lost its connection
    pub async fn notify(&self) -> Result<()> {
        self.0.send(Msg::ConnectionLost).await?;
        Ok(())
    }
}

impl From<&Addr> for ConnectionLostNotifier {
    fn from(addr: &Addr) -> Self {
        Self(addr.sender.clone())
    }
}

impl Reconnect {
    /// constructor
    pub(crate) fn new(connector_addr: &Addr, config: ReconnectConfig) -> Self {
        let interval_ms = config.interval_ms;
        Self {
            config,
            interval_ms,
            attempt: 0,
            addr: connector_addr.clone(),
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
    ) -> Result<Connectivity> {
        let notifier = ConnectionLostNotifier::from(&self.addr);
        match connector.connect(&ctx, notifier).await {
            Ok(true) => {
                self.reset();
                Ok(Connectivity::Connected)
            }
            Ok(false) => {
                self.update_and_retry()?;

                Ok(Connectivity::Disconnected)
            }

            Err(e) => {
                error!(
                    "[Connector::{}] Reconnect Error (Attempt {}): {}",
                    &ctx.url, self.attempt, e
                );
                self.update_and_retry()?;

                Ok(Connectivity::Disconnected)
            }
        }
    }

    /// update internal state for the current failed connect attempt
    /// and spawn a retry task
    fn update_and_retry(&mut self) -> Result<()> {
        // update internal state
        if let Some(max_retries) = self.config.max_retry {
            if self.attempt >= max_retries {
                return Err(ErrorKind::MaxRetriesExceeded(max_retries).into());
            }
        }
        self.attempt += 1;
        // TODO: trait out next interval computation, to support different strategies
        self.interval_ms = (self.interval_ms as f64 * self.config.growth_rate) as u64;

        // spawn retry
        let duration = Duration::from_millis(self.interval_ms);
        // TODO: meh, clones. But then again: *shrug*
        let sender = self.addr.sender.clone();
        let url = self.addr.url.clone();
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
