// Copyright 2026 Petr <https://github.com/ppetr>
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

//! The original watch-channel API, built on the handler API. Planned for removal in 0.2.0; nothing
//! else in the crate depends on this module.

use std::time::Duration;

use backoff::backoff::Backoff;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointId};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::handler::{ConnectionHandler, Outcome};
use crate::persistent::Builder;

/// Default for [`Builder::stable_after`]: how long a connection must be held before the outgoing
/// dial backoff is reset.
pub const DEFAULT_STABLE_AFTER: Duration = Duration::from_secs(5);

impl<B: Backoff + Send + 'static> Builder<B> {
    /// How long a connection must be held before the backoff between outgoing dials is reset
    /// (default [`DEFAULT_STABLE_AFTER`]). Only used by [`Builder::spawn`].
    ///
    /// A connection that is lost or replaced sooner -- typically one the peer immediately rejects
    /// -- counts as a failed attempt, so the backoff keeps growing instead of restarting from its
    /// initial delay. `Duration::ZERO` resets as soon as a connection is installed.
    pub fn stable_after(mut self, stable_after: Duration) -> Self {
        self.stable_after = stable_after;
        self
    }

    /// Starts the link and publishes its current connection through
    /// [`PersistentConnection::connection`].
    ///
    /// The link task is instrumented with [`tracing::Span::current()`] as seen at this call site
    /// -- wrap the call in your own span (e.g. carrying a peer name) if you manage more than one
    /// link and want to tell their log lines apart.
    pub fn spawn(self) -> PersistentConnection {
        let (tx, rx) = watch::channel(None);
        let handler = PublishHandler { tx, stable_after: self.stable_after };
        // The detached link task drops the handler, and with it the sender, when it ends; that is
        // how receivers learn about it.
        let (_task, guard) = self.launch(handler);
        PersistentConnection { connection: rx, _guard: guard }
    }
}

/// Publishes every connection it is given and reports it done once it was held for
/// `stable_after`.
struct PublishHandler {
    tx: watch::Sender<Option<Connection>>,
    stable_after: Duration,
}

impl ConnectionHandler for PublishHandler {
    async fn handle(&mut self, conn: Connection, cancel: CancellationToken) -> Outcome {
        let started = Instant::now();
        self.tx.send_replace(Some(conn.clone()));
        cancel.cancelled().await;
        // Publish `None` only if the connection is already closed. When it is merely replaced or
        // the link shuts down, the old connection stays published until the next call replaces it.
        if conn.close_reason().is_some() {
            self.tx.send_replace(None);
        }
        if started.elapsed() >= self.stable_after {
            Outcome::Done
        } else {
            Outcome::Failed
        }
    }
}

/// A single, self-healing Iroh connection to one peer.
///
/// Drop it to close the connection and stop all background work.
pub struct PersistentConnection {
    connection: watch::Receiver<Option<Connection>>,
    _guard: DropGuard,
}

impl PersistentConnection {
    /// Start configuring a persistent link to `remote` over `endpoint`, using `alpn` for outgoing
    /// dials. Same as [`Builder::new`].
    pub fn builder(endpoint: Endpoint, remote: EndpointId, alpn: impl Into<Vec<u8>>) -> Builder {
        Builder::new(endpoint, remote, alpn)
    }

    /// A [`watch`] receiver of the current connection: `Some` while one is held, `None` otherwise.
    pub fn connection(&self) -> watch::Receiver<Option<Connection>> {
        self.connection.clone()
    }

    /// The current connection, if any, without waiting.
    pub fn current(&self) -> Option<Connection> {
        self.connection.borrow().clone()
    }

    /// Wait for the next change and return the new state. `Err` only once the link has shut down.
    pub async fn changed(&mut self) -> Result<Option<Connection>, watch::error::RecvError> {
        self.connection.changed().await?;
        Ok(self.connection.borrow_and_update().clone())
    }
}
