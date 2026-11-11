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

//! Handler-driven links: the application hands over a [`ConnectionHandler`] and the library calls
//! it once for every connection it installs, one at a time.
//!
//! The handler's [`Outcome`] feeds the dial backoff (see [`crate::connect`]): a connection that
//! ends well resets it, one that fails keeps it growing. Connections the library ends itself
//! (replaced by the preferred one, shutdown) report no outcome.

use std::future::Future;
use std::time::Duration;

use iroh::endpoint::{Connection, VarInt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{self, LoopError};

/// How long a handler may take to return after its cancellation token fired before it is
/// abandoned (its future dropped).
const CANCEL_GRACE: Duration = Duration::from_secs(5);

/// What a [`ConnectionHandler`] reports about the connection it was given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The connection served its purpose; the backoff between outgoing dials is reset.
    Done,
    /// The connection was unusable (for example the application handshake failed); the backoff
    /// keeps growing.
    Failed,
}

/// Serves the connections of one link, one at a time.
pub trait ConnectionHandler: Send + 'static {
    /// Serves `conn` until it is done with it, then reports how it went.
    ///
    /// `cancel` fires when the link shuts down or the connection is replaced by a preferred one;
    /// the handler should then return promptly (after a grace period the future is dropped). When
    /// `handle` returns, the library closes `conn` and the next connection is handled only after
    /// that. The outcome is ignored when the library ended the connection itself.
    fn handle(
        &mut self,
        conn: Connection,
        cancel: CancellationToken,
    ) -> impl Future<Output = Outcome> + Send;
}

pub(crate) struct HandlerLoop<H> {
    pub handler: H,
    /// The currently installed connection.
    pub conn_rx: watch::Receiver<Option<Connection>>,
    /// Outcomes counted by the dial loop.
    pub outcomes: mpsc::UnboundedSender<Outcome>,
    /// `true` while `handler` is working on a connection.
    pub busy: watch::Sender<bool>,
    pub close_code: VarInt,
}

impl<H: ConnectionHandler> HandlerLoop<H> {
    /// Spawns the loop, instrumented with [`tracing::Span::current()`] -- see
    /// [`crate::actor::Actor::spawn`] for why.
    pub fn spawn(self, cancellation: CancellationToken) -> JoinHandle<(H, Result<(), LoopError>)> {
        tokio::spawn(self.run(cancellation).instrument(tracing::Span::current()))
    }

    async fn run(mut self, cancellation: CancellationToken) -> (H, Result<(), LoopError>) {
        let _cancel = cancellation.drop_guard_ref();
        let mut last_handled = None;
        let err = loop {
            let conn = match self.next_connection(last_handled, &cancellation).await {
                Ok(conn) => conn,
                Err(err) => break err,
            };
            last_handled = Some(conn.stable_id());

            self.busy.send_replace(true);
            if let Some(outcome) = self.serve(&conn, &cancellation).await {
                tracing::debug!(?outcome, id = conn.stable_id(), "connection handler finished");
                let _ = self.outcomes.send(outcome);
            }
            // The actor clears the slot once it sees the closure.
            conn.close(self.close_code, b"handler finished");
            self.busy.send_replace(false);
        };
        (self.handler, error::mask_cancelled(Err(error::settle(err, &cancellation))))
    }

    /// Waits for an installed connection that is open and not the one handled last.
    async fn next_connection(
        &mut self,
        last_handled: Option<usize>,
        cancellation: &CancellationToken,
    ) -> Result<Connection, LoopError> {
        loop {
            if let Some(conn) = self.conn_rx.borrow_and_update().clone() {
                if Some(conn.stable_id()) != last_handled && conn.close_reason().is_none() {
                    return Ok(conn);
                }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(LoopError::Cancelled),
                changed = self.conn_rx.changed() => {
                    if changed.is_err() {
                        return Err(LoopError::ChannelClosed);
                    }
                }
            }
        }
    }

    /// Runs the handler on `conn`. Returns the outcome to count, or `None` if the library ended
    /// the connection itself.
    async fn serve(
        &mut self,
        conn: &Connection,
        cancellation: &CancellationToken,
    ) -> Option<Outcome> {
        let conn_cancel = cancellation.child_token();
        let mut handling = std::pin::pin!(self.handler.handle(conn.clone(), conn_cancel.clone()));

        // Whether the connection was seen cleared (the peer closed it) before being replaced.
        let mut ended_by_peer = false;
        let ignore_outcome = loop {
            tokio::select! {
                outcome = &mut handling => return Some(outcome),
                _ = cancellation.cancelled() => break true,
                changed = self.conn_rx.changed() => {
                    if changed.is_err() {
                        break true;
                    }
                    match self.conn_rx.borrow_and_update().as_ref() {
                        Some(current) if current.stable_id() != conn.stable_id() => {
                            break !ended_by_peer;
                        }
                        Some(_) => {}
                        None => ended_by_peer = true,
                    }
                }
            }
        };

        conn_cancel.cancel();
        match tokio::time::timeout(CANCEL_GRACE, &mut handling).await {
            Ok(outcome) if !ignore_outcome => Some(outcome),
            Ok(_) => None,
            Err(_) => {
                tracing::warn!("connection handler ignored cancellation; abandoning it");
                None
            }
        }
    }
}
