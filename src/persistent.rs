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

//! [`PersistentConnection`] and its [`Builder`].

use std::pin::Pin;
use std::sync::Arc;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use futures_core::Stream;
use futures_util::StreamExt;
use iroh::endpoint::{Connection, VarInt};
use iroh::{Endpoint, EndpointId};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::actor::{Actor, ActorHandle};
use crate::connect::OutgoingConnectLoop;
use crate::error::{await_loop_result, LoopError};
use crate::iroh_conn::{IrohConnection, IrohConnector};

type ConnectionStream = Pin<Box<dyn Stream<Item = Connection> + Send>>;

/// Builds a [`PersistentConnection`]. Created via [`PersistentConnection::builder`].
pub struct Builder<B = ExponentialBackoff> {
    endpoint: Endpoint,
    remote: EndpointId,
    alpn: Vec<u8>,
    incoming: Option<ConnectionStream>,
    backoff: B,
    cancellation: Option<CancellationToken>,
    close_code: VarInt,
}

impl Builder<ExponentialBackoff> {
    pub(crate) fn new(endpoint: Endpoint, remote: EndpointId, alpn: impl Into<Vec<u8>>) -> Self {
        Self {
            endpoint,
            remote,
            alpn: alpn.into(),
            incoming: None,
            // Retry outgoing dials forever; the `backoff` default caps only total elapsed time.
            backoff: ExponentialBackoff { max_elapsed_time: None, ..Default::default() },
            cancellation: None,
            close_code: VarInt::from_u32(0),
        }
    }
}

impl<B: Backoff + Send + 'static> Builder<B> {
    /// Feed inbound connections for this peer. Without this the link is outgoing-only.
    ///
    /// The stream must already be filtered to the configured remote; a connection from any other
    /// peer is logged and closed.
    pub fn incoming<S>(mut self, stream: S) -> Self
    where
        S: Stream<Item = Connection> + Send + 'static,
    {
        self.incoming = Some(Box::pin(stream));
        self
    }

    /// Replace the default (unbounded) exponential backoff used between outgoing dials.
    pub fn backoff<B2: Backoff + Send + 'static>(self, backoff: B2) -> Builder<B2> {
        Builder {
            endpoint: self.endpoint,
            remote: self.remote,
            alpn: self.alpn,
            incoming: self.incoming,
            backoff,
            cancellation: self.cancellation,
            close_code: self.close_code,
        }
    }

    /// Drive shutdown from an external token in addition to dropping the returned
    /// [`PersistentConnection`].
    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// The QUIC application error code to close a connection with whenever `PersistentConnection`
    /// does so on its own -- losing the deterministic-preference race, being replaced by the
    /// preferred connection, an unexpected peer on the inbound stream, or shutdown. Defaults to
    /// `0`.
    ///
    /// This crate has no opinion on what the code should be; pick one from your own protocol's
    /// error-code space so a peer can tell these housekeeping closes apart from ones your
    /// application raises itself. The specific reason is still carried as text and readable from
    /// the connection's `close_reason()`.
    pub fn close_code(mut self, code: impl Into<VarInt>) -> Self {
        self.close_code = code.into();
        self
    }

    /// Spawn the background tasks and return the handle.
    pub fn spawn(self) -> PersistentConnection {
        let cancellation = self.cancellation.unwrap_or_default();
        let local_id = self.endpoint.id();
        let remote = self.remote;

        let connector = IrohConnector {
            endpoint: self.endpoint,
            remote,
            alpn: Arc::from(self.alpn.into_boxed_slice()),
            close_code: self.close_code,
        };

        // The actor owns the authoritative `Option<IrohConnection>` slot.
        let (state_tx, state_rx) = watch::channel(None);
        let actor = Actor::new(local_id, remote, state_tx);
        let (actor_handle, actor_join) = actor.spawn(cancellation.clone());

        let outgoing_join =
            OutgoingConnectLoop::new(connector, state_rx.clone(), actor_handle.conn_tx.clone())
                .spawn(self.backoff, cancellation.clone());

        let incoming_join = spawn_incoming_pump(
            self.incoming,
            remote,
            self.close_code,
            actor_handle.conn_tx.clone(),
            cancellation.clone(),
        );

        // Mirror the actor's slot into a channel of the raw `Connection`, so the internal
        // `IrohConnection` wrapper never reaches the caller.
        let (conn_tx, conn_rx) = watch::channel(None);
        spawn_state_mirror(state_rx, conn_tx, cancellation.clone());

        let supervisor = tokio::spawn(async move {
            await_loop_result(outgoing_join).await?;
            await_loop_result(incoming_join).await?;
            await_loop_result(actor_join).await
        });

        PersistentConnection {
            connection: conn_rx,
            // `ActorHandle` carries a drop guard for `cancellation`; holding it here is what keeps
            // the background tasks alive until this `PersistentConnection` is dropped.
            _actor: actor_handle,
            _supervisor: supervisor,
        }
    }
}

/// A single, self-healing Iroh connection to one peer.
///
/// Drop it to close the connection and stop all background work.
pub struct PersistentConnection {
    connection: watch::Receiver<Option<Connection>>,
    _actor: ActorHandle<IrohConnection>,
    _supervisor: JoinHandle<Result<(), LoopError>>,
}

impl PersistentConnection {
    /// Start configuring a persistent link to `remote` over `endpoint`, using `alpn` for outgoing
    /// dials.
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

/// Reads connections from `incoming`, drops any that are not from `remote`, and forwards the rest
/// to the actor. A closed or absent stream is not an error -- the task simply parks until
/// cancellation, leaving the outgoing dialer running.
fn spawn_incoming_pump(
    incoming: Option<ConnectionStream>,
    remote: EndpointId,
    close_code: VarInt,
    conn_tx: mpsc::Sender<IrohConnection>,
    cancellation: CancellationToken,
) -> JoinHandle<Result<(), LoopError>> {
    tokio::spawn(async move {
        let _cancel = cancellation.drop_guard_ref();
        let Some(mut incoming) = incoming else {
            cancellation.cancelled().await;
            return Err(LoopError::Cancelled);
        };
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Err(LoopError::Cancelled),
                next = incoming.next() => {
                    let Some(conn) = next else {
                        tracing::debug!("incoming connection stream ended; outgoing dials continue");
                        cancellation.cancelled().await;
                        return Err(LoopError::Cancelled);
                    };
                    let actual = conn.remote_id();
                    if actual != remote {
                        tracing::warn!(expected = %remote, %actual,
                            "incoming connection from an unexpected peer; closing it");
                        conn.close(close_code, b"unexpected peer");
                        continue;
                    }
                    if conn_tx.send(IrohConnection { conn, close_code }).await.is_err() {
                        return Err(LoopError::ChannelClosed);
                    }
                }
            }
        }
    })
}

/// Mirrors the actor's `Option<IrohConnection>` slot 1:1 into `conn_tx` as `Option<Connection>`.
fn spawn_state_mirror(
    mut state_rx: watch::Receiver<Option<IrohConnection>>,
    conn_tx: watch::Sender<Option<Connection>>,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        let _cancel = cancellation.drop_guard_ref();
        publish(&conn_tx, &state_rx.borrow_and_update());
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    publish(&conn_tx, &state_rx.borrow_and_update());
                }
            }
        }
    });
}

fn publish(conn_tx: &watch::Sender<Option<Connection>>, state: &Option<IrohConnection>) {
    let next = state.as_ref().map(|c| c.conn.clone());
    let changed = conn_tx.borrow().as_ref().map(Connection::stable_id)
        != next.as_ref().map(Connection::stable_id);
    if changed {
        conn_tx.send_replace(next);
    }
}
