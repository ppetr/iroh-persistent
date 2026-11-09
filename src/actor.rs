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

//! The single-connection state machine.
//!
//! `Actor` owns the authoritative "current connection" slot for one peer. Connection candidates --
//! from the outgoing dialer and from the inbound stream alike -- are submitted to
//! [`Actor::connection_queue`]; the actor applies the deterministic preference rule, watches the
//! winner for closure, closes losers, and publishes the slot through a [`watch`] channel.
//!
//! This module is generic over [`ManagedConnection`] so its logic can be tested without a network;
//! the Iroh implementation lives in [`crate::iroh_conn`].

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::Instrument;

use crate::error::{self, LoopError};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Direction {
    Incoming,
    Outgoing,
}

/// Operations the [`Actor`] needs over a connection handle.
pub trait ManagedConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Peer identity type, used only for the deterministic preference rule (see [`Actor::new`]).
    type Key: Ord + Clone;
    /// Identifies a connection reported closed by [`spawn_closed_watcher`], so the actor can tell
    /// whether it is the one currently installed.
    ///
    /// [`spawn_closed_watcher`]: ManagedConnection::spawn_closed_watcher
    type ConnectionId: PartialEq<Self> + Send;

    /// Whether this handle is an inbound or an outbound connection.
    fn direction(&self) -> Direction;
    /// Close this connection, carrying `reason` for the peer's benefit (informational).
    fn close(self, reason: &str);
    /// Spawn a task that sends this connection's id to `on_close` once it is closed.
    fn spawn_closed_watcher(&self, on_close: mpsc::Sender<Self::ConnectionId>);
}

pub struct Actor<C: ManagedConnection> {
    preferred: Direction,
    watch_tx: watch::Sender<Option<C>>,
    conn_tx: mpsc::Sender<C>,
    conn_rx: mpsc::Receiver<C>,
}

impl<C: ManagedConnection> Actor<C> {
    /// Creates an actor for the link between `local_key` and `remote_key`.
    ///
    /// The two keys pick a preferred direction: the endpoint with the larger key prefers its
    /// outgoing connection, the other prefers its incoming one. Because one peer's outgoing
    /// connection is the other's incoming connection, both sides converge on the same one.
    ///
    /// `watch_tx` receives every change to the current-connection slot.
    pub fn new(local_key: C::Key, remote_key: C::Key, watch_tx: watch::Sender<Option<C>>) -> Self {
        let (conn_tx, conn_rx) = mpsc::channel(32);
        Self {
            preferred: if local_key > remote_key {
                Direction::Outgoing
            } else {
                Direction::Incoming
            },
            watch_tx,
            conn_tx,
            conn_rx,
        }
    }

    /// Both incoming and outgoing connection candidates are submitted to this queue.
    pub fn connection_queue(&self) -> mpsc::Sender<C> {
        self.conn_tx.clone()
    }

    /// Spawns the actor's run loop, instrumented with [`tracing::Span::current()`] -- call this
    /// from inside the caller's own span (e.g. one carrying a peer name) so the actor's otherwise
    /// peer-agnostic log lines ("preferred direction: ...", candidate accept/reject decisions)
    /// can be told apart when a process manages more than one peer.
    pub fn spawn(
        self,
        cancellation: CancellationToken,
    ) -> (ActorHandle<C>, JoinHandle<Result<(), LoopError>>) {
        let drop_guard = cancellation.clone().drop_guard();
        let conn_tx = self.connection_queue();
        let handle = tokio::spawn(self.run(cancellation).instrument(tracing::Span::current()));
        (ActorHandle { conn_tx, _drop_guard: Arc::new(drop_guard) }, handle)
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<(), LoopError> {
        let _cancel = cancellation.drop_guard_ref();
        let (closed_tx, mut closed_rx) = mpsc::channel(32);
        tracing::info!("preferred direction: {:?}", self.preferred);

        let result = loop {
            tokio::select! {
                _ = cancellation.cancelled() => break LoopError::Cancelled,
                c = self.conn_rx.recv() => if let Some(conn) = c {
                        self.handle_connection_candidate(&closed_tx, conn)
                    } else {
                        break LoopError::ChannelClosed
                    },
                Some(closed_conn) = closed_rx.recv() => self.handle_connection_closed(closed_conn),
            }
        };
        self.watch_tx.send_if_modified(|current| {
            if let Some(conn) = current.take() {
                // `LoopError`'s `Display` (the full "Name: message") is what we want here, not
                // its shorter `AsRef<str>` (just "Name") -- clippy can't tell the two apart.
                #[allow(clippy::unnecessary_to_owned)]
                conn.close(&result.to_string());
            }
            // Do not notify the connector: we won't accept any more connections.
            false
        });
        error::mask_cancelled(Err(result))
    }

    fn handle_connection_candidate(&self, closed_tx: &mpsc::Sender<C::ConnectionId>, new_conn: C) {
        self.watch_tx.send_if_modified(|current| {
            // Apply the transition directly to the authoritative watch channel.
            match &mut *current {
                Some(current) if new_conn.direction() != self.preferred => {
                    tracing::debug!("rejecting new {:?}; current: {:?}", new_conn, current);
                    new_conn.close("rejected by the deterministic preference rule");
                    false
                }
                current => {
                    tracing::debug!("replacing current {:?} with new {:?}", current, new_conn);
                    // Start monitoring the newly installed connection for closure.
                    new_conn.spawn_closed_watcher(closed_tx.clone());
                    // Swap in the new connection without emitting an intermediate `None`.
                    let replaced = current.replace(new_conn);
                    // Gracefully close the replaced connection after the watch state transition.
                    if let Some(old_conn) = replaced {
                        old_conn.close("replaced by a preferred-direction connection");
                    }
                    true
                }
            }
        });
    }

    fn handle_connection_closed(&self, closed_conn_id: C::ConnectionId) {
        self.watch_tx.send_if_modified(|current| {
            // Only clear the slot if the closed connection is exactly the installed one.
            if let Some(active) = current {
                if closed_conn_id == *active {
                    tracing::debug!("installed {:?} closed; clearing the slot", active);
                    *current = None;
                    return true;
                }
            }
            tracing::debug!("ignoring closure of a connection that is not installed");
            false
        });
    }
}

#[derive(Clone, Debug)]
pub struct ActorHandle<C: ManagedConnection> {
    pub conn_tx: mpsc::Sender<C>,
    _drop_guard: Arc<DropGuard>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use tokio::sync::{mpsc, watch};
    use tokio::time::{timeout, Duration};
    use tokio_util::sync::CancellationToken;

    #[derive(Clone, Debug)]
    struct FakeConnection {
        id: isize,
        direction: Direction,
        closed_tx: watch::Sender<bool>,
        closed_rx: watch::Receiver<bool>,
        closed_signal: Arc<OnceLock<mpsc::Sender<isize>>>,
    }

    impl FakeConnection {
        fn new(id: isize, direction: Direction) -> Self {
            let (closed_tx, closed_rx) = watch::channel(false);
            Self { id, direction, closed_tx, closed_rx, closed_signal: Arc::new(OnceLock::new()) }
        }

        fn was_closed(&self) -> bool {
            *self.closed_rx.borrow()
        }

        async fn close_from_remote(&self) {
            self.closed_signal
                .get()
                .expect("closed watcher was not installed")
                .send(self.id)
                .await
                .unwrap();
        }
    }

    impl PartialEq<FakeConnection> for isize {
        fn eq(&self, other: &FakeConnection) -> bool {
            *self == other.id
        }
    }

    impl ManagedConnection for FakeConnection {
        type Key = isize;
        type ConnectionId = isize;

        fn direction(&self) -> Direction {
            self.direction
        }

        fn close(self, _: &str) {
            let _ = self.closed_tx.send(true);
        }

        fn spawn_closed_watcher(&self, on_close: mpsc::Sender<Self::ConnectionId>) {
            self.closed_signal.set(on_close).expect("closed watcher installed twice");
        }
    }

    struct TestActor {
        actor: ActorHandle<FakeConnection>,
        watch_rx: watch::Receiver<Option<FakeConnection>>,
        cancellation: CancellationToken,
    }

    impl TestActor {
        fn new(local_key: isize, remote_key: isize) -> Self {
            let (watch_tx, watch_rx) = watch::channel(None);

            let cancellation = CancellationToken::new();

            let actor = Actor::new(local_key, remote_key, watch_tx);
            let (actor, _) = actor.spawn(cancellation.clone());

            TestActor { actor, watch_rx, cancellation }
        }

        async fn send(&mut self, conn: &FakeConnection) {
            self.actor.conn_tx.send(conn.clone()).await.unwrap();
        }

        async fn send_and_wait(&mut self, conn: &FakeConnection) -> FakeConnection {
            self.actor.conn_tx.send(conn.clone()).await.unwrap();
            self.wait_for_connection().await.expect("connection was not installed")
        }

        fn receiver(&mut self) -> &mut watch::Receiver<Option<FakeConnection>> {
            &mut self.watch_rx
        }

        async fn wait_for_connection(&mut self) -> Option<FakeConnection> {
            timeout(Duration::from_secs(1), async {
                self.receiver().changed().await.expect("connection queue closed");
                self.receiver().borrow_and_update().clone()
            })
            .await
            .expect("timed out waiting for a connection")
        }
    }

    #[tokio::test]
    async fn subscribe_initially_returns_none() {
        let mut actor = TestActor::new(1, 2);

        assert!(actor.receiver().borrow_and_update().is_none());

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn accepts_first_connection_even_if_not_preferred() {
        let mut actor = TestActor::new(1, 2);

        // Local key is smaller, therefore incoming is preferred.
        // The first connection is accepted regardless of direction.

        let conn = FakeConnection::new(1, Direction::Outgoing);

        let current = actor.send_and_wait(&conn).await;

        assert_eq!(current.id, 1);
        assert!(!conn.was_closed());

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn preferred_connection_replaces_existing_connection() {
        let mut actor = TestActor::new(2, 1);

        // Local key is larger, therefore outgoing is preferred.
        let incoming = FakeConnection::new(1, Direction::Incoming);
        let outgoing = FakeConnection::new(2, Direction::Outgoing);

        let _ = actor.send_and_wait(&incoming).await;
        let current = actor.send_and_wait(&outgoing).await;

        assert_eq!(current.id, outgoing.id);
        assert!(incoming.was_closed());
        assert!(!outgoing.was_closed());

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn non_preferred_connection_is_rejected() {
        let mut actor = TestActor::new(2, 1);

        // Local key is larger, therefore outgoing is preferred.
        let outgoing = FakeConnection::new(1, Direction::Outgoing);
        let incoming = FakeConnection::new(2, Direction::Incoming);

        let _ = actor.send_and_wait(&outgoing).await;
        actor.send(&incoming).await;

        timeout(Duration::from_secs(1), async {
            while !incoming.was_closed() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("timed out waiting for `incoming` to be closed by `actor`");

        assert!(!actor.receiver().has_changed().unwrap());
        let current =
            actor.receiver().borrow_and_update().clone().expect("connection was not installed");
        assert_eq!(current.id, outgoing.id);

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn closing_active_connection_clears_watch() {
        let mut actor = TestActor::new(1, 2);

        let conn = FakeConnection::new(1, Direction::Incoming);

        let _ = actor.send_and_wait(&conn).await;

        conn.close_from_remote().await;

        let current = actor.wait_for_connection().await;
        assert!(current.is_none());

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn closing_replaced_connection_is_ignored() {
        let mut actor = TestActor::new(2, 1);

        let old = FakeConnection::new(1, Direction::Incoming);
        let new = FakeConnection::new(2, Direction::Outgoing);

        let _ = actor.send_and_wait(&old).await;

        let _ = actor.send_and_wait(&new).await;

        old.close_from_remote().await;

        let current =
            actor.receiver().borrow_and_update().clone().expect("connection was not installed");

        assert_eq!(current.id, new.id);

        actor.cancellation.cancel();
    }

    #[tokio::test]
    async fn shutdown_closes_active_connection() {
        let mut actor = TestActor::new(1, 2);

        let mut conn = FakeConnection::new(1, Direction::Incoming);

        let current = actor.send_and_wait(&conn).await;
        assert_eq!(current.id, conn.id);
        conn.closed_rx.mark_unchanged();

        actor.cancellation.cancel();

        conn.closed_rx.changed().await.unwrap();
        assert!(conn.was_closed());
    }

    #[tokio::test]
    async fn preference_depends_on_key_order() {
        let mut actor_a = TestActor::new(1, 2);
        let mut actor_b = TestActor::new(2, 1);

        let incoming = FakeConnection::new(1, Direction::Incoming);
        let outgoing = FakeConnection::new(2, Direction::Outgoing);

        let selected_a = actor_a.send_and_wait(&incoming).await;
        let selected_b = actor_b.send_and_wait(&outgoing).await;

        assert_eq!(selected_a.direction, Direction::Incoming);
        assert_eq!(selected_b.direction, Direction::Outgoing);

        actor_a.cancellation.cancel();
        actor_b.cancellation.cancel();
    }
}
