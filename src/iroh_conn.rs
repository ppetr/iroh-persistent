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

//! Iroh implementations of [`ManagedConnection`] and [`OutgoingConnector`].

use std::sync::Arc;

use futures_util::StreamExt;
use iroh::endpoint::{ConnectError, Connection, Endpoint, PathInfoList, Side, VarInt};
use iroh::{EndpointAddr, EndpointId, Watcher};
use tokio::sync::{mpsc, watch};

use crate::actor::{Direction, ManagedConnection};
use crate::connect::OutgoingConnector;

/// An Iroh [`Connection`], carried through the actor as the managed handle.
#[derive(Clone, Debug)]
pub struct IrohConnection {
    pub conn: Connection,
    /// QUIC application error code used when this crate closes `conn` on its own; see
    /// [`crate::Builder::close_code`].
    pub(crate) close_code: VarInt,
    /// Backs [`ManagedConnection::is_direct`]; fed by a task spawned once in [`Self::new`], so
    /// repeated reads are a cheap clone rather than a fresh subscription each time.
    is_direct: watch::Receiver<bool>,
}

impl IrohConnection {
    pub(crate) fn new(conn: Connection, close_code: VarInt) -> Self {
        let is_direct = spawn_is_direct_watcher(conn.clone());
        Self { conn, close_code, is_direct }
    }
}

fn path_is_direct(paths: PathInfoList) -> bool {
    matches!(paths.into_iter().find(|p| p.is_selected()), Some(p) if !p.is_relay())
}

/// Spawns a task that forwards `conn`'s path-selection changes into the returned receiver.
///
/// `IrohConnection::is_direct` hands out clones of that receiver rather than subscribing anew,
/// so as long as any `IrohConnection` sharing it is still alive, at least one receiver (the
/// field itself) exists -- `tx.send` only ever fails once the last one has been dropped, meaning
/// no further clone can be produced either. The loop below relies on exactly that to stop
/// itself; it cannot exit while a subscriber could still turn up.
fn spawn_is_direct_watcher(conn: Connection) -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(path_is_direct(conn.paths().get()));
    let mut changes = conn.paths().map(path_is_direct).stream();
    tokio::spawn(async move {
        while let Some(direct) = changes.next().await {
            if tx.send(direct).is_err() {
                break;
            }
        }
    });
    rx
}

impl PartialEq<IrohConnection> for Connection {
    fn eq(&self, other: &IrohConnection) -> bool {
        self.stable_id() == other.conn.stable_id()
    }
}

impl ManagedConnection for IrohConnection {
    type Key = EndpointId;
    type ConnectionId = Connection;

    fn direction(&self) -> Direction {
        match self.conn.side() {
            Side::Client => Direction::Outgoing,
            Side::Server => Direction::Incoming,
        }
    }

    fn is_direct(&self) -> watch::Receiver<bool> {
        self.is_direct.clone()
    }

    fn close(self, reason: &str) {
        self.conn.close(self.close_code, reason.as_bytes());
    }

    fn spawn_closed_watcher(&self, on_close: mpsc::Sender<Self::ConnectionId>) {
        let conn = self.conn.clone();
        tokio::spawn(async move {
            let _ = conn.closed().await;
            let _ = on_close.send(conn).await;
        });
    }
}

/// Dials a fixed remote [`EndpointId`] over a shared [`Endpoint`].
#[derive(Clone, Debug)]
pub struct IrohConnector {
    pub endpoint: Endpoint,
    pub remote: EndpointId,
    pub alpn: Arc<[u8]>,
    pub close_code: VarInt,
}

impl OutgoingConnector for IrohConnector {
    type Connection = IrohConnection;
    type Error = ConnectError;

    async fn connect(&self) -> Result<IrohConnection, Self::Error> {
        let conn =
            self.endpoint.connect(EndpointAddr::from(self.remote), self.alpn.as_ref()).await?;
        Ok(IrohConnection::new(conn, self.close_code))
    }
}
