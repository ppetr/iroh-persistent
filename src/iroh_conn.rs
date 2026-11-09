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
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::actor::{Direction, ManagedConnection};
use crate::connect::OutgoingConnector;

/// An Iroh [`Connection`], carried through the actor as the managed handle.
#[derive(Clone)]
pub struct IrohConnection {
    pub conn: Connection,
    /// QUIC application error code used when this crate closes `conn` on its own; see
    /// [`crate::Builder::close_code`].
    pub(crate) close_code: VarInt,
}

impl std::fmt::Debug for IrohConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrohConnection")
            .field("id", &self.conn.stable_id())
            .field("side", &self.conn.side())
            .finish()
    }
}

/// Logs `conn`'s network paths at debug level now and after every change, until it closes.
fn spawn_path_logger(conn: Connection) {
    fn log_paths(id: usize, list: PathInfoList) {
        let paths: Vec<_> = list
            .into_iter()
            .map(|p| (p.remote_addr().clone(), p.is_selected(), p.is_closed()))
            .collect();
        tracing::debug!(id, ?paths, "paths (addr, selected, closed)");
    }
    log_paths(conn.stable_id(), conn.paths().get());
    let mut changes = conn.paths().stream();
    tokio::spawn(
        async move {
            loop {
                tokio::select! {
                    _ = conn.closed() => break,
                    Some(list) = changes.next() => log_paths(conn.stable_id(), list),
                    else => break,
                }
            }
        }
        .instrument(tracing::Span::current()),
    );
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

    fn close(self, reason: &str) {
        tracing::debug!(id = self.conn.stable_id(), reason, "closing connection");
        self.conn.close(self.close_code, reason.as_bytes());
    }

    fn spawn_closed_watcher(&self, on_close: mpsc::Sender<Self::ConnectionId>) {
        let conn = self.conn.clone();
        spawn_path_logger(conn.clone());
        tokio::spawn(
            async move {
                let reason = conn.closed().await;
                tracing::debug!(id = conn.stable_id(), %reason, "connection closed");
                let _ = on_close.send(conn).await;
            }
            .instrument(tracing::Span::current()),
        );
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
        Ok(IrohConnection { conn, close_code: self.close_code })
    }
}
