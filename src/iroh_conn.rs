//! Iroh implementations of [`ManagedConnection`] and [`OutgoingConnector`].

use std::sync::Arc;

use iroh::endpoint::{ConnectError, Connection, Endpoint, Side, VarInt};
use iroh::{EndpointAddr, EndpointId};
use tokio::sync::mpsc;

use crate::actor::{Direction, ManagedConnection};
use crate::connect::OutgoingConnector;

/// An Iroh [`Connection`], carried through the actor as the managed handle.
#[derive(Clone, Debug)]
pub struct IrohConnection {
    pub conn: Connection,
    /// QUIC application error code used when this crate closes `conn` on its own; see
    /// [`crate::Builder::close_code`].
    pub(crate) close_code: VarInt,
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
        Ok(IrohConnection { conn, close_code: self.close_code })
    }
}
