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

//! Maintain exactly one live Iroh [`Connection`] to a single remote peer.
//!
//! Given a local [`Endpoint`], the remote's [`EndpointId`], and (optionally) a stream of inbound
//! [`Connection`]s already demultiplexed to that peer, [`PersistentConnection`]:
//!
//! * actively dials the remote with exponential backoff whenever no connection is held (the
//!   backoff restarts only after a connection has been held for [`DEFAULT_STABLE_AFTER`] or the
//!   configured [`Builder::stable_after`]);
//! * accepts inbound connections for that peer from the supplied stream;
//! * keeps a single connection, resolving simultaneous connects deterministically -- both peers
//!   agree on the winner from the ordering of their [`EndpointId`]s;
//! * publishes the current connection through a [`tokio::sync::watch`] channel of
//!   `Option<Connection>` (`None` while there is none), or serves it with a handler (see below).
//!
//! Demultiplexing inbound connections by peer (for example a single [`Endpoint`] shared across
//! many peers) is the caller's responsibility; see `examples/multi_peer.rs`.
//!
//! ```no_run
//! # async fn run(endpoint: iroh::Endpoint, remote: iroh::EndpointId) {
//! use iroh_persistent::PersistentConnection;
//!
//! let mut peer = PersistentConnection::builder(endpoint, remote, b"my-alpn/1".to_vec()).spawn();
//! while let Ok(state) = peer.changed().await {
//!     match state {
//!         Some(conn) => { /* use `conn` until it drops */ }
//!         None => { /* disconnected; a reconnect is already in flight */ }
//!     }
//! }
//! # }
//! ```
//!
//! # Handler-driven links
//!
//! [`Builder::spawn_with`] instead takes a [`ConnectionHandler`], which the library calls for each
//! connection it installs, one at a time. The handler's [`Outcome`] drives the dial backoff
//! ([`Outcome::Done`] resets it, [`Outcome::Failed`] keeps it growing), the library closes the
//! connection when the handler returns, and [`PersistentLink::join`] hands the handler back once the
//! link has ended, so the application can finish its own cleanup.
//!
//! ```no_run
//! # async fn run(endpoint: iroh::Endpoint, remote: iroh::EndpointId) {
//! use iroh::endpoint::Connection;
//! use iroh_persistent::{ConnectionHandler, Outcome, PersistentConnection};
//! use tokio_util::sync::CancellationToken;
//!
//! struct Echo;
//!
//! impl ConnectionHandler for Echo {
//!     async fn handle(&mut self, conn: Connection, cancel: CancellationToken) -> Outcome {
//!         tokio::select! {
//!             _ = cancel.cancelled() => Outcome::Done,
//!             _ = conn.closed() => Outcome::Done,
//!         }
//!     }
//! }
//!
//! let link = PersistentConnection::builder(endpoint, remote, b"my-alpn/1".to_vec())
//!     .spawn_with(Echo);
//! let end = link.join().await;
//! # }
//! ```
//!
//! [`Connection`]: iroh::endpoint::Connection
//! [`Endpoint`]: iroh::Endpoint
//! [`EndpointId`]: iroh::EndpointId

mod actor;
mod connect;
mod error;
mod handler;
mod iroh_conn;
mod persistent;

pub use actor::{Direction, ManagedConnection};
pub use connect::OutgoingConnector;
pub use error::LoopError;
pub use handler::{ConnectionHandler, Outcome};
pub use iroh_conn::{IrohConnection, IrohConnector};
pub use persistent::{
    Builder, LinkEnd, PersistentConnection, PersistentLink, DEFAULT_STABLE_AFTER,
};

/// Re-exported so callers can build a custom retry policy for [`Builder::backoff`].
pub use backoff;
