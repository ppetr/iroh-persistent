//! Maintain exactly one live Iroh [`Connection`] to a single remote peer.
//!
//! Given a local [`Endpoint`], the remote's [`EndpointId`], and (optionally) a stream of inbound
//! [`Connection`]s already demultiplexed to that peer, [`PersistentConnection`]:
//!
//! * actively dials the remote with exponential backoff whenever no connection is held;
//! * accepts inbound connections for that peer from the supplied stream;
//! * keeps a single connection, resolving simultaneous connects deterministically -- both peers
//!   agree on the winner from the ordering of their [`EndpointId`]s;
//! * publishes the current connection through a [`tokio::sync::watch`] channel of
//!   `Option<Connection>` (`None` while there is none).
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
//! [`Connection`]: iroh::endpoint::Connection
//! [`Endpoint`]: iroh::Endpoint
//! [`EndpointId`]: iroh::EndpointId

mod actor;
mod connect;
mod error;
mod iroh_conn;
mod persistent;

pub use actor::{Direction, ManagedConnection};
pub use connect::OutgoingConnector;
pub use error::LoopError;
pub use iroh_conn::{IrohConnection, IrohConnector};
pub use persistent::{Builder, PersistentConnection};

/// Re-exported so callers can build a custom retry policy for [`Builder::backoff`].
pub use backoff;
