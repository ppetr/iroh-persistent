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

//! The application's side of a link: a [`ConnectionHandler`] is called once for every connection
//! the link settles on, one at a time.
//!
//! The handler's [`Outcome`] feeds the dial back-off (see [`crate::link`]): a connection that ends
//! well resets it, one that fails keeps it growing. Connections the link ends itself (replaced by
//! the preferred one, shutdown) report no outcome.

use std::future::Future;

use iroh::endpoint::Connection;
use tokio_util::sync::CancellationToken;

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
    /// `cancel` fires as soon as `conn` should no longer be used: the peer closed it, a preferred
    /// connection replaces it, or the link shuts down. The handler need not tell these apart; it
    /// should return promptly (after a grace period the future is dropped). Because the peer
    /// closing a connection fires `cancel` too, a connection that was cancelled before the
    /// handler's own work finished is better reported as [`Outcome::Failed`].
    ///
    /// When `handle` returns, the link closes `conn` and handles the next connection only after
    /// that. The outcome is ignored when the link itself ended the connection (replacement,
    /// shutdown).
    fn handle(
        &mut self,
        conn: Connection,
        cancel: CancellationToken,
    ) -> impl Future<Output = Outcome> + Send;
}
