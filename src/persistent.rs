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

//! [`Builder`] and [`PersistentLink`]: a link to one peer whose connections are served by a
//! [`ConnectionHandler`].

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use futures_core::Stream;
use futures_util::StreamExt;
use iroh::endpoint::{Connection, VarInt};
use iroh::{Endpoint, EndpointId};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::Instrument;

use crate::error::LoopError;
use crate::handler::ConnectionHandler;
use crate::iroh_conn::{IrohConnection, IrohConnector};
use crate::link::{preferred_direction, Inbound, Link};

type LinkTask<H> = JoinHandle<(H, Result<(), LoopError>)>;
type ConnectionStream = Pin<Box<dyn Stream<Item = Connection> + Send>>;

/// Configures a link to one peer; finish with [`Builder::spawn_with`].
pub struct Builder<B = ExponentialBackoff> {
    endpoint: Endpoint,
    remote: EndpointId,
    alpn: Vec<u8>,
    incoming: Option<ConnectionStream>,
    backoff: B,
    pub(crate) stable_after: Duration,
    cancellation: Option<CancellationToken>,
    close_code: VarInt,
}

impl Builder<ExponentialBackoff> {
    /// Starts configuring a link to `remote` over `endpoint`, using `alpn` for outgoing dials.
    pub fn new(endpoint: Endpoint, remote: EndpointId, alpn: impl Into<Vec<u8>>) -> Self {
        Self {
            endpoint,
            remote,
            alpn: alpn.into(),
            incoming: None,
            // Retry outgoing dials forever; the `backoff` default caps only total elapsed time.
            backoff: ExponentialBackoff { max_elapsed_time: None, ..Default::default() },
            stable_after: crate::legacy::DEFAULT_STABLE_AFTER,
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
            stable_after: self.stable_after,
            cancellation: self.cancellation,
            close_code: self.close_code,
        }
    }

    /// Drive shutdown from an external token in addition to dropping the returned link.
    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// The QUIC application error code to close a connection with whenever the link does so on
    /// its own -- losing the deterministic-preference race, being replaced by the preferred
    /// connection, an unexpected peer on the inbound stream, the handler finishing, or shutdown.
    /// Defaults to `0`.
    ///
    /// This crate has no opinion on what the code should be; pick one from your own protocol's
    /// error-code space so a peer can tell these housekeeping closes apart from ones your
    /// application raises itself. The specific reason is still carried as text and readable from
    /// the connection's `close_reason()`.
    pub fn close_code(mut self, code: impl Into<VarInt>) -> Self {
        self.close_code = code.into();
        self
    }

    /// Starts the link and hands every connection it settles on to `handler`, one at a time.
    ///
    /// The handler's [`crate::Outcome`] is the rule for resetting the dial backoff:
    /// [`crate::Outcome::Done`] resets it, [`crate::Outcome::Failed`] keeps it growing. When
    /// [`ConnectionHandler::handle`] returns, the link closes the connection. Dropping the
    /// returned [`PersistentLink`] shuts everything down; see also [`PersistentLink::join`].
    ///
    /// The link task is instrumented with [`tracing::Span::current()`] as seen at this call site
    /// -- wrap the call in your own span (e.g. carrying a peer name) if you manage more than one
    /// link and want to tell their log lines apart.
    pub fn spawn_with<H: ConnectionHandler>(self, handler: H) -> PersistentLink<H> {
        let (task, guard) = self.launch(handler);
        PersistentLink { task, _guard: guard }
    }

    /// Spawns the link task. The guard cancels it when dropped.
    pub(crate) fn launch<H: ConnectionHandler>(self, handler: H) -> (LinkTask<H>, DropGuard) {
        let cancellation = self.cancellation.unwrap_or_default();
        let remote = self.remote;
        let close_code = self.close_code;

        let connector = IrohConnector {
            endpoint: self.endpoint.clone(),
            remote,
            alpn: Arc::from(self.alpn.into_boxed_slice()),
            close_code,
        };
        let link = Link {
            connector,
            handler,
            inbound: Inbound::new(self.incoming.map(|s| expect_remote(s, remote, close_code))),
            preferred: preferred_direction(&self.endpoint.id(), &remote),
        };
        let task = tokio::spawn(
            link.run(self.backoff, cancellation.clone()).instrument(tracing::Span::current()),
        );
        (task, cancellation.drop_guard())
    }
}

/// Closes connections of `incoming` that are not from `remote` and wraps the rest.
fn expect_remote(
    incoming: ConnectionStream,
    remote: EndpointId,
    close_code: VarInt,
) -> Pin<Box<dyn Stream<Item = IrohConnection> + Send>> {
    incoming
        .filter_map(move |conn| async move {
            let actual = conn.remote_id();
            if actual != remote {
                tracing::warn!(expected = %remote, %actual,
                    "incoming connection from an unexpected peer; closing it");
                conn.close(close_code, b"unexpected peer");
                None
            } else {
                Some(IrohConnection::new(conn, close_code))
            }
        })
        .boxed()
}

/// A link whose connections are served by a [`ConnectionHandler`]; see [`Builder::spawn_with`].
///
/// Drop it to close the connection and stop all background work.
pub struct PersistentLink<H> {
    task: LinkTask<H>,
    _guard: DropGuard,
}

/// How a [`PersistentLink`] ended.
#[derive(Debug)]
pub struct LinkEnd<H> {
    /// The handler, handed back so the application can finish its own cleanup. `None` if the
    /// link's task panicked (and `result` is then [`LoopError::Panicked`]).
    pub handler: Option<H>,
    /// `Ok` after a normal shutdown, otherwise why the link ended.
    pub result: Result<(), LoopError>,
}

impl<H> PersistentLink<H> {
    /// Waits until the link ends -- cancellation through [`Builder::cancellation`], or a failure
    /// of the link task -- and returns the handler. The handler is no longer running by then.
    pub async fn join(self) -> LinkEnd<H> {
        match self.task.await {
            Ok((handler, result)) => LinkEnd { handler: Some(handler), result },
            Err(err) if err.is_cancelled() => LinkEnd { handler: None, result: Ok(()) },
            Err(err) => {
                tracing::error!(error = ?err, "link task panicked");
                LinkEnd { handler: None, result: Err(LoopError::Panicked) }
            }
        }
    }
}
