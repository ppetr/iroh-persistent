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

//! End-to-end: two real Iroh endpoints on localhost, each running a `PersistentConnection`
//! pointed at the other. Verifies they settle on a single shared connection (the deterministic
//! rule collapses a simultaneous-connect race onto the same one), and that the link re-forms
//! after a drop.
//!
//! Hermetic: `presets::Minimal` (no relay, no n0 discovery) plus a `MemoryLookup` seeded with each
//! endpoint's own direct addresses, so nothing leaves the machine.

use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, ApplicationClose, Connection, ConnectionError};
use iroh::{Endpoint, EndpointId, SecretKey};
use iroh_persistent::{ConnectionHandler, Outcome, PersistentConnection};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const ALPN: &[u8] = b"iroh-persistent/test/two-peers/1";

/// Bind a Minimal endpoint with an attached (initially empty) `MemoryLookup`.
async fn bind() -> (Endpoint, MemoryLookup) {
    let lookup = MemoryLookup::new();
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![ALPN.to_vec()])
        .address_lookup(lookup.clone())
        .bind()
        .await
        .expect("bind");
    (endpoint, lookup)
}

/// Wait until the endpoint has published at least one direct address.
async fn wait_for_addrs(endpoint: &Endpoint) {
    timeout(Duration::from_secs(10), async {
        while endpoint.addr().addrs.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("endpoint never published a direct address");
}

/// A single accept loop feeding an mpsc, wrapped as a `Stream<Item = Connection>` for the builder.
fn incoming_stream(endpoint: &Endpoint) -> impl futures_util::Stream<Item = Connection> {
    let endpoint = endpoint.clone();
    let (tx, rx) = mpsc::channel::<Connection>(4);
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    let _ = tx.send(conn).await;
                }
            });
        }
    });
    futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|c| (c, rx)) })
}

/// Two endpoints that can reach each other, each with a `PersistentConnection` to the other.
struct Pair {
    low: PersistentConnection,
    high: PersistentConnection,
    low_ep: Endpoint,
    high_ep: Endpoint,
    low_id: EndpointId,
    high_id: EndpointId,
}

/// Two endpoints that can reach each other, ordered by id: `(low_ep, low_id, high_ep, high_id)`.
async fn connected_endpoints() -> (Endpoint, EndpointId, Endpoint, EndpointId) {
    let (ep1, lk1) = bind().await;
    let (ep2, lk2) = bind().await;
    wait_for_addrs(&ep1).await;
    wait_for_addrs(&ep2).await;
    lk1.add_endpoint_info(ep2.addr());
    lk2.add_endpoint_info(ep1.addr());

    let (id1, id2) = (ep1.id(), ep2.id());
    if id1 < id2 {
        (ep1, id1, ep2, id2)
    } else {
        (ep2, id2, ep1, id1)
    }
}

impl Pair {
    async fn setup() -> Self {
        let (low_ep, low_id, high_ep, high_id) = connected_endpoints().await;

        let low = PersistentConnection::builder(low_ep.clone(), high_id, ALPN.to_vec())
            .incoming(incoming_stream(&low_ep))
            .spawn();
        let high = PersistentConnection::builder(high_ep.clone(), low_id, ALPN.to_vec())
            .incoming(incoming_stream(&high_ep))
            .spawn();

        Pair { low, high, low_ep, high_ep, low_id, high_id }
    }

    /// Drop both links and close the endpoints gracefully, so the next test starts clean.
    async fn shutdown(self) {
        let Pair { low, high, low_ep, high_ep, .. } = self;
        drop(low);
        drop(high);
        low_ep.close().await;
        high_ep.close().await;
    }
}

/// Wait until `peer` holds a connection that stays put for a short grace period -- long enough to
/// see past the brief window where both peers have dialed and the deterministic rule is still
/// collapsing the two connections into one.
async fn wait_settled(peer: &mut PersistentConnection) -> Connection {
    timeout(Duration::from_secs(20), async {
        loop {
            match peer.current() {
                None => {
                    peer.changed().await.expect("link shut down before connecting");
                }
                Some(conn) => {
                    if timeout(Duration::from_millis(500), peer.changed()).await.is_err() {
                        return conn;
                    }
                }
            }
        }
    })
    .await
    .expect("connection never settled")
}

#[tokio::test]
#[test_log::test]
async fn settles_on_a_single_shared_connection() {
    let mut pair = Pair::setup().await;

    let low_conn = wait_settled(&mut pair.low).await;
    let high_conn = wait_settled(&mut pair.high).await;

    assert_eq!(low_conn.remote_id(), pair.high_id);
    assert_eq!(high_conn.remote_id(), pair.low_id);

    // Opposite sides prove both peers hold the two ends of *one* connection: a split onto two
    // separate connections would leave each side on its own outgoing one (both `Side::Client`).
    assert_ne!(low_conn.side(), high_conn.side(), "peers landed on two different connections");

    // The shared connection actually carries a stream both ways.
    let opener = async {
        let (mut send, _recv) = low_conn.open_bi().await.expect("open_bi");
        send.write_all(b"ping").await.expect("write_all");
        send.finish().expect("finish");
    };
    let accepter = async {
        let (_send, mut recv) = high_conn.accept_bi().await.expect("accept_bi");
        recv.read_to_end(16).await.expect("read_to_end")
    };
    let (_, got) = tokio::join!(opener, accepter);
    assert_eq!(got, b"ping");

    // Give the state a moment and confirm neither side is thrashing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let low_now = pair.low.current().expect("low side dropped its connection");
    let high_now = pair.high.current().expect("high side dropped its connection");
    assert_eq!(low_now.stable_id(), low_conn.stable_id(), "low side switched connections");
    assert_eq!(high_now.stable_id(), high_conn.stable_id(), "high side switched connections");

    pair.shutdown().await;
}

#[tokio::test]
#[test_log::test]
async fn actor_close_carries_the_configured_close_code() {
    // A single managed link plus one plain, passive endpoint on the far side -- nothing there
    // runs a `PersistentConnection` of its own, so nothing redials or otherwise races the check
    // below once the link's actor closes its connection.
    let (raw_ep, raw_lookup) = bind().await;
    let (link_ep, link_lookup) = bind().await;
    wait_for_addrs(&raw_ep).await;
    wait_for_addrs(&link_ep).await;
    // Only the raw side needs to resolve the link: the link's own outgoing dials (to a remote
    // that never accepts them) are left to fail harmlessly in the background.
    raw_lookup.add_endpoint_info(link_ep.addr());
    let _ = &link_lookup;

    let mut link = PersistentConnection::builder(link_ep.clone(), raw_ep.id(), ALPN.to_vec())
        .incoming(incoming_stream(&link_ep))
        .close_code(42u32)
        .spawn();

    let raw_conn = raw_ep.connect(link_ep.id(), ALPN).await.expect("raw_ep failed to connect");

    let _link_conn = wait_settled(&mut link).await;

    // Dropping the link cancels its actor, which closes its held connection as part of shutdown
    // -- an actor-initiated close, not one the test triggers directly.
    drop(link);

    // `raw_conn` belongs to no `PersistentConnection`, so the only close it can observe is the
    // one the link's actor just performed.
    let close = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(err) = raw_conn.close_reason() {
                return err;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the far side never observed the connection close");

    let ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason }) = close else {
        panic!("expected an application close, got {close:?}");
    };
    assert_eq!(error_code, 42u32.into(), "should use the configured close code");
    assert!(
        String::from_utf8_lossy(&reason).starts_with("Cancelled:"),
        "reason should start with the LoopError variant name: {reason:?}"
    );

    raw_ep.close().await;
    link_ep.close().await;
}

#[tokio::test]
#[test_log::test]
async fn reconnects_after_the_connection_is_dropped() {
    let mut pair = Pair::setup().await;

    let low_conn = wait_settled(&mut pair.low).await;
    let high_conn = wait_settled(&mut pair.high).await;
    let dropped_id = low_conn.stable_id();

    // Kill the shared connection from the high side.
    high_conn.close(0u32.into(), b"test drop");

    // The low side must observe the drop and then re-establish a fresh connection.
    timeout(Duration::from_secs(30), async {
        loop {
            if let Some(conn) = pair.low.current() {
                if conn.stable_id() != dropped_id {
                    return;
                }
            }
            pair.low.changed().await.expect("link shut down");
        }
    })
    .await
    .expect("low side never reconnected after the drop");

    pair.shutdown().await;
}

/// Reports every connection it is given, then holds it until the connection closes or the link
/// is cancelled. With `return_first`, the first connection is given back immediately.
struct HoldHandler {
    seen: mpsc::UnboundedSender<Connection>,
    return_first: bool,
    calls: usize,
    cancelled: usize,
}

impl HoldHandler {
    fn new(return_first: bool) -> (Self, mpsc::UnboundedReceiver<Connection>) {
        let (seen, seen_rx) = mpsc::unbounded_channel();
        (Self { seen, return_first, calls: 0, cancelled: 0 }, seen_rx)
    }
}

impl ConnectionHandler for HoldHandler {
    async fn handle(&mut self, conn: Connection, cancel: CancellationToken) -> Outcome {
        self.calls += 1;
        let _ = self.seen.send(conn.clone());
        if self.return_first && self.calls == 1 {
            return Outcome::Done;
        }
        tokio::select! {
            _ = cancel.cancelled() => self.cancelled += 1,
            _ = conn.closed() => {}
        }
        Outcome::Done
    }
}

/// The last connection a handler is given before things go quiet for a short grace period.
async fn settled_handler_conn(seen: &mut mpsc::UnboundedReceiver<Connection>) -> Connection {
    let mut latest = timeout(Duration::from_secs(20), seen.recv())
        .await
        .expect("handler was never given a connection")
        .expect("handler dropped");
    while let Ok(Some(next)) = timeout(Duration::from_millis(500), seen.recv()).await {
        latest = next;
    }
    latest
}

#[tokio::test]
#[test_log::test]
async fn handlers_share_one_connection_and_are_returned_on_shutdown() {
    let (low_ep, low_id, high_ep, high_id) = connected_endpoints().await;
    let (low_cancel, high_cancel) = (CancellationToken::new(), CancellationToken::new());
    let (low_handler, mut low_seen) = HoldHandler::new(false);
    let (high_handler, mut high_seen) = HoldHandler::new(false);

    let low = PersistentConnection::builder(low_ep.clone(), high_id, ALPN.to_vec())
        .incoming(incoming_stream(&low_ep))
        .cancellation(low_cancel.clone())
        .spawn_with(low_handler);
    let high = PersistentConnection::builder(high_ep.clone(), low_id, ALPN.to_vec())
        .incoming(incoming_stream(&high_ep))
        .cancellation(high_cancel.clone())
        .spawn_with(high_handler);

    let low_conn = settled_handler_conn(&mut low_seen).await;
    let high_conn = settled_handler_conn(&mut high_seen).await;
    assert_ne!(low_conn.side(), high_conn.side(), "peers landed on two different connections");
    assert!(low_conn.close_reason().is_none() && high_conn.close_reason().is_none());

    low_cancel.cancel();
    high_cancel.cancel();
    for link in [low, high] {
        let end = timeout(Duration::from_secs(10), link.join()).await.expect("link never ended");
        assert_eq!(end.result, Ok(()));
        let handler = end.handler.expect("handler was lost");
        assert!(handler.cancelled >= 1, "handler was not told to stop");
    }
    assert!(low_conn.close_reason().is_some(), "shutdown should close the connection");

    low_ep.close().await;
    high_ep.close().await;
}

#[tokio::test]
#[test_log::test]
async fn returning_from_the_handler_closes_the_connection_and_a_new_one_follows() {
    let (low_ep, low_id, high_ep, high_id) = connected_endpoints().await;
    let (low_cancel, high_cancel) = (CancellationToken::new(), CancellationToken::new());
    let (low_handler, mut low_seen) = HoldHandler::new(true);
    let (high_handler, mut high_seen) = HoldHandler::new(true);

    let low = PersistentConnection::builder(low_ep.clone(), high_id, ALPN.to_vec())
        .incoming(incoming_stream(&low_ep))
        .cancellation(low_cancel.clone())
        .spawn_with(low_handler);
    let high = PersistentConnection::builder(high_ep.clone(), low_id, ALPN.to_vec())
        .incoming(incoming_stream(&high_ep))
        .cancellation(high_cancel.clone())
        .spawn_with(high_handler);

    // Both handlers return at once on their first connection; the library must close it and
    // both sides must end up handling a different one.
    for seen in [&mut low_seen, &mut high_seen] {
        let first = timeout(Duration::from_secs(20), seen.recv()).await.unwrap().unwrap();
        timeout(Duration::from_secs(30), async {
            loop {
                let next = seen.recv().await.expect("handler dropped");
                if next.stable_id() != first.stable_id() {
                    return next;
                }
            }
        })
        .await
        .expect("no new connection was handled after the first one was returned");
        assert!(first.close_reason().is_some(), "returned connection was not closed");
    }

    low_cancel.cancel();
    high_cancel.cancel();
    for link in [low, high] {
        let end = timeout(Duration::from_secs(10), link.join()).await.expect("link never ended");
        assert_eq!(end.result, Ok(()));
    }
    low_ep.close().await;
    high_ep.close().await;
}
