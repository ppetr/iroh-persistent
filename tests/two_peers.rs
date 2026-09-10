//! End-to-end: two real Iroh endpoints on localhost, each running a `PersistentConnection`
//! pointed at the other. Verifies they settle on a single shared connection (the deterministic
//! rule collapses a simultaneous-connect race onto the same one), and that the link re-forms
//! after a drop.
//!
//! Hermetic: `presets::Minimal` (no relay, no n0 discovery) plus a `MemoryLookup` seeded with each
//! endpoint's own direct addresses, so nothing leaves the machine.

use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointId, SecretKey};
use iroh_persistent::PersistentConnection;
use tokio::sync::mpsc;
use tokio::time::timeout;

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

impl Pair {
    async fn setup() -> Self {
        let (ep1, lk1) = bind().await;
        let (ep2, lk2) = bind().await;
        wait_for_addrs(&ep1).await;
        wait_for_addrs(&ep2).await;
        lk1.add_endpoint_info(ep2.addr());
        lk2.add_endpoint_info(ep1.addr());

        let (id1, id2) = (ep1.id(), ep2.id());
        let ((low_ep, low_id), (high_ep, high_id)) =
            if id1 < id2 { ((ep1, id1), (ep2, id2)) } else { ((ep2, id2), (ep1, id1)) };

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
