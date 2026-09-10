//! One shared [`Endpoint`], many peers: accept connections once, demultiplex them by remote
//! [`EndpointId`], and hand each peer its own stream so every [`PersistentConnection`] sees only
//! its own inbound connections.
//!
//! Run with: `cargo run --example multi_peer` (it just sets the wiring up and exits).

use std::collections::HashMap;

use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointId, SecretKey};
use iroh_persistent::PersistentConnection;
use tokio::sync::mpsc;

const ALPN: &[u8] = b"iroh-persistent/example/multi-peer/1";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The peers this node maintains links to. In a real program these come from config.
    let remotes: Vec<EndpointId> = Vec::new();

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::generate())
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;

    // One inbound channel per peer; the accept loop routes into these by `remote_id()`.
    let mut inbound_tx: HashMap<EndpointId, mpsc::Sender<Connection>> = HashMap::new();
    let mut peers = Vec::new();

    for remote in remotes {
        let (tx, rx) = mpsc::channel::<Connection>(4);
        inbound_tx.insert(remote, tx);

        let incoming = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|conn| (conn, rx))
        });

        peers.push(
            PersistentConnection::builder(endpoint.clone(), remote, ALPN.to_vec())
                .incoming(incoming)
                .spawn(),
        );
    }

    // Single accept loop for the whole endpoint: complete each handshake, then dispatch the
    // connection to the matching peer (closing anything from an unconfigured peer).
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let inbound_tx = inbound_tx.clone();
            tokio::spawn(async move {
                let conn = match incoming.await {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::warn!(?err, "incoming handshake failed");
                        return;
                    }
                };
                let remote = conn.remote_id();
                match inbound_tx.get(&remote) {
                    Some(tx) => {
                        let _ = tx.send(conn).await;
                    }
                    None => {
                        conn.close(0u32.into(), b"unknown peer");
                    }
                }
            });
        }
    });

    // `peers` now each maintain exactly one connection to their remote; use
    // `peer.connection()` / `peer.changed()` to follow it.
    drop(peers);
    Ok(())
}
