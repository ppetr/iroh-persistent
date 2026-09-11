# iroh-persistent

*Disclaimer: This is a personal project. The views, code, and opinions expressed here are my own and do not represent those of my current or past employers.*

Keep **exactly one** live [Iroh](https://iroh.computer/) `Connection` to a peer, no matter which
side dials first or how often the network drops.

Given

* the local `Endpoint`,
* the remote peer's `EndpointId`,
* and (optionally) a stream of inbound `Connection`s already demultiplexed to that peer,

`PersistentConnection`:

* **actively dials** the remote with exponential backoff whenever no connection is held;
* **accepts** inbound connections for that peer from the supplied stream;
* keeps a **single** connection. It does not race for a "preferred" one — any connection is enough
  — but if both peers dial at once and two connections briefly exist, a **deterministic** rule
  (from the ordering of the two `EndpointId`s) makes both sides keep the *same* one and drop the
  other, so they never end up split with no shared connection;
* publishes the current connection through a `tokio::sync::watch` channel of `Option<Connection>`
  (`None` while there is none).

Demultiplexing inbound connections by peer — e.g. one `Endpoint` shared across many peers — is the
caller's job; see [`examples/multi_peer.rs`](examples/multi_peer.rs).

## Usage

```rust
use iroh_persistent::PersistentConnection;

let mut peer = PersistentConnection::builder(endpoint, remote_id, b"my-alpn/1".to_vec())
    .incoming(inbound_stream)   // optional; omit for an outgoing-only link
    .close_code(42)             // optional; QUIC app error code for our own housekeeping closes
    .spawn();

// Follow the current connection.
while let Ok(state) = peer.changed().await {
    match state {
        Some(conn) => { /* use `conn` until it drops */ }
        None       => { /* disconnected; a reconnect is already in flight */ }
    }
}

// Or grab a `watch::Receiver` and poll it however you like:
let mut rx = peer.connection();
```

Dropping the `PersistentConnection` closes the connection and stops all background tasks.

## Design

Three tasks around one connection slot owned by an actor:

| task | source file | role |
|---|---|---|
| actor | `src/actor.rs` | authoritative `Option<Connection>` slot; deterministic conflict rule; closed-connection watcher |
| outgoing dial loop | `src/connect.rs` | dials with exponential backoff while the slot is empty; parks while a connection is held |
| inbound pump | `src/persistent.rs` | filters the caller's stream to the configured peer, forwards to the actor |

The conflict rule: the first connection is installed regardless of direction; a later one replaces
the current only if its direction matches this side's preference (larger `EndpointId` → outgoing,
smaller → incoming), otherwise it is closed. Because one peer's outgoing connection is the other's
incoming one, when two connections briefly coexist both sides keep the same one.

`src/actor.rs` and `src/connect.rs` are generic (`ManagedConnection` / `OutgoingConnector`) so the
state machine and backoff are unit-tested without a network; `src/iroh_conn.rs` is the Iroh
binding. `tests/two_peers.rs` exercises the whole thing over two real localhost endpoints.

Extracted from the connection-management layer of `zeroconf-p2p-vpn`.

## License

Apache-2.0, see [LICENSE](LICENSE).
