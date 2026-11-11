# iroh-persistent

*Disclaimer: This is a personal project. The views, code, and opinions expressed here are my own and do not represent those of my current or past employers.*

Keep **exactly one** live [Iroh](https://iroh.computer/) `Connection` to a peer, no matter which
side dials first or how often the network drops.

Given

* the local `Endpoint`,
* the remote peer's `EndpointId`,
* and (optionally) a stream of inbound `Connection`s already demultiplexed to that peer,

a link:

* **actively dials** the remote with exponential backoff whenever it has no connection;
* **accepts** inbound connections for that peer from the supplied stream;
* keeps a **single** connection. It does not race for a "preferred" one -- any connection is enough
  -- but if both peers dial at once and two connections briefly exist, a **deterministic** rule
  (from the ordering of the two `EndpointId`s) makes both sides keep the *same* one and drop the
  other, so they never end up split with no shared connection;
* hands each connection to your `ConnectionHandler`, one at a time, and lets the handler's
  `Outcome` decide whether the redial backoff restarts (`Done`) or keeps growing (`Failed`).

Demultiplexing inbound connections by peer -- e.g. one `Endpoint` shared across many peers -- is the
caller's job; see [`examples/multi_peer.rs`](examples/multi_peer.rs).

## Usage

```rust
use iroh::endpoint::Connection;
use iroh_persistent::{Builder, ConnectionHandler, Outcome};
use tokio_util::sync::CancellationToken;

struct MyHandler;

impl ConnectionHandler for MyHandler {
    async fn handle(&mut self, conn: Connection, cancel: CancellationToken) -> Outcome {
        // Use `conn`; `cancel` fires when the peer closed it, a preferred connection replaces it,
        // or the link shuts down. Return promptly then.
        cancel.cancelled().await;
        Outcome::Done
    }
}

let link = Builder::new(endpoint, remote_id, b"my-alpn/1".to_vec())
    .incoming(inbound_stream)   // optional; omit for an outgoing-only link
    .close_code(42u32)          // optional; QUIC app error code for our own housekeeping closes
    .spawn_with(MyHandler);

// ... later, after cancelling the link (dropping it, or the token given to `.cancellation()`):
let end = link.join().await;   // the handler comes back for its own cleanup
```

Dropping the `PersistentLink` closes the connection and stops the link.

The older, simpler `PersistentConnection` publishes the current connection through a
`tokio::sync::watch` channel of `Option<Connection>` instead. It is built on the handler API and
planned for removal in 0.2.0.

## Design

One task per link, running a sequential loop whose state lives in local variables
(`src/link.rs`):

* **idle** -- waits for the next connection: an inbound one, or the result of a dial after the
  backoff delay (an inbound connection wins over a pending dial);
* **serving** -- runs the handler on the connection and waits for the first of:
  - the handler returning (the link then closes the connection and counts the outcome),
  - the peer closing the connection (the handler is cancelled and its outcome counted),
  - a preferred inbound connection (the handler is cancelled, the old
    connection closed, the outcome ignored, the new one served next),
  - or shutdown.
  A handler that ignores its cancellation is abandoned after a grace period.

The conflict rule: the first connection is accepted regardless of direction; a later one replaces
the current only if its direction matches this side's preference (larger `EndpointId` -> outgoing,
smaller -> incoming), otherwise it is closed. Because one peer's outgoing connection is the other's
incoming one, when two connections briefly coexist both sides keep the same one.

`src/link.rs` is generic over a small connection and connector interface, so the loop and backoff
are unit-tested without a network; `src/iroh_conn.rs` is the Iroh binding. `src/legacy.rs` holds
the watch-based API on top of the handler API. `tests/two_peers.rs` exercises the whole thing over
two real localhost endpoints.

Extracted from the connection-management layer of `zeroconf-p2p-vpn`.

## License

Apache-2.0, see [LICENSE](LICENSE).
