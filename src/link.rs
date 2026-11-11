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

//! The link loop: one task that owns everything about the connection to a single peer.
//!
//! The loop alternates between two states. While *idle* it waits for the next connection: an
//! inbound one from the caller's stream, or one it dials itself after an exponential back-off.
//! While *serving* it runs the handler on the connection and watches for four things: the handler
//! finishing, the peer closing the connection, a preferred inbound connection arriving that
//! replaces it, and shutdown. All state lives in local variables of the one task, so there is no
//! shared slot to keep in sync.
//!
//! Both peers apply the same deterministic preference rule (from the order of their keys), so when
//! two connections briefly coexist they keep the same one. The first connection is accepted
//! whatever its direction; later ones replace the current only if they have the preferred
//! direction and are otherwise rejected.
//!
//! This module is generic over [`ManagedConnection`] and [`OutgoingConnector`] so the loop is
//! unit-tested without a network; the Iroh binding lives in [`crate::iroh_conn`].

use std::future::Future;
use std::pin::{pin, Pin};
use std::time::Duration;

use backoff::backoff::Backoff;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::{self, LoopError};
use crate::handler::Outcome;

/// How long a handler may take to return after its cancellation token fired before it is
/// abandoned (its future dropped).
const CANCEL_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Direction {
    Incoming,
    Outgoing,
}

/// The direction this side prefers: the peer with the larger key prefers its outgoing connection,
/// the other its incoming one. One peer's outgoing connection is the other's incoming one, so both
/// sides prefer the same connection.
pub(crate) fn preferred_direction<K: Ord>(local: &K, remote: &K) -> Direction {
    if local > remote {
        Direction::Outgoing
    } else {
        Direction::Incoming
    }
}

/// Operations the link loop needs over a connection handle.
pub(crate) trait ManagedConnection: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Whether this handle is an inbound or an outbound connection.
    fn direction(&self) -> Direction;
    /// Closes this connection, carrying `reason` for the peer's benefit (informational).
    fn close(&self, reason: &str);
    /// Completes once the connection is closed, by either side.
    fn closed(&self) -> impl Future<Output = ()> + Send;
}

pub(crate) trait OutgoingConnector: Send + Sync + 'static {
    type Connection: ManagedConnection;
    type Error: std::fmt::Debug + std::fmt::Display + Send;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;
}

/// Serves the connections of a link; the crate-internal counterpart of
/// [`crate::ConnectionHandler`], generic over the connection type.
pub(crate) trait Serve<C>: Send + 'static {
    /// Serves `conn` until it is done with it. `cancel` fires when `conn` should no longer be used.
    fn serve(&mut self, conn: C, cancel: CancellationToken)
        -> impl Future<Output = Outcome> + Send;
}

/// The stream of inbound connections, already filtered to the link's peer.
pub(crate) struct Inbound<C>(Option<Pin<Box<dyn Stream<Item = C> + Send>>>);

impl<C> Inbound<C> {
    pub fn new(stream: Option<Pin<Box<dyn Stream<Item = C> + Send>>>) -> Self {
        Self(stream)
    }

    /// The next inbound connection. Waits forever if there is no stream or it has ended, so that
    /// the dialing goes on without it.
    async fn next(&mut self) -> C {
        if let Some(stream) = &mut self.0 {
            if let Some(conn) = stream.next().await {
                return conn;
            }
            tracing::debug!("incoming connection stream ended; outgoing dials continue");
            self.0 = None;
        }
        std::future::pending().await
    }
}

/// How serving a connection ended.
enum Served<C> {
    /// The handler finished or the peer closed the connection; the handler's outcome counts.
    Outcome(Outcome),
    /// A preferred connection replaced it; the outcome is ignored.
    Replaced(C),
    /// The peer closed the connection and the handler then ignored its cancellation.
    Abandoned,
    Shutdown,
}

/// Why the loop stopped waiting for the handler.
enum Stop<C> {
    ByPeer,
    Replaced(C),
    Shutdown,
}

pub(crate) struct Link<T: OutgoingConnector, H> {
    pub connector: T,
    pub handler: H,
    pub inbound: Inbound<T::Connection>,
    pub preferred: Direction,
}

impl<T: OutgoingConnector, H: Serve<T::Connection>> Link<T, H> {
    /// Runs the link until `cancel` fires (or the back-off runs out) and returns the handler.
    ///
    /// The back-off is reset when the loop starts and whenever the handler reports
    /// [`Outcome::Done`] for a connection; a connection that ends in [`Outcome::Failed`] leaves it
    /// growing.
    pub async fn run(
        mut self,
        mut backoff: impl Backoff + Send,
        cancel: CancellationToken,
    ) -> (H, Result<(), LoopError>) {
        let _guard = cancel.drop_guard_ref();
        tracing::info!("preferred direction: {:?}", self.preferred);
        backoff.reset();
        let mut pending = None;
        let err = loop {
            let conn = match pending.take() {
                Some(conn) => conn,
                None => match self.next_connection(&mut backoff, &cancel).await {
                    Ok(conn) => conn,
                    Err(err) => break err,
                },
            };
            match self.serve_connection(conn, &cancel).await {
                Served::Outcome(Outcome::Done) => {
                    tracing::debug!("connection handled successfully; resetting back-off");
                    backoff.reset();
                }
                Served::Outcome(Outcome::Failed) | Served::Abandoned => {}
                Served::Replaced(next) => pending = Some(next),
                Served::Shutdown => break LoopError::Cancelled,
            }
        };
        tracing::info!(error = ?err, "exiting the link loop");
        (self.handler, error::mask_cancelled(Err(err)))
    }

    /// Waits for the next connection, dialing the remote after each back-off delay in the
    /// meantime. An inbound connection wins over a pending dial.
    async fn next_connection(
        &mut self,
        backoff: &mut impl Backoff,
        cancel: &CancellationToken,
    ) -> Result<T::Connection, LoopError> {
        let connector = &self.connector;
        loop {
            let delay = backoff.next_backoff().ok_or(LoopError::BackoffExhausted)?;
            tracing::debug!(?delay, "no connection; dialing after the back-off");
            let dial = async {
                tokio::time::sleep(delay).await;
                connector.connect().await
            };
            tokio::select! {
                _ = cancel.cancelled() => return Err(LoopError::Cancelled),
                conn = self.inbound.next() => return Ok(conn),
                result = dial => match result {
                    Ok(conn) => return Ok(conn),
                    Err(err) => tracing::info!(
                        error = ?err, backoff = ?delay,
                        "connection attempt failed, retrying with a back-off"
                    ),
                },
            }
        }
    }

    /// Runs the handler on `conn` until it, the peer, a preferred inbound connection or the
    /// shutdown ends it.
    async fn serve_connection(
        &mut self,
        conn: T::Connection,
        cancel: &CancellationToken,
    ) -> Served<T::Connection> {
        let conn_cancel = cancel.child_token();
        let mut handling = pin!(self.handler.serve(conn.clone(), conn_cancel.clone()));
        let mut closed = pin!(conn.closed());

        let stop = loop {
            // Shutdown first: it also cancels the handler, which must not get to report it.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break Stop::Shutdown,
                outcome = &mut handling => {
                    tracing::debug!(?outcome, "connection handler finished");
                    conn.close("handler finished");
                    return Served::Outcome(outcome);
                }
                _ = &mut closed => break Stop::ByPeer,
                candidate = self.inbound.next() => {
                    if candidate.direction() == self.preferred {
                        tracing::debug!(?candidate, ?conn, "replacing the current connection");
                        break Stop::Replaced(candidate);
                    }
                    tracing::debug!(?candidate, ?conn, "rejecting the new connection");
                    candidate.close("rejected by the deterministic preference rule");
                }
            }
        };

        conn_cancel.cancel();
        let outcome = match tokio::time::timeout(CANCEL_GRACE, &mut handling).await {
            Ok(outcome) => Some(outcome),
            Err(_) => {
                tracing::warn!("connection handler ignored cancellation; abandoning it");
                None
            }
        };
        match stop {
            Stop::Shutdown => {
                // The full "Name: message" text, not the shorter `AsRef<str>` clippy suggests.
                #[allow(clippy::unnecessary_to_owned)]
                conn.close(&LoopError::Cancelled.to_string());
                Served::Shutdown
            }
            Stop::Replaced(next) => {
                conn.close("replaced by a preferred-direction connection");
                Served::Replaced(next)
            }
            Stop::ByPeer => outcome.map_or(Served::Abandoned, Served::Outcome),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, watch};
    use tokio::time::Instant;

    #[derive(Clone, Debug)]
    struct Fake {
        id: isize,
        direction: Direction,
        /// The reason the connection was closed with, once it is.
        state: Arc<watch::Sender<Option<String>>>,
    }

    impl Fake {
        fn new(id: isize, direction: Direction) -> Self {
            Self { id, direction, state: Arc::new(watch::channel(None).0) }
        }

        fn reason(&self) -> Option<String> {
            self.state.borrow().clone()
        }

        fn is_closed(&self) -> bool {
            self.reason().is_some()
        }
    }

    impl ManagedConnection for Fake {
        fn direction(&self) -> Direction {
            self.direction
        }

        fn close(&self, reason: &str) {
            self.state.send_if_modified(|state| {
                let first = state.is_none();
                if first {
                    *state = Some(reason.to_owned());
                }
                first
            });
        }

        fn closed(&self) -> impl Future<Output = ()> + Send {
            let mut rx = self.state.subscribe();
            async move {
                let _ = rx.wait_for(Option::is_some).await;
            }
        }
    }

    /// Yields the given delays in order; counts `reset` calls (the loop resets once at startup).
    struct MockBackoff {
        delays: Vec<Duration>,
        index: isize,
        resets: Arc<AtomicUsize>,
    }

    impl Backoff for MockBackoff {
        fn next_backoff(&mut self) -> Option<Duration> {
            self.index += 1;
            self.delays.get(self.index as usize).copied()
        }

        fn reset(&mut self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
            self.index = -1;
        }
    }

    /// Hands out the configured dial results in order; once they run out the dial never completes.
    struct MockConnector {
        results: Mutex<VecDeque<Result<Fake, &'static str>>>,
        times: Arc<Mutex<Vec<Instant>>>,
    }

    impl OutgoingConnector for MockConnector {
        type Connection = Fake;
        type Error = &'static str;

        async fn connect(&self) -> Result<Fake, &'static str> {
            self.times.lock().unwrap().push(Instant::now());
            let next = self.results.lock().unwrap().pop_front();
            match next {
                Some(result) => result,
                None => std::future::pending().await,
            }
        }
    }

    /// What the handler does with one connection.
    enum Behavior {
        /// Returns the outcome after the given time, whatever happens to the connection.
        Until(Duration, Outcome),
        /// Waits for the cancellation, takes the given time, then returns the outcome.
        WaitCancel(Duration, Outcome),
        IgnoreCancel,
    }

    struct TestHandler {
        script: VecDeque<Behavior>,
        served: Arc<Mutex<Vec<isize>>>,
    }

    impl Serve<Fake> for TestHandler {
        async fn serve(&mut self, conn: Fake, cancel: CancellationToken) -> Outcome {
            self.served.lock().unwrap().push(conn.id);
            match self.script.pop_front().expect("handler script ran out") {
                Behavior::Until(time, outcome) => {
                    tokio::time::sleep(time).await;
                    outcome
                }
                Behavior::WaitCancel(time, outcome) => {
                    cancel.cancelled().await;
                    tokio::time::sleep(time).await;
                    outcome
                }
                Behavior::IgnoreCancel => std::future::pending().await,
            }
        }
    }

    struct Harness {
        inbound: mpsc::UnboundedSender<Fake>,
        cancel: CancellationToken,
        resets: Arc<AtomicUsize>,
        dial_times: Arc<Mutex<Vec<Instant>>>,
        served: Arc<Mutex<Vec<isize>>>,
    }

    impl Harness {
        fn resets(&self) -> usize {
            self.resets.load(Ordering::SeqCst)
        }

        fn served(&self) -> Vec<isize> {
            self.served.lock().unwrap().clone()
        }

        fn dial_times(&self) -> Vec<Instant> {
            self.dial_times.lock().unwrap().clone()
        }
    }

    /// A link that sees `dials` as the results of its dials and the `delays` as its back-off.
    fn link(
        preferred: Direction,
        delays: &[f64],
        dials: Vec<Result<Fake, &'static str>>,
        script: Vec<Behavior>,
    ) -> (Harness, impl Future<Output = (TestHandler, Result<(), LoopError>)>) {
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let resets = Arc::new(AtomicUsize::new(0));
        let dial_times = Arc::new(Mutex::new(Vec::new()));
        let served = Arc::new(Mutex::new(Vec::new()));

        let backoff = MockBackoff {
            delays: delays.iter().map(|s| Duration::from_secs_f64(*s)).collect(),
            index: -1,
            resets: resets.clone(),
        };
        let link = Link {
            connector: MockConnector {
                results: Mutex::new(dials.into()),
                times: dial_times.clone(),
            },
            handler: TestHandler { script: script.into(), served: served.clone() },
            inbound: Inbound::new(Some(Box::pin(futures_util::stream::poll_fn(move |cx| {
                inbound_rx.poll_recv(cx)
            })))),
            preferred,
        };
        let run = link.run(backoff, cancel.clone());
        (Harness { inbound: inbound_tx, cancel, resets, dial_times, served }, run)
    }

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    /// The paused tokio clock rounds timer deadlines up to whole milliseconds.
    #[track_caller]
    fn assert_near(actual: Duration, expected: Duration) {
        let tolerance = Duration::from_millis(5);
        assert!(
            actual >= expected && actual <= expected + tolerance,
            "expected {expected:?} (+{tolerance:?}), got {actual:?}"
        );
    }

    use Direction::{Incoming, Outgoing};

    #[test]
    fn preferred_direction_follows_the_key_order() {
        assert_eq!(preferred_direction(&2, &1), Outgoing);
        assert_eq!(preferred_direction(&1, &2), Incoming);
        assert_eq!(preferred_direction(&1, &1), Incoming);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn done_resets_the_backoff() {
        // Scenario: a connection is held far longer than any timer would care about, then
        //           reported `Done` -> the next dial starts again from the first delay.
        tokio::time::pause();
        let c1 = Fake::new(1, Outgoing);
        let c2 = Fake::new(2, Outgoing);
        let (h, run) = link(
            Outgoing,
            &[1.0, 2.0],
            vec![Ok(c1), Ok(c2)],
            vec![
                Behavior::Until(secs(60.0), Outcome::Done),
                Behavior::WaitCancel(secs(0.0), Outcome::Done),
            ],
        );

        let scenario = async {
            tokio::time::sleep(secs(63.0)).await;
            assert_eq!(h.resets(), 2, "startup reset plus the `Done` one");
            let times = h.dial_times();
            assert_eq!(times.len(), 2);
            // First dial at 1 s, connection reported at 61 s, first delay (1 s) again.
            assert_near(times[1] - times[0], secs(61.0));
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn failed_keeps_the_backoff_growing() {
        // Scenario: a connection reported `Failed` -> no reset, the next dial waits for the
        //           second delay.
        tokio::time::pause();
        let (h, run) = link(
            Outgoing,
            &[1.0, 2.0],
            vec![Ok(Fake::new(1, Outgoing)), Ok(Fake::new(2, Outgoing))],
            vec![
                Behavior::Until(secs(60.0), Outcome::Failed),
                Behavior::WaitCancel(secs(0.0), Outcome::Done),
            ],
        );

        let scenario = async {
            tokio::time::sleep(secs(64.0)).await;
            assert_eq!(h.resets(), 1, "only the startup reset");
            let times = h.dial_times();
            assert_eq!(times.len(), 2);
            assert_near(times[1] - times[0], secs(62.0));
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn failed_dials_are_retried_with_growing_delays() {
        tokio::time::pause();
        let c1 = Fake::new(1, Outgoing);
        let (h, run) = link(
            Outgoing,
            &[1.0, 2.0],
            vec![Err("unreachable"), Ok(c1)],
            vec![Behavior::WaitCancel(secs(0.0), Outcome::Done)],
        );

        let scenario = async {
            tokio::time::sleep(secs(4.0)).await;
            let times = h.dial_times();
            assert_eq!(times.len(), 2);
            assert_near(times[1] - times[0], secs(2.0));
            assert_eq!(h.served(), vec![1]);
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn preferred_inbound_replaces_the_connection_being_served() {
        // Scenario: a connection is being served -> a preferred one arrives -> the handler is
        //           cancelled, the old connection is closed only after it returned, and the new
        //           one is served next. The old connection's outcome does not count.
        tokio::time::pause();
        let dialed = Fake::new(1, Outgoing);
        let inbound = Fake::new(2, Incoming);
        let (h, run) = link(
            Incoming,
            &[1.0],
            vec![Ok(dialed.clone())],
            vec![
                Behavior::WaitCancel(secs(2.0), Outcome::Done),
                Behavior::WaitCancel(secs(0.0), Outcome::Done),
            ],
        );

        let scenario = async {
            tokio::time::sleep(secs(10.0)).await;
            h.inbound.send(inbound.clone()).unwrap();
            tokio::time::sleep(secs(1.0)).await;
            assert!(!dialed.is_closed(), "closed while its handler was still running");
            tokio::time::sleep(secs(1.5)).await;
            assert_eq!(
                dialed.reason().as_deref(),
                Some("replaced by a preferred-direction connection")
            );
            assert!(!inbound.is_closed());
            assert_eq!(h.served(), vec![1, 2]);
            assert_eq!(h.resets(), 1, "the replaced connection's outcome must not count");
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn non_preferred_inbound_is_rejected_and_the_handler_keeps_running() {
        tokio::time::pause();
        let dialed = Fake::new(1, Outgoing);
        let inbound = Fake::new(2, Incoming);
        let (h, run) = link(
            Outgoing,
            &[1.0],
            vec![Ok(dialed.clone())],
            vec![Behavior::Until(secs(100.0), Outcome::Done)],
        );

        let scenario = async {
            tokio::time::sleep(secs(10.0)).await;
            h.inbound.send(inbound.clone()).unwrap();
            tokio::time::sleep(secs(1.0)).await;
            assert_eq!(
                inbound.reason().as_deref(),
                Some("rejected by the deterministic preference rule")
            );
            assert!(!dialed.is_closed());
            assert_eq!(h.served(), vec![1]);
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn the_first_connection_is_accepted_whatever_its_direction() {
        // Scenario: while idle, an inbound connection arrives during the back-off sleep -> it is
        //           served even though it is not preferred, and the dial never happens.
        tokio::time::pause();
        let inbound = Fake::new(1, Incoming);
        let (h, run) = link(
            Outgoing,
            &[10.0],
            vec![Ok(Fake::new(2, Outgoing))],
            vec![Behavior::WaitCancel(secs(0.0), Outcome::Done)],
        );

        let scenario = async {
            tokio::time::sleep(secs(1.0)).await;
            h.inbound.send(inbound.clone()).unwrap();
            tokio::time::sleep(secs(30.0)).await;
            assert_eq!(h.served(), vec![1]);
            assert!(h.dial_times().is_empty(), "dialed although a connection was held");
            assert!(!inbound.is_closed());
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    /// The peer closes the connection after 4 s of its service; returns the number of resets and the
    /// time between the two dials.
    async fn peer_closes_a_connection(outcome: Outcome) -> (usize, Duration) {
        tokio::time::pause();
        let c1 = Fake::new(1, Outgoing);
        let (h, run) = link(
            Outgoing,
            &[1.0, 2.0],
            vec![Ok(c1.clone()), Ok(Fake::new(2, Outgoing))],
            vec![
                Behavior::WaitCancel(secs(0.0), outcome),
                Behavior::WaitCancel(secs(0.0), Outcome::Done),
            ],
        );

        let scenario = async {
            tokio::time::sleep(secs(5.0)).await;
            c1.close("closed by the peer");
            tokio::time::sleep(secs(5.0)).await;
            let times = h.dial_times();
            assert_eq!(times.len(), 2);
            let result = (h.resets(), times[1] - times[0]);
            h.cancel.cancel();
            result
        };

        let (result, observed) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
        observed
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn peer_closing_the_connection_counts_the_handlers_done() {
        // Dial at 1 s, closed at 5 s, first delay (1 s) again -> second dial at 6 s.
        let (resets, between) = peer_closes_a_connection(Outcome::Done).await;
        assert_eq!(resets, 2);
        assert_near(between, secs(5.0));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn peer_closing_the_connection_counts_the_handlers_failure() {
        // Dial at 1 s, closed at 5 s, second delay (2 s) -> second dial at 7 s.
        let (resets, between) = peer_closes_a_connection(Outcome::Failed).await;
        assert_eq!(resets, 1);
        assert_near(between, secs(6.0));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn a_handler_ignoring_cancellation_is_abandoned_without_resetting_the_backoff() {
        tokio::time::pause();
        let c1 = Fake::new(1, Outgoing);
        let (h, run) = link(
            Outgoing,
            &[1.0, 2.0],
            vec![Ok(c1.clone()), Ok(Fake::new(2, Outgoing))],
            vec![Behavior::IgnoreCancel, Behavior::WaitCancel(secs(0.0), Outcome::Done)],
        );

        let scenario = async {
            tokio::time::sleep(secs(5.0)).await;
            c1.close("closed by the peer");
            tokio::time::sleep(secs(8.0)).await;
            // Closed at 5 s, abandoned after the grace period at 10 s, second delay (2 s).
            let times = h.dial_times();
            assert_eq!(times.len(), 2);
            assert_near(times[1] - times[0], secs(11.0));
            assert_eq!(h.served(), vec![1, 2]);
            assert_eq!(h.resets(), 1);
            h.cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn shutdown_closes_the_connection_and_returns_the_handler() {
        tokio::time::pause();
        let c1 = Fake::new(1, Outgoing);
        let (h, run) = link(
            Outgoing,
            &[1.0],
            vec![Ok(c1.clone())],
            vec![Behavior::WaitCancel(secs(0.0), Outcome::Done)],
        );

        let scenario = async {
            tokio::time::sleep(secs(3.0)).await;
            h.cancel.cancel();
        };

        let ((handler, result), _) = tokio::join!(run, scenario);
        assert_eq!(result, Ok(()));
        assert!(handler.script.is_empty());
        let reason = c1.reason().expect("the connection was not closed");
        assert!(reason.starts_with("Cancelled:"), "unexpected reason {reason:?}");
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn an_exhausted_backoff_ends_the_link() {
        tokio::time::pause();
        let (_h, run) = link(Outgoing, &[], vec![], vec![]);
        assert_eq!(run.await.1, Err(LoopError::BackoffExhausted));
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn an_ended_inbound_stream_does_not_stop_the_dialing() {
        // A hot loop on the ended stream would never let the paused clock advance.
        tokio::time::pause();
        let (h, run) = link(
            Outgoing,
            &[1.0],
            vec![Ok(Fake::new(1, Outgoing))],
            vec![Behavior::WaitCancel(secs(0.0), Outcome::Done)],
        );

        let Harness { inbound, cancel, served, .. } = h;
        drop(inbound);
        let scenario = async {
            tokio::time::sleep(secs(5.0)).await;
            assert_eq!(*served.lock().unwrap(), vec![1]);
            cancel.cancel();
        };

        let (result, _) = tokio::join!(run, scenario);
        assert_eq!(result.1, Ok(()));
    }
}
