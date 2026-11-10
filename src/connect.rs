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

//! The outgoing dial loop.
//!
//! `OutgoingConnectLoop` watches the current-connection slot and, whenever it is empty, dials the
//! remote with an exponential backoff, submitting each successful connection to the actor's queue.
//! While a connection is held it parks; a change back to `None` wakes it. The backoff is reset only
//! once a connection has been held for `stable_after` -- a connection that is lost sooner (for
//! example because the peer rejected it) counts as a failed attempt and the backoff keeps growing.

use backoff::backoff::Backoff;
use std::future::Future;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{self, LoopError};

pub trait OutgoingConnector: Send + 'static {
    type Connection: Send + Sync;
    type Error: std::fmt::Debug + std::fmt::Display + Send;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;
}

pub struct OutgoingConnectLoop<C: OutgoingConnector> {
    connector: C,
    watch_rx: watch::Receiver<Option<C::Connection>>,
    tx: mpsc::Sender<C::Connection>,
    stable_after: Duration,
}

impl<C: OutgoingConnector> OutgoingConnectLoop<C> {
    pub fn new(
        connector: C,
        watch_rx: watch::Receiver<Option<C::Connection>>,
        tx: mpsc::Sender<C::Connection>,
        stable_after: Duration,
    ) -> Self {
        Self { connector, watch_rx, tx, stable_after }
    }

    /// Spawns the dial loop, instrumented with [`tracing::Span::current()`] -- see
    /// [`crate::actor::Actor::spawn`] for why.
    pub fn spawn(
        self,
        retry_backoff: impl Backoff + Send + 'static,
        cancellation: CancellationToken,
    ) -> JoinHandle<Result<(), LoopError>> {
        tokio::spawn(self.run(retry_backoff, cancellation).instrument(tracing::Span::current()))
    }

    pub async fn run(
        mut self,
        mut retry_backoff: impl Backoff,
        cancellation: CancellationToken,
    ) -> Result<(), LoopError> {
        retry_backoff.reset();
        let _cancel = cancellation.drop_guard_ref();
        let err = loop {
            // Read the current state and mark it as seen.
            if self.watch_rx.borrow_and_update().is_some() {
                tokio::select! {
                    _ = cancellation.cancelled() => break LoopError::Cancelled,
                    changed_res = self.watch_rx.changed() => {
                        if changed_res.is_err() {
                            break LoopError::ChannelClosed;
                        }
                        tracing::debug!("connection changed before it proved stable; keeping the back-off");
                        continue;
                    }
                    _ = tokio::time::sleep(self.stable_after) => {}
                }
                tracing::debug!(
                    "connection is stable; resetting back-off and waiting until a new connection is needed"
                );
                retry_backoff.reset();
                tokio::select! {
                    _ = cancellation.cancelled() => break LoopError::Cancelled,
                    changed_res = self.watch_rx.changed() => if changed_res.is_err() { break LoopError::ChannelClosed },
                }
                continue;
            }

            tracing::debug!("no connection active; attempting to reconnect");
            let Some(delay) = retry_backoff.next_backoff() else {
                // `ExponentialBackoff` runs indefinitely by default, but if limits are customized,
                // exit when the max elapsed time is reached.
                break LoopError::BackoffExhausted;
            };

            if delay > Duration::ZERO {
                let span = tracing::debug_span!("backoff_sleep", delay = ?delay);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {
                        // Backoff delay expired, proceed to connect.
                    }
                    changed_res = self.watch_rx.changed() => {
                        span.record("interrupted", "connected/disconnected event");
                        if changed_res.is_err() {
                            break LoopError::ChannelClosed;
                        }
                        tracing::debug!("connection update during the back-off sleep");
                        continue;
                    }
                    _ = cancellation.cancelled() => break LoopError::Cancelled,
                }
            }

            tracing::debug!("initiating a new connection");
            tokio::select! {
                r = self.connector.connect() => match r {
                    Ok(connection) => {
                        tracing::debug!("submitting a connection to the queue");
                        if self.tx.send(connection).await.is_err() {
                            tracing::debug!("connection queue dropped, terminating the loop");
                            break LoopError::ChannelClosed;
                        }

                        // Wait for our update to reflect in the watch channel so the next
                        // iteration doesn't immediately read the stale `None` state.
                        tokio::select! {
                            _ = cancellation.cancelled() => break LoopError::Cancelled,
                            changed_res = self.watch_rx.changed() => if changed_res.is_err() { break LoopError::ChannelClosed },
                        }
                        tracing::debug!("successfully submitted a connection to the queue");
                    }
                    Err(err) => {
                        tracing::info!(error = ?err, backoff = ?delay, "connection attempt failed, retrying with a back-off");
                    }
                },
                _ = cancellation.cancelled() => break LoopError::Cancelled,
            }
        };
        tracing::info!(error = ?err, "exiting connector loop");
        error::mask_cancelled(Err(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, watch};
    use tokio::time::Instant;

    const STABLE: Duration = Duration::from_secs(5);

    /// Yields the given delays in order; counts `reset` calls (the loop resets once at startup).
    struct MockBackoff {
        delays: Vec<Duration>,
        index: isize,
        resets: Arc<AtomicUsize>,
    }

    impl MockBackoff {
        fn new(delays: Vec<Duration>) -> (Self, Arc<AtomicUsize>) {
            let resets = Arc::new(AtomicUsize::new(0));
            (Self { delays, index: -1, resets: resets.clone() }, resets)
        }
    }

    impl backoff::backoff::Backoff for MockBackoff {
        fn next_backoff(&mut self) -> Option<Duration> {
            self.index += 1;
            self.delays.get(self.index as usize).copied()
        }

        fn reset(&mut self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
            self.index = -1;
        }
    }

    struct MockConnector {
        /// Mutex allows mutating the vector even in the non-`mut` `connect` function.
        connect_results: Mutex<Vec<Result<i32, &'static str>>>,
        connect_times: Arc<Mutex<Vec<Instant>>>,
    }

    impl MockConnector {
        fn new(mut results: Vec<Result<i32, &'static str>>) -> (Self, Arc<Mutex<Vec<Instant>>>) {
            results.reverse(); // FIFO order from a vector
            let times = Arc::new(Mutex::new(Vec::new()));
            (Self { connect_results: Mutex::new(results), connect_times: times.clone() }, times)
        }
    }

    impl OutgoingConnector for MockConnector {
        type Connection = i32;
        type Error = &'static str;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            self.connect_times.lock().unwrap().push(Instant::now());
            let mut guard = self.connect_results.lock().unwrap();
            guard.pop().unwrap_or(Err("No more configured results"))
        }
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

    // --- Tests ---

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_backoff_reset_only_after_connection_is_stable() {
        // Scenario: Connects -> connection held -> no reset until `STABLE` has passed ->
        //           connection lost -> next dial starts again from the first backoff delay.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(1);
        let (connector, times) = MockConnector::new(vec![Ok(42)]);
        let (backoff, resets) = MockBackoff::new(vec![secs(1.0)]);
        let cancellation = CancellationToken::new();

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx, STABLE)
            .run(backoff, cancellation.clone());
        let scenario_fut = async {
            let start = Instant::now();
            assert_eq!(mpsc_rx.recv().await.unwrap(), 42);
            watch_tx.send(Some(42)).unwrap();

            tokio::time::sleep(secs(4.0)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 1, "only the startup reset so far");
            tokio::time::sleep(secs(1.5)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 2, "reset once the connection proved stable");

            watch_tx.send(None).unwrap();
            tokio::time::sleep(secs(1.5)).await;
            let times = times.lock().unwrap();
            assert_eq!(times.len(), 2);
            assert_near(times[0] - start, secs(1.0));
            // First dial at 1.0 s, connection lost at 6.5 s, first delay (1 s) again -> second dial at 7.5 s.
            assert_near(times[1] - times[0], secs(6.5));
            cancellation.cancel();
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_backoff_keeps_growing_when_connections_are_lost_early() {
        // Scenario: Each submitted connection is lost right away (e.g. rejected by the peer) ->
        //           no reset, and successive dials are spaced by the growing delays.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(1);
        let (connector, times) = MockConnector::new(vec![Ok(1), Ok(2), Ok(3)]);
        let (backoff, resets) = MockBackoff::new(vec![secs(1.0), secs(2.0), secs(4.0)]);
        let cancellation = CancellationToken::new();

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx, STABLE)
            .run(backoff, cancellation.clone());
        let scenario_fut = async {
            for expected in 1..=3 {
                assert_eq!(mpsc_rx.recv().await.unwrap(), expected);
                watch_tx.send(Some(expected)).unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                watch_tx.send(None).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;

            assert_eq!(resets.load(Ordering::SeqCst), 1, "only the startup reset");
            let times = times.lock().unwrap();
            assert_eq!(times.len(), 3);
            assert_near(times[1] - times[0], Duration::from_millis(2010));
            assert_near(times[2] - times[1], Duration::from_millis(4010));
            cancellation.cancel();
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_external_connection_during_backoff_sleep_does_not_reset_immediately() {
        // Scenario: Backoff sleep -> an inbound connection arrives -> the dial is skipped ->
        //           the backoff is reset only after `STABLE`.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, _mpsc_rx) = mpsc::channel(1);
        let (connector, times) = MockConnector::new(vec![]);
        let (backoff, resets) = MockBackoff::new(vec![secs(10.0)]);

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx, STABLE)
            .run(backoff, CancellationToken::new());
        let scenario_fut = async {
            tokio::time::sleep(secs(2.0)).await;
            watch_tx.send(Some(777)).unwrap();

            tokio::time::sleep(secs(4.0)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 1);
            tokio::time::sleep(secs(2.0)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 2);
            assert!(times.lock().unwrap().is_empty(), "no dial while a connection is held");

            // Drop the channel to break the loop and finish the test
            drop(watch_tx);
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_replacing_connection_restarts_stability_timer() {
        // Scenario: A connection is replaced by another one before `STABLE` -> the timer restarts.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(Some(1));
        let (mpsc_tx, _mpsc_rx) = mpsc::channel(1);
        let (connector, _times) = MockConnector::new(vec![]);
        let (backoff, resets) = MockBackoff::new(vec![secs(1.0)]);

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx, STABLE)
            .run(backoff, CancellationToken::new());
        let scenario_fut = async {
            tokio::time::sleep(secs(3.0)).await;
            watch_tx.send(Some(2)).unwrap();

            tokio::time::sleep(secs(4.5)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 1, "only 4.5 s since replacement");
            tokio::time::sleep(secs(1.0)).await;
            assert_eq!(resets.load(Ordering::SeqCst), 2);

            drop(watch_tx);
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }
}
