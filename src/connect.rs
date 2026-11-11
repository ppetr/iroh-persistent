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
//!
//! With [`HandlerFeedback`] the loop is instead driven by a connection handler: it also stays idle
//! while the handler is busy, and the backoff is reset by the handler's [`Outcome::Done`] rather
//! than by a timer.

use backoff::backoff::Backoff;
use std::future::Future;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{self, LoopError};
use crate::handler::Outcome;

/// What a connection handler tells the dial loop; replaces the `stable_after` timer.
pub(crate) struct HandlerFeedback {
    /// `true` while the handler is working on a connection, even after the actor cleared the slot.
    pub busy: watch::Receiver<bool>,
    /// Sent before `busy` turns `false`.
    pub outcomes: mpsc::UnboundedReceiver<Outcome>,
}

/// Waits for the next change of the connection slot or, if present, of the handler's busy flag.
///
/// A closed busy flag means the handler loop ended, which cancels the shared token; it is not
/// reported here so that the cancellation decides how the dial loop ends.
async fn changed<T>(
    slot: &mut watch::Receiver<T>,
    feedback: &mut Option<HandlerFeedback>,
) -> Result<(), ()> {
    let Some(feedback) = feedback else {
        return slot.changed().await.map_err(|_| ());
    };
    tokio::select! {
        r = slot.changed() => r.map_err(|_| ()),
        r = feedback.busy.changed() => match r {
            Ok(()) => Ok(()),
            Err(_) => std::future::pending().await,
        },
    }
}

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
    feedback: Option<HandlerFeedback>,
}

impl<C: OutgoingConnector> OutgoingConnectLoop<C> {
    pub fn new(
        connector: C,
        watch_rx: watch::Receiver<Option<C::Connection>>,
        tx: mpsc::Sender<C::Connection>,
        stable_after: Duration,
    ) -> Self {
        Self { connector, watch_rx, tx, stable_after, feedback: None }
    }

    /// Lets a connection handler drive the backoff instead of `stable_after`.
    pub(crate) fn with_feedback(mut self, feedback: HandlerFeedback) -> Self {
        self.feedback = Some(feedback);
        self
    }

    /// Whether a connection is installed or being handled. Marks both sources as seen.
    fn occupied(&mut self) -> bool {
        let installed = self.watch_rx.borrow_and_update().is_some();
        let handling = self.feedback.as_mut().is_some_and(|f| *f.busy.borrow_and_update());
        installed || handling
    }

    /// Resets `retry_backoff` for every connection the handler reported as done.
    fn apply_outcomes(&mut self, retry_backoff: &mut impl Backoff) {
        let Some(feedback) = &mut self.feedback else { return };
        while let Ok(outcome) = feedback.outcomes.try_recv() {
            if outcome == Outcome::Done {
                tracing::debug!("connection handled successfully; resetting back-off");
                retry_backoff.reset();
            }
        }
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
            if self.occupied() {
                if self.feedback.is_some() {
                    // The handler's outcome, not a timer, decides about the back-off.
                    tokio::select! {
                        _ = cancellation.cancelled() => break LoopError::Cancelled,
                        changed_res = changed(&mut self.watch_rx, &mut self.feedback) => {
                            if changed_res.is_err() { break LoopError::ChannelClosed }
                        }
                    }
                    continue;
                }
                tokio::select! {
                    _ = cancellation.cancelled() => break LoopError::Cancelled,
                    changed_res = changed(&mut self.watch_rx, &mut self.feedback) => {
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
                    changed_res = changed(&mut self.watch_rx, &mut self.feedback) => if changed_res.is_err() { break LoopError::ChannelClosed },
                }
                continue;
            }

            self.apply_outcomes(&mut retry_backoff);
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
                    changed_res = changed(&mut self.watch_rx, &mut self.feedback) => {
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
                            changed_res = changed(&mut self.watch_rx, &mut self.feedback) => if changed_res.is_err() { break LoopError::ChannelClosed },
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
        let err = error::settle(err, &cancellation);
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

    /// Runs the dial loop in feedback mode against a connector that succeeds `connects` times.
    struct FeedbackHarness {
        watch_tx: watch::Sender<Option<i32>>,
        busy_tx: watch::Sender<bool>,
        outcome_tx: mpsc::UnboundedSender<Outcome>,
        mpsc_rx: mpsc::Receiver<i32>,
        times: Arc<Mutex<Vec<Instant>>>,
        resets: Arc<AtomicUsize>,
        cancellation: CancellationToken,
    }

    fn feedback_loop(
        connects: usize,
        delays: Vec<Duration>,
        busy: bool,
    ) -> (FeedbackHarness, impl Future<Output = Result<(), LoopError>>) {
        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, mpsc_rx) = mpsc::channel(1);
        let (busy_tx, busy_rx) = watch::channel(busy);
        let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
        let (connector, times) = MockConnector::new((1..=connects as i32).map(Ok).collect());
        let (backoff, resets) = MockBackoff::new(delays);
        let cancellation = CancellationToken::new();
        let loop_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx, STABLE)
            .with_feedback(HandlerFeedback { busy: busy_rx, outcomes: outcome_rx })
            .run(backoff, cancellation.clone());
        let harness =
            FeedbackHarness { watch_tx, busy_tx, outcome_tx, mpsc_rx, times, resets, cancellation };
        (harness, loop_fut)
    }

    impl FeedbackHarness {
        /// Plays one connection: installed and busy for `lifetime`, then reported with `outcome`
        /// and cleared, as the handler loop does.
        async fn serve_connection(&mut self, lifetime: Duration, outcome: Outcome) {
            let conn = self.mpsc_rx.recv().await.unwrap();
            self.watch_tx.send(Some(conn)).unwrap();
            self.busy_tx.send(true).unwrap();
            tokio::time::sleep(lifetime).await;
            self.outcome_tx.send(outcome).unwrap();
            self.watch_tx.send(None).unwrap();
            self.busy_tx.send(false).unwrap();
        }
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_feedback_done_resets_the_backoff() {
        // Scenario: a connection is held far longer than `STABLE` without any reset, then reported
        //           `Done` -> the next dial starts again from the first delay.
        tokio::time::pause();
        let (mut h, loop_fut) = feedback_loop(2, vec![secs(1.0), secs(2.0)], false);

        let scenario_fut = async {
            h.serve_connection(secs(60.0), Outcome::Done).await;
            tokio::time::sleep(secs(1.5)).await;
            assert_eq!(h.resets.load(Ordering::SeqCst), 2, "startup reset plus the `Done` one");
            let times = h.times.lock().unwrap();
            assert_eq!(times.len(), 2);
            // First dial at 1 s, connection reported at 61 s, first delay (1 s) again.
            assert_near(times[1] - times[0], secs(61.0));
            h.cancellation.cancel();
        };

        let (result, _) = tokio::join!(loop_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_feedback_failed_keeps_the_backoff_growing() {
        // Scenario: a connection that outlives `STABLE` but is reported `Failed` -> no reset, the
        //           next dial waits for the second delay.
        tokio::time::pause();
        let (mut h, loop_fut) = feedback_loop(2, vec![secs(1.0), secs(2.0)], false);

        let scenario_fut = async {
            h.serve_connection(secs(60.0), Outcome::Failed).await;
            tokio::time::sleep(secs(2.5)).await;
            assert_eq!(h.resets.load(Ordering::SeqCst), 1, "only the startup reset");
            let times = h.times.lock().unwrap();
            assert_eq!(times.len(), 2);
            assert_near(times[1] - times[0], secs(62.0));
            h.cancellation.cancel();
        };

        let (result, _) = tokio::join!(loop_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_feedback_no_dial_while_the_handler_is_busy() {
        // Scenario: the slot is empty (the peer closed the connection) but the handler has not
        //           returned yet -> no dial until it has.
        tokio::time::pause();
        let (h, loop_fut) = feedback_loop(1, vec![secs(1.0)], true);

        let scenario_fut = async {
            tokio::time::sleep(secs(10.0)).await;
            assert!(h.times.lock().unwrap().is_empty(), "dialed while the handler was busy");
            h.busy_tx.send(false).unwrap();
            tokio::time::sleep(secs(1.5)).await;
            assert_eq!(h.times.lock().unwrap().len(), 1);
            h.cancellation.cancel();
        };

        let (result, _) = tokio::join!(loop_fut, scenario_fut);
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
