//! The outgoing dial loop.
//!
//! `OutgoingConnectLoop` watches the current-connection slot and, whenever it is empty, dials the
//! remote with an exponential backoff, submitting each successful connection to the actor's queue.
//! While a connection is held it parks and resets the backoff; a change back to `None` wakes it.

use backoff::backoff::Backoff;
use std::future::Future;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
}

impl<C: OutgoingConnector> OutgoingConnectLoop<C> {
    pub fn new(
        connector: C,
        watch_rx: watch::Receiver<Option<C::Connection>>,
        tx: mpsc::Sender<C::Connection>,
    ) -> Self {
        Self { connector, watch_rx, tx }
    }

    pub fn spawn(
        self,
        retry_backoff: impl Backoff + Send + 'static,
        cancellation: CancellationToken,
    ) -> JoinHandle<Result<(), LoopError>> {
        tokio::spawn(self.run(retry_backoff, cancellation))
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
                tracing::debug!(
                    "connection is active; resetting back-off and waiting until a new connection is needed"
                );
                tokio::select! {
                    _ = cancellation.cancelled() => break LoopError::Cancelled,
                    changed_res = self.watch_rx.changed() => if changed_res.is_err() { break LoopError::ChannelClosed },
                }
                retry_backoff.reset();
                continue;
            }

            tracing::debug!("no connection active; attempting to reconnect");
            let Some(delay) = retry_backoff.next_backoff() else {
                // `ExponentialBackoff` runs indefinitely by default, but if limits are customized,
                // exit when the max elapsed time is reached.
                break LoopError::BackoffExhausted;
            };

            if delay > std::time::Duration::ZERO {
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
                        tracing::debug!("connection update, resetting the backoff");
                        retry_backoff.reset();
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
                        retry_backoff.reset();
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
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::{mpsc, watch, Notify};
    use tokio::time::timeout;

    #[derive(Debug)]
    struct MockBackoff {
        delays: Vec<Duration>,
        index: isize,
        reset_tx: watch::Sender<isize>,
        reset_rx: watch::Receiver<isize>,
    }

    impl MockBackoff {
        fn new(delays: Vec<Duration>) -> Self {
            let (reset_tx, reset_rx) = watch::channel(0);
            Self { delays, index: -1, reset_tx, reset_rx }
        }
    }

    impl backoff::backoff::Backoff for MockBackoff {
        fn next_backoff(&mut self) -> Option<Duration> {
            self.index += 1;
            self.delays.get(self.index as usize).copied()
        }

        fn reset(&mut self) {
            self.reset_tx.send(self.index + 1).unwrap();
            self.index = -1;
        }
    }

    struct MockConnector {
        /// Mutex allows mutating the vector even in the non-`mut` `connect` function.
        connect_results: Mutex<Vec<Result<i32, &'static str>>>,
        connected: Notify,
    }

    impl MockConnector {
        fn new(mut results: Vec<Result<i32, &'static str>>) -> Self {
            results.reverse(); // FIFO order from a vector
            Self { connect_results: Mutex::new(results), connected: Notify::new() }
        }
    }

    impl OutgoingConnector for MockConnector {
        type Connection = i32;
        type Error = &'static str;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            self.connected.notify_one();
            let mut guard = self.connect_results.lock().unwrap();
            guard.pop().unwrap_or(Err("No more configured results"))
        }
    }

    // --- Tests ---

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_successful_connection_flow() {
        // Scenario: Starts with None -> Connects successfully -> Sends to mpsc ->
        //           Waits for watch_rx to update -> Resets backoff -> Enters active state loop.
        tokio::time::pause();

        let (watch_tx, mut watch_rx) = watch::channel(None);
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(1);

        let connector = MockConnector::new(vec![Ok(42)]);
        let backoff = MockBackoff::new(vec![Duration::from_secs(1)]);
        let mut reset_rx = backoff.reset_rx.clone();

        let cancellation = CancellationToken::new();

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx.clone(), mpsc_tx)
            .run(backoff, cancellation.clone());
        let scenario_fut = async {
            // Advance time to trigger the connect attempt after its initial backoff delay
            tokio::time::advance(Duration::from_secs(1)).await;

            // Verify the connection object was sent to the pipeline
            let received = mpsc_rx.recv().await.unwrap();
            assert_eq!(received, 42);

            // Simulate the manager updating the watch channel with the new active connection
            watch_tx.send(Some(42)).unwrap();
            assert!(
                timeout(Duration::from_millis(200), reset_rx.changed()).await.is_ok(),
                "Reset should be called after successful connection and watch channel update synchronization.");

            // Trigger a disconnect request to break the active loop and allow termination
            watch_rx.mark_unchanged();
            watch_tx.send(None).unwrap();
            watch_rx.changed().await.unwrap();
            assert!(
                timeout(Duration::from_millis(100), reset_rx.changed()).await.is_ok(),
                "Reset should be called after resetting the connection to None"
            );

            cancellation.cancel();
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_backoff_interrupted_by_external_connection() {
        // Scenario: Starts with None -> Fails once -> Next backoff is 10s ->
        //           During sleep, external system provides connection (Some) ->
        //           Sleep is interrupted -> Backoff resets -> Waits in active loop.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, _mpsc_rx) = mpsc::channel(1);

        let connector = MockConnector::new(vec![Err("Failed")]);
        let backoff = MockBackoff::new(vec![Duration::from_secs(10)]);
        let mut reset_rx = backoff.reset_rx.clone();

        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx)
            .run(backoff, CancellationToken::new());
        let scenario_fut = async {
            // Process the first failure and enter the 10-second backoff sleep
            tokio::time::advance(Duration::from_millis(100)).await;

            // Simulate external entity establishing a connection after 2 seconds
            tokio::time::advance(Duration::from_secs(2)).await;
            reset_rx.mark_unchanged();
            watch_tx.send(Some(777)).unwrap();
            assert!(
                timeout(Duration::from_millis(100), reset_rx.changed()).await.is_ok(),
                "Reset should be called exactly when the select! backoff sleep was interrupted by Some");

            // Drop the channel to break the loop and finish the test
            drop(watch_tx);
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }

    #[tokio::test]
    #[test_log::test]
    #[ntest::timeout(300)]
    async fn test_backoff_interrupted_by_explicit_immediate_retry() {
        // Scenario: Starts with None -> Fails -> Backoff is 10s ->
        //           User sends None again to force immediate retry ->
        //           Sleep is interrupted -> Backoff resets -> Connects immediately.
        tokio::time::pause();

        let (watch_tx, watch_rx) = watch::channel(None);
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(1);

        let connector = MockConnector::new(vec![Err("Failed"), Ok(99)]);
        let backoff = MockBackoff::new(vec![Duration::from_secs(10), Duration::from_secs(1)]);
        let mut reset_rx = backoff.reset_rx.clone();

        let cancellation = CancellationToken::new();
        let connector_fut = OutgoingConnectLoop::new(connector, watch_rx, mpsc_tx)
            .run(backoff, cancellation.clone());

        let scenario_fut = async {
            // Process the first failure and enter the 10-second sleep
            tokio::time::advance(Duration::from_millis(100)).await;

            // Force an immediate retry by explicitly pushing None again
            reset_rx.mark_unchanged();
            watch_tx.send(None).unwrap();
            assert!(
                timeout(Duration::from_millis(100), reset_rx.changed()).await.is_ok(),
                "Reset should be called when select! was interrupted by the explicit None signal"
            );

            // The delay was reset, so the next backoff fetched is 1s. Advance to trigger it.
            tokio::time::advance(Duration::from_secs(1)).await;

            // Verify it proceeded to connect immediately and successfully
            let received = mpsc_rx.recv().await.unwrap();
            assert_eq!(received, 99);

            cancellation.cancel();
            drop(watch_tx);
        };

        let (result, _) = tokio::join!(connector_fut, scenario_fut);
        tracing::info!("{:?}", result);
    }
}
