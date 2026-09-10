//! Error type and helpers shared by the internal supervision loops.

use strum_macros::{AsRefStr, EnumString};
use tokio::task::JoinHandle;

/// Why one of [`PersistentConnection`]'s internal loops stopped.
///
/// Normal operation never surfaces one of these: the loops run until the
/// [`PersistentConnection`] is dropped, which cancels them -- reported here as
/// [`LoopError::Cancelled`] and then masked back to `Ok` by [`mask_cancelled`].
///
/// [`Display`] always writes the variant's own name first (e.g. `"Cancelled"`), followed by `": "`
/// and a human-readable message -- `LoopError::try_from(name)` (from `#[derive(EnumString)]`)
/// recovers the variant from that prefix, for example out of a QUIC close reason this crate
/// closed a connection with.
///
/// [`PersistentConnection`]: crate::PersistentConnection
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumString)]
pub enum LoopError {
    /// The loop's cancellation token fired (normal shutdown).
    Cancelled,
    /// A channel the loop relies on was closed, so it can no longer make progress.
    ChannelClosed,
    /// The configured backoff ran out of retries (only reachable with a bounded backoff).
    BackoffExhausted,
    /// A spawned loop task panicked.
    Panicked,
}

impl LoopError {
    fn message(&self) -> &'static str {
        match self {
            LoopError::Cancelled => "loop cancelled",
            LoopError::ChannelClosed => "a required channel was closed",
            LoopError::BackoffExhausted => "backoff exhausted",
            LoopError::Panicked => "loop task panicked",
        }
    }
}

impl std::fmt::Display for LoopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())?;
        f.write_str(": ")?;
        f.write_str(self.message())
    }
}

impl std::error::Error for LoopError {}

/// Turns the [`LoopError::Cancelled`] sentinel back into `Ok(())`; passes everything else through.
pub fn mask_cancelled(result: Result<(), LoopError>) -> Result<(), LoopError> {
    match result {
        Err(LoopError::Cancelled) => {
            tracing::debug!("loop task cancelled");
            Ok(())
        }
        other => other,
    }
}

/// Awaits a spawned loop, mapping a cancelled/aborted `JoinError` to `Ok` and a panic to
/// [`LoopError::Panicked`], then applies [`mask_cancelled`] to the loop's own result.
pub async fn await_loop_result(handle: JoinHandle<Result<(), LoopError>>) -> Result<(), LoopError> {
    match handle.await {
        Ok(result) => mask_cancelled(result),
        Err(join_err) if join_err.is_cancelled() => Ok(()),
        Err(join_err) => {
            tracing::error!(error = ?join_err, "loop task panicked");
            Err(LoopError::Panicked)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_prefixes_the_message_with_the_parseable_variant_name() {
        assert_eq!(LoopError::Cancelled.to_string(), "Cancelled: loop cancelled");
        assert_eq!(
            LoopError::ChannelClosed.to_string(),
            "ChannelClosed: a required channel was closed"
        );
    }

    #[test]
    fn try_from_recovers_the_variant_from_its_display_prefix() {
        let displayed = LoopError::BackoffExhausted.to_string();
        let prefix = displayed.split(':').next().unwrap();
        assert_eq!(LoopError::try_from(prefix), Ok(LoopError::BackoffExhausted));
        assert!(LoopError::try_from("NotARealVariant").is_err());
    }

    #[test]
    fn mask_cancelled_swallows_only_the_cancelled_variant() {
        assert_eq!(mask_cancelled(Ok(())), Ok(()));
        assert_eq!(mask_cancelled(Err(LoopError::Cancelled)), Ok(()));
        assert_eq!(mask_cancelled(Err(LoopError::ChannelClosed)), Err(LoopError::ChannelClosed));
    }

    #[tokio::test]
    async fn await_loop_result_masks_an_aborted_loop() {
        let ok = tokio::spawn(async { Ok::<(), LoopError>(()) });
        assert_eq!(await_loop_result(ok).await, Ok(()));

        let aborted = tokio::spawn(std::future::pending::<Result<(), LoopError>>());
        aborted.abort();
        assert_eq!(await_loop_result(aborted).await, Ok(()));
    }

    #[tokio::test]
    async fn await_loop_result_reports_a_panic_as_panicked() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let handle: JoinHandle<Result<(), LoopError>> = tokio::spawn(async { panic!("boom") });
        let joined = await_loop_result(handle).await;
        std::panic::set_hook(prev);

        assert_eq!(joined, Err(LoopError::Panicked));
    }
}
