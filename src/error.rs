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

//! Error type and helpers of the link loop.

use strum_macros::{AsRefStr, EnumString};

/// Why a link loop stopped.
///
/// Normal operation never surfaces one of these: the loop runs until its link is dropped or its
/// cancellation token fires -- reported here as [`LoopError::Cancelled`] and then masked back to
/// `Ok` by [`mask_cancelled`].
///
/// [`Display`] always writes the variant's own name first (e.g. `"Cancelled"`), followed by `": "`
/// and a human-readable message -- `LoopError::try_from(name)` (from `#[derive(EnumString)]`)
/// recovers the variant from that prefix, for example out of a QUIC close reason this crate
/// closed a connection with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumString)]
#[non_exhaustive]
pub enum LoopError {
    /// The loop's cancellation token fired (normal shutdown).
    Cancelled,
    /// The configured backoff ran out of retries (only reachable with a bounded backoff).
    BackoffExhausted,
    /// A spawned loop task panicked.
    Panicked,
    /// The stream of inbound connections ended while the link was still running.
    IncomingEnded,
}

impl LoopError {
    fn message(&self) -> &'static str {
        match self {
            LoopError::Cancelled => "loop cancelled",
            LoopError::BackoffExhausted => "backoff exhausted",
            LoopError::Panicked => "loop task panicked",
            LoopError::IncomingEnded => "inbound connection stream ended",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_prefixes_the_message_with_the_parseable_variant_name() {
        assert_eq!(LoopError::Cancelled.to_string(), "Cancelled: loop cancelled");
        assert_eq!(LoopError::BackoffExhausted.to_string(), "BackoffExhausted: backoff exhausted");
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
        assert_eq!(mask_cancelled(Err(LoopError::Panicked)), Err(LoopError::Panicked));
    }
}
