//! The signed wire protocol shared between `panel-api` (Go) and
//! `sky-daemon` (Rust). `panel-api` mirrors these types by convention
//! (separate repos communicating over plain signed JSON, not a shared
//! package) — see that repo's `internal/agenthub` for the Go side.

mod envelope;
mod payloads;
pub mod sign;

pub use envelope::{Envelope, EnvelopeError, MAX_CLOCK_SKEW_SECS};
pub use payloads::*;
