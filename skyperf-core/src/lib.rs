//! skyperf-core: the perf-sensitive slice of sky-daemon, linked directly in
//! (no subprocess spawn) — recursive directory sizing, streaming tar+zstd
//! backups, and log tailing.

pub mod backup;
pub mod dirsize;
pub mod tail;
