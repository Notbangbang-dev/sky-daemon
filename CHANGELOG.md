# Changelog

All notable changes to sky-daemon are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-07-01

First release. This repo replaces the Go `node-agent` that used to live in
[sky-panel](https://github.com/Notbangbang-dev/sky-panel)'s monorepo — a
full rewrite in Rust, not a port, with fresh history.

### ✨ New Features

- `skyperf-core`: directory sizing, tar+zstd backup create/restore (with a
  path-traversal guard on restore), and `tail --follow` — folded in as a
  linked library instead of a subprocess-per-call, ported from the old Go
  monorepo's `skyperf` Rust CLI.
- `protocol`: the signed `Envelope` wire format (HMAC-SHA256, timestamp +
  nonce replay protection) and every payload type shared with panel-api,
  fully tested including a round trip over real JSON.
- `daemon`: the `sky-daemon` binary itself — Docker-over-unix-socket
  `ContainerRuntime` (create/start/stop/kill/remove/inspect/stats/attach),
  a `Fake` runtime for tests, and the WS agent (client/session/dispatcher)
  that talks to panel-api.
- File manager command actions (`list_files`, `read_file`, `write_file`,
  `rename_file`, `delete_file`, `mkdir`), scoped per-server under the host
  volumes root and guarded against path traversal.
- Live console streaming: output from an attached container is pumped to
  the panel as signed `event` envelopes as it happens, not polled.

### 🚀 Improvements

- No more subprocess spawn for backup/dirsize/tail — `skyperf-core` is a
  linked lib now, which is both faster and removes a whole crate+binary's
  worth of attack surface compared to the old Go+Rust two-process setup.
- Docker's `Attach` hijacked stream and its stdout/stderr frame demuxer are
  hand-rolled over `tokio::net::UnixStream`, avoiding a full Docker SDK
  dependency while still being fully tested (frame parsing, leftover-byte
  handling across `recv()` boundaries).

### 🔒 Security

- Every message after the initial `hello` is signed and must pass signature,
  timestamp-freshness (±30s), and nonce-replay checks — a tampered or
  replayed message drops the connection rather than being processed.
- File-manager paths are guarded lexically against traversal (`..`,
  absolute paths, prefix escapes) before any filesystem call.
- Container creation drops all capabilities (`cap_drop: ["ALL"]`) and sets
  `no-new-privileges`.
