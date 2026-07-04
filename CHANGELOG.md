# Changelog

All notable changes to sky-daemon are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.9] - 2026-07-04

### 🛠 Fixes

- **Containers now get working DNS.** Server installs could fail to resolve download hosts (e.g. `install-paper` → `Failed to resolve 'fill.papermc.io'`) when the container inherited the node's own resolver — notably on EC2, where a VPC resolver + `*.compute.internal` search domain fails these public lookups. Created containers now use public DNS (`1.1.1.1`, `8.8.8.8`) by default so installs resolve regardless of the node's `/etc/resolv.conf`. Override with `SKY_CONTAINER_DNS` (comma-separated; set it empty to fall back to Docker's default). Existing servers pick this up on their next reinstall.

## [0.4.8] - 2026-07-04

### 🛠 Fixes

- **Reinstall no longer fails with "port is already allocated".** When starting a container fails because a leftover container is still bound to its host port, the daemon now frees that port and retries the start once — the same self-healing idea as the existing container-name conflict recovery. The cleanup is strictly guarded: it only ever force-removes containers **this daemon created** (by the `sky-panel.server_id` label or a `sky-<uuid>` name), and among those only the same server's leftover instance or one that isn't running — so a foreign container on the host, or a healthy different server, that merely shares a port number is never touched.

## [0.4.7] - 2026-07-04

### 🛠 Fixes

- **A single undecodable command no longer kills the whole session.** If the panel sent a command whose payload the daemon couldn't decode, the daemon ended the entire WebSocket session (`session ended: decode command payload`) — which stopped heartbeats, stats, and console, and could crash-loop as the panel re-sent it on reconnect. Such a command is now logged and skipped, keeping the connection (and live stats) alive.
- **Logs default to INFO** so the startup reconcile count and any dropped-command / connection warnings are visible without setting `RUST_LOG`.

## [0.4.6] - 2026-07-04

### 🛠 Fixes

- **Startup reconcile now also matches containers by name**, not just the `sky-panel.server_id` label. `list_managed` lists every container and recovers the ones this daemon created from the label **or** the `sky-<serverID>` name — so a running server whose label was cleared/lost (or that predates it) is re-tracked after a daemon restart instead of its live stats going dark until a reinstall.

## [0.4.5] - 2026-07-04

### 🛠 Fixes

- **Live stats survive a daemon restart.** The dispatcher's server-id → container-id map was in-memory only, so after a restart it started empty: heartbeats reported nothing and the panel's CPU/memory/network cards sat on a dash for still-running servers until the panel happened to send another create/start. The daemon now **reconciles running containers on startup** — it lists containers carrying the `sky-panel.server_id` label and rebuilds its tracking — so stats and heartbeats resume immediately after a restart.

## [0.4.4] - 2026-07-02

### 🛠 Fixes

- **Reinstalls/creates no longer fail with `409 Conflict … container name is already in use`.** A leftover container from a previous failed create (or one the daemon lost track of after a restart, since tracking is in-memory) held the `sky-<id>` name, so the next create collided. `create` now self-heals: on a name conflict it force-removes the clashing container by name and retries. The volume is a host bind mount, so the server's files are untouched. This also lets the panel drop its separate pre-remove step, which could otherwise race the create.

## [0.4.3] - 2026-07-02

### 🛠 Fixes

- **Server images that drop privileges now start (`failed switching to 'minecraft:minecraft': operation not permitted`).** The daemon hardened containers with `CapDrop: ALL`, which strips `CAP_SETUID`/`CAP_SETGID`/`CAP_CHOWN` — but many server images (itzg/minecraft-server, SteamCMD-based images, …) start as root and drop to an unprivileged user at boot, which needs those. Containers now keep Docker's **default** capability set (which already excludes the dangerous caps like `SYS_ADMIN`/`NET_ADMIN`/`SYS_PTRACE`), while `no-new-privileges`, memory/CPU limits and non-privileged mode stay in place. Update the node and reinstall the server.

## [0.4.2] - 2026-07-02

### 🛠 Fixes

- **A `null` collection in a command no longer drops the connection.** The panel used to send `"cmd":null` for an egg with no startup command, which failed to decode ("invalid type: null, expected a sequence"), tore down the whole command, and disconnected the node — so most server creates errored with `node reported command failure: node disconnected`. The daemon now decodes a `null` list/map (in `cmd`, `env`, `binds`, `port_bindings`, `labels`) as empty instead of erroring. The panel-side fix (v0.15.1) stops sending `null` at all; this makes the daemon robust to it either way. Session errors are also now logged with their full cause chain so a decode failure names the offending field.

## [0.4.1] - 2026-07-02

### 🛠 Fixes

- **Nodes no longer drop out mid-provision ("node disconnected").** Commands are now dispatched off the connection's event loop, so a slow operation — most importantly a first-time image pull, which can take minutes — no longer blocks heartbeats and inbound reads. Before this, the loop went silent for the whole pull and the idle connection got reaped, so creating a server on a large image (e.g. a Minecraft server) often failed with `node reported command failure: node disconnected`. Heartbeats (every 5s) now keep flowing throughout a pull, and the pull finishes in the background even if the connection blips — warming the cache for the next attempt.

## [0.4.0] - 2026-07-02

### ✨ New Features

- **`pull_image` command + capability handshake.** The daemon can now pre-pull (warm) an egg's image ahead of time, so a later `create` hits the local cache instead of a multi-minute registry download. It's idempotent (a no-op when the image is already present) and streams "pulling…/ready" progress to the server console. The daemon advertises a `pull_image` capability in its hello so an updated panel only sends the command to daemons that understand it — older panels and daemons keep working unchanged.

## [0.3.0] - 2026-07-02

### 🛠 Fixes

- **Pull the image on create.** Docker's create endpoint doesn't pull, so on a
  fresh node the egg's image was missing and container creation failed. `create`
  now detects the missing-image 404, pulls the image (`POST /images/create`,
  blocking until the pull finishes), and retries — so a brand-new node can host
  a server without any manual `docker pull`. Pull failures (bad tag, etc.) are
  surfaced from the streamed progress. Pairs with sky-panel v0.11.0, which
  provisions asynchronously to accommodate the (potentially minutes-long) pull.

## [0.2.0] - 2026-07-01

### ✨ New Features

- Backup command actions: `backup`, `list_backups`, `restore_backup`, and
  `delete_backup`. `backup` tar+zstd's a server's volume into a timestamped
  `backup-<unix>.tar.zst` archive under the backups root and emits a
  `BackupDone` event; `list_backups` returns each archive's name, size, and
  modified time (newest first); restore and delete operate on a single named
  archive. All four validate the backup filename is a single safe component,
  so a caller can't escape the server's backups directory.
- New `SKY_BACKUPS_ROOT` config (default `/srv/sky-panel/backups`) for where
  those archives live, one subdirectory per server.

### 🔗 Compatibility

- Pairs with **sky-panel v0.5.0**, which drives these actions from the panel's
  new Backups tab and scheduled-backup loop. Older panels simply never send
  the new actions.

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
