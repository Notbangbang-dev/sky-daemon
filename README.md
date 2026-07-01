# sky-daemon

The per-node daemon for [Sky Panel](https://github.com/Notbangbang-dev/sky-panel) — the game-server hosting panel's control plane. `sky-daemon` runs on each VPS that hosts game servers, drives Docker on that host, and connects outbound to the panel over a signed WebSocket so no inbound ports need to be opened on the node.

Written in Rust, replacing the original Go `node-agent` from the panel's monorepo, for lower resource usage, a smaller attack surface (no shelling out, no subprocess-per-operation), and memory safety on a component that parses untrusted-ish network input and drives a privileged Docker socket.

## What it does

- Maintains a single outbound WebSocket connection to `panel-api`, reconnecting with exponential backoff if it drops.
- Every message on that connection (after an initial hello) is signed and replay-protected — see [sky-panel's `docs/ARCHITECTURE.md`](https://github.com/Notbangbang-dev/sky-panel/blob/main/docs/ARCHITECTURE.md) for the wire format (the canonical spec lives there since it's implemented identically on both ends; this repo's `protocol` crate is one of the two implementations).
- Drives Docker over its Unix socket to create/start/stop/kill/remove containers, stream stats, and attach to a container's console (stdin/stdout/stderr) for the panel's live terminal.
- Runs file-manager operations (list/read/write/rename/delete/mkdir) directly against each server's volume on the host filesystem — no `docker exec` needed since the daemon and the volumes share the host.
- Ships `skyperf-core`, folded in as a linked library (not a subprocess) for directory sizing, tar+zstd backup create/restore, and log tailing.

## Workspace layout

```
skyperf-core/   lib — dirsize, backup (tar+zstd), tail --follow, ported from sky-panel's old skyperf crate
protocol/       lib — the signed Envelope wire format + payload types, shared contract with panel-api
daemon/         bin `sky-daemon` — config, runtime (ContainerRuntime trait + Docker-over-unix-socket + a Fake for tests), agent (WS client/session/dispatcher)
```

## Running it

```bash
SKY_PANEL_WS_URL=wss://panel.example.com/agent/ws \
SKY_NODE_TOKEN=<token from the panel's admin console> \
./sky-daemon
```

Environment variables (all have defaults suited to a single-node dev setup except the token, which is required):

| Variable                  | Default                              | Meaning                                      |
|----------------------------|---------------------------------------|-----------------------------------------------|
| `SKY_NODE_TOKEN`           | *(required)*                          | issued when a node is created in the panel   |
| `SKY_PANEL_WS_URL`         | `ws://127.0.0.1:8080/agent/ws`         | panel's agent WebSocket endpoint             |
| `SKY_DOCKER_SOCKET`        | `/var/run/docker.sock`                | Docker Engine API socket                     |
| `SKY_HEARTBEAT_INTERVAL`   | `5s`                                   | container stats push interval                |
| `SKY_VOLUMES_ROOT`         | `/srv/sky-panel/volumes`               | host root under which each server gets `{server_id}/` |

`sky-daemon` only supports Linux/Unix targets — it drives Docker over a Unix socket and doesn't have a Windows code path (the binary builds on Windows for development, but its `main()` immediately exits with an explanatory error there; this is intentional, not a bug).

Normally you won't run this directly — see [sky-panel's installer](https://github.com/Notbangbang-dev/sky-panel/tree/main/installer), which installs `sky-daemon` as a systemd service and keeps it updated via `sky-panel-update`.

## Development

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo build
cargo test
```

Most of the daemon's own code paths (anything touching Docker) only compile on `unix` targets; on Windows, `cargo build`/`clippy` will show large parts of `daemon` as unused — that's expected there and not a signal to act on. CI (Linux) is the authoritative source for real dead-code warnings.

## License

MIT — see [LICENSE](LICENSE).
