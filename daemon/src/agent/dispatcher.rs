//! Executes commands from panel-api against a `ContainerRuntime` and tracks
//! which container backs which server so heartbeats can report stats for
//! all of them. File-manager actions are handled directly against the
//! host's bind-mount path for that server (the daemon runs on the host, so
//! no `docker exec` is needed).

use anyhow::{bail, Context, Result};
use base64::Engine;
use protocol::{
    AckPayload, Action, CommandPayload, ContainerHeartbeat, EventKind, EventPayload, FileEntry,
    HeartbeatPayload, ListFilesResult, ReadFileResult,
};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use crate::runtime::ContainerRuntime;

/// Files handled through the command channel are for config-editing, not
/// bulk transfer — anything bigger should go through a dedicated channel
/// added later.
const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;

/// The write half of an attached container session, kept alive for the
/// lifetime of the stream so `console_input` writes go to the same session
/// the daemon is pumping output from (rather than opening a fresh attach
/// per keystroke line).
struct ActiveConsole {
    stdin: AsyncMutex<Box<dyn AsyncWrite + Unpin + Send>>,
}

pub struct Dispatcher {
    rt: Arc<dyn ContainerRuntime>,
    volumes_root: PathBuf,
    tracked: StdMutex<HashMap<String, String>>,
    /// server_id -> its one live attached console, if any. Presence in this
    /// map is what prevents `start` from attaching a second time.
    consoles: Arc<StdMutex<HashMap<String, Arc<ActiveConsole>>>>,
    events_tx: mpsc::UnboundedSender<EventPayload>,
}

impl Dispatcher {
    /// Returns the dispatcher along with the receiving end of its event
    /// stream (console lines, state changes) — the caller (`Session`) folds
    /// this into the same outbound signed-envelope stream as heartbeats.
    pub fn new(
        rt: Arc<dyn ContainerRuntime>,
        volumes_root: impl Into<PathBuf>,
    ) -> (Self, mpsc::UnboundedReceiver<EventPayload>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let dispatcher = Self {
            rt,
            volumes_root: volumes_root.into(),
            tracked: StdMutex::new(HashMap::new()),
            consoles: Arc::new(StdMutex::new(HashMap::new())),
            events_tx,
        };
        (dispatcher, events_rx)
    }

    /// Executes one command and returns the ack to send back. Never
    /// returns an `Err` itself — failures are reported inside `AckPayload`
    /// so the caller always has something to send upstream.
    pub async fn handle(&self, cmd: &CommandPayload) -> AckPayload {
        match self.handle_inner(cmd).await {
            Ok(result) => AckPayload {
                command_id: cmd.command_id.clone(),
                ok: true,
                error: None,
                result,
            },
            Err(err) => AckPayload {
                command_id: cmd.command_id.clone(),
                ok: false,
                error: Some(err.to_string()),
                result: None,
            },
        }
    }

    async fn handle_inner(&self, cmd: &CommandPayload) -> Result<Option<serde_json::Value>> {
        match cmd.action {
            Action::Create => {
                let spec = cmd.spec.as_ref().context("create command missing spec")?;
                let id = self.rt.create(spec).await?;
                self.track(&cmd.server_id, &id);
                Ok(None)
            }
            Action::Start => {
                let id = self.container_for(cmd)?;
                self.rt.start(&id).await?;
                self.emit_state_changed(&cmd.server_id, "running");
                self.start_console_stream(cmd.server_id.clone(), id).await;
                Ok(None)
            }
            Action::Stop => {
                let id = self.container_for(cmd)?;
                self.rt.stop(&id, Duration::from_secs(15)).await?;
                self.emit_state_changed(&cmd.server_id, "offline");
                Ok(None)
            }
            Action::Kill => {
                let id = self.container_for(cmd)?;
                self.rt.kill(&id).await?;
                self.emit_state_changed(&cmd.server_id, "offline");
                Ok(None)
            }
            Action::Remove => {
                let id = self.container_for(cmd)?;
                self.rt.remove(&id).await?;
                self.untrack(&cmd.server_id);
                self.consoles.lock().unwrap().remove(&cmd.server_id);
                Ok(None)
            }
            Action::ConsoleInput => {
                let input = cmd
                    .input
                    .as_deref()
                    .context("console_input command missing input")?;
                let console = self
                    .consoles
                    .lock()
                    .unwrap()
                    .get(&cmd.server_id)
                    .cloned()
                    .with_context(|| {
                        format!("no active console session for server {}", cmd.server_id)
                    })?;
                let mut stdin = console.stdin.lock().await;
                stdin.write_all(input.as_bytes()).await?;
                stdin.write_all(b"\n").await?;
                Ok(None)
            }
            Action::ListFiles => {
                let dir = self.resolve_path(&cmd.server_id, cmd.path.as_deref().unwrap_or(""))?;
                let entries = list_files(&dir).await?;
                Ok(Some(serde_json::to_value(ListFilesResult { entries })?))
            }
            Action::ReadFile => {
                let path = self.resolve_path(
                    &cmd.server_id,
                    cmd.path.as_deref().context("read_file missing path")?,
                )?;
                let result = read_file(&path).await?;
                Ok(Some(serde_json::to_value(result)?))
            }
            Action::WriteFile => {
                let path = self.resolve_path(
                    &cmd.server_id,
                    cmd.path.as_deref().context("write_file missing path")?,
                )?;
                let content_b64 = cmd
                    .content_base64
                    .as_deref()
                    .context("write_file missing content")?;
                write_file(&path, content_b64).await?;
                Ok(None)
            }
            Action::RenameFile => {
                let from = self.resolve_path(
                    &cmd.server_id,
                    cmd.path.as_deref().context("rename_file missing path")?,
                )?;
                let to = self.resolve_path(
                    &cmd.server_id,
                    cmd.new_path
                        .as_deref()
                        .context("rename_file missing new_path")?,
                )?;
                tokio::fs::rename(&from, &to).await.context("rename file")?;
                Ok(None)
            }
            Action::DeleteFile => {
                let path = self.resolve_path(
                    &cmd.server_id,
                    cmd.path.as_deref().context("delete_file missing path")?,
                )?;
                delete_path(&path).await?;
                Ok(None)
            }
            Action::Mkdir => {
                let path = self.resolve_path(
                    &cmd.server_id,
                    cmd.path.as_deref().context("mkdir missing path")?,
                )?;
                tokio::fs::create_dir_all(&path)
                    .await
                    .context("create directory")?;
                Ok(None)
            }
        }
    }

    fn track(&self, server_id: &str, container_id: &str) {
        self.tracked
            .lock()
            .unwrap()
            .insert(server_id.to_string(), container_id.to_string());
    }

    fn untrack(&self, server_id: &str) {
        self.tracked.lock().unwrap().remove(server_id);
    }

    fn container_for(&self, cmd: &CommandPayload) -> Result<String> {
        if let Some(id) = &cmd.container_id {
            return Ok(id.clone());
        }
        self.tracked
            .lock()
            .unwrap()
            .get(&cmd.server_id)
            .cloned()
            .with_context(|| format!("no known container for server {}", cmd.server_id))
    }

    /// The container currently tracked for `server_id`, if any.
    pub fn container_id_for_server(&self, server_id: &str) -> Option<String> {
        self.tracked.lock().unwrap().get(server_id).cloned()
    }

    /// Resolves a server-relative path against that server's volume root,
    /// rejecting anything that could escape it. Purely lexical (no
    /// `canonicalize`, since the target may not exist yet for `write_file`
    /// /`mkdir`) — a symlink inside the volume pointing outside of it could
    /// still be followed, the same trade-off `skyperf-core`'s backup
    /// restore makes, and an acceptable one: escaping via a symlink you
    /// planted requires already having write access to your own volume.
    fn resolve_path(&self, server_id: &str, rel: &str) -> Result<PathBuf> {
        let rel_path = Path::new(rel);
        for component in rel_path.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("path escapes the server volume: {rel}");
                }
            }
        }
        Ok(self.volumes_root.join(server_id).join(rel_path))
    }

    /// Reports live stats for every tracked container. Containers that fail
    /// to report (e.g. mid-removal) are skipped rather than failing the
    /// whole heartbeat.
    pub async fn heartbeat(&self) -> HeartbeatPayload {
        let snapshot: Vec<(String, String)> = {
            let tracked = self.tracked.lock().unwrap();
            tracked
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        let mut containers = Vec::new();
        for (server_id, container_id) in snapshot {
            let Ok(state) = self.rt.inspect(&container_id).await else {
                continue;
            };

            let mut hb = ContainerHeartbeat {
                server_id,
                running: state.running,
                cpu_percent: 0.0,
                mem_used_bytes: 0,
                mem_limit_bytes: 0,
                net_rx_bytes: 0,
                net_tx_bytes: 0,
            };
            if state.running {
                if let Ok(stats) = self.rt.stats(&container_id).await {
                    hb.cpu_percent = stats.cpu_percent;
                    hb.mem_used_bytes = stats.mem_used_bytes;
                    hb.mem_limit_bytes = stats.mem_limit_bytes;
                    hb.net_rx_bytes = stats.net_rx_bytes;
                    hb.net_tx_bytes = stats.net_tx_bytes;
                }
            }
            containers.push(hb);
        }

        HeartbeatPayload { containers }
    }

    fn emit_state_changed(&self, server_id: &str, state: &str) {
        let _ = self.events_tx.send(EventPayload {
            server_id: server_id.to_string(),
            kind: EventKind::StateChanged,
            message: state.to_string(),
        });
    }

    /// Attaches to `container_id` once and keeps pumping its combined
    /// stdout/stderr to `events_tx` as `console_line` events for as long as
    /// the attach connection stays open. A no-op if a console for this
    /// server is already active. Attach failures are logged and swallowed
    /// — the container still started successfully; it just won't have a
    /// live console until the next `start`.
    async fn start_console_stream(&self, server_id: String, container_id: String) {
        if self.consoles.lock().unwrap().contains_key(&server_id) {
            return;
        }

        let console = match self.rt.attach(&container_id).await {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!("failed to attach console for server {server_id}: {err}");
                return;
            }
        };

        let active = Arc::new(ActiveConsole {
            stdin: AsyncMutex::new(console.stdin),
        });
        self.consoles
            .lock()
            .unwrap()
            .insert(server_id.clone(), active);

        let mut output = console.output;
        let events_tx = self.events_tx.clone();
        let consoles = self.consoles.clone();
        tokio::spawn(async move {
            while let Some(line) = output.recv().await {
                let event = EventPayload {
                    server_id: server_id.clone(),
                    kind: EventKind::ConsoleLine,
                    message: line,
                };
                if events_tx.send(event).is_err() {
                    break;
                }
            }
            consoles.lock().unwrap().remove(&server_id);
        });
    }
}

async fn list_files(dir: &Path) -> Result<Vec<FileEntry>> {
    let mut read_dir = tokio::fs::read_dir(dir).await.context("list files")?;
    let mut entries = Vec::new();
    while let Some(entry) = read_dir
        .next_entry()
        .await
        .context("read directory entry")?
    {
        let metadata = entry.metadata().await.context("read entry metadata")?;
        entries.push(FileEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            is_dir: metadata.is_dir(),
            size_bytes: metadata.len(),
        });
    }
    Ok(entries)
}

async fn read_file(path: &Path) -> Result<ReadFileResult> {
    let metadata = tokio::fs::metadata(path).await.context("stat file")?;
    if metadata.len() > MAX_FILE_BYTES {
        bail!(
            "file is too large for the command channel ({} bytes, max {MAX_FILE_BYTES})",
            metadata.len()
        );
    }
    let bytes = tokio::fs::read(path).await.context("read file")?;
    Ok(ReadFileResult {
        content_base64: base64::engine::general_purpose::STANDARD.encode(&bytes),
        size_bytes: bytes.len() as u64,
    })
}

async fn write_file(path: &Path, content_base64: &str) -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(content_base64)
        .context("decode base64 content")?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        bail!(
            "file is too large for the command channel ({} bytes, max {MAX_FILE_BYTES})",
            bytes.len()
        );
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("create parent directory")?;
    }
    tokio::fs::write(path, bytes).await.context("write file")?;
    Ok(())
}

async fn delete_path(path: &Path) -> Result<()> {
    let metadata = tokio::fs::metadata(path).await.context("stat path")?;
    if metadata.is_dir() {
        tokio::fs::remove_dir_all(path)
            .await
            .context("delete directory")?;
    } else {
        tokio::fs::remove_file(path).await.context("delete file")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::FakeRuntime;
    use protocol::ContainerSpec;

    fn dispatcher_with_volumes(volumes_root: impl Into<PathBuf>) -> (Dispatcher, Arc<FakeRuntime>) {
        let rt = Arc::new(FakeRuntime::new());
        let (dispatcher, _events_rx) = Dispatcher::new(rt.clone(), volumes_root);
        (dispatcher, rt)
    }

    fn cmd(action: Action, server_id: &str) -> CommandPayload {
        CommandPayload {
            command_id: "cmd-1".into(),
            action,
            server_id: server_id.into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_start_stop_remove_lifecycle() {
        let (d, _rt) = dispatcher_with_volumes(std::env::temp_dir());

        let create = cmd(Action::Create, "server-1");
        let create = CommandPayload {
            spec: Some(ContainerSpec {
                image: "test".into(),
                ..Default::default()
            }),
            ..create
        };
        assert!(d.handle(&create).await.ok);

        assert!(d.handle(&cmd(Action::Start, "server-1")).await.ok);
        assert!(d.handle(&cmd(Action::Stop, "server-1")).await.ok);
        assert!(d.handle(&cmd(Action::Remove, "server-1")).await.ok);

        // No longer tracked, so referencing it by server ID alone must fail.
        assert!(!d.handle(&cmd(Action::Start, "server-1")).await.ok);
    }

    #[tokio::test]
    async fn start_with_explicit_container_id_works_without_tracking() {
        let (d, rt) = dispatcher_with_volumes(std::env::temp_dir());
        let id = rt
            .create(&ContainerSpec {
                image: "test".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut start = cmd(Action::Start, "server-1");
        start.container_id = Some(id);
        assert!(d.handle(&start).await.ok);
    }

    #[tokio::test]
    async fn heartbeat_reports_tracked_containers() {
        let (d, _rt) = dispatcher_with_volumes(std::env::temp_dir());

        let create = CommandPayload {
            spec: Some(ContainerSpec {
                image: "test".into(),
                ..Default::default()
            }),
            ..cmd(Action::Create, "server-1")
        };
        d.handle(&create).await;
        d.handle(&cmd(Action::Start, "server-1")).await;

        let hb = d.heartbeat().await;
        assert_eq!(hb.containers.len(), 1);
        assert_eq!(hb.containers[0].server_id, "server-1");
        assert!(hb.containers[0].running);
    }

    #[tokio::test]
    async fn file_roundtrip_write_list_read_rename_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let (d, _rt) = dispatcher_with_volumes(tmp.path());
        tokio::fs::create_dir_all(tmp.path().join("server-1"))
            .await
            .unwrap();

        let content = base64::engine::general_purpose::STANDARD.encode(b"hello world");
        let write = CommandPayload {
            path: Some("config.txt".into()),
            content_base64: Some(content.clone()),
            ..cmd(Action::WriteFile, "server-1")
        };
        assert!(d.handle(&write).await.ok);

        let list = CommandPayload {
            path: Some("".into()),
            ..cmd(Action::ListFiles, "server-1")
        };
        let ack = d.handle(&list).await;
        assert!(ack.ok);
        let result: ListFilesResult = serde_json::from_value(ack.result.unwrap()).unwrap();
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].name, "config.txt");

        let read = CommandPayload {
            path: Some("config.txt".into()),
            ..cmd(Action::ReadFile, "server-1")
        };
        let ack = d.handle(&read).await;
        let result: ReadFileResult = serde_json::from_value(ack.result.unwrap()).unwrap();
        assert_eq!(result.content_base64, content);

        let rename = CommandPayload {
            path: Some("config.txt".into()),
            new_path: Some("renamed.txt".into()),
            ..cmd(Action::RenameFile, "server-1")
        };
        assert!(d.handle(&rename).await.ok);
        assert!(tmp.path().join("server-1/renamed.txt").exists());

        let delete = CommandPayload {
            path: Some("renamed.txt".into()),
            ..cmd(Action::DeleteFile, "server-1")
        };
        assert!(d.handle(&delete).await.ok);
        assert!(!tmp.path().join("server-1/renamed.txt").exists());
    }

    #[tokio::test]
    async fn mkdir_creates_nested_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let (d, _rt) = dispatcher_with_volumes(tmp.path());

        let mkdir = CommandPayload {
            path: Some("a/b/c".into()),
            ..cmd(Action::Mkdir, "server-1")
        };
        assert!(d.handle(&mkdir).await.ok);
        assert!(tmp.path().join("server-1/a/b/c").is_dir());
    }

    #[tokio::test]
    async fn path_traversal_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (d, _rt) = dispatcher_with_volumes(tmp.path());

        let read = CommandPayload {
            path: Some("../../etc/passwd".into()),
            ..cmd(Action::ReadFile, "server-1")
        };
        let ack = d.handle(&read).await;
        assert!(!ack.ok);
        assert!(ack.error.unwrap().contains("escapes"));
    }

    #[tokio::test]
    async fn absolute_path_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (d, _rt) = dispatcher_with_volumes(tmp.path());

        let read = CommandPayload {
            path: Some("/etc/passwd".into()),
            ..cmd(Action::ReadFile, "server-1")
        };
        let ack = d.handle(&read).await;
        assert!(!ack.ok);
        assert!(ack.error.unwrap().contains("escapes"));
    }

    #[tokio::test]
    async fn unknown_action_on_missing_container_fails_gracefully() {
        let (d, _rt) = dispatcher_with_volumes(std::env::temp_dir());
        let ack = d.handle(&cmd(Action::Start, "server-1")).await;
        assert!(!ack.ok);
        assert!(ack.error.is_some());
    }
}
