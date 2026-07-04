//! In-memory `ContainerRuntime` used by tests (and available for local dev
//! without Docker installed). Tracks enough state to make dispatch-logic
//! tests meaningful without touching a real container engine.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use protocol::ContainerSpec;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{ConsoleWrites, ContainerRuntime, ContainerState, Stats};

struct FakeContainer {
    running: bool,
    /// Labels the container was created with, so `list_managed` can surface
    /// the `sky-panel.server_id` the same way the real runtime does.
    labels: HashMap<String, String>,
    console_writes: ConsoleWrites,
    /// Set by the most recent `attach()` call, so tests can drive fake
    /// output through whichever attach is currently active.
    output_tx: Option<mpsc::UnboundedSender<String>>,
}

#[derive(Default)]
pub struct FakeRuntime {
    containers: Mutex<HashMap<String, FakeContainer>>,
}

impl FakeRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: everything written to `id`'s stdin via `attach`.
    pub fn console_writes(&self, id: &str) -> Vec<String> {
        self.containers
            .lock()
            .unwrap()
            .get(id)
            .map(|c| c.console_writes.lock().unwrap().clone())
            .unwrap_or_default()
    }

    /// Test helper: pushes a fake output line through `id`'s currently
    /// attached console, as if the container had printed it. No-op if
    /// nothing is attached.
    pub fn emit_output(&self, id: &str, line: impl Into<String>) {
        if let Some(tx) = self
            .containers
            .lock()
            .unwrap()
            .get(id)
            .and_then(|c| c.output_tx.clone())
        {
            let _ = tx.send(line.into());
        }
    }
}

#[async_trait]
impl ContainerRuntime for FakeRuntime {
    async fn pull(&self, _image: &str) -> Result<()> {
        Ok(())
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        self.containers.lock().unwrap().insert(
            id.clone(),
            FakeContainer {
                running: false,
                labels: spec.labels.clone(),
                console_writes: ConsoleWrites::default(),
                output_tx: None,
            },
        );
        Ok(id)
    }

    async fn list_managed(&self) -> Result<Vec<super::ManagedContainer>> {
        Ok(self
            .containers
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, c)| {
                c.labels
                    .get("sky-panel.server_id")
                    .map(|server_id| super::ManagedContainer {
                        server_id: server_id.clone(),
                        container_id: id.clone(),
                    })
            })
            .collect())
    }

    async fn start(&self, id: &str) -> Result<()> {
        let mut containers = self.containers.lock().unwrap();
        let c = containers
            .get_mut(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;
        c.running = true;
        Ok(())
    }

    async fn stop(&self, id: &str, _timeout: Duration) -> Result<()> {
        let mut containers = self.containers.lock().unwrap();
        let c = containers
            .get_mut(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;
        c.running = false;
        Ok(())
    }

    async fn kill(&self, id: &str) -> Result<()> {
        self.stop(id, Duration::ZERO).await
    }

    async fn remove(&self, id: &str) -> Result<()> {
        self.containers
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;
        Ok(())
    }

    async fn inspect(&self, id: &str) -> Result<ContainerState> {
        let containers = self.containers.lock().unwrap();
        let c = containers
            .get(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;
        Ok(ContainerState { running: c.running })
    }

    async fn stats(&self, id: &str) -> Result<Stats> {
        let containers = self.containers.lock().unwrap();
        containers
            .get(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;
        // Deterministic non-zero fixture values so callers can assert
        // plumbing works without depending on real container metrics.
        Ok(Stats {
            cpu_percent: 12.5,
            mem_used_bytes: 128 * 1024 * 1024,
            mem_limit_bytes: 512 * 1024 * 1024,
            net_rx_bytes: 1024,
            net_tx_bytes: 2048,
        })
    }

    async fn attach(&self, id: &str) -> Result<super::Console> {
        let mut containers = self.containers.lock().unwrap();
        let c = containers
            .get_mut(id)
            .ok_or_else(|| anyhow!("fake runtime: container {id} not found"))?;

        let (tx, rx) = mpsc::unbounded_channel();
        c.output_tx = Some(tx);
        Ok(super::Console {
            stdin: Box::new(RecordingWriter {
                writes: c.console_writes.clone(),
            }),
            output: rx,
        })
    }
}

/// An `AsyncWrite` that just records every write for test assertions.
struct RecordingWriter {
    writes: ConsoleWrites,
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.writes
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(buf).into_owned());
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ContainerSpec {
        ContainerSpec {
            image: "test".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_start_stop_remove_lifecycle() {
        let rt = FakeRuntime::new();
        let id = rt.create(&spec()).await.unwrap();

        assert!(!rt.inspect(&id).await.unwrap().running);
        rt.start(&id).await.unwrap();
        assert!(rt.inspect(&id).await.unwrap().running);
        rt.stop(&id, Duration::from_secs(1)).await.unwrap();
        assert!(!rt.inspect(&id).await.unwrap().running);
        rt.remove(&id).await.unwrap();
        assert!(rt.inspect(&id).await.is_err());
    }

    #[tokio::test]
    async fn operations_on_unknown_container_fail() {
        let rt = FakeRuntime::new();
        assert!(rt.start("nonexistent").await.is_err());
        assert!(rt.stop("nonexistent", Duration::ZERO).await.is_err());
        assert!(rt.inspect("nonexistent").await.is_err());
        assert!(rt.stats("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn attach_records_stdin_writes() {
        use tokio::io::AsyncWriteExt;

        let rt = FakeRuntime::new();
        let id = rt.create(&spec()).await.unwrap();
        let mut console = rt.attach(&id).await.unwrap();
        console.stdin.write_all(b"say hello\n").await.unwrap();

        assert_eq!(rt.console_writes(&id), vec!["say hello\n"]);
    }
}
