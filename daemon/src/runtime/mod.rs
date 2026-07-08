mod demux;
mod stats;

// Best-effort ufw automation. It's only invoked from the unix-only Docker
// runtime, but its parsing/port-extraction helpers are unit-tested from any
// dev machine — so compile it whenever we build for unix OR run tests, exactly
// like the docker module below.
#[cfg(any(unix, test))]
mod firewall;

// The real Docker client dials a unix socket, so it (and its tests) only
// build on unix targets. sky-daemon itself only ever ships for Linux, but
// gating this way keeps `cargo build`/`cargo test` for the rest of the
// workspace working from a non-unix dev machine too.
#[cfg(unix)]
mod docker;
#[cfg(unix)]
pub use docker::DockerRuntime;

// FakeRuntime is a test double only — nothing in the production binary
// constructs one, so it (and its ConsoleWrites recording buffer) are gated
// out of non-test builds instead of tripping dead-code lints there.
#[cfg(test)]
mod fake;
#[cfg(test)]
pub use fake::FakeRuntime;

use anyhow::Result;
use async_trait::async_trait;
use protocol::ContainerSpec;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

/// Shared, cheaply-cloneable recording buffer used by `FakeRuntime`'s
/// console to let tests inspect what was written to a container's stdin.
#[cfg(test)]
pub type ConsoleWrites = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

#[derive(Debug, Clone, Copy, Default)]
pub struct ContainerState {
    pub running: bool,
}

/// A container the daemon created (identified by its `sky-panel.server_id`
/// label), returned by `list_managed` so the dispatcher can rebuild its
/// server-id -> container-id map after a restart.
#[derive(Debug, Clone)]
pub struct ManagedContainer {
    pub server_id: String,
    pub container_id: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Stats {
    pub cpu_percent: f64,
    pub mem_used_bytes: u64,
    pub mem_limit_bytes: u64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
}

/// A live attached session: `stdin` writes to the container's stdin,
/// `output` yields de-multiplexed combined stdout+stderr lines. Dropping
/// `stdin` detaches (it does not stop the container).
pub struct Console {
    pub stdin: Box<dyn AsyncWrite + Unpin + Send>,
    pub output: mpsc::UnboundedReceiver<String>,
}

#[async_trait]
pub trait ContainerRuntime: Send + Sync {
    /// Ensure an image is present locally, pulling it if missing. Idempotent
    /// and cheap when the image is already cached, so it's safe to call as a
    /// warm-up ahead of `create` (which is what keeps first-boot fast).
    async fn pull(&self, image: &str) -> Result<()>;
    async fn create(&self, spec: &ContainerSpec) -> Result<String>;
    /// Lists containers this daemon manages (those carrying the
    /// `sky-panel.server_id` label), so tracking can be rebuilt on startup.
    /// The default returns nothing — only the real Docker runtime overrides it.
    async fn list_managed(&self) -> Result<Vec<ManagedContainer>> {
        Ok(Vec::new())
    }
    async fn start(&self, id: &str) -> Result<()>;
    async fn stop(&self, id: &str, timeout: Duration) -> Result<()>;
    async fn kill(&self, id: &str) -> Result<()>;
    async fn remove(&self, id: &str) -> Result<()>;
    async fn inspect(&self, id: &str) -> Result<ContainerState>;
    async fn stats(&self, id: &str) -> Result<Stats>;
    async fn attach(&self, id: &str) -> Result<Console>;
}
