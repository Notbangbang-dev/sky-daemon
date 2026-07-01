mod demux;
mod fake;
mod stats;

// The real Docker client dials a unix socket, so it (and its tests) only
// build on unix targets. sky-daemon itself only ever ships for Linux, but
// gating this way keeps `cargo build`/`cargo test` for the rest of the
// workspace working from a non-unix dev machine too.
#[cfg(unix)]
mod docker;
#[cfg(unix)]
pub use docker::DockerRuntime;

pub use fake::FakeRuntime;

use anyhow::Result;
use async_trait::async_trait;
use protocol::ContainerSpec;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

/// Shared, cheaply-cloneable recording buffer used by `FakeRuntime`'s
/// console to let tests inspect what was written to a container's stdin.
pub type ConsoleWrites = Arc<Mutex<Vec<String>>>;

#[derive(Debug, Clone, Copy, Default)]
pub struct ContainerState {
    pub running: bool,
    pub exit_code: i32,
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
    async fn create(&self, spec: &ContainerSpec) -> Result<String>;
    async fn start(&self, id: &str) -> Result<()>;
    async fn stop(&self, id: &str, timeout: Duration) -> Result<()>;
    async fn kill(&self, id: &str) -> Result<()>;
    async fn remove(&self, id: &str) -> Result<()>;
    async fn inspect(&self, id: &str) -> Result<ContainerState>;
    async fn stats(&self, id: &str) -> Result<Stats>;
    async fn attach(&self, id: &str) -> Result<Console>;
}
