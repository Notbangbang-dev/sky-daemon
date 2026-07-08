//! Best-effort host firewall (ufw) automation, ported from cloud-panel's
//! `src/services/firewall.js`. When a container is created we try to open its
//! published ports in ufw so the operator doesn't have to remember to. This is
//! purely a CONVENIENCE for the host OS firewall:
//!
//!   - BEST-EFFORT: if ufw is absent, the daemon lacks privileges, or anything
//!     fails, we log one actionable line ("sudo ufw allow <port>") and move on.
//!     We never pretend it worked, and never fail the container create over it.
//!   - On a cloud host (AWS/GCP/Azure) the provider's SECURITY GROUP is the
//!     real gate and the daemon cannot touch it — this does not replace opening
//!     the port in your cloud console.
//!
//! SECURITY: ports are parsed as `u16` and passed as separate argv to the
//! process (never a shell string), and only the fixed `ufw`/`sudo` binaries are
//! invoked — so a port value can't inject a command.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::process::Command;

use crate::config::FirewallMode;

const UFW_PATHS: &[&str] = &["/usr/sbin/ufw", "/sbin/ufw", "/usr/bin/ufw", "/bin/ufw"];
const UFW_TIMEOUT: Duration = Duration::from_secs(8);

/// Emit the "couldn't open" hint at most once per process (matches firewall.js,
/// so a node with dozens of servers doesn't spam the log on every create).
static WARNED: AtomicBool = AtomicBool::new(false);

/// Parse the unique host ports out of a spec's port bindings. Container-port
/// strings look like `"25565/tcp"`; the host port is what we open in ufw. A
/// tcp+udp pair on the same number collapses to one entry, because a bare
/// `ufw allow <port>` opens the port for BOTH protocols (as cloud-panel relies
/// on too).
pub fn ports_from_bindings(bindings: &[protocol::PortBinding]) -> Vec<u16> {
    let mut out: Vec<u16> = Vec::new();
    for b in bindings {
        if let Ok(p) = b.host_port.parse::<u16>() {
            if p != 0 && !out.contains(&p) {
                out.push(p);
            }
        }
    }
    out
}

/// Locate the ufw binary, or `None` when it isn't installed.
fn ufw_bin() -> Option<&'static str> {
    UFW_PATHS.iter().copied().find(|p| Path::new(p).exists())
}

/// Best-effort open each port (tcp + udp) in ufw. Never returns an error; a
/// failure just logs. No-op when the mode is `Off`, when there are no ports, or
/// when the host isn't Linux (ufw is Linux-only — on a macOS dev box or under
/// the Windows test harness this returns immediately without spawning ufw).
pub async fn open_ports(mode: FirewallMode, ports: &[u16]) {
    if mode == FirewallMode::Off || ports.is_empty() {
        return;
    }
    if !cfg!(target_os = "linux") {
        return;
    }
    let bin = match ufw_bin() {
        Some(b) => b,
        None => {
            warn_once(ports, "ufw is not installed");
            return;
        }
    };
    for &port in ports {
        let (cmd, args): (&str, Vec<String>) = match mode {
            FirewallMode::Sudo => (
                "sudo",
                vec!["-n".into(), bin.into(), "allow".into(), port.to_string()],
            ),
            _ => (bin, vec!["allow".into(), port.to_string()]),
        };
        match run(cmd, &args).await {
            Ok(true) => tracing::info!("firewall: opened port {port} in ufw (allow {port})"),
            Ok(false) => warn_once(&[port], "ufw returned a non-zero status"),
            Err(e) => warn_once(&[port], &e),
        }
    }
}

/// Run a command to completion under a timeout, returning whether it exited 0.
/// Never spawns a shell — `cmd`/`args` go straight to execve.
async fn run(cmd: &str, args: &[String]) -> Result<bool, String> {
    let fut = Command::new(cmd).args(args).output();
    match tokio::time::timeout(UFW_TIMEOUT, fut).await {
        Ok(Ok(out)) => Ok(out.status.success()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("timed out".into()),
    }
}

fn warn_once(ports: &[u16], detail: &str) {
    let hint = ports
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    if WARNED.swap(true, Ordering::Relaxed) {
        tracing::debug!("firewall: couldn't open port(s) {hint} ({detail})");
    } else {
        tracing::warn!(
            "firewall: couldn't open port(s) {hint} automatically ({detail}). \
             Open manually with: sudo ufw allow <port>"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pb(container_port: &str, host_port: &str) -> protocol::PortBinding {
        protocol::PortBinding {
            container_port: container_port.into(),
            host_port: host_port.into(),
        }
    }

    #[test]
    fn dedupes_tcp_udp_pairs_and_preserves_order() {
        let bindings = vec![
            pb("25565/tcp", "25565"),
            pb("25565/udp", "25565"),
            pb("25570/tcp", "25570"),
            pb("25570/udp", "25570"),
        ];
        assert_eq!(ports_from_bindings(&bindings), vec![25565, 25570]);
    }

    #[test]
    fn skips_unparseable_and_zero_ports() {
        let bindings = vec![
            pb("x/tcp", "not-a-port"),
            pb("0/tcp", "0"),
            pb("25565/tcp", "25565"),
        ];
        assert_eq!(ports_from_bindings(&bindings), vec![25565]);
    }

    #[tokio::test]
    async fn off_mode_and_empty_ports_are_noops() {
        // Neither should ever spawn a process; they just return promptly.
        open_ports(FirewallMode::Off, &[25565]).await;
        open_ports(FirewallMode::Auto, &[]).await;
    }
}
