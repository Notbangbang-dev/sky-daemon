//! Parses the Docker Engine API's `/containers/{id}/stats` response. Kept
//! pure (no I/O) so the one genuinely fiddly part — the CPU-percent formula
//! — is unit-testable without a daemon.

use serde::Deserialize;
use std::collections::HashMap;

use super::Stats;

#[derive(Debug, Default, Deserialize)]
pub struct DockerStatsResponse {
    #[serde(default)]
    pub cpu_stats: CpuStats,
    #[serde(default)]
    pub precpu_stats: CpuStats,
    #[serde(default)]
    pub memory_stats: MemoryStats,
    #[serde(default)]
    pub networks: HashMap<String, NetworkStats>,
    #[serde(default)]
    pub blkio_stats: BlkioStats,
}

#[derive(Debug, Default, Deserialize)]
pub struct CpuStats {
    #[serde(default)]
    pub cpu_usage: CpuUsage,
    #[serde(default)]
    pub system_cpu_usage: u64,
    #[serde(default)]
    pub online_cpus: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct CpuUsage {
    #[serde(default)]
    pub total_usage: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct MemoryStats {
    #[serde(default)]
    pub usage: u64,
    #[serde(default)]
    pub limit: u64,
    #[serde(default)]
    pub stats: MemoryDetailStats,
}

#[derive(Debug, Default, Deserialize)]
pub struct MemoryDetailStats {
    #[serde(default)]
    pub cache: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct NetworkStats {
    #[serde(default)]
    pub rx_bytes: u64,
    #[serde(default)]
    pub tx_bytes: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct BlkioStats {
    #[serde(default)]
    pub io_service_bytes_recursive: Vec<BlkioEntry>,
}

#[derive(Debug, Default, Deserialize)]
pub struct BlkioEntry {
    #[serde(default)]
    pub op: String,
    #[serde(default)]
    pub value: u64,
}

/// Converts a raw Engine API stats payload into our `Stats` type.
pub fn parse_docker_stats(raw: &DockerStatsResponse) -> Stats {
    let (mut rx, mut tx) = (0u64, 0u64);
    for net in raw.networks.values() {
        rx += net.rx_bytes;
        tx += net.tx_bytes;
    }

    let mem_used = raw
        .memory_stats
        .usage
        .saturating_sub(raw.memory_stats.stats.cache);

    Stats {
        cpu_percent: cpu_percent(&raw.cpu_stats, &raw.precpu_stats),
        mem_used_bytes: mem_used,
        mem_limit_bytes: raw.memory_stats.limit,
        net_rx_bytes: rx,
        net_tx_bytes: tx,
    }
}

/// Same formula the official `docker stats` CLI uses: the container's share
/// of total CPU delta since the previous sample, scaled by online CPUs.
fn cpu_percent(cur: &CpuStats, prev: &CpuStats) -> f64 {
    let cpu_delta = cur.cpu_usage.total_usage as f64 - prev.cpu_usage.total_usage as f64;
    let system_delta = cur.system_cpu_usage as f64 - prev.system_cpu_usage as f64;

    if system_delta <= 0.0 || cpu_delta < 0.0 {
        return 0.0;
    }

    let online_cpus = if cur.online_cpus == 0 {
        1.0
    } else {
        cur.online_cpus as f64
    };
    (cpu_delta / system_delta) * online_cpus * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_percent_matches_docker_formula() {
        let cur = CpuStats {
            cpu_usage: CpuUsage {
                total_usage: 50_000_000,
            },
            system_cpu_usage: 200_000_000,
            online_cpus: 4,
        };
        let prev = CpuStats {
            cpu_usage: CpuUsage {
                total_usage: 25_000_000,
            },
            system_cpu_usage: 100_000_000,
            online_cpus: 4,
        };
        // cpuDelta=25_000_000, systemDelta=100_000_000 -> 0.25 * 4 * 100 = 100%
        assert_eq!(cpu_percent(&cur, &prev), 100.0);
    }

    #[test]
    fn cpu_percent_zero_when_system_delta_not_positive() {
        let cur = CpuStats {
            system_cpu_usage: 100,
            online_cpus: 2,
            ..Default::default()
        };
        let prev = CpuStats {
            system_cpu_usage: 100,
            ..Default::default()
        };
        assert_eq!(cpu_percent(&cur, &prev), 0.0);
    }

    #[test]
    fn parse_docker_stats_subtracts_cache_from_memory() {
        let mut raw = DockerStatsResponse::default();
        raw.memory_stats.usage = 1000;
        raw.memory_stats.limit = 2000;
        raw.memory_stats.stats.cache = 300;

        let stats = parse_docker_stats(&raw);
        assert_eq!(stats.mem_used_bytes, 700);
        assert_eq!(stats.mem_limit_bytes, 2000);
    }

    #[test]
    fn parse_docker_stats_aggregates_network_across_interfaces() {
        let mut raw = DockerStatsResponse::default();
        raw.networks.insert(
            "eth0".into(),
            NetworkStats {
                rx_bytes: 100,
                tx_bytes: 50,
            },
        );
        raw.networks.insert(
            "eth1".into(),
            NetworkStats {
                rx_bytes: 200,
                tx_bytes: 75,
            },
        );

        let stats = parse_docker_stats(&raw);
        assert_eq!(stats.net_rx_bytes, 300);
        assert_eq!(stats.net_tx_bytes, 125);
    }
}
