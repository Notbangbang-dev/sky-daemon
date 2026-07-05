use std::time::Duration;

pub struct Config {
    pub panel_ws_url: String,
    pub node_token: String,
    pub docker_socket: String,
    pub heartbeat_interval: Duration,
    pub volumes_root: String,
    pub backups_root: String,
    pub container_dns: Vec<String>,
    pub database: Option<DatabaseConfig>,
}

/// Connection details for the node's local MariaDB/MySQL server. Present only
/// when the operator has configured `SKY_DB_ADMIN_*`, in which case the node can
/// provision per-user databases on request.
#[derive(Clone)]
pub struct DatabaseConfig {
    /// Host the daemon itself dials to run admin DDL (usually 127.0.0.1).
    pub admin_host: String,
    pub admin_port: u16,
    pub admin_user: String,
    pub admin_password: String,
    /// Host reported back to users for their connection string — the node's
    /// public address. Falls back to admin_host when unset.
    pub public_host: String,
    /// Port reported back to users — the externally-reachable MariaDB port,
    /// which can differ from admin_port behind a NAT/proxy. Falls back to
    /// admin_port when unset.
    pub public_port: u16,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            panel_ws_url: env_or("SKY_PANEL_WS_URL", "ws://127.0.0.1:8080/agent/ws"),
            node_token: std::env::var("SKY_NODE_TOKEN").unwrap_or_default(),
            docker_socket: env_or("SKY_DOCKER_SOCKET", "/var/run/docker.sock"),
            heartbeat_interval: env_duration_secs("SKY_HEARTBEAT_INTERVAL", Duration::from_secs(5)),
            volumes_root: env_or("SKY_VOLUMES_ROOT", "/srv/sky-panel/volumes"),
            backups_root: env_or("SKY_BACKUPS_ROOT", "/srv/sky-panel/backups"),
            // DNS servers for created containers so they can resolve download
            // hosts regardless of the node's resolver. Comma-separated; set to
            // empty ("SKY_CONTAINER_DNS=") to use Docker's default instead.
            container_dns: env_list("SKY_CONTAINER_DNS", "1.1.1.1,8.8.8.8"),
            database: DatabaseConfig::from_env(),
        }
    }
}

impl DatabaseConfig {
    /// Returns a config only when an admin user is set — otherwise the database
    /// feature stays off on this node and create requests are rejected cleanly.
    fn from_env() -> Option<Self> {
        let admin_user = std::env::var("SKY_DB_ADMIN_USER").unwrap_or_default();
        if admin_user.is_empty() {
            return None;
        }
        let admin_host = env_or("SKY_DB_ADMIN_HOST", "127.0.0.1");
        let public_host = {
            let h = std::env::var("SKY_DB_PUBLIC_HOST").unwrap_or_default();
            if h.is_empty() {
                admin_host.clone()
            } else {
                h
            }
        };
        let admin_port = env_u16("SKY_DB_ADMIN_PORT", 3306);
        Some(Self {
            admin_host,
            admin_port,
            admin_user,
            admin_password: std::env::var("SKY_DB_ADMIN_PASSWORD").unwrap_or_default(),
            public_host,
            public_port: env_u16("SKY_DB_PUBLIC_PORT", admin_port),
        })
    }
}

fn env_list(key: &str, fallback: &str) -> Vec<String> {
    env_or(key, fallback)
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn env_duration_secs(key: &str, fallback: Duration) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(fallback)
}

fn env_u16(key: &str, fallback: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(fallback)
}
