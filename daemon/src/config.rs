use std::time::Duration;

pub struct Config {
    pub panel_ws_url: String,
    pub node_token: String,
    pub docker_socket: String,
    pub heartbeat_interval: Duration,
    pub volumes_root: String,
    pub backups_root: String,
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
        }
    }
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
