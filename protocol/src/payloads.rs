use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Envelope `type` values.
pub mod envelope_type {
    pub const HELLO: &str = "hello";
    pub const HEARTBEAT: &str = "heartbeat";
    pub const EVENT: &str = "event";
    pub const ACK: &str = "ack";
    pub const COMMAND: &str = "command";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloPayload {
    pub node_token: String,
    pub agent_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerHeartbeat {
    pub server_id: String,
    pub running: bool,
    pub cpu_percent: f64,
    pub mem_used_bytes: u64,
    pub mem_limit_bytes: u64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HeartbeatPayload {
    #[serde(default)]
    pub containers: Vec<ContainerHeartbeat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ConsoleLine,
    StateChanged,
    BackupDone,
    BackupFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    pub server_id: String,
    pub kind: EventKind,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortBinding {
    /// e.g. "25565/tcp"
    pub container_port: String,
    pub host_port: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub cmd: Vec<String>,
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub working_dir: String,
    #[serde(default)]
    pub binds: Vec<String>,
    #[serde(default)]
    pub port_bindings: Vec<PortBinding>,
    #[serde(default)]
    pub memory_bytes: i64,
    #[serde(default)]
    pub nano_cpus: i64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Command actions the panel can dispatch to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    #[default]
    Create,
    Start,
    Stop,
    Kill,
    Remove,
    ConsoleInput,
    ListFiles,
    ReadFile,
    WriteFile,
    RenameFile,
    DeleteFile,
    Mkdir,
    Backup,
    ListBackups,
    RestoreBackup,
    DeleteBackup,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CommandPayload {
    pub command_id: String,
    pub action: Action,
    pub server_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ContainerSpec>,
    /// `console_input`: the line to write to stdin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// File-manager actions: path relative to the server's volume root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `rename_file`: destination path relative to the server's volume root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_path: Option<String>,
    /// `write_file`: base64-encoded file content (capped server-side; this
    /// channel is for config-file-sized edits, not bulk transfer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_base64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AckPayload {
    pub command_id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Synchronous result data for read-style actions (`list_files`,
    /// `read_file`) — shaped per-action, so left as a generic JSON value
    /// rather than a fixed struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
    pub is_dir: bool,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListFilesResult {
    pub entries: Vec<FileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadFileResult {
    pub content_base64: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupResult {
    pub filename: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupEntry {
    pub filename: String,
    pub size_bytes: u64,
    /// Unix seconds (the daemon has no date library; the panel/web format it).
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListBackupsResult {
    pub backups: Vec<BackupEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_serializes_to_snake_case_matching_go_constants() {
        assert_eq!(
            serde_json::to_string(&Action::ConsoleInput).unwrap(),
            "\"console_input\""
        );
        assert_eq!(
            serde_json::to_string(&Action::ListFiles).unwrap(),
            "\"list_files\""
        );
        assert_eq!(
            serde_json::to_string(&Action::Create).unwrap(),
            "\"create\""
        );
    }

    #[test]
    fn command_payload_omits_absent_optional_fields() {
        let cmd = CommandPayload {
            command_id: "1".into(),
            action: Action::Start,
            server_id: "s1".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(!json.contains("container_id"));
        assert!(!json.contains("spec"));
        assert!(!json.contains("path"));
    }

    #[test]
    fn event_kind_round_trips() {
        let ev = EventPayload {
            server_id: "s1".into(),
            kind: EventKind::ConsoleLine,
            message: "hello".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"console_line\""));
        let decoded: EventPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.kind, EventKind::ConsoleLine);
    }
}
