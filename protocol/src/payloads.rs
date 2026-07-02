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
    /// Capability tokens this daemon supports (e.g. `"pull_image"`). Lets the
    /// panel avoid sending commands an older daemon wouldn't understand.
    /// Defaults to empty when absent, so a pre-capability daemon is simply
    /// treated as supporting nothing new.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Capability token: this daemon understands the `pull_image` command
/// (image pre-warming), added in the v0.4.0 line.
pub const CAP_PULL_IMAGE: &str = "pull_image";

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

/// Deserialize a field that may arrive as `null` (or be absent) into its
/// default. Without this, a panel that serializes an empty collection as
/// `null` (e.g. `"cmd":null` for an egg with no startup command) triggers an
/// "invalid type: null, expected a sequence" error that fails the whole
/// command decode and tears down the connection.
fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    #[serde(default, deserialize_with = "null_default")]
    pub cmd: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub env: Vec<String>,
    #[serde(default)]
    pub working_dir: String,
    #[serde(default, deserialize_with = "null_default")]
    pub binds: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub port_bindings: Vec<PortBinding>,
    #[serde(default)]
    pub memory_bytes: i64,
    #[serde(default)]
    pub nano_cpus: i64,
    #[serde(default, deserialize_with = "null_default")]
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
    /// Pull (warm) a Docker image ahead of time so a later `Create` hits the
    /// local cache instead of a multi-minute registry download. Idempotent —
    /// a no-op when the image is already present.
    PullImage,
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
    /// `pull_image`: the image reference to warm on the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
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
        assert_eq!(
            serde_json::to_string(&Action::PullImage).unwrap(),
            "\"pull_image\""
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

    // Faithful reproductions of the exact JSON panel-api (Go) puts on the wire,
    // to catch any Go↔Rust field/variant drift that only bites at runtime.
    #[test]
    fn decodes_panel_warm_pull_image_command() {
        let json = r#"{"command_id":"abc-123","action":"pull_image","server_id":"","image":"itzg/minecraft-server"}"#;
        let cmd: CommandPayload = serde_json::from_str(json).expect("panel pull_image must decode");
        assert_eq!(cmd.action, Action::PullImage);
        assert_eq!(cmd.image.as_deref(), Some("itzg/minecraft-server"));
    }

    #[test]
    fn decodes_panel_create_command() {
        let json = r#"{"command_id":"c2","action":"create","server_id":"s1","spec":{"name":"sky-s1","image":"itzg/minecraft-server","cmd":[],"env":["EULA=TRUE","MEMORY=5632M"],"working_dir":"/home/container","binds":["/srv/sky-panel/volumes/s1:/home/container"],"port_bindings":[{"container_port":"25565/tcp","host_port":"25565"},{"container_port":"25565/udp","host_port":"25565"}],"memory_bytes":5905580032,"nano_cpus":1000000000,"labels":{"sky-panel.server_id":"s1"}}}"#;
        let cmd: CommandPayload = serde_json::from_str(json).expect("panel create must decode");
        assert_eq!(cmd.action, Action::Create);
        assert_eq!(cmd.spec.as_ref().unwrap().image, "itzg/minecraft-server");
    }

    #[test]
    fn decodes_create_with_null_cmd() {
        // An egg with an empty startup makes the panel send "cmd":null. This
        // must decode (an absent command means "use the image's own CMD"), not
        // blow up the whole connection.
        let json = r#"{"command_id":"c3","action":"create","server_id":"s1","spec":{"name":"sky-s1","image":"itzg/minecraft-server","cmd":null,"env":["EULA=TRUE"],"working_dir":"/home/container","binds":["/v:/home/container"],"port_bindings":[],"memory_bytes":1,"nano_cpus":0,"labels":{}}}"#;
        let cmd: CommandPayload =
            serde_json::from_str(json).expect("create with null cmd must decode");
        assert!(cmd.spec.as_ref().unwrap().cmd.is_empty());
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
