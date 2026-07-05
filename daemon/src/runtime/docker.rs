//! Talks directly to the Docker Engine API over a unix socket using only
//! `tokio` + `httparse` for the HTTP framing — no `bollard`/Docker SDK
//! dependency, keeping the daemon's footprint small. `httparse` (the same
//! parser hyper uses internally) handles the one part worth leaning on a
//! well-audited crate for; `Attach` still needs a raw hijacked duplex
//! stream, which no HTTP client abstracts cleanly, so it's hand-rolled here
//! exactly like the framing on the JSON endpoints.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use protocol::ContainerSpec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::demux::{parse_frame_header, LineSplitter, STREAM_STDERR, STREAM_STDOUT};
use super::stats::{parse_docker_stats, DockerStatsResponse};
use super::{Console, ContainerRuntime, ContainerState, ManagedContainer, Stats};

const API_VERSION: &str = "v1.43";

pub struct DockerRuntime {
    socket_path: PathBuf,
    /// DNS servers set on every container created (empty => Docker's default).
    dns: Vec<String>,
}

impl DockerRuntime {
    pub fn new(socket_path: impl Into<PathBuf>, dns: Vec<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            dns,
        }
    }

    async fn connect(&self) -> Result<UnixStream> {
        UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| format!("connect to docker socket {}", self.socket_path.display()))
    }

    /// One-shot JSON request/response over a fresh connection.
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<(u16, Vec<u8>)> {
        let mut stream = self.connect().await?;

        let mut req = format!(
            "{method} /{API_VERSION}{path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n"
        );
        if let Some(b) = body {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        req.push_str("\r\n");

        stream.write_all(req.as_bytes()).await?;
        if let Some(b) = body {
            stream.write_all(b).await?;
        }

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        parse_response(&buf)
    }

    async fn expect(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        ok: &[u16],
    ) -> Result<Vec<u8>> {
        let (status, body_bytes) = self.request(method, path, body).await?;
        if ok.contains(&status) {
            return Ok(body_bytes);
        }
        bail!(
            "docker api {method} {path}: unexpected status {status}: {}",
            extract_docker_error(&body_bytes)
        )
    }

    /// Pull an image via the Docker Engine API, blocking until the pull
    /// finishes (the streamed response ends). `/images/create` returns 200 up
    /// front and streams NDJSON progress; a failure is reported as an
    /// `{"error":...}` object within that stream even though the status is 200,
    /// so we scan for it.
    async fn pull_image(&self, image: &str) -> Result<()> {
        let (name, tag) = split_image_ref(image);
        let mut path = format!("/images/create?fromImage={}", encode_query(&name));
        if !tag.is_empty() {
            path.push_str(&format!("&tag={}", encode_query(&tag)));
        }
        let (status, body) = self.request("POST", &path, None).await?;
        if status != 200 {
            bail!(
                "pull image {image}: unexpected status {status}: {}",
                extract_docker_error(&body)
            );
        }
        if let Some(msg) = extract_pull_error(&body) {
            bail!("pull image {image}: {msg}");
        }
        Ok(())
    }

    /// POST the create request, transparently recovering from the two expected
    /// soft failures so provisioning is idempotent:
    /// - **404**: the image isn't present — pull it and retry.
    /// - **409**: the name is already taken by a leftover container (a previous
    ///   failed create, or the daemon lost its in-memory tracking on restart) —
    ///   remove it by name and retry. The volume is a bind mount, so removing
    ///   the container never touches the server's files.
    ///
    /// Each recovery happens at most once, so a persistent failure still surfaces
    /// instead of looping.
    async fn create_container(
        &self,
        path: &str,
        body: &[u8],
        image: &str,
        name: &str,
    ) -> Result<Vec<u8>> {
        let mut pulled = false;
        let mut cleared = false;
        loop {
            let (status, resp) = self.request("POST", path, Some(body)).await?;
            match status {
                201 => return Ok(resp),
                404 if !pulled => {
                    self.pull_image(image).await?;
                    pulled = true;
                }
                409 if !cleared && !name.is_empty() => {
                    self.remove_by_name(name).await?;
                    cleared = true;
                }
                other => bail!(
                    "docker api POST {path}: unexpected status {other}: {}",
                    extract_docker_error(&resp)
                ),
            }
        }
    }

    /// Force-remove a container by name, treating "already gone" (404) as
    /// success. force=true stops it first; the bind-mounted volume is untouched.
    async fn remove_by_name(&self, name: &str) -> Result<()> {
        self.expect(
            "DELETE",
            &format!("/containers/{name}?force=true"),
            None,
            &[204, 404],
        )
        .await?;
        Ok(())
    }

    /// Whether an image is already present in the local Docker cache. Used to
    /// short-circuit a warm-up pull so repeatedly warming the same image
    /// costs one cheap inspect rather than a registry round-trip.
    async fn image_present(&self, image: &str) -> bool {
        // Encode the ref the same way pull_image does (encode_query keeps '/'
        // and ':' literal, so normal refs like "node:22-alpine" or
        // "reg:5000/img:tag" are unchanged while stray chars are escaped).
        let path = format!("/images/{}/json", encode_query(image));
        matches!(self.request("GET", &path, None).await, Ok((200, _)))
    }

    /// The host ports `id` asks to publish (from its HostConfig.PortBindings).
    async fn container_host_ports(&self, id: &str) -> Result<Vec<u16>> {
        let body = self
            .expect("GET", &format!("/containers/{id}/json"), None, &[200])
            .await?;
        #[derive(Deserialize)]
        struct Inspect {
            #[serde(rename = "HostConfig", default)]
            host_config: HostConfig,
        }
        #[derive(Deserialize, Default)]
        struct HostConfig {
            #[serde(rename = "PortBindings", default)]
            port_bindings: HashMap<String, Option<Vec<Binding>>>,
        }
        #[derive(Deserialize)]
        struct Binding {
            #[serde(rename = "HostPort", default)]
            host_port: String,
        }
        let parsed: Inspect = serde_json::from_slice(&body).context("decode inspect ports")?;
        let mut ports = Vec::new();
        for list in parsed.host_config.port_bindings.values().flatten() {
            for b in list {
                if let Ok(p) = b.host_port.parse::<u16>() {
                    ports.push(p);
                }
            }
        }
        Ok(ports)
    }

    /// Free a leftover container that still holds a host port `target` wants to
    /// bind. Strictly guarded so it can only ever remove containers WE created
    /// (by the sky-panel.server_id label or a "sky-<uuid>" name) — never a
    /// foreign workload on the shared host that merely shares a port number —
    /// and, among ours, only the same server's old instance or one that isn't
    /// running. A healthy *different* server that (shouldn't but) collides is
    /// left alone so the real conflict surfaces instead of causing an outage.
    /// Best-effort — a failure here just lets the caller's retry surface it.
    async fn free_conflicting_ports(&self, target: &str) {
        let wanted = match self.container_host_ports(target).await {
            Ok(p) if !p.is_empty() => p,
            _ => return,
        };
        let Ok(body) = self
            .expect("GET", "/containers/json?all=true", None, &[200])
            .await
        else {
            return;
        };
        #[derive(Deserialize)]
        struct Port {
            #[serde(rename = "PublicPort")]
            public_port: Option<u16>,
        }
        #[derive(Deserialize)]
        struct Item {
            #[serde(rename = "Id")]
            id: String,
            #[serde(rename = "Names", default)]
            names: Vec<String>,
            #[serde(rename = "Labels", default)]
            labels: HashMap<String, String>,
            #[serde(rename = "State", default)]
            state: String,
            #[serde(rename = "Ports", default)]
            ports: Vec<Port>,
        }
        let Ok(items) = serde_json::from_slice::<Vec<Item>>(&body) else {
            return;
        };
        let target_sid = items
            .iter()
            .find(|it| it.id == target)
            .and_then(|it| server_id_from(&it.names, &it.labels));
        for it in items {
            if it.id == target {
                continue;
            }
            let conflicts = it
                .ports
                .iter()
                .any(|p| p.public_port.is_some_and(|pp| wanted.contains(&pp)));
            if !conflicts {
                continue;
            }
            // Only ever touch containers this daemon created.
            let Some(sid) = server_id_from(&it.names, &it.labels) else {
                continue;
            };
            let same_server = target_sid.as_deref() == Some(sid.as_str());
            let running = it.state.eq_ignore_ascii_case("running");
            if same_server || !running {
                // remove_by_name takes the path segment, which accepts an id too.
                let _ = self.remove_by_name(&it.id).await;
            }
        }
    }
}

/// Split an image reference into (repository, tag), defaulting the tag to
/// "latest". Only a ':' in the final path segment marks a tag, so a registry
/// host:port ("reg:5000/img") isn't mistaken for one; a digest ref
/// ("name@sha256:…") has no tag (empty string — the caller omits the tag param).
fn split_image_ref(image: &str) -> (String, String) {
    let seg_start = image.rfind('/').map_or(0, |i| i + 1);
    let segment = &image[seg_start..];
    if segment.contains('@') {
        return (image.to_string(), String::new());
    }
    if let Some(rel) = segment.rfind(':') {
        let colon = seg_start + rel;
        let tag = &image[colon + 1..];
        if !tag.is_empty() {
            return (image[..colon].to_string(), tag.to_string());
        }
    }
    (image.to_string(), "latest".to_string())
}

/// Percent-encode a query-string value, leaving characters that are valid and
/// safe inside an image reference ('/' and ':') intact so Docker reads them
/// literally, while escaping anything that would otherwise break the query
/// (e.g. '&', '=', '?', '#', whitespace).
/// Whether `s` has the shape of a server id (a UUID: 8-4-4-4-12 lowercase-hex
/// with hyphens). Used to gate the container-name reconcile fallback so only
/// "sky-<uuid>" containers this daemon created are matched, never a foreign
/// "sky-*" name on the shared host.
fn looks_like_server_id(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    s.bytes().enumerate().all(|(i, b)| match i {
        8 | 13 | 18 | 23 => b == b'-',
        _ => b.is_ascii_hexdigit(),
    })
}

/// Extract the sky server id from a container's names/labels — the
/// `sky-panel.server_id` label, or a "sky-<uuid>" name. `None` if the container
/// isn't one we created (so callers never act on foreign containers).
fn server_id_from(names: &[String], labels: &HashMap<String, String>) -> Option<String> {
    if let Some(v) = labels.get("sky-panel.server_id") {
        if !v.is_empty() {
            return Some(v.clone());
        }
    }
    names.iter().find_map(|n| {
        n.trim_start_matches('/')
            .strip_prefix("sky-")
            .filter(|s| looks_like_server_id(s))
            .map(|s| s.to_string())
    })
}

fn encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Scan an `/images/create` NDJSON progress stream for a terminal error.
/// Docker reports pull failures as either `{"error":"…"}` or a nested
/// `{"errorDetail":{"message":"…"}}`, so check both.
fn extract_pull_error(body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct ErrDetail {
        message: Option<String>,
    }
    #[derive(Deserialize)]
    struct Line {
        error: Option<String>,
        #[serde(rename = "errorDetail")]
        error_detail: Option<ErrDetail>,
    }
    for line in body.split(|&b| b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if let Ok(l) = serde_json::from_slice::<Line>(line) {
            if let Some(e) = l.error {
                if !e.is_empty() {
                    return Some(e);
                }
            }
            if let Some(msg) = l.error_detail.and_then(|d| d.message) {
                if !msg.is_empty() {
                    return Some(msg);
                }
            }
        }
    }
    None
}

fn parse_response(buf: &[u8]) -> Result<(u16, Vec<u8>)> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut headers);
    let header_len = match resp.parse(buf).context("parse docker api response")? {
        httparse::Status::Complete(n) => n,
        httparse::Status::Partial => bail!("docker api response headers incomplete"),
    };
    let code = resp
        .code
        .ok_or_else(|| anyhow!("docker api response missing status code"))?;

    // Docker streams larger responses (notably `/containers/json`) with
    // Transfer-Encoding: chunked. The body then carries hex chunk-size framing
    // that must be stripped before it's valid JSON — otherwise the parser sees
    // the leading chunk size (e.g. `1054`) instead of the `[`.
    let chunked = resp.headers.iter().any(|h| {
        h.name.eq_ignore_ascii_case("transfer-encoding")
            && String::from_utf8_lossy(h.value)
                .to_ascii_lowercase()
                .contains("chunked")
    });

    let body = &buf[header_len..];
    let body = if chunked {
        dechunk(body)?
    } else {
        body.to_vec()
    };
    Ok((code, body))
}

/// Decodes an HTTP/1.1 chunked transfer-encoding body into its raw payload:
/// each chunk is a hex size line, CRLF, that many bytes, CRLF; a zero-size
/// chunk terminates.
fn dechunk(mut data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let nl = data
            .windows(2)
            .position(|w| w == b"\r\n")
            .context("chunked body: missing chunk-size line")?;
        // A chunk size line may carry `;ext` extensions after the hex size.
        let size_field = data[..nl]
            .split(|&b| b == b';')
            .next()
            .unwrap_or(&data[..nl]);
        let size_str = std::str::from_utf8(size_field)
            .context("chunked body: chunk size not UTF-8")?
            .trim();
        let size =
            usize::from_str_radix(size_str, 16).context("chunked body: invalid chunk size")?;
        data = &data[nl + 2..];
        if size == 0 {
            break;
        }
        if data.len() < size {
            bail!("chunked body: truncated chunk");
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size..];
        // Skip the CRLF that follows each chunk's data.
        if data.len() >= 2 && &data[..2] == b"\r\n" {
            data = &data[2..];
        }
    }
    Ok(out)
}

fn extract_docker_error(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct ErrBody {
        message: String,
    }
    match serde_json::from_slice::<ErrBody>(body) {
        Ok(e) if !e.message.is_empty() => e.message,
        _ => String::from_utf8_lossy(body).into_owned(),
    }
}

#[derive(Serialize)]
struct CreateContainerRequest {
    #[serde(rename = "Image")]
    image: String,
    #[serde(rename = "Cmd", skip_serializing_if = "Vec::is_empty")]
    cmd: Vec<String>,
    #[serde(rename = "Env", skip_serializing_if = "Vec::is_empty")]
    env: Vec<String>,
    #[serde(rename = "WorkingDir", skip_serializing_if = "String::is_empty")]
    working_dir: String,
    #[serde(rename = "Labels", skip_serializing_if = "HashMap::is_empty")]
    labels: HashMap<String, String>,
    #[serde(rename = "Tty")]
    tty: bool,
    #[serde(rename = "OpenStdin")]
    open_stdin: bool,
    #[serde(rename = "HostConfig")]
    host_config: HostConfig,
}

#[derive(Serialize)]
struct HostConfig {
    #[serde(rename = "Binds", skip_serializing_if = "Vec::is_empty")]
    binds: Vec<String>,
    #[serde(rename = "PortBindings", skip_serializing_if = "HashMap::is_empty")]
    port_bindings: HashMap<String, Vec<PortBindingEntry>>,
    #[serde(rename = "Memory")]
    memory: i64,
    #[serde(rename = "NanoCpus")]
    nano_cpus: i64,
    #[serde(rename = "CapDrop", skip_serializing_if = "Vec::is_empty")]
    cap_drop: Vec<String>,
    #[serde(rename = "SecurityOpt")]
    security_opt: Vec<String>,
    // Public resolvers by default so a container can resolve download hosts
    // (e.g. fill.papermc.io) regardless of the node's own /etc/resolv.conf —
    // on some hosts (notably EC2) the container inherits a VPC resolver + search
    // domain that fails these lookups. Empty => Docker's default.
    #[serde(rename = "Dns", skip_serializing_if = "Vec::is_empty")]
    dns: Vec<String>,
}

#[derive(Serialize)]
struct PortBindingEntry {
    #[serde(rename = "HostPort")]
    host_port: String,
}

fn to_create_request(spec: &ContainerSpec, dns: &[String]) -> CreateContainerRequest {
    let mut port_bindings: HashMap<String, Vec<PortBindingEntry>> = HashMap::new();
    for pb in &spec.port_bindings {
        port_bindings
            .entry(pb.container_port.clone())
            .or_default()
            .push(PortBindingEntry {
                host_port: pb.host_port.clone(),
            });
    }

    CreateContainerRequest {
        image: spec.image.clone(),
        cmd: spec.cmd.clone(),
        env: spec.env.clone(),
        working_dir: spec.working_dir.clone(),
        labels: spec.labels.clone(),
        tty: false,
        open_stdin: true,
        host_config: HostConfig {
            binds: spec.binds.clone(),
            port_bindings,
            memory: spec.memory_bytes,
            nano_cpus: spec.nano_cpus,
            // Keep Docker's default capability set rather than dropping ALL.
            // Dropping ALL strips CAP_SETUID/CAP_SETGID/CAP_CHOWN, which the
            // common server-image pattern needs — images like
            // itzg/minecraft-server start as root and drop to an unprivileged
            // user at boot ("failed switching to 'minecraft:minecraft':
            // operation not permitted" otherwise). Docker's defaults already
            // exclude the genuinely dangerous caps (SYS_ADMIN, NET_ADMIN,
            // SYS_PTRACE, SYS_MODULE, …), and no-new-privileges still blocks
            // setuid-bit privilege escalation.
            cap_drop: vec![],
            security_opt: vec!["no-new-privileges".to_string()],
            dns: dns.to_vec(),
        },
    }
}

#[async_trait]
impl ContainerRuntime for DockerRuntime {
    async fn pull(&self, image: &str) -> Result<()> {
        if self.image_present(image).await {
            return Ok(());
        }
        self.pull_image(image).await
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        let mut path = "/containers/create".to_string();
        if !spec.name.is_empty() {
            path.push_str("?name=");
            path.push_str(&spec.name);
        }

        let body_bytes = serde_json::to_vec(&to_create_request(spec, &self.dns))?;
        let body = self
            .create_container(&path, &body_bytes, &spec.image, &spec.name)
            .await?;

        #[derive(Deserialize)]
        struct CreateResponse {
            #[serde(rename = "Id")]
            id: String,
        }
        let parsed: CreateResponse =
            serde_json::from_slice(&body).context("decode create response")?;
        Ok(parsed.id)
    }

    async fn list_managed(&self) -> Result<Vec<ManagedContainer>> {
        // List every container (all=true includes stopped) and recover the ones
        // this daemon created. We identify them two ways and take whichever is
        // present: the sky-panel.server_id label, or the container name
        // "sky-<serverID>". Relying on the name too means a container whose
        // label was cleared/lost (or predates it) is still re-tracked, instead
        // of its stats going dark until a reinstall.
        let body = self
            .expect("GET", "/containers/json?all=true", None, &[200])
            .await?;

        #[derive(Deserialize)]
        struct ListItem {
            #[serde(rename = "Id")]
            id: String,
            #[serde(rename = "Names", default)]
            names: Vec<String>,
            #[serde(rename = "Labels", default)]
            labels: std::collections::HashMap<String, String>,
        }
        let items: Vec<ListItem> =
            serde_json::from_slice(&body).context("decode container list")?;

        Ok(items
            .into_iter()
            .filter_map(|it| {
                // Prefer the label; fall back to the "sky-<id>" name. Docker
                // returns names with a leading slash (e.g. "/sky-<id>").
                let from_label = it
                    .labels
                    .get("sky-panel.server_id")
                    .filter(|s| !s.is_empty())
                    .cloned();
                // The name fallback must require a real server-id (UUID) suffix,
                // not just any "sky-*" name: the daemon talks to the host's
                // Docker, so it can see unrelated containers ("sky-panel",
                // "sky-cache", …). Real servers are always "sky-<uuid>", so
                // gating on UUID shape keeps recovery working while never
                // tracking a foreign container we didn't create.
                let from_name = it.names.iter().find_map(|n| {
                    n.trim_start_matches('/')
                        .strip_prefix("sky-")
                        .filter(|s| looks_like_server_id(s))
                        .map(|s| s.to_string())
                });
                from_label.or(from_name).map(|server_id| ManagedContainer {
                    server_id,
                    container_id: it.id,
                })
            })
            .collect())
    }

    async fn start(&self, id: &str) -> Result<()> {
        let (status, body) = self
            .request("POST", &format!("/containers/{id}/start"), None)
            .await?;
        if status == 204 || status == 304 {
            return Ok(());
        }
        let err = extract_docker_error(&body);
        // Self-heal a port conflict the same way create_container self-heals a
        // name conflict: a leftover container (e.g. an old one from a failed
        // reinstall) still holds our host port, so "port is already allocated".
        // Free the port(s) this container wants by force-removing whatever else
        // publishes them, then retry start once.
        if status == 500
            && (err.contains("port is already allocated") || err.contains("address already in use"))
        {
            self.free_conflicting_ports(id).await;
            self.expect(
                "POST",
                &format!("/containers/{id}/start"),
                None,
                &[204, 304],
            )
            .await?;
            return Ok(());
        }
        bail!("docker api POST /containers/{id}/start: unexpected status {status}: {err}");
    }

    async fn stop(&self, id: &str, timeout: Duration) -> Result<()> {
        let seconds = timeout.as_secs();
        self.expect(
            "POST",
            &format!("/containers/{id}/stop?t={seconds}"),
            None,
            &[204, 304],
        )
        .await?;
        Ok(())
    }

    async fn kill(&self, id: &str) -> Result<()> {
        self.expect("POST", &format!("/containers/{id}/kill"), None, &[204])
            .await?;
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<()> {
        self.expect(
            "DELETE",
            &format!("/containers/{id}?force=true"),
            None,
            &[204],
        )
        .await?;
        Ok(())
    }

    async fn inspect(&self, id: &str) -> Result<ContainerState> {
        let body = self
            .expect("GET", &format!("/containers/{id}/json"), None, &[200])
            .await?;

        #[derive(Deserialize)]
        struct InspectResponse {
            #[serde(rename = "State")]
            state: InspectState,
        }
        #[derive(Deserialize)]
        struct InspectState {
            #[serde(rename = "Running")]
            running: bool,
        }
        let parsed: InspectResponse =
            serde_json::from_slice(&body).context("decode inspect response")?;
        Ok(ContainerState {
            running: parsed.state.running,
        })
    }

    async fn stats(&self, id: &str) -> Result<Stats> {
        let body = self
            .expect(
                "GET",
                &format!("/containers/{id}/stats?stream=false"),
                None,
                &[200],
            )
            .await?;
        let raw: DockerStatsResponse =
            serde_json::from_slice(&body).context("decode stats response")?;
        Ok(parse_docker_stats(&raw))
    }

    async fn attach(&self, id: &str) -> Result<Console> {
        let mut stream = self.connect().await?;

        let req = format!(
            "POST /{API_VERSION}/containers/{id}/attach?stream=1&stdin=1&stdout=1&stderr=1 HTTP/1.1\r\nHost: docker\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await?;

        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let header_len = loop {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                bail!("attach: connection closed before response headers completed");
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_double_crlf(&buf) {
                break pos + 4;
            }
        };

        let (status, _) = parse_response(&buf[..header_len])?;
        if status != 101 {
            bail!(
                "attach: unexpected status {status}: {}",
                extract_docker_error(&buf[header_len..])
            );
        }

        let leftover = buf[header_len..].to_vec();
        let (read_half, write_half) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(pump_output(leftover, read_half, tx));

        Ok(Console {
            stdin: Box::new(write_half),
            output: rx,
        })
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Reads de-multiplexed stdout/stderr frames from `reader` (seeded with any
/// bytes already read past the attach response headers) and forwards whole
/// lines to `tx` until the connection closes or the receiver is dropped.
async fn pump_output(
    mut buf: Vec<u8>,
    mut reader: impl tokio::io::AsyncRead + Unpin,
    tx: mpsc::UnboundedSender<String>,
) {
    let mut splitter = LineSplitter::new();

    loop {
        while let Some(header) = parse_frame_header(&buf) {
            if buf.len() < 8 + header.size {
                break;
            }
            let payload = buf[8..8 + header.size].to_vec();
            buf.drain(0..8 + header.size);

            if header.stream_type == STREAM_STDOUT || header.stream_type == STREAM_STDERR {
                for line in splitter.feed(&payload) {
                    if tx.send(line).is_err() {
                        return;
                    }
                }
            }
        }

        let mut tmp = [0u8; 4096];
        match reader.read(&mut tmp).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_image_ref_handles_tags_registries_and_defaults() {
        assert_eq!(
            split_image_ref("itzg/minecraft-server"),
            ("itzg/minecraft-server".into(), "latest".into())
        );
        assert_eq!(
            split_image_ref("node:22-alpine"),
            ("node".into(), "22-alpine".into())
        );
        assert_eq!(
            split_image_ref("itzg/minecraft-server:java21"),
            ("itzg/minecraft-server".into(), "java21".into())
        );
        // A registry host:port must not be mistaken for a tag.
        assert_eq!(
            split_image_ref("registry.example.com:5000/team/img"),
            ("registry.example.com:5000/team/img".into(), "latest".into())
        );
        assert_eq!(
            split_image_ref("registry.example.com:5000/team/img:v2"),
            ("registry.example.com:5000/team/img".into(), "v2".into())
        );
        // A digest ref has no tag.
        assert_eq!(
            split_image_ref("itzg/minecraft-server@sha256:abc123"),
            ("itzg/minecraft-server@sha256:abc123".into(), String::new())
        );
    }

    #[test]
    fn looks_like_server_id_accepts_uuids_rejects_foreign_names() {
        assert!(looks_like_server_id("b24aa5c8-a1ab-4a42-9edd-956dd23d017f"));
        // foreign "sky-*" suffixes must NOT match
        assert!(!looks_like_server_id("panel"));
        assert!(!looks_like_server_id("cache"));
        assert!(!looks_like_server_id(""));
        assert!(!looks_like_server_id("b24aa5c8-a1ab-4a42-9edd-956dd23d017")); // too short
        assert!(!looks_like_server_id(
            "b24aa5c8xa1abx4a42x9eddx956dd23d017f"
        )); // no hyphens
        assert!(!looks_like_server_id(
            "g24aa5c8-a1ab-4a42-9edd-956dd23d017f"
        )); // non-hex
    }

    #[test]
    fn encode_query_escapes_only_unsafe_chars() {
        assert_eq!(
            encode_query("itzg/minecraft-server"),
            "itzg/minecraft-server"
        );
        assert_eq!(encode_query("node:22-alpine"), "node:22-alpine");
        assert_eq!(encode_query("weird&tag=x"), "weird%26tag%3Dx");
    }

    #[test]
    fn extract_pull_error_finds_terminal_error_in_stream() {
        let ok = b"{\"status\":\"Pulling\"}\n{\"status\":\"Download complete\"}\n";
        assert_eq!(extract_pull_error(ok), None);

        let failed = b"{\"status\":\"Pulling\"}\n{\"error\":\"manifest unknown\"}\n";
        assert_eq!(extract_pull_error(failed), Some("manifest unknown".into()));

        // Docker sometimes only populates the nested errorDetail.
        let detail = b"{\"errorDetail\":{\"message\":\"not found\"}}\n";
        assert_eq!(extract_pull_error(detail), Some("not found".into()));
    }

    #[test]
    fn to_create_request_groups_port_bindings_by_container_port() {
        let spec = ContainerSpec {
            image: "itzg/minecraft-server".into(),
            port_bindings: vec![
                protocol::PortBinding {
                    container_port: "25565/tcp".into(),
                    host_port: "25565".into(),
                },
                protocol::PortBinding {
                    container_port: "25565/udp".into(),
                    host_port: "25565".into(),
                },
            ],
            ..Default::default()
        };
        let req = to_create_request(&spec, &[]);
        assert_eq!(req.host_config.port_bindings.len(), 2);
        assert_eq!(
            req.host_config.port_bindings["25565/tcp"][0].host_port,
            "25565"
        );
    }

    #[test]
    fn to_create_request_keeps_default_caps_with_no_new_privileges() {
        let spec = ContainerSpec {
            image: "test".into(),
            ..Default::default()
        };
        let req = to_create_request(&spec, &[]);
        // Must NOT drop ALL — that strips CAP_SETUID/SETGID and breaks images
        // that drop from root to an unprivileged user at boot. We keep Docker's
        // default (safe) cap set and rely on no-new-privileges for hardening.
        assert!(!req.host_config.cap_drop.contains(&"ALL".to_string()));
        assert_eq!(
            req.host_config.security_opt,
            vec!["no-new-privileges".to_string()]
        );
    }

    #[test]
    fn to_create_request_sets_dns_when_provided() {
        let spec = ContainerSpec {
            image: "test".into(),
            ..Default::default()
        };
        let dns = vec!["1.1.1.1".to_string(), "8.8.8.8".to_string()];
        let req = to_create_request(&spec, &dns);
        assert_eq!(req.host_config.dns, dns);
        // Empty dns must serialize to no Dns field (Docker default).
        let req_none = to_create_request(&spec, &[]);
        assert!(req_none.host_config.dns.is_empty());
    }

    #[test]
    fn parse_response_extracts_status_and_body() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n\r\n{\"Id\":\"abc\"}";
        let (status, body) = parse_response(raw).unwrap();
        assert_eq!(status, 201);
        assert_eq!(body, b"{\"Id\":\"abc\"}");
    }

    #[test]
    fn parse_response_dechunks_chunked_body() {
        // Docker returns `/containers/json` chunked: the body carries hex
        // chunk-size framing that must be stripped to yield valid JSON.
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n[{\"a\r\n4\r\n\":1}]\r\n0\r\n\r\n";
        let (status, body) = parse_response(raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"[{\"a\":1}]");
        // And it's now valid JSON the container-list parser can accept.
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.is_array());
    }

    #[test]
    fn dechunk_handles_size_extensions_and_terminator() {
        let out = dechunk(b"3;name=x\r\nabc\r\n0\r\n\r\n").unwrap();
        assert_eq!(out, b"abc");
    }

    #[test]
    fn find_double_crlf_locates_header_terminator() {
        let buf = b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\n\r\nleftover-bytes";
        let pos = find_double_crlf(buf).unwrap();
        assert_eq!(&buf[pos + 4..], b"leftover-bytes");
    }

    #[tokio::test]
    async fn pump_output_demuxes_leftover_and_streamed_frames() {
        let mut frame1 = vec![STREAM_STDOUT, 0, 0, 0];
        frame1.extend_from_slice(&5u32.to_be_bytes());
        frame1.extend_from_slice(b"hello");
        frame1.push(b'\n');
        // fix size to include the newline
        frame1[4..8].copy_from_slice(&6u32.to_be_bytes());

        let (tx, mut rx) = mpsc::unbounded_channel();
        let empty_reader = tokio::io::empty();
        pump_output(frame1, empty_reader, tx).await;

        assert_eq!(rx.recv().await, Some("hello".to_string()));
        assert_eq!(rx.recv().await, None);
    }
}
