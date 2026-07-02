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
use super::{Console, ContainerRuntime, ContainerState, Stats};

const API_VERSION: &str = "v1.43";

pub struct DockerRuntime {
    socket_path: PathBuf,
}

impl DockerRuntime {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
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
    Ok((code, buf[header_len..].to_vec()))
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
    #[serde(rename = "CapDrop")]
    cap_drop: Vec<String>,
    #[serde(rename = "SecurityOpt")]
    security_opt: Vec<String>,
}

#[derive(Serialize)]
struct PortBindingEntry {
    #[serde(rename = "HostPort")]
    host_port: String,
}

fn to_create_request(spec: &ContainerSpec) -> CreateContainerRequest {
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
            // Secure-by-default: no capabilities beyond what the image
            // itself needs, no privilege escalation.
            cap_drop: vec!["ALL".to_string()],
            security_opt: vec!["no-new-privileges".to_string()],
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

        let body_bytes = serde_json::to_vec(&to_create_request(spec))?;

        // Docker's create endpoint does NOT pull — a missing image returns 404.
        // On a fresh node the egg's image won't be present, so pull it and
        // retry once. (This can take a while for large images; the panel
        // provisions asynchronously to accommodate that.)
        let (status, body) = self.request("POST", &path, Some(&body_bytes)).await?;
        let body = match status {
            201 => body,
            404 => {
                self.pull_image(&spec.image).await?;
                self.expect("POST", &path, Some(&body_bytes), &[201])
                    .await?
            }
            other => bail!(
                "docker api POST {path}: unexpected status {other}: {}",
                extract_docker_error(&body)
            ),
        };

        #[derive(Deserialize)]
        struct CreateResponse {
            #[serde(rename = "Id")]
            id: String,
        }
        let parsed: CreateResponse =
            serde_json::from_slice(&body).context("decode create response")?;
        Ok(parsed.id)
    }

    async fn start(&self, id: &str) -> Result<()> {
        self.expect(
            "POST",
            &format!("/containers/{id}/start"),
            None,
            &[204, 304],
        )
        .await?;
        Ok(())
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
        let req = to_create_request(&spec);
        assert_eq!(req.host_config.port_bindings.len(), 2);
        assert_eq!(
            req.host_config.port_bindings["25565/tcp"][0].host_port,
            "25565"
        );
    }

    #[test]
    fn to_create_request_always_drops_all_capabilities() {
        let spec = ContainerSpec {
            image: "test".into(),
            ..Default::default()
        };
        let req = to_create_request(&spec);
        assert_eq!(req.host_config.cap_drop, vec!["ALL".to_string()]);
        assert_eq!(
            req.host_config.security_opt,
            vec!["no-new-privileges".to_string()]
        );
    }

    #[test]
    fn parse_response_extracts_status_and_body() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n\r\n{\"Id\":\"abc\"}";
        let (status, body) = parse_response(raw).unwrap();
        assert_eq!(status, 201);
        assert_eq!(body, b"{\"Id\":\"abc\"}");
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
