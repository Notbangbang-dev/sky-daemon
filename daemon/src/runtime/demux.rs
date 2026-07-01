//! Pure helpers for de-multiplexing a Docker-attached stream (used when a
//! container is attached with `Tty: false`, which is how `DockerRuntime`
//! always creates containers, so stdout/stderr frames can be told apart).
//! Kept free of any I/O so they're trivially unit-testable; the async
//! read loop that drives these lives in `docker.rs`.

pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

pub struct FrameHeader {
    pub stream_type: u8,
    pub size: usize,
}

/// Parses one 8-byte Docker stream-frame header
/// (`[type, 0, 0, 0, size(4 bytes big-endian)]`) from the start of `buf`,
/// without consuming anything. Returns `None` if fewer than 8 bytes are
/// available yet.
pub fn parse_frame_header(buf: &[u8]) -> Option<FrameHeader> {
    if buf.len() < 8 {
        return None;
    }
    let stream_type = buf[0];
    let size = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    Some(FrameHeader { stream_type, size })
}

/// Buffers partial writes and reports complete `\n`-terminated lines (with
/// the newline, and any trailing `\r`, stripped).
#[derive(Default)]
pub struct LineSplitter {
    buf: Vec<u8>,
}

impl LineSplitter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `data` and returns any newly completed lines.
    pub fn feed(&mut self, data: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(data);

        let mut lines = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let mut line = self.buf[..pos].to_vec();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            lines.push(String::from_utf8_lossy(&line).into_owned());
            self.buf.drain(..=pos);
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(stream_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![stream_type, 0, 0, 0];
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn parse_frame_header_reads_type_and_size() {
        let buf = frame(STREAM_STDOUT, b"hello");
        let header = parse_frame_header(&buf).unwrap();
        assert_eq!(header.stream_type, STREAM_STDOUT);
        assert_eq!(header.size, 5);
    }

    #[test]
    fn parse_frame_header_none_when_incomplete() {
        assert!(parse_frame_header(&[1, 2, 3]).is_none());
    }

    #[test]
    fn line_splitter_yields_complete_lines() {
        let mut s = LineSplitter::new();
        let lines = s.feed(b"hello\nworld\n");
        assert_eq!(lines, vec!["hello", "world"]);
    }

    #[test]
    fn line_splitter_handles_partial_line_across_feeds() {
        let mut s = LineSplitter::new();
        assert!(s.feed(b"hel").is_empty());
        assert_eq!(s.feed(b"lo\nwor"), vec!["hello"]);
        assert_eq!(s.feed(b"ld\n"), vec!["world"]);
    }

    #[test]
    fn line_splitter_strips_carriage_return() {
        let mut s = LineSplitter::new();
        assert_eq!(s.feed(b"hello\r\n"), vec!["hello"]);
    }
}
