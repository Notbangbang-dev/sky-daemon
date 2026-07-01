//! Log tailing: last-N-lines plus a caller-driven polling "follow" mode.
//!
//! This is a library, not a CLI: `follow` is meant to be run inside
//! `tokio::task::spawn_blocking` by the daemon, which owns the `stop` flag
//! and the per-line callback (typically forwarding each line as a WS
//! event). There is no stdin/stdout handling here — that was only ever
//! needed for the old standalone `skyperf` binary.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Default number of trailing lines returned before following.
pub const DEFAULT_TAIL_LINES: usize = 200;

/// How often the follow loop polls the file for new content.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Reads `reader` line-by-line and returns (at most) the last `n` lines,
/// oldest first, with trailing `\n`/`\r\n` stripped.
///
/// This only ever keeps `n` lines in memory at a time (via a bounded
/// `VecDeque`), regardless of how long the input is.
pub fn last_n_lines<R: Read>(reader: R, n: usize) -> io::Result<Vec<String>> {
    let mut buf = BufReader::new(reader);
    let mut window: VecDeque<String> = VecDeque::with_capacity(n);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = buf.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        strip_newline(&mut line);
        if window.len() == n {
            window.pop_front();
        }
        if n > 0 {
            window.push_back(line.clone());
        }
    }
    Ok(window.into_iter().collect())
}

/// Convenience wrapper: the last [`DEFAULT_TAIL_LINES`] lines of the file at
/// `path`.
pub fn read_last_lines(path: &Path) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    last_n_lines(file, DEFAULT_TAIL_LINES)
}

fn strip_newline(line: &mut String) {
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
}

/// Polls `path` for appended content, calling `on_line` once per newly
/// completed line, until `stop` is set to `true` or the file disappears.
/// Blocking — run it via `tokio::task::spawn_blocking` from async code.
pub fn follow<F: FnMut(&str)>(path: &Path, stop: &AtomicBool, mut on_line: F) -> io::Result<()> {
    let mut pos = fs::metadata(path)?.len();

    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        std::thread::sleep(POLL_INTERVAL);

        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return Ok(()), // file disappeared: exit cleanly
        };
        let len = metadata.len();

        if len < pos {
            // File was truncated or replaced (e.g. log rotation); restart
            // from the beginning of the now-shorter file.
            pos = 0;
        }
        if len == pos {
            continue;
        }

        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(pos))?;
        let mut reader = BufReader::new(file);

        loop {
            let mut buf = String::new();
            let bytes_read = reader.read_line(&mut buf)?;
            if bytes_read == 0 {
                break;
            }
            if !buf.ends_with('\n') {
                // Incomplete line at EOF: leave it unconsumed for next poll.
                break;
            }
            strip_newline(&mut buf);
            on_line(&buf);
            pos += bytes_read as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    #[test]
    fn returns_all_lines_when_fewer_than_n() {
        let data = "one\ntwo\nthree\n";
        let lines = last_n_lines(Cursor::new(data), 200).unwrap();
        assert_eq!(lines, vec!["one", "two", "three"]);
    }

    #[test]
    fn returns_only_last_n_lines() {
        let data = "1\n2\n3\n4\n5\n";
        let lines = last_n_lines(Cursor::new(data), 3).unwrap();
        assert_eq!(lines, vec!["3", "4", "5"]);
    }

    #[test]
    fn handles_no_trailing_newline_on_final_line() {
        let data = "a\nb\nc";
        let lines = last_n_lines(Cursor::new(data), 200).unwrap();
        assert_eq!(lines, vec!["a", "b", "c"]);
    }

    #[test]
    fn handles_crlf_line_endings() {
        let data = "a\r\nb\r\nc\r\n";
        let lines = last_n_lines(Cursor::new(data), 200).unwrap();
        assert_eq!(lines, vec!["a", "b", "c"]);
    }

    #[test]
    fn empty_input_yields_no_lines() {
        let lines = last_n_lines(Cursor::new(""), 200).unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn n_zero_yields_no_lines() {
        let data = "a\nb\nc\n";
        let lines = last_n_lines(Cursor::new(data), 0).unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn follow_emits_appended_lines_and_stops_on_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.txt");
        fs::write(&path, "first\n").unwrap();

        let stop = AtomicBool::new(false);
        let mut seen = Vec::new();

        // Append a line from another thread shortly after follow starts,
        // then flip the stop flag once we've observed it.
        let path2 = path.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let mut f = fs::OpenOptions::new().append(true).open(&path2).unwrap();
            f.write_all(b"second\n").unwrap();
        });

        follow(&path, &stop, |line| {
            seen.push(line.to_string());
            stop.store(true, Ordering::Relaxed);
        })
        .unwrap();

        assert_eq!(seen, vec!["second"]);
    }
}
