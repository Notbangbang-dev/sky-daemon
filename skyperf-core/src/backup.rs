//! Streaming tar+zstd backup creation and restoration.
//!
//! Archives are plain tar streams piped through a zstd compressor/decompressor,
//! so creation and extraction never need to hold the whole archive in memory.

use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};

/// Creates a zstd-compressed tar archive of everything under `src_dir` at
/// `dest_path`, streaming the whole way through. Returns the size in bytes
/// of the resulting archive file.
pub fn create_backup(src_dir: &Path, dest_path: &Path) -> io::Result<u64> {
    if !src_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("source directory not found: {}", src_dir.display()),
        ));
    }

    let out_file = File::create(dest_path)?;
    let encoder = zstd::Encoder::new(out_file, 0)?;
    let mut tar_builder = tar::Builder::new(encoder);
    tar_builder.append_dir_all(".", src_dir)?;
    let encoder = tar_builder.into_inner()?;
    let out_file = encoder.finish()?;
    out_file.sync_all()?;
    drop(out_file);

    let bytes = fs::metadata(dest_path)?.len();
    Ok(bytes)
}

/// Extracts a zstd+tar archive (as produced by [`create_backup`]) into
/// `dest_dir`, creating `dest_dir` if necessary.
///
/// Any archive entry whose path is absolute or contains a `..` component
/// (i.e. would escape `dest_dir` once joined) is rejected: it is skipped and
/// a warning is logged to stderr, rather than being extracted.
pub fn restore_backup(archive_path: &Path, dest_dir: &Path) -> io::Result<()> {
    let in_file = File::open(archive_path)?;
    let decoder = zstd::Decoder::new(in_file)?;
    let mut archive = tar::Archive::new(decoder);

    fs::create_dir_all(dest_dir)?;

    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let rel_path: PathBuf = entry.path()?.to_path_buf();

        if !is_safe_relative_path(&rel_path) {
            eprintln!(
                "skyperf: skipping unsafe archive entry (path traversal): {}",
                rel_path.display()
            );
            continue;
        }

        let target = dest_dir.join(&rel_path);
        entry.unpack(&target)?;
    }

    Ok(())
}

/// Returns true if `path` is relative and contains no `..` / root / prefix
/// components, i.e. joining it onto a destination directory cannot escape
/// that directory.
fn is_safe_relative_path(path: &Path) -> bool {
    if path.as_os_str().is_empty() {
        return false;
    }
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn create_and_restore_round_trip() {
        let src = tempfile::tempdir().unwrap();
        write_file(src.path().join("top.txt"), b"top level");
        fs::create_dir_all(src.path().join("sub/nested")).unwrap();
        write_file(src.path().join("sub/mid.txt"), b"middle level");
        write_file(src.path().join("sub/nested/deep.txt"), b"deep level");

        let workdir = tempfile::tempdir().unwrap();
        let archive_path = workdir.path().join("backup.tar.zst");
        let bytes = create_backup(src.path(), &archive_path).unwrap();
        assert!(bytes > 0);
        assert_eq!(fs::metadata(&archive_path).unwrap().len(), bytes);

        let dest = workdir.path().join("restored");
        restore_backup(&archive_path, &dest).unwrap();

        assert_eq!(fs::read(dest.join("top.txt")).unwrap(), b"top level");
        assert_eq!(fs::read(dest.join("sub/mid.txt")).unwrap(), b"middle level");
        assert_eq!(
            fs::read(dest.join("sub/nested/deep.txt")).unwrap(),
            b"deep level"
        );
    }

    #[test]
    fn create_fails_for_missing_source() {
        let workdir = tempfile::tempdir().unwrap();
        let missing_src = workdir.path().join("nope");
        let archive_path = workdir.path().join("backup.tar.zst");
        assert!(create_backup(&missing_src, &archive_path).is_err());
    }

    #[test]
    fn restore_rejects_path_traversal_entries() {
        let workdir = tempfile::tempdir().unwrap();
        let archive_path = workdir.path().join("evil.tar.zst");
        build_malicious_archive(&archive_path, "../evil.txt", b"pwned");

        let dest = workdir.path().join("dest");
        restore_backup(&archive_path, &dest).unwrap();

        // The traversal target, one level above `dest`, must not exist.
        assert!(!workdir.path().join("evil.txt").exists());
        // Nothing should have been written into dest for the rejected entry.
        assert!(!dest.join("evil.txt").exists());
    }

    #[test]
    fn restore_rejects_absolute_path_entries() {
        let workdir = tempfile::tempdir().unwrap();
        let archive_path = workdir.path().join("evil_abs.tar.zst");
        let absolute_target = if cfg!(windows) {
            "C:/evil_abs_test_marker.txt"
        } else {
            "/tmp/evil_abs_test_marker.txt"
        };
        build_malicious_archive(&archive_path, absolute_target, b"pwned");

        let dest = workdir.path().join("dest");
        restore_backup(&archive_path, &dest).unwrap();

        assert!(!Path::new(absolute_target).exists());
    }

    #[test]
    fn is_safe_relative_path_rejects_traversal_and_absolute() {
        assert!(!is_safe_relative_path(Path::new("../escape.txt")));
        assert!(!is_safe_relative_path(Path::new("a/../../escape.txt")));
        assert!(!is_safe_relative_path(Path::new("/absolute/path.txt")));
        assert!(is_safe_relative_path(Path::new("fine/relative/path.txt")));
        assert!(is_safe_relative_path(Path::new("./fine.txt")));
    }

    fn write_file(path: PathBuf, contents: &[u8]) {
        let mut f = fs::File::create(path).unwrap();
        f.write_all(contents).unwrap();
    }

    /// Hand-builds a tar+zstd archive containing a single entry with an
    /// attacker-controlled path, bypassing our own `create_backup` (which
    /// can only ever produce safe relative paths from real filesystem
    /// walks) so we can exercise `restore_backup`'s traversal guard.
    ///
    /// `tar::Header::set_path` refuses to write `..`-containing or absolute
    /// paths, so a malicious path is poked directly into the raw GNU header
    /// bytes instead, exactly as an attacker-crafted archive would.
    fn build_malicious_archive(archive_path: &Path, entry_path: &str, contents: &[u8]) {
        let file = File::create(archive_path).unwrap();
        let encoder = zstd::Encoder::new(file, 0).unwrap();
        let mut builder = tar::Builder::new(encoder);

        let mut header = tar::Header::new_gnu();
        {
            let gnu = header.as_gnu_mut().unwrap();
            let name_bytes = entry_path.as_bytes();
            assert!(
                name_bytes.len() < gnu.name.len(),
                "test entry path too long"
            );
            gnu.name = [0u8; 100];
            gnu.name[..name_bytes.len()].copy_from_slice(name_bytes);
        }
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append(&header, contents).unwrap();

        let encoder = builder.into_inner().unwrap();
        let file = encoder.finish().unwrap();
        file.sync_all().unwrap();
    }
}
