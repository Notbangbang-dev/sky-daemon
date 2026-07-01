//! Recursive directory size computation, without following symlinks.

use std::io;
use std::path::Path;

use walkdir::WalkDir;

/// Recursively sums the size in bytes of every regular file found under
/// `path` (including `path` itself, if it is a plain file).
///
/// Symlinks are never followed, so this is safe against symlink cycles.
/// The size of a symlink entry itself (its target-path string storage) is
/// not counted, only the size of real files encountered while walking.
pub fn compute_dir_size(path: &Path) -> io::Result<u64> {
    // Fail fast with a clear error if the root path does not exist at all,
    // rather than relying on WalkDir's (less specific) error for this case.
    std::fs::symlink_metadata(path)?;

    let mut total: u64 = 0;
    for entry in WalkDir::new(path).follow_links(false) {
        let entry = entry.map_err(walkdir_err_to_io)?;
        let file_type = entry.file_type();
        if file_type.is_file() {
            let metadata = entry.metadata().map_err(walkdir_err_to_io)?;
            total += metadata.len();
        }
    }
    Ok(total)
}

fn walkdir_err_to_io(err: walkdir::Error) -> io::Error {
    io::Error::other(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn sums_files_in_flat_directory() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path().join("a.txt"), b"hello"); // 5 bytes
        write_file(dir.path().join("b.txt"), b"world!"); // 6 bytes

        let size = compute_dir_size(dir.path()).unwrap();
        assert_eq!(size, 11);
    }

    #[test]
    fn sums_files_recursively_in_subdirectories() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path().join("a.txt"), b"12345"); // 5
        fs::create_dir_all(dir.path().join("sub/nested")).unwrap();
        write_file(dir.path().join("sub/b.txt"), b"1234567890"); // 10
        write_file(dir.path().join("sub/nested/c.txt"), b"123"); // 3

        let size = compute_dir_size(dir.path()).unwrap();
        assert_eq!(size, 18);
    }

    #[test]
    fn empty_directory_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let size = compute_dir_size(dir.path()).unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn missing_path_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(compute_dir_size(&missing).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn does_not_follow_symlink_cycles() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path().join("real.txt"), b"abcde"); // 5
                                                           // Symlink pointing back at the root directory: if we followed
                                                           // symlinks this would recurse forever.
        std::os::unix::fs::symlink(dir.path(), dir.path().join("loop")).unwrap();

        let size = compute_dir_size(dir.path()).unwrap();
        assert_eq!(size, 5);
    }

    fn write_file(path: std::path::PathBuf, contents: &[u8]) {
        let mut f = fs::File::create(path).unwrap();
        f.write_all(contents).unwrap();
    }
}
