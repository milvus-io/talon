//! Bounded, repeatable disk discovery of checkpoint leftovers, independent of TTL.
use crate::page_gc::CleanupReport;
use crate::page_lifecycle::PageLifecycle;
use std::fs::{self, ReadDir};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

struct DirectoryScan {
    path: PathBuf,
    digest: u64,
    files: ReadDir,
    exhausted: bool,
    failed: bool,
}

/// At most three directory iterators are retained; no list of all blocks/files
/// or in-memory retry queue. Failed work is rediscovered on the next disk pass.
pub(crate) struct CleanupCursor {
    root: PathBuf,
    shards: Option<ReadDir>,
    directories: Option<ReadDir>,
    current: Option<DirectoryScan>,
    started: Instant,
    pending: usize,
    #[cfg(test)]
    pub fail_deletes: bool,
}

impl CleanupCursor {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            shards: None,
            directories: None,
            current: None,
            started: Instant::now(),
            pending: 0,
            #[cfg(test)]
            fail_deletes: false,
        }
    }

    fn failed(report: &mut CleanupReport, path: &Path, error: io::Error) {
        report.errors += 1;
        tracing::warn!(path = %path.display(), %error, "page metadata cleanup failed; will rescan");
    }

    fn unlink(&self, path: &Path, directory: bool, report: &mut CleanupReport) -> bool {
        report.attempted += 1;
        #[cfg(test)]
        if self.fail_deletes {
            Self::failed(
                report,
                path,
                io::Error::new(io::ErrorKind::PermissionDenied, "injected cleanup failure"),
            );
            return false;
        }
        let result = if directory {
            fs::remove_dir(path)
        } else {
            fs::remove_file(path)
        };
        match result {
            Ok(()) => {
                report.removed += 1;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(error) => {
                Self::failed(report, path, error);
                false
            }
        }
    }

    /// Returns true if finalization needs another deletion budget. Under the
    /// exclusive directory gate, at most three entries prove whether only the
    /// two known metadata files remain. Never recursively delete a directory.
    fn finish_directory(
        &self,
        dir: &DirectoryScan,
        budget: usize,
        report: &mut CleanupReport,
    ) -> io::Result<bool> {
        let files = match fs::read_dir(&dir.path) {
            Ok(files) => files,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let mut metadata = Vec::with_capacity(2);
        for entry in files.take(3) {
            let entry = entry?;
            let name = entry.file_name();
            if !entry.file_type()?.is_file() || (name != "access.meta" && name != "block.meta") {
                // Includes live pages, unknown files, symlinks and failed temps.
                return Ok(false);
            }
            metadata.push(entry.path());
        }
        // The two accepted names are unique, so this implies a complete listing.
        for path in metadata {
            if report.attempted == budget {
                return Ok(true);
            }
            if !self.unlink(&path, false, report) {
                return Ok(false);
            }
        }
        if report.attempted == budget {
            return Ok(true);
        }
        self.unlink(&dir.path, true, report);
        Ok(false)
    }

    pub fn run_batch(
        &mut self,
        lifecycle: &PageLifecycle,
        scan_budget: usize,
        delete_budget: usize,
    ) -> CleanupReport {
        let mut report = CleanupReport::default();
        if self.shards.is_none() {
            self.started = Instant::now();
            self.pending = 0;
            // Do not follow a replaced/symlinked paged root.
            let root = fs::symlink_metadata(&self.root).and_then(|meta| {
                if !meta.is_dir() {
                    return Err(io::Error::other("paged root is not a directory"));
                }
                fs::read_dir(&self.root)
            });
            match root {
                Ok(shards) => self.shards = Some(shards),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    report.completed_scan = true;
                    return report;
                }
                Err(error) => {
                    Self::failed(&mut report, &self.root, error);
                    report.pending = 1;
                    report.completed_scan = true;
                    return report;
                }
            }
        }
        while report.checked < scan_budget && report.attempted < delete_budget {
            if let Some(mut dir) = self.current.take() {
                let gate = lifecycle.directory_gate(dir.digest);
                let _exclusive = gate.blocking_write();
                // Revalidate the path after acquiring the gate, including the
                // shard parent. Metadata is not needed to establish gate identity.
                let safe = fs::symlink_metadata(dir.path.parent().unwrap()).and_then(|m| {
                    if m.is_dir() {
                        fs::symlink_metadata(&dir.path)
                    } else {
                        Err(io::Error::other("shard is not a directory"))
                    }
                });
                match safe {
                    Ok(m) if m.is_dir() => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    result => {
                        Self::failed(
                            &mut report,
                            &dir.path,
                            result.err().unwrap_or_else(|| {
                                io::Error::other("page path is not a directory")
                            }),
                        );
                        self.pending += 1;
                        continue;
                    }
                }
                // Release the stripe between chunks, allowing foreground writes
                // in unrelated blocks on this stripe to keep making progress.
                for _ in 0..64 {
                    if dir.exhausted
                        || report.checked == scan_budget
                        || report.attempted == delete_budget
                    {
                        break;
                    }
                    match dir.files.next() {
                        Some(Ok(entry)) => {
                            report.checked += 1;
                            match entry.file_type() {
                                Ok(kind)
                                    if kind.is_file()
                                        && entry
                                            .file_name()
                                            .to_str()
                                            .is_some_and(is_owned_temp) =>
                                {
                                    if !self.unlink(&entry.path(), false, &mut report) {
                                        dir.failed = true;
                                    }
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    Self::failed(&mut report, &entry.path(), error);
                                    dir.failed = true;
                                }
                            }
                        }
                        Some(Err(error)) => {
                            report.checked += 1;
                            Self::failed(&mut report, &dir.path, error);
                            dir.failed = true;
                            dir.exhausted = true;
                        }
                        None => dir.exhausted = true,
                    }
                }
                if dir.exhausted {
                    let errors = report.errors;
                    match self.finish_directory(&dir, delete_budget, &mut report) {
                        Ok(true) => {
                            self.current = Some(dir);
                        }
                        Ok(false) => {
                            self.pending += usize::from(dir.failed || report.errors > errors);
                        }
                        Err(error) => {
                            Self::failed(&mut report, &dir.path, error);
                            self.pending += 1;
                        }
                    }
                } else {
                    self.current = Some(dir);
                }
                continue;
            }
            if let Some(directories) = &mut self.directories {
                match directories.next() {
                    Some(Ok(entry)) => {
                        report.checked += 1;
                        let path = entry.path();
                        let Some(digest) = directory_digest(&path) else {
                            continue;
                        };
                        match entry.file_type().and_then(|kind| {
                            if !kind.is_dir() {
                                return Ok(None);
                            }
                            fs::read_dir(&path).map(Some)
                        }) {
                            Ok(Some(files)) => {
                                self.current = Some(DirectoryScan {
                                    path,
                                    digest,
                                    files,
                                    exhausted: false,
                                    failed: false,
                                })
                            }
                            Ok(None) => {}
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                            Err(error) => {
                                Self::failed(&mut report, &path, error);
                                self.pending += 1;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        report.checked += 1;
                        Self::failed(&mut report, &self.root, error);
                        self.pending += 1;
                        self.directories = None;
                    }
                    None => self.directories = None,
                }
                continue;
            }
            match self.shards.as_mut().unwrap().next() {
                Some(Ok(entry)) => {
                    report.checked += 1;
                    if !entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| is_hex(name, 2))
                    {
                        continue;
                    }
                    match entry.file_type().and_then(|kind| {
                        if !kind.is_dir() {
                            return Ok(None);
                        }
                        fs::read_dir(entry.path()).map(Some)
                    }) {
                        Ok(dirs) => self.directories = dirs,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => {
                            Self::failed(&mut report, &entry.path(), error);
                            self.pending += 1;
                        }
                    }
                }
                Some(Err(error)) => {
                    report.checked += 1;
                    Self::failed(&mut report, &self.root, error);
                    self.pending += 1;
                    self.complete(&mut report);
                    break;
                }
                None => {
                    self.complete(&mut report);
                    break;
                }
            }
        }
        report
    }

    fn complete(&mut self, report: &mut CleanupReport) {
        self.shards = None;
        self.directories = None;
        report.completed_scan = true;
        report.pending = self.pending;
        report.scan_seconds = self.started.elapsed().as_secs_f64();
    }
}

fn is_owned_temp(name: &str) -> bool {
    if let Some(suffix) = name.strip_prefix("access.meta.tmp.") {
        return !suffix.is_empty();
    }
    let writer = if let Some(writer) = name.strip_prefix("block.meta.tmp.") {
        writer
    } else if let Some((page, writer)) = name.split_once(".page.tmp.") {
        if page.parse::<u32>().is_err() {
            return false;
        }
        writer
    } else {
        return false;
    };
    let Some((pid, sequence)) = writer.split_once('.') else {
        return false;
    };
    pid.parse::<u32>().is_ok() && sequence.parse::<u64>().is_ok()
}

fn is_hex(name: &str, len: usize) -> bool {
    name.len() == len
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn directory_digest(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?.strip_suffix(".pages")?;
    if !is_hex(name, 16) || path.parent()?.file_name()?.to_str()? != &name[..2] {
        return None;
    }
    u64::from_str_radix(name, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory(root: &Path, digest: u64) -> PathBuf {
        let hex = format!("{digest:016x}");
        let path = root.join(&hex[..2]).join(format!("{hex}.pages"));
        fs::create_dir_all(&path).unwrap();
        path
    }
    fn complete(cursor: &mut CleanupCursor, life: &PageLifecycle) -> CleanupReport {
        for _ in 0..1000 {
            let r = cursor.run_batch(life, 1, 1);
            assert!(r.checked <= 1);
            assert!(r.attempted <= 1);
            if r.completed_scan {
                return r;
            }
        }
        panic!("cleanup made no bounded progress");
    }

    #[test]
    fn bounded_cleanup_preserves_live_pages_unknown_files_and_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let live = directory(root.path(), 1);
        fs::write(live.join("0.page"), b"page").unwrap();
        fs::write(live.join("access.meta"), b"keep").unwrap();
        fs::write(live.join("access.meta.tmp.crashed"), b"partial").unwrap();
        let empty = directory(root.path(), 2);
        for name in [
            "block.meta",
            "access.meta",
            "block.meta.tmp.1.2",
            "0.page.tmp.1.2",
        ] {
            fs::write(empty.join(name), b"leftover").unwrap();
        }
        let unknown = directory(root.path(), 3);
        fs::write(unknown.join("user.data"), b"keep").unwrap();
        fs::write(unknown.join("block.meta"), b"keep").unwrap();
        fs::write(external.path().join("access.meta.tmp.external"), b"keep").unwrap();
        let linked = root.path().join("00/0000000000000004.pages");
        std::os::unix::fs::symlink(external.path(), &linked).unwrap();
        std::os::unix::fs::symlink(
            external.path().join("access.meta.tmp.external"),
            live.join("access.meta.tmp.link"),
        )
        .unwrap();
        let life = PageLifecycle::new();
        let mut cursor = CleanupCursor::new(root.path().to_owned());
        assert_eq!(complete(&mut cursor, &life).pending, 0);
        assert!(!empty.exists());
        assert!(!live.join("access.meta.tmp.crashed").exists());
        assert_eq!(fs::read(live.join("0.page")).unwrap(), b"page");
        assert_eq!(fs::read(live.join("access.meta")).unwrap(), b"keep");
        assert_eq!(fs::read(unknown.join("block.meta")).unwrap(), b"keep");
        assert!(live
            .join("access.meta.tmp.link")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            fs::read(external.path().join("access.meta.tmp.external")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn failed_cleanup_is_rediscovered_after_restart_without_metadata() {
        let root = tempfile::tempdir().unwrap();
        let dir = directory(root.path(), 7);
        fs::write(dir.join("access.meta.tmp.crashed"), b"partial").unwrap();
        let life = PageLifecycle::new();
        let mut cursor = CleanupCursor::new(root.path().to_owned());
        cursor.fail_deletes = true;
        assert_eq!(complete(&mut cursor, &life).pending, 1);
        assert!(dir.exists());
        drop(cursor); // No retry list is persisted, and there is no block.meta.
        let mut cursor = CleanupCursor::new(root.path().to_owned());
        assert_eq!(complete(&mut cursor, &life).pending, 0);
        assert!(!dir.exists());
    }

    #[test]
    fn empty_directory_finalization_rechecks_pages_created_between_batches() {
        let root = tempfile::tempdir().unwrap();
        let dir = directory(root.path(), 9);
        fs::write(dir.join("access.meta"), b"checkpoint").unwrap();
        fs::write(dir.join("block.meta"), b"identity").unwrap();
        let life = PageLifecycle::new();
        let mut cursor = CleanupCursor::new(root.path().to_owned());
        // A budget of one forces metadata removal and rmdir into separate calls.
        for _ in 0..20 {
            let r = cursor.run_batch(&life, 1, 1);
            if r.removed > 0 {
                break;
            }
        }
        // A new commit runs while the cleanup gate is released between batches.
        fs::write(dir.join("block.meta"), b"new identity").unwrap();
        fs::write(dir.join("0.page"), b"new page").unwrap();
        complete(&mut cursor, &life);
        assert_eq!(fs::read(dir.join("block.meta")).unwrap(), b"new identity");
        assert_eq!(fs::read(dir.join("0.page")).unwrap(), b"new page");
    }
}
