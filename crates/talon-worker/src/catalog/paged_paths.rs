//! File names shared by page I/O and replay of durable residency intents.
use super::DiskId;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub(crate) fn disk_dir(root: &Path, id: DiskId) -> PathBuf {
    // The legacy directory scanner must leave journal-managed files to replay.
    id.path(root, "pages")
}

pub(crate) fn page_file(dir: &Path, page: u32) -> PathBuf {
    dir.join(format!("{page}.page"))
}

pub(crate) fn staged_file(page_file: &Path, transaction: u64) -> PathBuf {
    let mut path = page_file.as_os_str().to_owned();
    path.push(format!(".catalog.{transaction}"));
    path.into()
}

pub(crate) fn remove_empty_disk_dir(dir: &Path) -> io::Result<()> {
    match fs::remove_dir(dir) {
        Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        result => result?,
    }
    match fs::File::open(dir.parent().unwrap()) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        file => file?.sync_all(),
    }
}
