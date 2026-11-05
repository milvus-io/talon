//! Bind namespace cache routing before opening any residency directory.
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::cache_reset::incompatible;
use crate::page_access_store::CacheRootLock;
use crate::small_store::SmallObjectStore;
use talon_core::Result;

pub(super) fn open_small_store(
    root: &Path,
    threshold: u64,
    segment_size: u64,
    capacity: u64,
    root_lock: Option<Arc<CacheRootLock>>,
    service: Option<&Arc<talon_journal::JournalService>>,
    maintenance: Option<Arc<crate::background::BackgroundBudget>>,
) -> Result<Option<Arc<SmallObjectStore>>> {
    check_small_routing(root, threshold)?;
    if threshold == 0 {
        return Ok(None);
    }
    Ok(Some(Arc::new(SmallObjectStore::open_with_service(
        root.join("small"),
        threshold,
        segment_size,
        capacity,
        root_lock,
        service,
        maintenance,
    )?)))
}

fn check_small_routing(root: &Path, threshold: u64) -> Result<()> {
    let path = root.join("small-routing.json");
    // The version includes the single-directory residency model. Do not
    // silently adopt old mixed layouts or reinterpret them after restart.
    let expected = (1_u32, threshold);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let stored: (u32, u64) = serde_json::from_slice(&bytes).map_err(|e| {
                incompatible(format!(
                    "invalid small store routing at {}: {e}; use a new empty cache directory",
                    path.display()
                ))
            })?;
            if stored != expected {
                return Err(incompatible(format!("incompatible small store routing at {}: stored {stored:?}, requested {expected:?}; use a new empty cache directory", path.display())).into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                let name = entry.file_name();
                if name != "cache_geometry.json"
                    && name != ".worker.lock"
                    && name != "small-routing.json.tmp"
                {
                    return Err(incompatible(format!("cache directory {} predates small store routing metadata; use a new empty cache directory", root.display())).into());
                }
            }
            let tmp = path.with_extension("json.tmp");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&serde_json::to_vec(&expected).expect("routing tuple serializes"))?;
            file.sync_all()?;
            std::fs::rename(tmp, &path)?;
            std::fs::File::open(root)?.sync_all()?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
