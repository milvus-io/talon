//! Last-drop never joins an I/O thread on a serving reactor. Startup-only
//! reopen waits for a retiring writer, before touching that cache's files.
use super::*;
use std::sync::Weak;
use std::sync::{Condvar, OnceLock};

pub(crate) type Registry = Mutex<BTreeMap<PathBuf, Weak<Life>>>;
static WRITERS: OnceLock<Registry> = OnceLock::new();
pub(crate) fn enter_service(root: &Path) -> io::Result<Arc<Life>> {
    enter(WRITERS.get_or_init(Default::default), root)
}
pub(crate) fn enter(registry: &Registry, root: &Path) -> io::Result<Arc<Life>> {
    Life::enter(registry, root)
}

/// Startup may acquire the outer cache lock before opening any namespace.
/// Wait only for this process's already-retiring writers, never a live owner.
pub(crate) fn wait_for_retiring(root: &Path) -> io::Result<()> {
    let root = root.canonicalize()?;
    let Some(writers) = WRITERS.get() else {
        return Ok(());
    };
    let retiring: Vec<_> = writers
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.starts_with(&root))
        .filter_map(|(_, life)| life.upgrade())
        .filter(|life| life.closing.load(Ordering::Acquire))
        .collect();
    for life in retiring {
        life.wait();
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct Life {
    closing: AtomicBool,
    done: Mutex<bool>,
    changed: Condvar,
}

pub(crate) struct Exit(pub Arc<Life>);
impl Drop for Exit {
    fn drop(&mut self) {
        self.0.closing.store(true, Ordering::Release);
        *self.0.done.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}

impl Life {
    pub fn enter(registry: &Registry, root: &Path) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let writers = registry;
        loop {
            let previous = {
                let mut writers = writers.lock().unwrap();
                writers.retain(|_, life| life.strong_count() != 0);
                if let Some(previous) = writers.get(&root).and_then(Weak::upgrade) {
                    if !previous.closing.load(Ordering::Acquire) {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "journal owner already open",
                        ));
                    }
                    if *previous.done.lock().unwrap() {
                        writers.remove(&root);
                        continue;
                    }
                    previous
                } else {
                    let life = Arc::new(Self::default());
                    writers.insert(root.clone(), Arc::downgrade(&life));
                    return Ok(life);
                }
            };
            previous.wait();
        }
    }
    pub fn close(&self) {
        self.closing.store(true, Ordering::Release);
    }
    fn wait(&self) {
        let mut done = self.done.lock().unwrap();
        while !*done {
            done = self.changed.wait(done).unwrap();
        }
    }
}
