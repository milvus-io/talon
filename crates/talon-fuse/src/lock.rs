//! Poison-tolerant lock accessors.
//!
//! Every piece of shared state in this crate is a plain data structure behind a
//! `Mutex` or `RwLock` — a namespace map, an idle-connection bucket, a placement
//! cache. None of them has a multi-step invariant that a panic could leave
//! half-applied, so Rust's default poisoning behaviour buys nothing here and
//! costs a great deal: `lock().unwrap()` turns *one* panic anywhere under the
//! lock into a permanent, unrecoverable failure of every later access.
//!
//! On a FUSE mount that failure mode is severe. The namespace lives behind a
//! single `Mutex`, so a poisoned lock means every subsequent `lookup`,
//! `getattr`, `read` and `release` panics too — the mount hangs rather than
//! returning an error, and it cannot be unmounted cleanly. A localized bug
//! becomes a wedged filesystem.
//!
//! These extension traits recover the guard instead
//! ([`PoisonError::into_inner`]), so a panic degrades to "possibly stale data in
//! one entry" rather than "the mount is dead". Prefer them over
//! `lock().unwrap()` throughout the crate.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Poison-tolerant access to a [`Mutex`].
pub(crate) trait MutexExt<T> {
    /// Lock, recovering the guard if the mutex was poisoned by a panic.
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Poison-tolerant access to an [`RwLock`].
pub(crate) trait RwLockExt<T> {
    /// Read-lock, recovering the guard if the lock was poisoned by a panic.
    fn read_recover(&self) -> RwLockReadGuard<'_, T>;
    /// Write-lock, recovering the guard if the lock was poisoned by a panic.
    fn write_recover(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_recover(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_recover(&self) -> RwLockWriteGuard<'_, T> {
        self.write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The whole point: a panic under the lock must not brick every later
    /// access. With `lock().unwrap()` the second access panics too.
    #[test]
    fn mutex_survives_a_panic_under_the_lock() {
        let m = Arc::new(Mutex::new(vec![1u32, 2, 3]));
        let m2 = Arc::clone(&m);
        let panicked = std::thread::spawn(move || {
            let mut g = m2.lock_recover();
            g.push(4);
            panic!("boom while holding the lock");
        })
        .join();
        assert!(panicked.is_err(), "the thread must actually have panicked");
        assert!(m.is_poisoned(), "and the mutex must actually be poisoned");

        // Still usable, and the pre-panic mutation is visible.
        let mut g = m.lock_recover();
        assert_eq!(*g, vec![1, 2, 3, 4]);
        g.push(5);
        assert_eq!(g.len(), 5);
    }

    #[test]
    fn rwlock_survives_a_panic_under_the_write_lock() {
        let l = Arc::new(RwLock::new(String::from("a")));
        let l2 = Arc::clone(&l);
        let panicked = std::thread::spawn(move || {
            let mut g = l2.write_recover();
            g.push('b');
            panic!("boom");
        })
        .join();
        assert!(panicked.is_err());
        assert!(l.is_poisoned());

        assert_eq!(&*l.read_recover(), "ab");
        l.write_recover().push('c');
        assert_eq!(&*l.read_recover(), "abc");
    }
}
