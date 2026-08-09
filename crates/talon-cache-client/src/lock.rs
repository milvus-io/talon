//! Poison-tolerant lock accessors for client caches and connection pools.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub(crate) trait MutexExt<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(crate) trait RwLockExt<T> {
    fn read_recover(&self) -> RwLockReadGuard<'_, T>;
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

    #[test]
    fn mutex_recovers_after_a_panic() {
        let value = Arc::new(Mutex::new(vec![1]));
        let panicking = Arc::clone(&value);
        assert!(std::thread::spawn(move || {
            panicking.lock_recover().push(2);
            panic!("poison mutex");
        })
        .join()
        .is_err());

        value.lock_recover().push(3);
        assert_eq!(*value.lock_recover(), vec![1, 2, 3]);
    }

    #[test]
    fn rwlock_recovers_after_a_panic() {
        let value = Arc::new(RwLock::new(String::from("a")));
        let panicking = Arc::clone(&value);
        assert!(std::thread::spawn(move || {
            panicking.write_recover().push('b');
            panic!("poison rwlock");
        })
        .join()
        .is_err());

        value.write_recover().push('c');
        assert_eq!(&*value.read_recover(), "abc");
    }
}
