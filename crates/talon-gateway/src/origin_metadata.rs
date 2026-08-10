//! Bounded process-local metadata used to address versioned cached blocks.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use talon_core::{ObjectId, Version};

/// Origin response metadata needed to reconstruct a cached object response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginObjectMetadata {
    pub version: Version,
    pub size: u64,
    /// Allowlisted origin response headers only. Request credentials never enter this value.
    pub response_headers: Vec<(String, String)>,
}

#[derive(Clone)]
struct Entry {
    metadata: OriginObjectMetadata,
    expires_at: Instant,
    touched: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<ObjectId, Entry>,
    clock: u64,
}

/// Capacity- and TTL-bounded metadata retained only for this gateway process.
pub struct OriginMetadataIndex {
    capacity: usize,
    ttl: Duration,
    state: Mutex<State>,
}

impl OriginMetadataIndex {
    pub fn new(capacity: usize, ttl: Duration) -> Result<Self, &'static str> {
        if capacity == 0 || ttl.is_zero() {
            return Err("origin metadata capacity and TTL must be greater than zero");
        }
        Ok(Self {
            capacity,
            ttl,
            state: Mutex::new(State::default()),
        })
    }

    pub fn get(&self, object: &ObjectId, now: Instant) -> Option<OriginObjectMetadata> {
        let mut state = self.state.lock().unwrap();
        state.entries.retain(|_, entry| entry.expires_at > now);
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        let entry = state.entries.get_mut(object)?;
        entry.touched = touched;
        Some(entry.metadata.clone())
    }

    pub fn insert(&self, object: ObjectId, metadata: OriginObjectMetadata, now: Instant) {
        let mut state = self.state.lock().unwrap();
        state.entries.retain(|_, entry| entry.expires_at > now);
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        state.entries.insert(
            object,
            Entry {
                metadata,
                expires_at: now + self.ttl,
                touched,
            },
        );
        while state.entries.len() > self.capacity {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(object, _)| object.clone());
            if let Some(oldest) = oldest {
                state.entries.remove(&oldest);
            }
        }
    }

    pub fn invalidate(&self, object: &ObjectId) -> bool {
        self.state.lock().unwrap().entries.remove(object).is_some()
    }

    pub fn len(&self, now: Instant) -> usize {
        let mut state = self.state.lock().unwrap();
        state.entries.retain(|_, entry| entry.expires_at > now);
        state.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    fn object(key: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", key)
    }

    fn metadata(version: &str) -> OriginObjectMetadata {
        OriginObjectMetadata {
            version: Version::new(version),
            size: 42,
            response_headers: vec![("content-type".into(), "application/octet-stream".into())],
        }
    }

    #[test]
    fn entries_expire_and_mutations_invalidate() {
        let index = OriginMetadataIndex::new(2, Duration::from_secs(10)).unwrap();
        let now = Instant::now();
        index.insert(object("a"), metadata("v1"), now);
        assert_eq!(
            index.get(&object("a"), now).unwrap().version,
            Version::new("v1")
        );
        assert_eq!(index.len(now + Duration::from_secs(10)), 0);
        index.insert(object("a"), metadata("v2"), now);
        assert!(index.invalidate(&object("a")));
        assert!(index.get(&object("a"), now).is_none());
    }

    #[test]
    fn capacity_evicts_the_least_recently_used_entry() {
        let index = OriginMetadataIndex::new(2, Duration::from_secs(10)).unwrap();
        let now = Instant::now();
        index.insert(object("a"), metadata("a"), now);
        index.insert(object("b"), metadata("b"), now);
        assert!(index.get(&object("a"), now).is_some());
        index.insert(object("c"), metadata("c"), now);
        assert!(index.get(&object("a"), now).is_some());
        assert!(index.get(&object("b"), now).is_none());
        assert!(index.get(&object("c"), now).is_some());
    }

    #[test]
    fn zero_bounds_are_rejected() {
        assert!(OriginMetadataIndex::new(0, Duration::from_secs(1)).is_err());
        assert!(OriginMetadataIndex::new(1, Duration::ZERO).is_err());
    }
}
