//! Startup-only construction before publishing the policy.
use super::*;

pub(crate) struct RecoveredUnit {
    pub independent: bool,
    pub unit: SharedUnit,
    pub bytes: u64,
    pub access: AccessHandle,
}

impl Lru {
    pub(crate) fn from_runtime(
        index: Arc<crate::BlockIndex>,
        units: Vec<RecoveredUnit>,
        blocks: usize,
        track_blocks: bool,
    ) -> Self {
        index.enable_compact_policy(track_blocks);
        Self::from_recovered(index, units, blocks, track_blocks)
    }

    /// Restore explicit tokens for Small Store's legacy policy. Standalone
    /// policy tests also exercise Whole and Page units through this path.
    pub(crate) fn from_recovered(
        index: Arc<crate::BlockIndex>,
        units: Vec<RecoveredUnit>,
        blocks: usize,
        track_blocks: bool,
    ) -> Self {
        let workers = if units.len() < 4096 {
            1
        } else {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(2)
        };
        Self::recover(index, units, blocks, workers, track_blocks)
    }

    #[cfg(test)]
    pub(crate) fn assert_empty_ordinary_policy(&self) {
        let inner = self.inner.lock().unwrap();
        assert_eq!(inner.legacy_units, 0);
        assert_eq!(inner.index.compact.totals().1, 0);
        assert_eq!(inner.index.policy_scan_ceiling(), 0);
    }

    #[cfg(test)]
    fn recover_with_workers(units: Vec<RecoveredUnit>, blocks: usize, workers: usize) -> Self {
        Self::recover(
            Arc::new(crate::BlockIndex::new()),
            units,
            blocks,
            workers,
            true,
        )
    }

    fn recover(
        index: Arc<crate::BlockIndex>,
        units: Vec<RecoveredUnit>,
        blocks: usize,
        workers: usize,
        track_blocks: bool,
    ) -> Self {
        let started = std::time::Instant::now();
        for unit in &units {
            let revision = index.compact.next_revision();
            index.compact.admitted_at(revision);
            index.policy_insert(
                &unit.unit,
                PolicyEntry {
                    state: PolicyState {
                        independent: unit.independent,
                        bytes: unit.bytes,
                        revision,
                        pins: 0,
                    },
                    access: unit.access.clone(),
                },
            );
            if track_blocks
                && index
                    .policy_block_update(unit.unit.block(), |activity| {
                        activity.revision = revision;
                    })
                    .is_none()
            {
                index.policy_block_insert(
                    unit.unit.block.clone(),
                    LegacyBlockActivity {
                        pins: 0,
                        revision,
                        access: Some(unit.access.clock().clone()),
                    },
                );
            }
        }
        let total_bytes = units
            .iter()
            .fold(0u64, |total, u| total.saturating_add(u.bytes));
        let clock = units.len() as u64;
        let lru = Self {
            inner: Mutex::new(Inner {
                index,
                legacy_units: units.len(),
                capacity_hand: (0, 0),
                track_blocks,
                track_small_clock: track_blocks,
                legacy_bytes: total_bytes,
                small_cursor: 0,
                small_first: false,
            }),
            eviction: Mutex::new(()),
            #[cfg(test)]
            before_small_publish: Mutex::default(),
        };
        let drop_started = std::time::Instant::now();
        drop(units);
        tracing::info!(
            blocks,
            legacy_units = clock,
            resident_units = lru.inner.lock().unwrap().index.compact.totals().1,
            workers,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            temporary_drop_ms = drop_started.elapsed().as_secs_f64() * 1000.0,
            "cache policy indexes completed"
        );
        lru
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use talon_core::ObjectId;

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(talon_core::Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    #[test]
    fn recovery_without_block_tracking_allocates_only_capacity_state() {
        let index = Arc::new(crate::BlockIndex::new());
        let id = block(0);
        index.init_paged(id.clone(), 1 << 20, 1 << 20);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let units = index.restore_policy_units(false);
        assert!(units.iter().all(|unit| !unit.access.tracks_clock()));
        let lru = Lru::from_recovered(index.clone(), units, 1, false);
        assert!(index.policy_block_get(&id).is_none());
        assert_eq!(lru.total_bytes(), 4096);
        assert!(index
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_some());
        let unit = CacheUnit::Page(id, PageIndex(0));
        let access = lru.insert(unit.clone(), 8192);
        assert!(!access.tracks_clock());
        assert_eq!(lru.evict_to_fit(0), [unit]);
        assert!(lru.is_empty());
    }

    #[test]
    fn bulk_independent_keys_match_admission() {
        for workers in 1..=3 {
            let first = block(10);
            let mut second = first.clone();
            second.version = Version::new("v2");
            let ordinary = Lru::new();
            let units = [&first, &second]
                .into_iter()
                .map(|id| {
                    let unit = CacheUnit::Whole(id.clone());
                    let access = ordinary.insert_with_policy(unit.clone(), 100, true);
                    RecoveredUnit {
                        independent: true,
                        unit: unit.into(),
                        bytes: 100,
                        access,
                    }
                })
                .collect();
            let recovered = Lru::recover_with_workers(units, 2, workers);
            {
                let a = ordinary.inner.lock().unwrap();
                let b = recovered.inner.lock().unwrap();
                assert_eq!(b.index.policy_versioned_count(&first), 0);
                assert_eq!(b.index.policy_versioned_count(&second), 0);
                assert_eq!(a.legacy_bytes, b.legacy_bytes);
            }
            assert!(recovered.superseded_candidates(&second).is_empty());
            assert_eq!(recovered.block_candidates(&first).len(), 1);
            recovered.remove_block(&first);
            assert_eq!(recovered.block_candidates(&second).len(), 1);
            recovered.remove_block(&second);
            assert!(recovered.is_empty());
        }
    }

    #[test]
    fn bulk_recovery_matches_admission_and_supports_later_mutations() {
        let old = block(1);
        let mut new = old.clone();
        new.version = Version::new("new");
        let input = [
            (CacheUnit::Page(old.clone(), PageIndex(0)), 0),
            (CacheUnit::Whole(block(2)), 19),
            (CacheUnit::Page(old.clone(), PageIndex(255)), 4096),
            (CacheUnit::Whole(new.clone()), 31),
        ];
        for workers in 1..=3 {
            let ordinary = Lru::new();
            let mut shared = HashMap::new();
            let units = input
                .iter()
                .map(|(unit, bytes)| {
                    ordinary.insert(unit.clone(), *bytes);
                    let access = shared
                        .entry(unit.block().clone())
                        .or_insert_with(AccessHandle::default);
                    RecoveredUnit {
                        independent: false,
                        unit: unit.clone().into(),
                        bytes: *bytes,
                        access: access.sibling(),
                    }
                })
                .collect();
            let recovered = Lru::recover_with_workers(units, 3, workers);
            {
                let a = ordinary.inner.lock().unwrap();
                let b = recovered.inner.lock().unwrap();
                assert_eq!(a.legacy_units, b.legacy_units);
                assert_eq!(
                    a.index.compact.admission_ceiling(),
                    b.index.compact.admission_ceiling()
                );
                assert_eq!(a.legacy_bytes, b.legacy_bytes);
                for (unit, _) in &input {
                    let entry = a.index.policy_get(unit).unwrap();
                    let restored = b.index.policy_get(unit).unwrap();
                    assert_eq!(
                        (entry.bytes, entry.revision, entry.pins),
                        (restored.bytes, restored.revision, restored.pins)
                    );
                    let activity = b.index.policy_block_get(unit.block()).unwrap();
                    assert_eq!(restored.access.last_access(), activity.last_access);
                }
            }
            for lru in [&ordinary, &recovered] {
                assert!(lru.pin(&input[0].0));
                assert_eq!(lru.evict_superseded(&new), vec![input[2].0.clone()]);
                lru.unpin(&input[0].0);
                let candidate = lru.block_candidates(&old).pop().unwrap();
                lru.touch(&input[0].0);
                assert!(!lru.candidate_is_current(&candidate));
                lru.remove_block(&old);
                lru.insert(input[0].0.clone(), 7);
                assert_eq!(lru.total_bytes(), 57);
                assert_eq!(lru.len(), 3);
                assert_eq!(lru.evict_to_fit(0).len(), 3);
                assert_eq!(lru.total_bytes(), 0);
                assert!(lru.is_empty());
            }
        }
    }

    #[test]
    fn empty_recovery_accepts_first_admission() {
        let lru = Lru::from_recovered(Arc::new(crate::BlockIndex::new()), Vec::new(), 0, true);
        let unit = CacheUnit::Whole(block(1));
        lru.insert(unit.clone(), 10);
        assert_eq!(lru.evict_to_fit(0), vec![unit]);
    }

    #[test]
    fn shared_block_identity_survives_its_page_and_retires_with_last_sibling() {
        for recover in [false, true] {
            let id = block(11);
            let first = CacheUnit::Page(id.clone(), PageIndex(7));
            let second = CacheUnit::Page(id.clone(), PageIndex(19));
            let lru = if recover {
                let shared = AccessHandle::default();
                Lru::recover_with_workers(
                    vec![
                        RecoveredUnit {
                            independent: false,
                            unit: first.clone().into(),
                            bytes: 4096,
                            access: shared.clone(),
                        },
                        RecoveredUnit {
                            independent: false,
                            unit: second.clone().into(),
                            bytes: 8192,
                            access: shared.sibling(),
                        },
                    ],
                    1,
                    2,
                )
            } else {
                let lru = Lru::new();
                lru.insert(first.clone(), 4096);
                lru.insert(second.clone(), 8192);
                lru
            };
            let identity = lru.index().shared_identity(&id).unwrap().downgrade();
            let limit = lru.ownership_admission_ceiling();
            let candidate = lru.first_ownership_candidate(0..limit).unwrap();
            assert_eq!(candidate.units, 2);
            assert_eq!(lru.remove(&first), Some(4096));
            assert_eq!(
                lru.inner
                    .lock()
                    .unwrap()
                    .index
                    .policy_block_get(&id)
                    .unwrap()
                    .units,
                1
            );
            assert!(identity());
            assert!(lru.pin(&second));
            assert!(lru.evict_to_fit(0).is_empty());
            lru.unpin(&second);
            // A later admission requeues ownership using the retained identity,
            // without restoring residency of the original page.
            lru.insert(CacheUnit::Page(id.clone(), PageIndex(31)), 16384);
            assert!(lru.inner.lock().unwrap().index.policy_get(&first).is_none());
            lru.remove_block(&id);
            assert_eq!(lru.total_bytes(), 0);
            assert!(!identity());
            lru.insert(first, 7);
            assert_eq!(lru.total_bytes(), 7);
            assert!(!lru.stale_candidate_is_current(&candidate));
        }
    }
}
