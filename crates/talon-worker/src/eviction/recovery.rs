//! Startup-only construction. Build independent policy indexes without taking
//! the live policy mutex for each unit; sorted queues are collected in bulk.
use super::*;

pub(crate) struct RecoveredUnit {
    pub independent: bool,
    pub unit: SharedUnit,
    pub bytes: u64,
    pub access: AccessHandle,
}

impl Lru {
    /// The input contains each resident unit exactly once. Sibling pages must
    /// share their idle clock, just as they do after ordinary admission.
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
        assert!(inner.queue.is_empty());
        assert!(inner.block_queue.is_empty());
        assert!(inner.ownership_queue.is_empty());
        assert_eq!(inner.versions.capacity(), 0);
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
        let unit_index = || {
            let started = std::time::Instant::now();
            for (position, u) in units.iter().enumerate() {
                index.policy_insert(
                    &u.unit,
                    PolicyEntry {
                        state: PolicyState {
                            independent: u.independent,
                            bytes: u.bytes,
                            position: position as u64,
                            revision: position as u64,
                            pins: 0,
                        },
                        access: u.access.clone(),
                    },
                );
            }
            let queue = units
                .iter()
                .enumerate()
                .map(|(position, u)| (position as u64, u.unit.clone()))
                .collect();
            tracing::info!(
                units = units.len(),
                workers,
                phase = "membership",
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "cache policy index restored"
            );
            queue
        };
        let version_index = || {
            let started = std::time::Instant::now();
            let mut versions = VersionIndex::with_capacity(
                units.iter().filter(|u| !u.independent).count().min(blocks),
            );
            for u in &units {
                if !u.independent {
                    versions.insert(u.unit.block.clone());
                }
            }
            tracing::info!(
                units = units.len(),
                workers,
                phase = "versions",
                version_blocks = versions.len(),
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "cache policy index restored"
            );
            versions
        };
        let block_index = || {
            if !track_blocks {
                return (BTreeMap::new(), BTreeMap::new());
            }
            let started = std::time::Instant::now();
            let mut queue = Vec::with_capacity(units.len().min(blocks));
            for (position, u) in units.iter().enumerate() {
                let position = position as u64;
                if index
                    .policy_block_update(u.unit.block(), |activity| {
                        activity.revision = position;
                    })
                    .is_none()
                {
                    let key = index.policy_block_insert(
                        u.unit.block.clone(),
                        BlockPolicy {
                            pins: 0,
                            position,
                            revision: position,
                            ownership_position: AtomicU64::new(position),
                            ownership_pending: AtomicBool::new(true),
                            access: u.access.clock().clone(),
                        },
                    );
                    queue.push((position, key));
                }
            }
            let ownership = queue.iter().cloned().collect();
            let queue = queue.into_iter().collect();
            tracing::info!(
                units = units.len(),
                workers,
                phase = "blocks",
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "cache policy index restored"
            );
            (queue, ownership)
        };
        // Version construction runs beside membership recovery. Both unit and
        // block state now share the directory, so update them on this thread.
        let (queue, versions, (block_queue, ownership_queue)) = std::thread::scope(|scope| {
            let version_task = (workers > 1).then(|| scope.spawn(version_index));
            let units = unit_index();
            let blocks = block_index();
            let versions = match version_task {
                Some(task) => task.join().expect("version recovery panicked"),
                None => version_index(),
            };
            (units, versions, blocks)
        });
        let total_bytes = units
            .iter()
            .fold(0u64, |total, u| total.saturating_add(u.bytes));
        let clock = units.len() as u64;
        let lru = Self {
            inner: Mutex::new(Inner {
                index,
                queue,
                versions,
                block_queue,
                ownership_queue,
                track_blocks,
                track_small_clock: track_blocks,
                total_bytes,
                clock,
                admission_ceiling: clock,
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
            units = clock,
            workers,
            temporary_drop_ms = drop_started.elapsed().as_secs_f64() * 1000.0,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
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
        let g = lru.inner.lock().unwrap();
        assert!(g.block_queue.is_empty());
        assert!(g.ownership_queue.is_empty());
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
                assert!(b.versions.is_empty());
                assert_eq!(b.versions.capacity(), 0);
                assert_eq!(a.total_bytes, b.total_bytes);
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
                assert_eq!(a.queue, b.queue);
                assert_eq!(a.block_queue, b.block_queue);
                assert_eq!(a.ownership_queue, b.ownership_queue);
                assert_eq!(a.clock, b.clock);
                assert_eq!(a.admission_ceiling, b.admission_ceiling);
                assert_eq!(a.total_bytes, b.total_bytes);
                assert!(a.versions == b.versions);
                for id in a.block_queue.values() {
                    let activity = a.index.policy_block_get(id).unwrap();
                    let restored = b.index.policy_block_get(id).unwrap();
                    assert_eq!(
                        (
                            activity.units,
                            activity.pins,
                            activity.position,
                            activity.revision,
                            activity.ownership_position,
                            activity.ownership_pending
                        ),
                        (
                            restored.units,
                            restored.pins,
                            restored.position,
                            restored.revision,
                            restored.ownership_position,
                            restored.ownership_pending
                        )
                    );
                }
                for unit in a.queue.values() {
                    let entry = a.index.policy_get(unit).unwrap();
                    let restored = b.index.policy_get(unit).unwrap();
                    assert_eq!(
                        (entry.bytes, entry.position, entry.revision, entry.pins),
                        (
                            restored.bytes,
                            restored.position,
                            restored.revision,
                            restored.pins
                        )
                    );
                    assert!(Arc::ptr_eq(
                        restored.access.clock(),
                        &b.index.policy_block_get(unit.block()).unwrap().access
                    ));
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
                let g = lru.inner.lock().unwrap();
                assert!(
                    g.versions.is_empty()
                        && g.block_queue.is_empty()
                        && g.ownership_queue.is_empty()
                );
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
            let identity = {
                let inner = lru.inner.lock().unwrap();
                let activity = inner.index.policy_block_get(&id).unwrap();
                let key = &inner.block_queue[&activity.position];
                assert!(BlockKey::ptr_eq(
                    key,
                    &inner.ownership_queue[&activity.ownership_position]
                ));
                key.downgrade()
            };
            let limit = lru.ownership_admission_ceiling();
            let candidate = lru.next_ownership_block(&mut (0, limit), limit).unwrap().1;
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
