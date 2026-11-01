//! Startup-only construction. Build independent policy indexes without taking
//! the live policy mutex for each unit; sorted queues are collected in bulk.
use super::*;

pub(crate) struct RecoveredUnit {
    pub independent: bool,
    pub unit: Arc<CacheUnit>,
    pub bytes: u64,
    pub access: AccessHandle,
}

impl Lru {
    /// The input contains each resident unit exactly once. Sibling pages must
    /// share their idle clock, just as they do after ordinary admission.
    pub(crate) fn from_recovered(
        units: Vec<RecoveredUnit>,
        blocks: usize,
        track_checks: bool,
    ) -> Self {
        let workers = if units.len() < 4096 {
            1
        } else {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(3)
        };
        Self::recover_with_workers(units, blocks, workers, track_checks)
    }

    fn recover_with_workers(
        units: Vec<RecoveredUnit>,
        blocks: usize,
        workers: usize,
        track_checks: bool,
    ) -> Self {
        let started = std::time::Instant::now();
        let unit_index = || {
            let started = std::time::Instant::now();
            let entries = units
                .iter()
                .enumerate()
                .map(|(position, u)| {
                    (
                        u.unit.clone(),
                        Entry {
                            independent: u.independent,
                            bytes: u.bytes,
                            position: position as u64,
                            revision: position as u64,
                            access: u.access.clone(),
                            pins: 0,
                        },
                    )
                })
                .collect();
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
            (entries, queue)
        };
        let version_index = || {
            let started = std::time::Instant::now();
            let mut versions: HashMap<LogicalBlock, HashMap<Version, HashSet<Option<PageIndex>>>> =
                HashMap::with_capacity(units.iter().filter(|u| !u.independent).count().min(blocks));
            let mut checks = HashMap::with_capacity(if track_checks { blocks } else { 0 });
            for u in &units {
                let block = u.unit.block();
                if !u.independent {
                    versions
                        .entry(LogicalBlock::from(block))
                        .or_default()
                        .entry(block.version.clone())
                        .or_default()
                        .insert(u.unit.page());
                }
                if track_checks {
                    checks.entry(block.object.clone()).or_insert((0, None)).0 += 1;
                }
            }
            tracing::info!(
                units = units.len(),
                workers,
                phase = "versions",
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "cache policy index restored"
            );
            (versions, checks)
        };
        let block_index = || {
            let started = std::time::Instant::now();
            let mut access: HashMap<BlockKey, BlockActivity> = HashMap::with_capacity(blocks);
            let mut queue = Vec::with_capacity(blocks);
            for (position, u) in units.iter().enumerate() {
                let position = position as u64;
                let key = BlockKey(u.unit.clone());
                let activity = access.entry(key.clone()).or_insert_with(|| {
                    queue.push((position, key));
                    BlockActivity {
                        units: 0,
                        pins: 0,
                        position,
                        revision: position,
                        ownership_position: position,
                        ownership_pending: true,
                        access: u.access.1.clone(),
                    }
                });
                activity.units += 1;
                activity.revision = position;
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
            (access, queue, ownership)
        };
        // At most three CPU workers, including this thread. Small namespaces
        // avoid thread creation. No map is shared mutably during construction.
        let (
            (entries, queue),
            (versions, object_checks),
            (block_access, block_queue, ownership_queue),
        ) = std::thread::scope(|scope| {
            let block_task = (workers > 1).then(|| scope.spawn(block_index));
            let version_task = (workers > 2).then(|| scope.spawn(version_index));
            let units = unit_index();
            let versions = match version_task {
                Some(task) => task.join().expect("version recovery panicked"),
                None => version_index(),
            };
            let blocks = match block_task {
                Some(task) => task.join().expect("block recovery panicked"),
                None => block_index(),
            };
            (units, versions, blocks)
        });
        let total_bytes = units
            .iter()
            .fold(0u64, |total, u| total.saturating_add(u.bytes));
        let clock = units.len() as u64;
        let lru = Self {
            inner: Mutex::new(Inner {
                entries,
                queue,
                versions,
                block_access,
                block_queue,
                ownership_queue,
                object_checks,
                track_checks,
                total_bytes,
                clock,
                admission_ceiling: clock,
            }),
            eviction: Mutex::new(()),
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

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(talon_core::Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
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
                        unit: Arc::new(unit.clone()),
                        bytes: *bytes,
                        access: access.sibling(),
                    }
                })
                .collect();
            let recovered = Lru::recover_with_workers(units, 3, workers, true);
            {
                let a = ordinary.inner.lock().unwrap();
                let b = recovered.inner.lock().unwrap();
                assert_eq!(a.queue, b.queue);
                assert_eq!(a.block_queue, b.block_queue);
                assert_eq!(a.ownership_queue, b.ownership_queue);
                assert_eq!(a.object_checks, b.object_checks);
                assert_eq!(a.clock, b.clock);
                assert_eq!(a.admission_ceiling, b.admission_ceiling);
                assert_eq!(a.total_bytes, b.total_bytes);
                assert!(a.versions == b.versions);
                for (id, activity) in &a.block_access {
                    let restored = &b.block_access[id];
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
                for (unit, entry) in &a.entries {
                    let restored = &b.entries[unit];
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
                        &restored.access.1,
                        &b.block_access[unit.block()].access
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
                        && g.object_checks.is_empty()
                );
            }
        }
    }

    #[test]
    fn empty_recovery_accepts_first_admission() {
        let lru = Lru::from_recovered(Vec::new(), 0, true);
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
                            unit: Arc::new(first.clone()),
                            bytes: 4096,
                            access: shared.clone(),
                        },
                        RecoveredUnit {
                            independent: false,
                            unit: Arc::new(second.clone()),
                            bytes: 8192,
                            access: shared.sibling(),
                        },
                    ],
                    1,
                    2,
                    true,
                )
            } else {
                let lru = Lru::new();
                lru.insert(first.clone(), 4096);
                lru.insert(second.clone(), 8192);
                lru
            };
            let identity = {
                let inner = lru.inner.lock().unwrap();
                let (key, activity) = inner.block_access.get_key_value(&id).unwrap();
                let unit = inner.entries.get_key_value(&first).unwrap().0;
                assert!(Arc::ptr_eq(&key.0, unit));
                assert!(Arc::ptr_eq(
                    &key.0,
                    &inner.block_queue[&activity.position].0
                ));
                assert!(Arc::ptr_eq(
                    &key.0,
                    &inner.ownership_queue[&activity.ownership_position].0
                ));
                Arc::downgrade(&key.0)
            };
            let limit = lru.ownership_admission_ceiling();
            let candidate = lru.next_ownership_block(&mut (0, limit), limit).unwrap().1;
            assert_eq!(candidate.units, 2);
            assert_eq!(lru.remove(&first), Some(4096));
            assert_eq!(lru.inner.lock().unwrap().block_access[&id].units, 1);
            assert!(identity.upgrade().is_some());
            assert!(lru.pin(&second));
            assert!(lru.evict_to_fit(0).is_empty());
            lru.unpin(&second);
            // A later admission requeues ownership using the retained identity,
            // without restoring residency of the original page.
            lru.insert(CacheUnit::Page(id.clone(), PageIndex(31)), 16384);
            assert!(!lru.inner.lock().unwrap().entries.contains_key(&first));
            lru.remove_block(&id);
            assert_eq!(lru.total_bytes(), 0);
            assert!(identity.upgrade().is_none());
            lru.insert(first, 7);
            assert_eq!(lru.total_bytes(), 7);
            assert!(!lru.stale_candidate_is_current(&candidate));
        }
    }

    #[test]
    fn borrowed_block_keys_hash_like_the_complete_block_identity() {
        let id = block(12);
        let first = BlockKey(Arc::new(CacheUnit::Whole(id.clone())));
        let second = BlockKey(Arc::new(CacheUnit::Page(id.clone(), PageIndex(9))));
        assert_eq!(first, second);
        let map = HashMap::from([(first, 1)]);
        assert_eq!(map.get(&id), Some(&1));
        assert_eq!(map.get(&second), Some(&1));
        let mut changed = id.clone();
        changed.version = Version::new("v2");
        assert!(!map.contains_key(&changed));
        let mut changed = id.clone();
        changed.offset += 1;
        assert!(!map.contains_key(&changed));
        let mut changed = id;
        changed.block_size /= 2;
        assert!(!map.contains_key(&changed));
    }
}
