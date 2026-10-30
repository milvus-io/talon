//! Startup-only construction. Build independent policy indexes without taking
//! the live policy mutex for each unit; sorted queues are collected in bulk.
use super::*;

pub(crate) struct RecoveredUnit {
    pub unit: Arc<CacheUnit>,
    pub bytes: u64,
    pub access: AccessHandle,
}

impl Lru {
    /// The input contains each resident unit exactly once. Sibling pages must
    /// share their idle clock, just as they do after ordinary admission.
    pub(crate) fn from_recovered(units: Vec<RecoveredUnit>, blocks: usize) -> Self {
        let workers = if units.len() < 4096 {
            1
        } else {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(3)
        };
        Self::recover_with_workers(units, blocks, workers)
    }

    fn recover_with_workers(units: Vec<RecoveredUnit>, blocks: usize, workers: usize) -> Self {
        let unit_index = || {
            let entries = units
                .iter()
                .enumerate()
                .map(|(position, u)| {
                    (
                        u.unit.clone(),
                        Entry {
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
            (entries, queue)
        };
        let version_index = || {
            let mut versions: HashMap<LogicalBlock, HashMap<Version, HashSet<Option<PageIndex>>>> =
                HashMap::with_capacity(blocks);
            let mut checks = HashMap::with_capacity(blocks);
            for u in &units {
                let block = u.unit.block();
                versions
                    .entry(LogicalBlock::from(block))
                    .or_default()
                    .entry(block.version.clone())
                    .or_default()
                    .insert(u.unit.page());
                checks.entry(block.object.clone()).or_insert((0, None)).0 += 1;
            }
            (versions, checks)
        };
        let block_index = || {
            let mut access: HashMap<BlockId, BlockActivity> = HashMap::with_capacity(blocks);
            let mut queue = Vec::with_capacity(blocks);
            for (position, u) in units.iter().enumerate() {
                let position = position as u64;
                let block = u.unit.block();
                let activity = access.entry(block.clone()).or_insert_with(|| {
                    queue.push((position, block.clone()));
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
            (access, queue.into_iter().collect(), ownership)
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
        Self {
            inner: Mutex::new(Inner {
                entries,
                queue,
                versions,
                block_access,
                block_queue,
                ownership_queue,
                object_checks,
                total_bytes,
                clock,
                admission_ceiling: clock,
            }),
            eviction: Mutex::new(()),
        }
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
        let input = vec![
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
                        unit: Arc::new(unit.clone()),
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
        let lru = Lru::from_recovered(Vec::new(), 0);
        let unit = CacheUnit::Whole(block(1));
        lru.insert(unit.clone(), 10);
        assert_eq!(lru.evict_to_fit(0), vec![unit]);
    }
}
