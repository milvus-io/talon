//! Reclaim cold cache blocks after authoritative backend NotFound.
use super::WorkerRuntime;
use crate::eviction::{access_millis, StaleBlockCandidate};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use talon_core::Error;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

pub(super) struct StaleGc;
impl StaleGc {
    pub(super) async fn run_round(worker: &WorkerRuntime) {
        Self::run_round_at(worker, access_millis()).await;
    }

    async fn run_round_at(worker: &WorkerRuntime, now: u64) {
        let mut cursor = (0, worker.lru.stale_scan_ceiling());
        let mut pending = VecDeque::new();
        let limits = (
            worker.page_gc_config.scan_batch_size.min(4096),
            worker.page_gc_config.delete_batch_size,
        );
        while cursor.0 < cursor.1 || !pending.is_empty() {
            if pending.is_empty() {
                let lru = worker.lru.clone();
                let scanned = tokio::task::spawn_blocking(move || {
                    let candidates = lru.scan_stale_blocks(&mut cursor, now, DAY_MS, limits, false);
                    (cursor, candidates)
                })
                .await
                .expect("stale GC scan panicked");
                cursor = scanned.0;
                pending = scanned.1.into();
            }
            Self::reclaim_backend_deleted(worker, &mut pending, now).await;
            if cursor.0 < cursor.1 || !pending.is_empty() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    /// Drain at most one backend HEAD batch; callers pace any remaining work.
    async fn reclaim_backend_deleted(
        worker: &WorkerRuntime,
        pending: &mut VecDeque<StaleBlockCandidate>,
        now: u64,
    ) {
        // Backend work is bounded independently of local disk pacing. Warm blocks
        // and successful checks within 24 hours never issue a HEAD.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut checked = HashMap::new();
        while let Some(candidate) = pending.front() {
            if now.saturating_sub(candidate.last_access) <= DAY_MS {
                pending.pop_front();
                continue;
            }
            let object = &candidate.block.object;
            if !checked.contains_key(object)
                && (checked.len() >= MAX_HEADS || tokio::time::Instant::now() >= deadline)
            {
                break;
            }
            let candidate = pending.pop_front().expect("pending candidate");
            if !worker.lru.stale_candidate_is_current(&candidate) {
                continue;
            }
            let object = &candidate.block.object;
            if !worker.lru.stale_check_due(object, now, DAY_MS) {
                continue;
            }
            let missing = if let Some(missing) = checked.get(object) {
                *missing
            } else {
                let missing =
                    match tokio::time::timeout_at(deadline, worker.backend.head(object)).await {
                        Ok(Ok(_)) => {
                            worker.lru.stale_verified(object, now.max(access_millis()));
                            false
                        }
                        Ok(Err(Error::NotFound(_))) => true,
                        Ok(Err(error)) => {
                            tracing::debug!(%error, "stale GC HEAD failed; retaining cache");
                            false
                        }
                        Err(_) => break,
                    };
                checked.insert(object.clone(), missing);
                missing
            };
            if missing {
                worker.unlink_stale_block(candidate).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::gc_test_support::*;
    use super::*;
    use crate::CacheUnit;
    use bytes::Bytes;
    use std::sync::{atomic::Ordering, Arc};
    use talon_core::{Backend, ObjectId, PageIndex};
    #[tokio::test]
    async fn stale_gc_filters_warm_entries_then_removes_both_cache_forms() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            StaleGc::run_round_at(&r, access_millis()).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            StaleGc::run_round_at(&r, cold_now()).await;
            assert_eq!(
                origin.calls.load(Ordering::Relaxed),
                1,
                "one HEAD per object per batch"
            );
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(r.lru.total_bytes(), 0);
            assert!(r.index.get(&block(0)).is_none());
            assert!(r.index.get(&block(1)).is_none());
            let recovered = runtime(root.path(), paged, origin);
            assert_eq!(recovered.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_positive_checks_cover_other_offsets_for_24_hours() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(1, Ordering::Relaxed);
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let now = cold_now();
            StaleGc::run_round_at(&r, now).await;
            origin.mode.store(0, Ordering::Relaxed);
            StaleGc::run_round_at(&r, now + DAY_MS - 1).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                r.resident_bytes(),
                64,
                "a version change alone is not deletion"
            );
            StaleGc::run_round_at(&r, now + DAY_MS).await;
            assert_eq!(r.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_errors_retain_cache_and_retry() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            let now = cold_now();
            StaleGc::run_round_at(&r, now).await;
            assert_eq!(r.resident_bytes(), 32);
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..2 {
                StaleGc::run_round_at(&r, now).await;
            }
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn stale_gc_head_timeout_is_bounded_and_non_destructive() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        fill(&r, &block(0), false).await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::run_round_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        task.await.unwrap();
        tokio::time::resume();
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_read_during_head_invalidates_the_entire_block() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.hold.store(true, Ordering::Relaxed);
            let r = Arc::new(runtime(root.path(), paged, origin.clone()));
            fill(&r, &block(0), paged).await;
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::run_round_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            if paged {
                let guards = r
                    .index
                    .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                    .unwrap();
                drop(guards);
            } else {
                assert!(r.index.is_whole_and_touch(&block(0)));
            }
            origin.release.notify_one();
            task.await.unwrap();
            assert_eq!(r.resident_bytes(), 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_failed_block_unlink_reopens_all_pages_and_requires_a_fresh_head() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&block(0));
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        // A file where the block directory should be makes remove_dir_all fail
        // deterministically, including when the test runs as root.
        std::fs::write(&dir, b"not a directory").unwrap();
        StaleGc::run_round_at(&r, cold_now()).await;
        assert_eq!(r.resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), 32);
        let state = r.page_lifecycle.block(&block(0));
        for page in 0..2 {
            assert!(
                state.acquire(PageIndex(page)).is_some(),
                "failed unlink must reopen every page"
            );
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        origin.mode.store(1, Ordering::Relaxed);
        r.gc_once().await;
        assert_eq!(
            r.resident_bytes(),
            32,
            "TTI GC cannot retry an old NotFound"
        );
        for _ in 0..2 {
            StaleGc::run_round_at(&r, cold_now()).await;
        }
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_respects_pins_and_scan_delete_limits() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let unit = if paged {
                CacheUnit::Page(block(0), PageIndex(0))
            } else {
                CacheUnit::Whole(block(0))
            };
            let pin = r.lru.pin_guard(unit).unwrap();
            let now = cold_now();
            StaleGc::run_round_at(&r, now).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                r.resident_bytes(),
                32,
                "one candidate reclaims a complete block"
            );
            drop(pin);
            StaleGc::run_round_at(&r, now).await;
            assert_eq!(r.resident_bytes(), 0);
        }
    }
    #[tokio::test]
    async fn stale_gc_rechecks_sibling_activity_after_waiting_for_mutation_gate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = Arc::new(runtime(root.path(), true, origin));
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let gate = state.gate.lock().await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::run_round_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        drop(
            r.index
                .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                .unwrap(),
        );
        drop(gate);
        task.await.unwrap();
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_caps_heads_per_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..40 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let now = cold_now();
        let mut pending = r
            .lru
            .scan_stale_blocks(
                &mut (0, r.lru.stale_scan_ceiling()),
                now,
                DAY_MS,
                (4096, 4096),
                false,
            )
            .into();
        StaleGc::reclaim_backend_deleted(&r, &mut pending, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        assert_eq!(r.resident_bytes(), 40 * 32);
        StaleGc::reclaim_backend_deleted(&r, &mut pending, now).await;
        assert_eq!(
            origin.calls.load(Ordering::Relaxed),
            40,
            "resume skipped candidates instead of starving them"
        );
    }
    #[tokio::test]
    async fn stale_gc_pending_candidates_honor_positive_checks_from_previous_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..33 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut id = block(1);
        id.object = ObjectId::new(Backend::S3, "bucket", "key-0");
        fill(&r, &id, false).await;
        let now = cold_now();
        let mut pending = r
            .lru
            .scan_stale_blocks(
                &mut (0, r.lru.stale_scan_ceiling()),
                now,
                DAY_MS,
                (4096, 4096),
                false,
            )
            .into();
        StaleGc::reclaim_backend_deleted(&r, &mut pending, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        StaleGc::reclaim_backend_deleted(&r, &mut pending, now + 1).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 33);
        assert_eq!(r.resident_bytes(), 34 * 32);
    }
    #[tokio::test]
    async fn stale_gc_round_drains_batches_without_rescanning_or_chasing_new_entries() {
        for scan_budget in [16, 4096] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            origin.hold.store(true, Ordering::Relaxed);
            let mut r = runtime(root.path(), false, origin.clone());
            r.page_gc_config.scan_batch_size = scan_budget;
            for n in 0..40 {
                let mut id = block(0);
                id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
                fill(&r, &id, false).await;
            }
            let r = Arc::new(r);
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::run_round_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            let mut late = block(0);
            late.object = ObjectId::new(Backend::S3, "bucket", "late");
            fill(&r, &late, false).await;
            origin.hold.store(false, Ordering::Relaxed);
            origin.release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(origin.calls.load(Ordering::Relaxed), 40,
                "drain the scan/HEAD batches exactly once, retaining the round's original upper bound");
            assert_eq!(r.resident_bytes(), 41 * 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_busy_page_defers_the_entire_block_and_releases_partial_claims() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let reader = state.acquire(PageIndex(1)).unwrap();
        let now = cold_now();
        StaleGc::run_round_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(r.resident_bytes(), 32);
        assert!(
            state.acquire(PageIndex(0)).is_some(),
            "page 0's claim must roll back if page 1 is busy"
        );
        assert!(r
            .paged
            .as_ref()
            .unwrap()
            .dir_for(&block(0))
            .join("0.page")
            .exists());
        drop(reader);
        StaleGc::run_round_at(&r, now).await;
        assert_eq!(r.resident_bytes(), 0);
        assert!(state.acquire(PageIndex(0)).is_none());
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }

    #[tokio::test]
    async fn stale_gc_new_sibling_during_head_invalidates_the_block_candidate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.hold.store(true, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), true, origin.clone()));
        r.commit_fetched_page(&block(0), PageIndex(0), 32, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::run_round_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        r.commit_fetched_page(&block(0), PageIndex(1), 32, Bytes::from_static(&[2; 16]))
            .await
            .unwrap();
        origin.release.notify_one();
        task.await.unwrap();
        assert_eq!(r.resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_cancelled_caller_still_finishes_whole_block_mutation_with_disk_budget() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), true, origin);
        let config = talon_core::WorkerConfig {
            background_io_concurrency: 1,
            background_delete_max_per_sec: 0,
            ..Default::default()
        };
        let budget = crate::background::BackgroundBudget::new(&config);
        r.background_budget = Some(budget.clone());
        fill(&r, &block(0), true).await;
        let slot = budget.io().await;
        let r = Arc::new(r);
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::run_round_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            r.resident_bytes(),
            32,
            "shared I/O budget holds back the entire block"
        );
        drop(slot);
        r.drain_page_mutations().await;
        assert_eq!(r.resident_bytes(), 0);
        assert_eq!(r.lru.total_bytes(), 0);
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }
    #[tokio::test]
    async fn failed_membership_does_not_disable_backend_deletion() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), false, origin.clone());
        fill(&r, &block(0), false).await;
        r.update_membership(None, std::time::Instant::now())
            .unwrap();
        StaleGc::run_round_at(&r, cold_now()).await;
        assert!(r.index.is_empty());
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
    }
}
