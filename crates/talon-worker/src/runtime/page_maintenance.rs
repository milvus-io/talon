//! Page mutations are owned worker tasks: cancellation never splits disk and index updates.
use super::*;
use crate::page_access_store::{AccessSnapshot, PageAccessStore};
use crate::page_gc::{CheckpointReport, GcReport};
use crate::page_lifecycle::GcCandidate;

impl WorkerRuntime {
    /// Configure page idle collection and restore persisted ages before serving traffic.
    pub fn with_page_gc(mut self, config: PageGcConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.interval_ms > 0
                && config.checkpoint_interval_ms > 0
                && config.scan_batch_size > 0
                && config.delete_batch_size > 0
                && config.io_concurrency > 0,
            "invalid page GC budget"
        );
        anyhow::ensure!(
            config.io_concurrency <= tokio::sync::Semaphore::MAX_PERMITS,
            "page GC concurrency exceeds semaphore capacity"
        );
        for millis in [
            config.ttl_ms,
            config.interval_ms,
            config.checkpoint_interval_ms,
        ] {
            anyhow::ensure!(
                Instant::now()
                    .checked_add(Duration::from_millis(millis))
                    .is_some(),
                "page GC duration overflows clock"
            );
        }
        if config.ttl_ms > 0 {
            let paged = self
                .paged
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("page TTL requires paged L2"))?;
            anyhow::ensure!(
                config.checkpoint_interval_ms <= config.ttl_ms,
                "checkpoint interval exceeds TTL"
            );
            let now = self.page_clock.now();
            for (id, _) in self.index.snapshot_lens() {
                let state = self.page_lifecycle.block(&id);
                let recovered =
                    PageAccessStore::load(&paged.dir_for(&id), &id, paged.page_size(), now);
                let pages: Vec<_> = state.inner.lock().unwrap().pages.keys().copied().collect();
                if recovered.corrupt {
                    self.page_gc_metrics.corrupt.add(pages.len() as u64);
                }
                self.page_gc_metrics.future.add(recovered.future as u64);
                for page in pages {
                    let access = recovered.records.get(&page).copied();
                    if access.is_none() {
                        self.page_gc_metrics.missing.inc();
                    }
                    state.register(PageIndex(page), access, now, false);
                }
            }
        }
        self.page_gc_metrics
            .ttl_seconds
            .set(config.ttl_ms as f64 / 1000.0);
        self.page_gc_metrics
            .checkpoint_interval
            .set(config.checkpoint_interval_ms as f64 / 1000.0);
        self.page_gc_io = Arc::new(tokio::sync::Semaphore::new(config.io_concurrency));
        self.page_gc_config = config;
        Ok(self)
    }

    pub(crate) async fn drain_page_mutations(&self) {
        self.page_mutations.drain().await;
    }

    /// Run one disk cleanup batch, including when page TTL or paged reads are disabled.
    pub async fn cleanup_page_files_once(&self) -> crate::page_gc::CleanupReport {
        let runtime = self.clone();
        let permit = self
            .page_gc_io
            .clone()
            .acquire_owned()
            .await
            .expect("page cleanup semaphore closed");
        self.page_mutations
            .run(async move {
                let _permit = permit;
                let worker = runtime.clone();
                let report = tokio::task::spawn_blocking(move || {
                    // The runtime retains the root lease until blocking I/O finishes.
                    worker.page_cleanup.lock().unwrap().run_batch(
                        &worker.page_lifecycle,
                        worker.page_gc_config.scan_batch_size,
                        worker.page_gc_config.delete_batch_size,
                    )
                })
                .await
                .expect("page cleanup task panicked");
                runtime
                    .page_gc_metrics
                    .cleanup_scanned
                    .add(report.checked as u64);
                runtime
                    .page_gc_metrics
                    .cleanup_removed
                    .add(report.removed as u64);
                runtime
                    .page_gc_metrics
                    .cleanup_errors
                    .add(report.errors as u64);
                if report.completed_scan {
                    runtime
                        .page_gc_metrics
                        .cleanup_pending
                        .set(report.pending as f64);
                    runtime
                        .page_gc_metrics
                        .cleanup_scan_seconds
                        .set(report.scan_seconds);
                    runtime
                        .page_gc_metrics
                        .cleanup_scan_at
                        .set(runtime.page_clock.now() as f64 / 1000.0);
                }
                report
            })
            .await
    }

    /// Discover and attempt one complete disk pass before serving traffic.
    /// Errors stay on disk and are rediscovered by the background loop.
    pub async fn recover_page_file_cleanup(&self) {
        loop {
            if self.cleanup_page_files_once().await.completed_scan {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Run at most one bounded TTL batch; independent of capacity and control-plane health.
    pub async fn gc_once(&self) -> GcReport {
        let Ok(mut scan) = self.page_scan.try_lock() else {
            return GcReport::default();
        };
        let started = Instant::now();
        let mut cursor = std::mem::take(&mut scan.0);
        let lifecycle = self.page_lifecycle.clone();
        let limit = self.page_gc_config.scan_batch_size;
        let delete_limit = self.page_gc_config.delete_batch_size;
        let ttl = (self.page_gc_config.ttl_ms > 0).then_some(self.page_gc_config.ttl_ms);
        let now = self.page_clock.now();
        let (cursor, candidates, stats) = tokio::task::spawn_blocking(move || {
            let (candidates, stats) = lifecycle.scan(&mut cursor, limit, now, ttl, delete_limit);
            (cursor, candidates, stats)
        })
        .await
        .expect("page scanner panicked");
        scan.0 = cursor;
        self.page_gc_metrics.scanned.add(stats.checked as u64);
        scan.2 += stats.retries;
        let mut report = GcReport {
            checked: stats.checked,
            completed_scan: stats.completed,
            ..Default::default()
        };
        // The scanner stops when either budget is exhausted, so its cursor never
        // skips unattempted deletions (including retries for empty directories).
        for block in stats.empty {
            self.cleanup_empty_block(block).await;
        }
        let work = futures::stream::iter(candidates.into_iter().map(|c| {
            let reason = c.reason;
            self.evict_page_candidate(c, reason)
        }));
        let mut work = work.buffer_unordered(self.page_gc_config.io_concurrency);
        while let Some(result) = work.next().await {
            if let Some(bytes) = result {
                report.reclaimed += 1;
                report.bytes += bytes;
            }
        }
        if stats.completed {
            self.page_gc_metrics
                .scan_duration
                .set(scan.1.elapsed().as_secs_f64());
            self.page_gc_metrics.retries.set(scan.2 as f64);
            scan.1 = Instant::now();
            scan.2 = 0;
        }
        self.page_gc_metrics
            .batch_duration
            .observe(started.elapsed().as_secs_f64());
        report
    }

    pub(super) async fn evict_page_candidate(
        &self,
        candidate: GcCandidate,
        reason: usize,
    ) -> Option<u64> {
        let runtime = self.clone();
        let permit = self.page_gc_io.clone().acquire_owned().await.ok()?;
        self.page_mutations.run(async move {
            let _permit = permit;
            let block = &candidate.block;
            let _gate = block.gate.lock().await;
            if reason == 1 && (runtime.capacity_bytes == 0 || runtime.lru.total_bytes() <= runtime.capacity_bytes) { return None; }
            let ttl = (reason == 0).then_some(runtime.page_gc_config.ttl_ms);
            if !block.claim(&candidate, runtime.page_clock.now(), ttl) {return None;}
            let paged = runtime.paged.as_ref()?;
            let page = candidate.page;
            let id = &block.id;
            let bytes = runtime.index.get(id).map(|m| talon_core::page_len(m.len, paged.page_size(), page)).unwrap_or(0);
            if let Err(error) = paged.evict_page_async(id, page).await {
                block.abort(&candidate, reason); runtime.page_gc_metrics.delete_errors.inc();
                tracing::warn!(block = %id, page = page.0, %error, "page unlink failed; retaining accounting for retry");
                return None;
            }
            runtime.l1.remove_page(id, page);
            #[cfg(test)]
            crate::page_access_store::crash_barrier("last_page_unlink").unwrap();
            runtime.index.clear_page(id, page);
            runtime.lru.remove(&CacheUnit::Page(id.clone(), page));
            block.finish(&candidate, runtime.page_clock.now());
            runtime.metrics.record_eviction(); runtime.refresh_l1_metrics();
            runtime.page_gc_metrics.deleted[reason].inc(); runtime.page_gc_metrics.bytes[reason].add(bytes);
            if block.inner.lock().unwrap().pages.is_empty() {
                match paged.delete_block_async(id).await {
                    Ok(()) => { block.inner.lock().unwrap().cleanup_pending = false; if runtime.index.get(id).is_some_and(|m| matches!(m.form, BlockForm::Paged {..})) {runtime.index.remove(id);} }
                    Err(error) => {runtime.page_gc_metrics.delete_errors.inc(); tracing::warn!(block = %id, %error, "empty page directory cleanup failed");}
                }
            }
            Some(bytes)
        }).await
    }

    async fn cleanup_empty_block(&self, block: Arc<crate::page_lifecycle::BlockState>) {
        let runtime = self.clone();
        self.page_mutations
            .run(async move {
                let _gate = block.gate.lock().await;
                if !block.inner.lock().unwrap().pages.is_empty() {
                    return;
                }
                let Some(paged) = &runtime.paged else {
                    return;
                };
                match paged.delete_block_async(&block.id).await {
                    Ok(()) => {
                        block.inner.lock().unwrap().cleanup_pending = false;
                        if runtime
                            .index
                            .get(&block.id)
                            .is_some_and(|m| matches!(m.form, BlockForm::Paged { .. }))
                        {
                            runtime.index.remove(&block.id);
                        }
                    }
                    Err(error) => {
                        runtime.page_gc_metrics.delete_errors.inc();
                        tracing::warn!(%error, "empty block cleanup retry failed");
                    }
                }
            })
            .await;
    }

    /// Capacity and superseded page eviction share the same commit protocol as TTL.
    pub(super) async fn unlink_units(
        &self,
        units: Vec<crate::eviction::EvictionCandidate>,
        reason: usize,
    ) {
        // Capture generations before the first await, not after earlier deletions finish.
        let candidates: Vec<_> = units
            .into_iter()
            .map(|unit| {
                let page = match &unit.unit {
                    CacheUnit::Page(id, page) => self.page_lifecycle.block(id).candidate(*page),
                    _ => None,
                };
                (unit, page)
            })
            .collect();
        for (unit, candidate) in candidates {
            let retired = match &unit.unit {
                CacheUnit::Page(id, _) | CacheUnit::Whole(id) => id.clone(),
            };
            match unit.unit.clone() {
                CacheUnit::Page(_, _) => {
                    if self.lru.candidate_is_current(&unit) {
                        if let Some(c) = candidate {
                            self.evict_page_candidate(c, reason).await;
                        }
                    }
                }
                CacheUnit::Whole(id) => {
                    let state = self.page_lifecycle.block(&id);
                    let runtime = self.clone();
                    self.page_mutations
                        .run(async move {
                            let _gate = state.gate.lock().await;
                            if reason == 1
                                && (runtime.capacity_bytes == 0
                                    || runtime.lru.total_bytes() <= runtime.capacity_bytes)
                            {
                                return;
                            }
                            if !runtime.lru.candidate_is_current(&unit) {
                                return;
                            }
                            // A stale whole candidate must never delete newly
                            // materialized pages through a directory fallback.
                            if !state.inner.lock().unwrap().pages.is_empty() {
                                return;
                            }
                            if let Err(error) = runtime.store.delete(&id).await {
                                tracing::warn!(block = %id, %error, "whole block unlink failed");
                                return;
                            }
                            if let Some(paged) = &runtime.paged {
                                if let Err(error) = paged.delete_block_async(&id).await {
                                    tracing::warn!(block = %id, %error, "paged directory unlink failed");
                                    return;
                                }
                            }
                            runtime.invalidate_l1(&id);
                            runtime.index.remove(&id);
                            runtime.lru.remove(&CacheUnit::Whole(id));
                            runtime.metrics.record_eviction();
                        })
                        .await;
                }
            }
            self.page_lifecycle.retire_empty(&retired);
        }
    }

    /// Visit dirty blocks in bounded batches; no per-access queue or global snapshot.
    pub async fn checkpoint_access_times(&self) -> CheckpointReport {
        if self.page_gc_config.ttl_ms == 0 {
            return CheckpointReport::default();
        }
        let Ok(_single) = self.page_checkpoint.try_lock() else {
            return CheckpointReport::default();
        };
        let mut cursor = ScanCursor::default();
        let mut report = CheckpointReport::default();
        let mut dirty = 0;
        let mut oldest = 0;
        loop {
            let (blocks, done) = self.page_lifecycle.dirty_batch(&mut cursor, 64);
            for block in &blocks {
                let state = block.inner.lock().unwrap();
                if let Some(since) = state.dirty_since {
                    dirty += 1;
                    oldest = oldest.max(self.page_clock.now().saturating_sub(since));
                }
            }
            self.page_gc_metrics.dirty_blocks.set(dirty as f64);
            self.page_gc_metrics.dirty_age.set(oldest as f64 / 1000.0);
            let work = futures::stream::iter(blocks.into_iter().map(|block| {
                let runtime = self.clone();
                async move {
                    self.page_mutations
                        .run(async move {
                            let _gate = block.gate.lock().await;
                            let snapshot = {
                                let state = block.inner.lock().unwrap();
                                if state.dirty_since.is_none() || state.pages.is_empty() {
                                    return Ok::<_, anyhow::Error>(0);
                                }
                                AccessSnapshot {
                                    revision: state.revision,
                                    sampled_at: runtime.page_clock.now(),
                                    records: state
                                        .pages
                                        .iter()
                                        .filter_map(|(&page, entry)| {
                                            entry.last_access.map(|access| (page, access))
                                        })
                                        .collect(),
                                }
                            };
                            let revision = snapshot.revision;
                            let sampled_at = snapshot.sampled_at;
                            let paged = runtime.paged.as_ref().expect("paged TTL").clone();
                            let id = block.id.clone();
                            let bytes = tokio::task::spawn_blocking(move || {
                                PageAccessStore::checkpoint(
                                    &paged.dir_for(&id),
                                    &id,
                                    paged.page_size(),
                                    &snapshot,
                                )
                            })
                            .await??;
                            let mut state = block.inner.lock().unwrap();
                            if state.revision == revision {
                                state.dirty_since = None;
                            } else {
                                // Only post-snapshot accesses remain unsaved. A hot block
                                // must not report the age of already-persisted accesses.
                                state.dirty_since = Some(sampled_at);
                            }
                            runtime
                                .page_gc_metrics
                                .checkpoint_at
                                .set(runtime.page_clock.now() as f64 / 1000.0);
                            Ok(bytes)
                        })
                        .await
                }
            }));
            let mut work = work.buffer_unordered(2);
            while let Some(result) = work.next().await {
                match result {
                    Ok(bytes) if bytes > 0 => {
                        report.blocks += 1;
                        report.bytes += bytes as u64;
                        self.page_gc_metrics.checkpoint_bytes.add(bytes as u64);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        report.failures += 1;
                        self.page_gc_metrics.checkpoint_errors.inc();
                        tracing::warn!(%error, "page access checkpoint failed; keeping dirty state");
                    }
                }
            }
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
        // A touch racing a successful checkpoint leaves the block dirty. Recount
        // in bounded batches rather than inferring cleanliness from write counts.
        let mut cursor = ScanCursor::default();
        let mut remaining = 0;
        let mut oldest = 0;
        loop {
            let (blocks, done) = self.page_lifecycle.dirty_batch(&mut cursor, 64);
            for block in blocks {
                if let Some(since) = block.inner.lock().unwrap().dirty_since {
                    remaining += 1;
                    oldest = oldest.max(self.page_clock.now().saturating_sub(since));
                }
            }
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
        self.page_gc_metrics.dirty_blocks.set(remaining as f64);
        self.page_gc_metrics.dirty_age.set(oldest as f64 / 1000.0);
        report
    }
}
