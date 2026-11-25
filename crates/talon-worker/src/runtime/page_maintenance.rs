//! Page mutations are owned worker tasks: cancellation never splits disk and index updates.
use super::*;
use crate::block_key::BlockIdentity;
use crate::page_access_store::{AccessSnapshot, PageAccessStore};
use crate::page_gc::{CheckpointReport, GcReport};
use crate::page_lifecycle::{GcCandidate, PagedMutation};

impl WorkerRuntime {
    pub(super) async fn background_io(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match &self.background_budget {
            Some(budget) => Some(budget.io().await),
            None => None,
        }
    }
    async fn background_delete(&self) {
        if let Some(budget) = &self.background_budget {
            budget.delete().await;
        }
    }

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
            config.tti_ms,
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
        let effective_tti = self
            .dynamic_settings
            .load()
            .as_ref()
            .and_then(|c| {
                c.settings_for(self.namespace.effective_name())
                    .page_tti_hours
            })
            .map(|hours| hours * 3_600_000)
            .unwrap_or(config.tti_ms);
        if effective_tti > 0 {
            anyhow::ensure!(
                self.paged.is_some() || self.small.as_ref().is_some(),
                "TTI requires paged L2 or small-object storage"
            );
            anyhow::ensure!(
                config.checkpoint_interval_ms <= effective_tti,
                "checkpoint interval exceeds TTI"
            );
        }
        if let Some(small) = self.small.as_ref() {
            small.enable_access_tracking()?;
        }
        if let Some(paged) = self.paged.as_ref() {
            // Checkpoints run even with TTI disabled. Keep ages for
            // recovery and for enabling TTI through live namespace settings.
            self.page_lifecycle.enable_access_tracking();
            let now = self.page_clock.now();
            let slots = self.page_lifecycle.recovery_slots();
            for (shard, slots) in slots.into_iter().enumerate() {
                let dir = paged.root().join(format!("{shard:02x}"));
                let mut recovered_shard =
                    crate::page_access_shard::load(&dir, shard, paged.page_size(), now);
                if let Some(recovered) = &recovered_shard {
                    self.page_lifecycle
                        .set_checkpoint_journal(shard, recovered.journal);
                }
                if recovered_shard.is_some() || !slots.is_empty() {
                    self.page_lifecycle.checkpoint_recovered(shard);
                }
                for slot in slots {
                    let Some(state) = self.page_lifecycle.recovery_block(slot) else {
                        continue;
                    };
                    let id = state.id.materialize();
                    let recovered = match &mut recovered_shard {
                        Some(shard) => shard.records.remove(&id).unwrap_or_else(|| {
                            crate::page_access_store::AccessRecovery {
                                corrupt: shard.corrupt,
                                ..Default::default()
                            }
                        }),
                        None if paged.journaled() => Default::default(),
                        None => {
                            PageAccessStore::load(&paged.dir_for(&id), &id, paged.page_size(), now)
                        }
                    };
                    let (resident, missing) = state.restore_ages(&recovered.records);
                    if recovered.corrupt {
                        self.page_gc_metrics.corrupt.add(resident);
                    }
                    self.page_gc_metrics.future.add(recovered.future as u64);
                    self.page_gc_metrics.missing.add(missing);
                }
            }
        }
        self.page_gc_metrics
            .tti_seconds
            .set(effective_tti as f64 / 1000.0);
        self.page_gc_metrics
            .checkpoint_interval
            .set(config.checkpoint_interval_ms as f64 / 1000.0);
        self.page_gc_io = Arc::new(tokio::sync::Semaphore::new(config.io_concurrency));
        self.page_gc_config = config;
        Ok(self)
    }

    pub(crate) async fn drain_page_mutations(&self) {
        self.page_mutations.drain().await;
        self.store.drain().await;
    }

    /// Complete one disk cleanup pass, including when page TTI or paged reads are disabled.
    pub async fn cleanup_page_files_once(&self) -> crate::page_gc::CleanupReport {
        self.cleanup_page_files_batch(usize::MAX, usize::MAX).await
    }

    pub(crate) async fn cleanup_page_files_batch(
        &self,
        scans: usize,
        deletes: usize,
    ) -> crate::page_gc::CleanupReport {
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
                let _background_io = runtime.background_io().await;
                let report = runtime
                    .page_cleanup
                    .lock()
                    .await
                    .run_batch_with_budget(
                        &runtime.page_lifecycle,
                        scans,
                        deletes,
                        runtime.background_budget.as_deref(),
                    )
                    .await;
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

    /// Explicitly complete a disk cleanup pass. Normal startup uses the bounded
    /// background loop instead. Errors stay on disk for the next pass.
    pub async fn recover_page_file_cleanup(&self) {
        self.cleanup_page_files_once().await;
    }

    /// Complete one TTI scan and its deletion attempts, independent of batch limits.
    pub async fn gc_once(&self) -> GcReport {
        let mut cursor = self.page_scan.lock().await;
        *cursor = ScanCursor::default();
        let mut report = GcReport::default();
        loop {
            let batch = self.gc_batch(&mut cursor).await;
            report.checked += batch.checked;
            report.reclaimed += batch.reclaimed;
            report.bytes += batch.bytes;
            if batch.completed_scan {
                report.completed_scan = true;
                return report;
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn gc_next_batch(&self) -> GcReport {
        let mut cursor = self.page_scan.lock().await;
        self.gc_batch(&mut cursor).await
    }

    async fn gc_batch(&self, cursor: &mut ScanCursor) -> GcReport {
        let started = Instant::now();
        let lifecycle = self.page_lifecycle.clone();
        let age = self.current_tti_ms();
        let tti = (age > 0).then_some(age);
        let now = self.page_clock.now();
        let mut scan = std::mem::take(cursor);
        let (scan, candidates, stats) = tokio::task::spawn_blocking(move || {
            let (candidates, stats) = lifecycle.scan(&mut scan, 4096, now, tti, 64);
            (scan, candidates, stats)
        })
        .await
        .expect("page scanner panicked");
        *cursor = scan;
        self.page_gc_metrics.scanned.add(stats.checked as u64);
        let mut report = GcReport {
            checked: stats.checked,
            completed_scan: stats.completed,
            ..Default::default()
        };
        // Finish all candidates before the next scan interval. Disk concurrency,
        // deletion pacing, and revalidation still apply to every attempt.
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
        if let Some((seconds, retries)) = cursor.record_batch(
            started.elapsed().as_secs_f64(),
            stats.retries,
            stats.completed,
        ) {
            self.page_gc_metrics.scan_duration.set(seconds);
            self.page_gc_metrics.retries.set(retries as f64);
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
        self.evict_page_candidate_to_target(candidate, reason, self.capacity_bytes)
            .await
    }

    async fn evict_page_candidate_to_target(
        &self,
        candidate: GcCandidate,
        reason: usize,
        target: u64,
    ) -> Option<u64> {
        let runtime = self.clone();
        let permit = self.page_gc_io.clone().acquire_owned().await.ok()?;
        self.page_mutations.run(async move {
            let _permit = permit;
            runtime.background_delete().await;
            let _background_io = runtime.background_io().await;
            let block = &candidate.block;
            let _gate = block.lock().await;
            if reason == 1 && (runtime.capacity_bytes == 0 || runtime.resident_bytes() <= target) { return None; }
            let age = runtime.current_tti_ms();
            if reason == 0 && age == 0 { return None; }
            let tti = (reason == 0).then_some(age);
            if !block.claim(&candidate, runtime.page_clock.now(), tti) {return None;}
            let paged = runtime.paged.as_ref()?;
            let page = candidate.page;
            let id = &block.id.materialize();
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
            runtime.metrics.record_eviction(); runtime.refresh_l1_metrics();
            runtime.page_gc_metrics.deleted[reason].inc(); runtime.page_gc_metrics.bytes[reason].add(bytes);
            if !block.has_pages() {
                match paged.delete_block_async(id).await {
                    Ok(()) => { block.cleanup_finished(); if runtime.index.get(id).is_some_and(|m| matches!(m.form, BlockForm::Paged {..})) {runtime.index.remove(id);} }
                    Err(error) => {runtime.page_gc_metrics.delete_errors.inc(); tracing::warn!(block = %id, %error, "empty page directory cleanup failed");}
                }
            }
            Some(bytes)
        }).await
    }

    async fn cleanup_empty_block(&self, block: PagedMutation) {
        let runtime = self.clone();
        self.page_mutations
            .run(async move {
                runtime.background_delete().await;
                let _background_io = runtime.background_io().await;
                let _gate = block.lock().await;
                if block.has_pages() {
                    return;
                }
                let Some(paged) = &runtime.paged else {
                    return;
                };
                let id = block.id.materialize();
                match paged.delete_block_async(&id).await {
                    Ok(()) => {
                        block.cleanup_finished();
                        if runtime
                            .index
                            .get(&id)
                            .is_some_and(|m| matches!(m.form, BlockForm::Paged { .. }))
                        {
                            runtime.index.remove(&id);
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

    /// Retire an entire stale block in one owned mutation. Page count affects
    /// disk pacing, never which subset of the block is selected for eviction.
    #[cfg(test)]
    pub(super) async fn unlink_stale_block(&self, candidate: crate::eviction::StaleBlockCandidate) {
        self.unlink_stale_block_for(candidate, None).await;
    }

    /// Ownership supplies the observed placement after paying deletion credits.
    pub(super) async fn unlink_stale_block_for(
        &self,
        candidate: crate::eviction::StaleBlockCandidate,
        membership: Option<Arc<super::MembershipSnapshot>>,
    ) {
        self.unlink_stale_block_inner(candidate, membership, false, None)
            .await;
    }

    pub(super) async fn unlink_stale_block_prepaid(
        &self,
        candidate: crate::eviction::StaleBlockCandidate,
    ) {
        self.unlink_stale_block_inner(candidate, None, true, None)
            .await;
    }

    pub(super) async fn unlink_idle_block(
        &self,
        candidate: crate::eviction::StaleBlockCandidate,
        now: u64,
    ) {
        self.unlink_stale_block_inner(candidate, None, false, Some(now))
            .await;
    }

    pub(super) async fn unlink_idle_small(
        &self,
        small: Arc<SmallObjectStore>,
        unit: CacheUnit,
        ticket: Arc<crate::small_store::policy::SmallAccess>,
        now: u64,
    ) {
        let runtime = self.clone();
        self.page_mutations
            .run(async move {
                runtime.background_delete().await;
                let _permit = runtime
                    .page_gc_io
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("page GC semaphore closed");
                let _background_io = runtime.background_io().await;
                let block = unit.block().clone();
                let whole = matches!(&unit, CacheUnit::Whole(_));
                let state = runtime.page_lifecycle.block(&block);
                {
                    let _gate = state.lock().await;
                    let settings = runtime.clone();
                    match small
                        .delete_idle(unit, ticket, move || {
                            (
                                now.max(crate::eviction::access_millis()),
                                settings.current_tti_ms(),
                            )
                        })
                        .await
                    {
                        Ok(true) => {
                            if whole {
                                runtime.invalidate_l1(&block);
                            }
                            runtime.metrics.record_eviction();
                        }
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(%block, %error, "small object idle unlink failed")
                        }
                    }
                }
            })
            .await;
    }

    pub(super) async fn unlink_small_gc(
        &self,
        candidate: crate::small_store::SmallGcCandidate,
        revision: u64,
        membership: Option<Arc<super::MembershipSnapshot>>,
    ) {
        let runtime = self.clone();
        self.page_mutations
            .run(async move {
                let deadline = membership.as_ref().map(|view| view.valid_until);
                let Some(permit) =
                    before_deadline(deadline, runtime.page_gc_io.clone().acquire_owned()).await
                else {
                    return;
                };
                let _permit = permit.expect("page GC semaphore closed");
                let Some(_background_io) = before_deadline(deadline, runtime.background_io()).await
                else {
                    return;
                };
                let block = candidate.unit.block().clone();
                let whole = matches!(candidate.unit, CacheUnit::Whole(_));
                let state = runtime.page_lifecycle.block(&block);
                if let Some(_gate) = before_deadline(deadline, state.lock()).await {
                    let validator = runtime.clone();
                    let small = runtime.small.as_ref().unwrap();
                    match small
                        .delete_gc(candidate, revision, deadline, move || {
                            membership.as_ref().map_or(true, |view| {
                                uncontended(validator.membership.try_lock()).is_some_and(|held| {
                                    let now = Instant::now();
                                    now < view.valid_until
                                        && held.as_ref().is_some_and(|current| {
                                            current.generation == view.generation
                                                && now < current.valid_until
                                        })
                                })
                            })
                        })
                        .await
                    {
                        Ok(true) => {
                            if whole {
                                runtime.invalidate_l1(&block);
                            }
                            runtime.metrics.record_eviction();
                        }
                        Ok(false) => {}
                        Err(error) => {
                            runtime.page_gc_metrics.delete_errors.inc();
                            tracing::warn!(%block, %error, "small object GC unlink failed");
                        }
                    }
                }
                drop(state);
            })
            .await;
    }

    async fn unlink_stale_block_inner(
        &self,
        candidate: crate::eviction::StaleBlockCandidate,
        membership: Option<Arc<super::MembershipSnapshot>>,
        prepaid: bool,
        idle_at: Option<u64>,
    ) {
        let runtime = self.clone();
        self.page_mutations.run(async move {
            let ownership_deadline = membership.as_ref().map(|view| view.valid_until);
            let reason = if ownership_deadline.is_some() { 4 } else { 3 };
            let Some(permit) = before_deadline(ownership_deadline, runtime.page_gc_io.clone().acquire_owned()).await else { return; };
            let _permit = permit.expect("page GC semaphore closed");
            if ownership_deadline.is_none() && !prepaid {
                if let Some(budget) = &runtime.background_budget {
                    budget.delete_units(candidate.units as u64).await;
                }
            }
            let Some(_background_io) = before_deadline(ownership_deadline, runtime.background_io()).await else { return; };
            let state = runtime.page_lifecycle.block(&candidate.block);
            {
                let Some(_gate) = before_deadline(ownership_deadline, state.lock()).await else { return; };
                if idle_at.is_some_and(|now| !candidate.is_idle(now.max(crate::eviction::access_millis()), runtime.current_tti_ms())) { return; }
                if !runtime.lru.stale_candidate_is_current(&candidate) { return; }
                let Some(claim) = state.claim_all_pages() else { return; };
                // A reader may have completed while the pages were being closed.
                if !runtime.lru.stale_candidate_is_current(&candidate) { return; }
                if membership.as_ref().is_some_and(|view|
                    Instant::now() >= view.valid_until || !runtime.membership_is_current(view)) { return; }
                let id = &candidate.block;
                let bytes = runtime.index.get(id).map_or(0, |meta| meta.resident_bytes());
                let pages = claim.len();
                let compact = runtime.small.as_ref().is_some_and(|small| small.has_recency());
                let whole = if compact {
                    runtime.store.delete(id).await
                } else {
                    runtime.delete_whole(id).await
                };
                if let Err(error) = whole {
                    runtime.page_gc_metrics.delete_errors.inc();
                    tracing::warn!(block = %id, %error, "stale block unlink failed; retaining accounting");
                    return;
                }
                if let Some(small) = runtime.small.as_ref().filter(|_| !compact) {
                    if let Err(error) = small.delete(&CacheUnit::ExactRange(id.clone())).await {
                        tracing::warn!(block = %id, %error, "exact range unlink failed");
                        return;
                    }
                }
                if let Some(paged) = runtime.paged.as_ref().filter(|_| pages > 0) {
                    if let Err(error) = paged.delete_block_async(id).await {
                        runtime.page_gc_metrics.delete_errors.inc();
                        tracing::warn!(block = %id, %error, "stale paged block unlink failed; retaining accounting");
                        return;
                    }
                }
                runtime.invalidate_l1(id);
                runtime.index.remove(id);
                runtime.lru.remove_tracked_block(id);
                claim.finish();
                runtime.metrics.record_eviction();
                if pages > 0 {
                    runtime.page_gc_metrics.deleted[reason].add(pages as u64);
                    runtime.page_gc_metrics.bytes[reason].add(bytes);
                }
            }
        }).await;
    }

    /// Capacity and superseded page eviction share the same commit protocol as TTI.
    pub(super) async fn unlink_units(
        &self,
        units: Vec<crate::eviction::EvictionCandidate>,
        reason: usize,
    ) {
        self.unlink_units_to_target(units, reason, self.capacity_bytes)
            .await;
    }

    pub(super) async fn unlink_units_to_target(
        &self,
        units: Vec<crate::eviction::EvictionCandidate>,
        reason: usize,
        target: u64,
    ) {
        if units.is_empty() {
            return;
        }
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
        let work =
            futures::stream::iter(candidates.into_iter().map(|(unit, candidate)| async move {
                match unit.unit.clone() {
                    CacheUnit::Page(_, _) => {
                        if self.lru.candidate_is_current(&unit) {
                            if let Some(c) = candidate {
                                self.evict_page_candidate_to_target(c, reason, target).await;
                            }
                        }
                    }
                    CacheUnit::ExactRange(id) => {
                        let state = self.page_lifecycle.block(&id);
                        let runtime = self.clone();
                        self.page_mutations.run(async move {
                            runtime.background_delete().await;
                            let _background_io = runtime.background_io().await;
                            let _gate = state.lock().await;
                            if reason == 1 && (runtime.capacity_bytes == 0 || runtime.resident_bytes() <= target) {
                                return;
                            }
                            if !runtime.lru.candidate_is_current(&unit) { return; }
                            if let Some(small) = &runtime.small {
                                if let Err(error) = small.delete(&unit.unit).await {
                                    tracing::warn!(block = %id, %error, "exact range unlink failed");
                                    return;
                                }
                            }
                            runtime.lru.remove(&unit.unit);
                            runtime.metrics.record_eviction();
                        }).await;
                    }
                    CacheUnit::Whole(id) => {
                        let state = self.page_lifecycle.block(&id);
                        let runtime = self.clone();
                        self.page_mutations
                        .run(async move {
                            runtime.background_delete().await;
                            let _background_io = runtime.background_io().await;
                            let _gate = state.lock().await;
                            if reason == 1
                                && (runtime.capacity_bytes == 0
                                    || runtime.resident_bytes() <= target)
                            {
                                return;
                            }
                            if !runtime.lru.candidate_is_current(&unit) {
                                return;
                            }
                            // A stale whole candidate must never delete newly
                            // materialized pages through a directory fallback.
                            if state.has_pages() {
                                return;
                            }
                            if let Err(error) = runtime.delete_whole(&id).await {
                                tracing::warn!(block = %id, %error, "whole block unlink failed");
                                return;
                            }
                            runtime.invalidate_l1(&id);
                            runtime.index.remove(&id);
                            runtime.lru.remove(&CacheUnit::Whole(id));
                            runtime.metrics.record_eviction();
                        })
                        .await;
                    }
                }
            }));
        let concurrency = if reason == 1 {
            self.page_gc_config.io_concurrency
        } else {
            1
        };
        let mut work = work.buffer_unordered(concurrency);
        while work.next().await.is_some() {}
    }

    /// Explicit flush (including shutdown). Normal background work advances one
    /// shard per tick, spreading syncs over the configured checkpoint interval.
    pub async fn checkpoint_access_times(&self) -> CheckpointReport {
        self.checkpoint_shards(true).await
    }

    pub(crate) async fn checkpoint_next_shard(&self) -> CheckpointReport {
        self.checkpoint_shards(false).await
    }

    async fn checkpoint_shards(&self, all: bool) -> CheckpointReport {
        if self.paged.is_none() || !self.page_lifecycle.tracks_access() {
            return CheckpointReport::default();
        }
        let Ok(mut cursor) = self.page_checkpoint.clone().try_lock_owned() else {
            return CheckpointReport::default();
        };
        let runtime = self.clone();
        // Retain serialization and dirty-state ownership even if the caller is cancelled.
        self.page_mutations.run(async move {
            let mut report = CheckpointReport::default();
            for _ in 0..if all { crate::page_lifecycle::SHARDS } else { 1 } {
                let shard = *cursor;
                *cursor = (shard + 1) % crate::page_lifecycle::SHARDS;
                let _background_io = runtime.background_io().await;
                let result = runtime.checkpoint_shard(shard).await;
                match result {
                    Ok((blocks, bytes)) => {
                        report.blocks += blocks;
                        report.bytes += bytes as u64;
                        if bytes > 0 {
                            runtime.page_gc_metrics.checkpoint_bytes.add(bytes as u64);
                            runtime.page_gc_metrics.checkpoint_at.set(runtime.page_clock.now() as f64 / 1000.0);
                        }
                    }
                    Err(error) => {
                        report.failures += 1;
                        runtime.page_gc_metrics.checkpoint_errors.inc();
                        tracing::warn!(shard, %error, "page shard checkpoint failed; keeping dirty state");
                    }
                }
            }
            if all || *cursor == 0 { runtime.refresh_checkpoint_metrics().await; }
            report
        }).await
    }

    async fn checkpoint_shard(&self, shard: usize) -> anyhow::Result<(usize, usize)> {
        let paged = self.paged.as_ref().expect("paged TTI");
        let gate = self.page_lifecycle.checkpoint_gate(shard);
        let _guard = gate.lock().await;
        let worker = self.clone();
        let prepared = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let (ceiling, membership, dirty) = worker.page_lifecycle.checkpoint_start(shard);
            let (journal, removed) = worker.page_lifecycle.checkpoint_journal(shard, membership);
            if !dirty {
                return Ok(None);
            }
            let full = journal.compact();
            let mut retry = journal;
            retry.full = true;
            worker.page_lifecycle.set_checkpoint_journal(shard, retry);
            let mut slot = 0;
            let mut snapshots = Vec::new();
            let mut size = 24;
            while slot < ceiling {
                for sampled in worker
                    .page_lifecycle
                    .checkpoint_slots(shard, &mut slot, ceiling, full)
                {
                    if let Some(snapshot) = worker.page_lifecycle.checkpoint_snapshot(
                        shard,
                        sampled,
                        full,
                        membership,
                        worker.page_clock.now(),
                        &mut size,
                    )? {
                        snapshots.push(snapshot);
                    }
                }
            }
            let paged = worker.paged.as_ref().expect("paged TTI");
            let dir = paged.root().join(format!("{shard:02x}"));
            if !dir.exists() && snapshots.iter().all(|sample| !sample.resident) {
                for sample in snapshots {
                    sample.commit();
                }
                worker.page_lifecycle.checkpoint_finished(shard, membership);
                return Ok(None);
            }
            let bytes = if full {
                crate::page_access_shard::encode(
                    paged.page_size(),
                    snapshots
                        .iter()
                        .filter(|sample| sample.resident)
                        .map(|sample| (sample.block.view(), &sample.snapshot)),
                )?
            } else {
                let empty = AccessSnapshot {
                    revision: 0,
                    sampled_at: worker.page_clock.now(),
                    records: Vec::new(),
                };
                crate::page_access_shard::encode_delta(
                    paged.page_size(),
                    removed.iter().map(|id| (id.view(), &empty, true)).chain(
                        snapshots
                            .iter()
                            .map(|sample| (sample.block.view(), &sample.snapshot, sample.replace)),
                    ),
                )?
            };
            Ok(Some((membership, snapshots, journal, full, bytes)))
        })
        .await
        .expect("access checkpoint preparation panicked")?;
        let Some((membership, snapshots, journal, full, bytes)) = prepared else {
            return Ok((0, 0));
        };
        let dir = paged.root().join(format!("{shard:02x}"));
        let (journal, bytes) = crate::page_access_shard::flush(
            &dir,
            journal,
            full,
            bytes,
            self.background_budget.as_deref(),
        )
        .await?;
        let worker = self.clone();
        tokio::task::spawn_blocking(move || {
            let count = snapshots.iter().filter(|sample| sample.resident).count();
            worker.page_lifecycle.set_checkpoint_journal(shard, journal);
            for sample in snapshots {
                sample.commit();
            }
            worker.page_lifecycle.checkpoint_finished(shard, membership);
            Ok((count, bytes))
        })
        .await
        .expect("access checkpoint completion panicked")
    }

    async fn refresh_checkpoint_metrics(&self) {
        let (remaining, since) = self.page_lifecycle.dirty_summary();
        let oldest = since.map_or(0, |since| self.page_clock.now().saturating_sub(since));
        self.page_gc_metrics.dirty_blocks.set(remaining as f64);
        self.page_gc_metrics.dirty_age.set(oldest as f64 / 1000.0);
    }
}

pub(crate) async fn before_deadline<F: std::future::Future>(
    deadline: Option<Instant>,
    work: F,
) -> Option<F::Output> {
    match deadline {
        Some(at) => tokio::time::timeout_at(tokio::time::Instant::from_std(at), work)
            .await
            .ok(),
        None => Some(work.await),
    }
}

pub(crate) fn uncontended<T>(result: std::sync::TryLockResult<T>) -> Option<T> {
    match result {
        Err(std::sync::TryLockError::WouldBlock) => None,
        acquired => Some(acquired.unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::page_access_store::CacheRootLock;
    use bytes::Bytes;
    use talon_core::{ObjectStat, WorkerConfig};

    struct UnusedOrigin;

    #[async_trait::async_trait]
    impl BackendStore for UnusedOrigin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("cache eviction must not fetch origin data")
        }

        async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
            panic!("cache eviction must not stat origin data")
        }
    }

    fn runtime(root: &Path) -> (WorkerRuntime, Arc<Catalog>, WorkerConfig) {
        let config = WorkerConfig {
            small_object_threshold_bytes: 0,
            block_size: 16,
            capacity_bytes: 1 << 20,
            l1_capacity_bytes: 0,
            l2_page_size_bytes: 4,
            page_tti_hours: 0,
            ..Default::default()
        };
        let lock = Arc::new(CacheRootLock::acquire(root).unwrap());
        let scope = root.join("default/shared");
        std::fs::create_dir_all(&scope).unwrap();
        let service = crate::catalog::service(lock.clone()).unwrap();
        let catalog =
            Catalog::with_service(&service, &scope, crate::catalog::transaction_budget(), None)
                .unwrap();
        let path = "paged";
        catalog
            .register(path, config.l2_page_size_bytes as u32)
            .unwrap();
        let paged = PagedBlockStore::open(scope.join(path), config.l2_page_size_bytes as u32)
            .unwrap()
            .with_catalog(catalog.clone(), path.into())
            .with_root_lock(lock.clone());
        let worker = WorkerRuntime::new(
            WholeBlockStore::open_with_catalog(
                &scope,
                Arc::new(crate::fd_cache::FdCache::new(
                    config.whole_fd_cache_capacity,
                )),
                catalog.clone(),
            )
            .unwrap(),
            Arc::new(catalog.index(config.l2_page_size_bytes as u32)),
            Arc::new(InFlightLoads::new()),
            Arc::new(UnusedOrigin),
            config.block_size,
            config.capacity_bytes,
            WorkerMetrics::new(config.capacity_bytes),
        )
        .with_paged_store(paged)
        .with_page_gc(PageGcConfig::from(&config))
        .unwrap();
        (worker, catalog, config)
    }

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            0,
            16,
            Version::new("v1"),
        )
    }

    async fn evict(worker: &WorkerRuntime, config: &WorkerConfig, stale: bool) {
        if stale {
            let candidates = worker.lru.idle_candidates(
                &mut (0, 0),
                crate::eviction::access_millis() + 1,
                0,
                16,
                16,
            );
            assert_eq!(candidates.len(), 1);
            worker
                .unlink_stale_block(candidates.into_iter().next().unwrap())
                .await;
        } else {
            super::super::async_eviction::EvictionCycle::new(worker.resident_bytes(), config)
                .tick(worker)
                .await;
        }
    }

    #[tokio::test]
    async fn whole_eviction_retains_catalog_failure_accounting() {
        for stale in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (worker, catalog, config) = runtime(root.path());
            worker
                .admit_cached_block(
                    &CachedBlockPutRequest {
                        block: block(),
                        object_len: 16,
                        body_len: 16,
                    },
                    Bytes::from_static(&[1; 16]),
                )
                .await
                .unwrap();
            assert!(matches!(
                worker.index.get(&block()).unwrap().form,
                BlockForm::Whole
            ));
            let allocated = worker.resident_bytes();
            assert!(allocated > 0);
            assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
            let disk_id = catalog.disk_id("whole", &block()).unwrap().unwrap();
            let path = disk_id.path(&worker.store.root().join("whole"), "blk");
            catalog.poison();

            evict(&worker, &config, stale).await;

            assert!(worker.store.scan_allocated().is_err());
            assert!(path.exists());
            assert!(worker.index.get(&block()).is_some());
            assert_eq!(
                worker
                    .cached_block_range(&block(), 0, 16)
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from_static(&[1; 16])
            );
            assert!(!worker.lru.is_empty());
            assert_eq!(worker.resident_bytes(), allocated);
        }
    }

    #[tokio::test]
    async fn delayed_whole_miss_keeps_a_refilled_block() {
        let root = tempfile::tempdir().unwrap();
        let (worker, _, config) = runtime(root.path());
        let id = block();
        let request = CachedBlockPutRequest {
            block: id.clone(),
            object_len: 16,
            body_len: 16,
        };
        worker
            .admit_cached_block(&request, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let old = worker.index.whole_and_touch(&id).unwrap().unwrap();
        evict(&worker, &config, true).await;
        assert!(worker.index.get(&id).is_none());
        worker
            .admit_cached_block(&request, Bytes::from_static(&[2; 16]))
            .await
            .unwrap();

        assert!(matches!(
            worker.store.get_range_bytes_at(&id, Some(old), 0, 16).await,
            Err(talon_core::Error::NotFound(_))
        ));
        worker
            .forget_missing_l2_parent(&id, Some(old))
            .await
            .unwrap();
        assert_eq!(
            worker
                .cached_block_range(&id, 0, 16)
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(&[2; 16])
        );
        assert_eq!(worker.lru.block_candidates(&id).len(), 1);
    }

    #[tokio::test]
    async fn delayed_whole_miss_keeps_same_address_repair_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let id = block();
        let repaired = Bytes::from_static(&[2; 16]);
        let (worker, catalog, _) = runtime(root.path());
        let request = CachedBlockPutRequest {
            block: id.clone(),
            object_len: 16,
            body_len: 16,
        };
        worker
            .admit_cached_block(&request, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let address = catalog.disk_id("whole", &id).unwrap().unwrap();
        std::fs::remove_file(address.path(&worker.store.root().join("whole"), "blk")).unwrap();
        assert!(matches!(
            worker
                .store
                .get_range_bytes_at(&id, Some(address), 0, 16)
                .await,
            Err(talon_core::Error::NotFound(_))
        ));

        // Run the repair between the observed miss and its delayed cleanup.
        worker
            .admit_cached_block(&request, repaired.clone())
            .await
            .unwrap();
        assert_eq!(catalog.disk_id("whole", &id).unwrap(), Some(address));
        worker
            .forget_missing_l2_parent(&id, Some(address))
            .await
            .unwrap();
        assert_eq!(catalog.disk_id("whole", &id).unwrap(), Some(address));
        assert_eq!(
            worker.cached_block_range(&id, 0, 16).await.unwrap(),
            Some(repaired.clone())
        );
        drop(worker);
        drop(catalog);

        let (worker, catalog, _) = runtime(root.path());
        assert_eq!(catalog.disk_id("whole", &id).unwrap(), Some(address));
        assert_eq!(
            worker.cached_block_range(&id, 0, 16).await.unwrap(),
            Some(repaired)
        );
    }

    #[tokio::test]
    async fn whole_miss_recheck_propagates_lookup_error() {
        let root = tempfile::tempdir().unwrap();
        let (worker, catalog, _) = runtime(root.path());
        let id = block();
        worker
            .commit_cached_block(&id, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let address = catalog.disk_id("whole", &id).unwrap().unwrap();
        let path = address.path(&worker.store.root().join("whole"), "blk");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&path, &path).unwrap();

        let error = worker
            .forget_missing_l2_parent(&id, Some(address))
            .await
            .unwrap_err();
        assert!(matches!(error, talon_core::Error::Io(e) if e.raw_os_error() == Some(libc::ELOOP)));
        assert_eq!(catalog.disk_id("whole", &id).unwrap(), Some(address));
        assert_eq!(worker.lru.block_candidates(&id).len(), 1);
    }

    #[tokio::test]
    async fn missing_whole_file_retires_shared_catalog_and_policy() {
        let root = tempfile::tempdir().unwrap();
        let (worker, catalog, _) = runtime(root.path());
        let id = block();
        worker
            .commit_cached_block(&id, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();

        let disk_id = catalog.disk_id("whole", &id).unwrap().unwrap();
        std::fs::remove_file(disk_id.path(&worker.store.root().join("whole"), "blk")).unwrap();

        assert!(worker
            .cached_block_range(&id, 0, 16)
            .await
            .unwrap()
            .is_none());
        assert!(catalog.recover("whole").unwrap().unwrap().is_empty());
        assert!(worker.index.is_empty());
        assert_eq!(worker.lru.total_bytes(), 0);
    }

    #[tokio::test]
    async fn journaled_pages_checkpoint_and_restore_with_tti_disabled() {
        let root = tempfile::tempdir().unwrap();
        let id = block();
        let data = Bytes::from_static(b"data");
        let (worker, catalog, _) = runtime(root.path());
        assert_eq!(worker.current_tti_ms(), 0);
        let now = worker.page_clock.now() - 2;
        worker.page_clock.set(now);
        worker
            .commit_fetched_page(&id, PageIndex(0), 16, data.clone())
            .await
            .unwrap();
        worker
            .commit_fetched_page(&id, PageIndex(2), 16, data.clone())
            .await
            .unwrap();
        worker.page_clock.set(now + 1);
        assert_eq!(
            worker.cached_page(&id, PageIndex(2), 16).await.unwrap(),
            Some(data)
        );
        let state = worker.page_lifecycle.block(&id);
        assert_eq!(state.last_access(0), Some(now));
        assert_eq!(state.last_access(2), Some(now + 1));
        assert_eq!(worker.checkpoint_access_times().await.blocks, 1);
        worker.drain_page_mutations().await;
        drop(state);
        drop(worker);
        drop(catalog);

        let (worker, _, _) = runtime(root.path());
        let state = worker.page_lifecycle.block(&id);
        assert_eq!(state.last_access(0), Some(now));
        assert_eq!(state.last_access(2), Some(now + 1));
        let candidate = state.candidate(PageIndex(0)).unwrap();
        assert!(worker.evict_page_candidate(candidate, 2).await.is_some());
        assert_eq!(worker.checkpoint_access_times().await.blocks, 1);
        assert_eq!(state.page_keys(), vec![2]);
        let candidate = state.candidate(PageIndex(2)).unwrap();
        assert!(worker.evict_page_candidate(candidate, 2).await.is_some());
        worker.checkpoint_access_times().await;
        let paged = worker.paged.as_ref().unwrap();
        let shard = crate::page_lifecycle::disk_shard(&id);
        let dir = paged.root().join(format!("{shard:02x}"));
        let snapshot =
            crate::page_access_shard::load(&dir, shard, paged.page_size(), now + 1).unwrap();
        assert!(!snapshot.records.contains_key(&id));
        assert_eq!(worker.page_lifecycle.dirty_summary(), (0, None));
    }

    #[tokio::test]
    async fn paged_eviction_retains_catalog_failure_accounting() {
        for stale in [false, true] {
            for poisoned in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let (worker, catalog, config) = runtime(root.path());
                worker
                    .commit_fetched_page(&block(), PageIndex(0), 16, Bytes::from_static(b"data"))
                    .await
                    .unwrap();
                let allocated = worker.resident_bytes();
                assert!(allocated > 0);
                let disk_id = catalog.disk_id("paged", &block()).unwrap().unwrap();
                let path = disk_id
                    .path(worker.paged.as_ref().unwrap().root(), "pages")
                    .join("0.page");
                if poisoned {
                    catalog.poison();
                }

                evict(&worker, &config, stale).await;

                assert_eq!(path.exists(), poisoned);
                assert_eq!(worker.index.get(&block()).is_some(), poisoned);
                assert_eq!(!worker.lru.is_empty(), poisoned);
                assert_eq!(
                    worker.resident_bytes(),
                    if poisoned { allocated } else { 0 }
                );
                if poisoned {
                    assert_eq!(
                        worker
                            .cached_page(&block(), PageIndex(0), 16)
                            .await
                            .unwrap()
                            .unwrap(),
                        Bytes::from_static(b"data")
                    );
                } else {
                    assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
                }
            }
        }
    }
}
