//! Capacity targets, victim selection and packed-space reclamation.
use crate::small_store::SmallObjectStore;
use std::{collections::HashSet, future::Future, sync::Arc};
use tokio::sync::Mutex;

use crate::{background::BackgroundBudget, eviction::EvictionCandidate, BlockIndex, Lru};

#[derive(Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    Selecting {
        empty_scan_remaining: usize,
    },
    Reclaiming,
}

pub(super) struct CapacityPlan {
    index: Arc<BlockIndex>,
    lru: Arc<Lru>,
    small: Option<Arc<SmallObjectStore>>,
    phase: Mutex<Phase>,
}

impl CapacityPlan {
    pub(super) fn new(
        index: Arc<BlockIndex>,
        lru: Arc<Lru>,
        small: Option<Arc<SmallObjectStore>>,
    ) -> Self {
        Self {
            index,
            lru,
            small,
            phase: Mutex::new(Phase::Idle),
        }
    }

    /// Re-sample shared pressure after collecting credit; peers can reclaim during the await.
    pub(super) async fn target_for<'a>(
        &self,
        peers: impl Iterator<Item = &'a Self> + Clone,
        limit: u64,
    ) -> Option<u64> {
        if peers.clone().map(Self::resident_bytes).sum::<u64>() <= limit {
            return None;
        }
        let mut credit = 0;
        for peer in peers.clone() {
            if !std::ptr::eq(peer, self) {
                credit += peer.reclaim_credit().await;
            }
        }
        let total: u64 = peers.clone().map(Self::resident_bytes).sum();
        (total > limit).then(|| {
            self.resident_bytes()
                .saturating_sub((total - limit).saturating_sub(credit))
        })
    }

    pub(super) async fn finish_cycle(&self) {
        *self.phase.lock().await = Phase::Idle;
    }

    /// Routine maintenance keeps the same gate until its relocation work finishes.
    pub(super) async fn maintain_if_idle(&self, maintenance: impl Future<Output = ()>) {
        let phase = self.phase.lock().await;
        if *phase == Phase::Idle {
            maintenance.await;
        }
    }

    pub(super) fn resident_bytes(&self) -> u64 {
        let indexed = self.index.resident_bytes();
        match self.small.as_ref().and_then(|s| s.stats().ok()) {
            Some(stats) => indexed.saturating_add(stats.allocated_bytes),
            None => indexed,
        }
    }

    async fn reclaim_credit(&self) -> u64 {
        match self.small.as_ref() {
            Some(small) => {
                small.reclaimable_bytes().await + small.stats().map_or(0, |s| s.retired_bytes)
            }
            None => 0,
        }
    }

    /// Capacity reclamation for foreground admissions, bounded by initial residency.
    pub(super) async fn foreground<F: Future<Output = ()>>(
        &self,
        target: u64,
        mut unlink: impl FnMut(Vec<EvictionCandidate>) -> F,
    ) {
        let mut phase = if self.small.as_ref().is_some() {
            Some(self.phase.lock().await)
        } else {
            None
        };
        if let Some(phase) = phase.as_mut() {
            **phase = Phase::Selecting {
                empty_scan_remaining: self.lru.len().saturating_mul(2),
            };
        }
        let budget = self.lru.len();
        let mut attempted = HashSet::new();
        // Finish victim selection before relocation. Previously one compaction
        // per bounded batch could repeatedly copy the same surviving payloads.
        while attempted.len() < budget && self.resident_bytes() > target {
            let credit = self.reclaim_credit().await;
            let missing = self
                .resident_bytes()
                .saturating_sub(target)
                .saturating_sub(credit);
            if missing == 0 {
                break;
            }
            let mut evicted = self
                .lru
                .candidates_to_fit(self.lru.total_bytes().saturating_sub(missing), &attempted);
            evicted.truncate(budget - attempted.len());
            if evicted.is_empty() {
                break;
            }
            attempted.extend(evicted.iter().map(|c| c.unit.clone()));
            unlink(evicted).await;
        }
        if let Some(phase) = phase.as_mut() {
            **phase = Phase::Reclaiming;
        }
        let segments = self
            .small
            .as_ref()
            .and_then(|s| s.stats().ok())
            .map_or(0, |s| s.segments);
        for _ in 0..segments.saturating_add(1) {
            if self.resident_bytes() <= target || self.readers_cover_excess(target) {
                break;
            }
            match self.reclaim_to_target(target, None).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    tracing::warn!(%error, "small object pressure reclamation failed");
                    break;
                }
            }
        }
        if let Some(phase) = phase.as_mut() {
            **phase = Phase::Idle;
        }
    }

    pub(super) async fn background_batch<F: Future<Output = ()>>(
        &self,
        target: u64,
        limits: impl FnOnce() -> (usize, usize),
        budget: Option<Arc<BackgroundBudget>>,
        unlink: impl FnOnce(Vec<EvictionCandidate>) -> F,
    ) {
        let mut phase = if self.small.as_ref().is_some() {
            Some(self.phase.lock().await)
        } else {
            None
        };
        let mut local_phase = Phase::Idle;
        let phase = phase.as_deref_mut().unwrap_or(&mut local_phase);
        if self.resident_bytes() <= target {
            *phase = Phase::Idle;
            return;
        }
        if *phase == Phase::Idle {
            *phase = Phase::Selecting {
                empty_scan_remaining: self.lru.len().saturating_mul(2),
            };
        }
        let missing = self
            .resident_bytes()
            .saturating_sub(target)
            .saturating_sub(self.reclaim_credit().await);
        if missing == 0 {
            *phase = Phase::Reclaiming;
        }
        if let Phase::Selecting {
            empty_scan_remaining,
        } = phase
        {
            let policy_target = self.lru.total_bytes().saturating_sub(missing);
            let lru = self.lru.clone();
            let count = lru.len();
            let (scan, deletes) = limits();
            let units = tokio::task::spawn_blocking(move || {
                lru.candidates_to_fit_bounded(policy_target, &Default::default(), scan, deletes)
            })
            .await
            .expect("eviction scan panicked");
            // Keep a bounded no-progress walk across ticks. Hot reference bits
            // may be refreshed between every batch, so an individual scan need
            // never reach its forced second pass. Partial reclaim must still run.
            let logical_before = self.lru.total_bytes();
            unlink(units).await;
            if self.lru.total_bytes() >= logical_before {
                *empty_scan_remaining = empty_scan_remaining.saturating_sub(scan);
            } else {
                *empty_scan_remaining = count.saturating_mul(2);
            }
            let exhausted = *empty_scan_remaining == 0;
            let excess = self.resident_bytes().saturating_sub(target);
            if self.reclaim_credit().await >= excess || exhausted {
                // Exhausted unpinned victims may yield only partial progress.
                *phase = Phase::Reclaiming;
            } else {
                // Keep tombstones as credit across bounded selection batches.
                // Routine GC also defers relocation while this plan is active.
                return;
            }
        }
        if self.resident_bytes() <= target {
            *phase = Phase::Idle;
            return;
        }
        if self.readers_cover_excess(target) {
            return;
        }
        match self.reclaim_to_target(target, budget).await {
            Ok(true) => {} // Continue relocation next tick, without selecting more victims.
            Ok(false) => *phase = Phase::Idle,
            Err(error) => {
                tracing::warn!(%error, "small object background pressure reclamation failed")
            }
        }
    }
    async fn reclaim_to_target(
        &self,
        target: u64,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> std::io::Result<bool> {
        let excess = self.resident_bytes().saturating_sub(target);
        if excess == 0 {
            return Ok(false);
        }
        match self.small.as_ref() {
            Some(small) => small.reclaim_with_budget(excess, budget).await,
            None => Ok(false),
        }
    }

    // Already unlinked inodes cannot be freed by evicting more objects. Wait for
    // their readers when they cover the remaining excess over the target.
    fn readers_cover_excess(&self, target: u64) -> bool {
        self.small
            .as_ref()
            .and_then(|s| s.stats().ok())
            .is_some_and(|s| {
                let excess = self.resident_bytes().saturating_sub(target);
                excess > 0 && s.retired_bytes >= excess
            })
    }
}
