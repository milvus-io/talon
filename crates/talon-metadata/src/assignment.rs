//! Write-shard ownership: desired ranking and effective assignment
//! (ADR 0003 §9.2, §9.3).
//!
//! # The distinction the design rests on
//!
//! > HRW produces a **desired assignment**, not an immediately effective one. A
//! > worker joining or leaving changes the desired ranking, but no client may act
//! > on that new ranking until the affected shard has completed handoff or
//! > recovery and TMS has committed the new effective assignment.
//!
//! A membership change is instantaneous and unilateral: every coordinator
//! recomputes the same new ranking the moment a worker's lease expires. Acting
//! on that directly would hand a shard to a new owner while the old one still
//! holds un-flushed bytes for it.
//!
//! So [`DesiredAssignment`] and [`WriteShardDescriptor`] are separate types.
//! Only the second answers "who owns this shard", and it exists only once TMS
//! has committed it. That is a type-level boundary rather than a convention,
//! because the two are otherwise easy to mix up: both are "the owner", and one
//! of them is a computation while the other is a fact.
//!
//! # The fencing term
//!
//! > `term` is a monotonically increasing fencing number. It **must survive
//! > owner lease expiry**; an ephemeral owner/session key may disappear on
//! > expiry, but the last committed term and replica history must remain.
//!
//! And the rule that makes it a fence rather than a label:
//!
//! > Expiry of the TMS owner session, or a fresh ADR 0001 snapshot that marks
//! > the worker unhealthy, permits a coordinator to propose reassignment.
//! > **Neither event grants ownership by itself**: the TMS compare-and-swap must
//! > increment the fencing term.
//!
//! A paused or partitioned old owner may still believe it holds the shard. What
//! stops it writing is not the loss of its lease — which it may not have noticed
//! — but that workers reject operations carrying a term below the highest they
//! have seen.

use core::fmt;

use crate::error::{MetadataError, MetadataResult};
use crate::shard::WriteShard;

/// A worker eligible to own or replicate a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleWorker {
    /// Stable worker identity.
    pub id: String,
    /// Failure domain, e.g. host or zone.
    ///
    /// Replica selection separates by this, so a single host or rack failure
    /// cannot take a write quorum with it.
    pub failure_domain: String,
}

/// A monotonically increasing fencing number for one shard.
///
/// Deliberately not `u64` so it cannot be confused with a mapping revision, a
/// capability revision, or a placement epoch. All four are counters and none is
/// interchangeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct FencingTerm(u64);

impl FencingTerm {
    /// The term of a shard that has never been assigned.
    pub const INITIAL: Self = Self(0);

    /// Construct from a raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw value, for durable encoding and the wire protocol.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next term.
    ///
    /// # Panics
    ///
    /// Panics on overflow. A wrapped term would compare as older than terms
    /// already persisted by workers, so a new owner's writes would be rejected
    /// while a stale owner's were accepted.
    #[must_use]
    pub const fn next(self) -> Self {
        match self.0.checked_add(1) {
            Some(value) => Self(value),
            None => panic!("fencing term overflowed"),
        }
    }
}

impl fmt::Display for FencingTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lifecycle state of a write shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShardState {
    /// Serving writes normally.
    Active,
    /// Handing off to a new owner; the old owner still serves while the new
    /// acting set receives the dirty manifest.
    Draining,
    /// Rebuilding after an abrupt owner loss.
    ///
    /// Workers return a retryable response; §9.8 bounds the client's retry at 30
    /// seconds per filesystem operation before surfacing `EAGAIN`.
    Recovering,
    /// Recovery could not assemble the acknowledged data.
    ///
    /// > If that quorum or a required payload cannot be obtained, the shard
    /// > remains `INCOMPLETE`; writes are refused and reads do not fall back to
    /// > the origin.
    ///
    /// Falling back would serve an older object as though it were current, which
    /// is why this is `EIO` rather than a retry: retrying cannot prove the
    /// acknowledged data is available.
    Incomplete,
}

impl ShardState {
    /// Whether the shard accepts new writes.
    pub const fn accepts_writes(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether a client should retry rather than fail.
    ///
    /// `Recovering` is transient and bounded. `Incomplete` is not: retry cannot
    /// establish that the data exists, so the honest answer is an error.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Recovering | Self::Draining)
    }
}

/// What HRW says *should* own a shard.
///
/// Not authoritative. Holding one of these means "if a transition happened now,
/// this is where the shard would go" — it never answers "who owns this shard".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredAssignment {
    /// Shard this ranking is for.
    pub shard: WriteShard,
    /// Desired primary.
    pub primary: String,
    /// Desired replicas, in descending score order and failure-domain separated.
    pub replicas: Vec<String>,
}

/// The committed ownership of a shard (ADR 0003 §9.3).
///
/// Authoritative, and only ever produced by a TMS commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShardDescriptor {
    /// Shard this describes.
    pub shard: WriteShard,
    /// Fencing term. Survives owner lease expiry.
    pub term: FencingTerm,
    /// Current lifecycle state.
    pub state: ShardState,
    /// Owning worker.
    pub owner: String,
    /// Process incarnation of the owner.
    ///
    /// Binds ownership to one process: a restarted worker returns with a new
    /// incarnation and does not inherit its predecessor's authority.
    pub owner_incarnation: String,
    /// Acting set before the most recent transition, retained for recovery.
    pub previous_acting_set: Vec<String>,
    /// Workers currently holding replicas.
    pub acting_set: Vec<String>,
    /// Configured replica count.
    pub replication_factor: u8,
    /// Durable copies required before acknowledging a write.
    pub write_quorum: u8,
}

impl WriteShardDescriptor {
    /// Recovery responses required from the previous acting set.
    ///
    /// `Q = R - W + 1`, which makes `W + Q > R` and therefore guarantees the
    /// recovery responses intersect every write that could have been
    /// acknowledged. With `R = 3, W = 2` this is 2.
    pub const fn recovery_quorum(&self) -> u8 {
        self.replication_factor - self.write_quorum + 1
    }

    /// Whether `term` is current for this shard.
    ///
    /// Workers persist the highest term seen and reject anything lower. This is
    /// what stops a paused or partitioned old owner from mutating data after
    /// reassignment — it may not have noticed losing its lease, but its term is
    /// stale regardless.
    pub const fn accepts_term(&self, term: FencingTerm) -> bool {
        term.get() >= self.term.get()
    }
}

/// Rank eligible workers for a shard by HRW score.
///
/// > score = H(placement_scheme || shard_id || worker_id)
///
/// Deterministic, so every active-active coordinator computes the same ranking
/// from the same worker set. Note what is *not* an input: §9.2 excludes rapidly
/// changing load measurements, "because they would continually move ownership" —
/// a shard whose owner moves with load could never keep a stable dirty set.
///
/// Replicas are failure-domain separated: the first worker in each new domain is
/// taken until `replication_factor` is reached.
///
/// # Errors
///
/// Returns [`MetadataError::InvalidRecord`] when no worker is eligible, or when
/// distinct failure domains cannot satisfy `replication_factor`. Silently
/// returning a co-located replica set would make `write_quorum` a fiction: one
/// host failure could take every acknowledged copy.
pub fn rank(
    shard: WriteShard,
    workers: &[EligibleWorker],
    replication_factor: u8,
) -> MetadataResult<DesiredAssignment> {
    if workers.is_empty() {
        return Err(MetadataError::InvalidRecord {
            detail: format!("no eligible workers for shard {shard}"),
        });
    }

    let mut scored: Vec<(u64, &EligibleWorker)> = workers
        .iter()
        .map(|worker| (score(shard, &worker.id), worker))
        .collect();
    // Descending score; worker id breaks ties so the order is total and every
    // coordinator agrees even on a hash collision.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));

    let primary = scored[0].1;
    let mut domains = vec![primary.failure_domain.as_str()];
    let mut replicas = Vec::new();
    for (_, worker) in scored.iter().skip(1) {
        if replicas.len() + 1 >= replication_factor as usize {
            break;
        }
        if domains.contains(&worker.failure_domain.as_str()) {
            continue;
        }
        domains.push(&worker.failure_domain);
        replicas.push(worker.id.clone());
    }

    if replicas.len() + 1 < replication_factor as usize {
        return Err(MetadataError::InvalidRecord {
            detail: format!(
                "shard {shard} needs {replication_factor} failure domains, found {}",
                replicas.len() + 1
            ),
        });
    }

    Ok(DesiredAssignment {
        shard,
        primary: primary.id.clone(),
        replicas,
    })
}

/// Deterministic HRW score for one (shard, worker) pair.
fn score(shard: WriteShard, worker_id: &str) -> u64 {
    let mut buf = Vec::with_capacity(16 + worker_id.len());
    buf.extend_from_slice(&PLACEMENT_SCHEME.to_le_bytes());
    buf.extend_from_slice(&shard.get().to_le_bytes());
    buf.extend_from_slice(&(worker_id.len() as u64).to_le_bytes());
    buf.extend_from_slice(worker_id.as_bytes());
    xxhash_rust::xxh3::xxh3_64(&buf)
}

/// Version of the placement scheme, mixed into every score.
///
/// Separate from the shard-hash scheme version: the two may change
/// independently, and conflating them would force a reshard to change a
/// placement rule.
const PLACEMENT_SCHEME: u32 = 1;

/// Propose the descriptor that would result from transferring a shard.
///
/// Always increments the term. §9.3: expiry of the owner session or an unhealthy
/// membership snapshot "permits a coordinator to propose reassignment. Neither
/// event grants ownership by itself: the TMS compare-and-swap must increment the
/// fencing term."
///
/// This returns a *proposal*; only a successful TMS compare-and-swap makes it
/// effective, which is what resolves races between active-active coordinators.
pub fn propose_transfer(
    current: &WriteShardDescriptor,
    desired: &DesiredAssignment,
    new_owner_incarnation: impl Into<String>,
    state: ShardState,
) -> WriteShardDescriptor {
    WriteShardDescriptor {
        shard: current.shard,
        term: current.term.next(),
        state,
        owner: desired.primary.clone(),
        owner_incarnation: new_owner_incarnation.into(),
        // Retained so recovery can interrogate the workers that may hold
        // acknowledged writes. Dropping this would make the recovery quorum
        // unanswerable.
        previous_acting_set: current.acting_set.clone(),
        acting_set: core::iter::once(desired.primary.clone())
            .chain(desired.replicas.iter().cloned())
            .collect(),
        replication_factor: current.replication_factor,
        write_quorum: current.write_quorum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workers(specs: &[(&str, &str)]) -> Vec<EligibleWorker> {
        specs
            .iter()
            .map(|(id, domain)| EligibleWorker {
                id: (*id).to_owned(),
                failure_domain: (*domain).to_owned(),
            })
            .collect()
    }

    fn descriptor() -> WriteShardDescriptor {
        WriteShardDescriptor {
            shard: WriteShard::from_index(7),
            term: FencingTerm::new(4),
            state: ShardState::Active,
            owner: "w0".to_owned(),
            owner_incarnation: "inc-0".to_owned(),
            previous_acting_set: Vec::new(),
            acting_set: vec!["w0".to_owned(), "w1".to_owned(), "w2".to_owned()],
            replication_factor: 3,
            write_quorum: 2,
        }
    }

    #[test]
    fn every_coordinator_ranks_the_same_worker_set_identically() {
        // Active-active coordinators must agree without coordinating; if they
        // did not, two could propose conflicting transfers and rely on TMS to
        // arbitrate every time.
        let shard = WriteShard::from_index(42);
        let set = workers(&[("w0", "a"), ("w1", "b"), ("w2", "c")]);
        let first = rank(shard, &set, 3).expect("ranked");
        let mut shuffled = set.clone();
        shuffled.reverse();
        assert_eq!(rank(shard, &shuffled, 3).expect("ranked"), first);
    }

    #[test]
    fn replicas_are_failure_domain_separated() {
        // Two replicas on one host would make write_quorum a fiction: a single
        // host failure could take every acknowledged copy.
        let set = workers(&[
            ("w0", "host-a"),
            ("w1", "host-a"),
            ("w2", "host-b"),
            ("w3", "host-c"),
        ]);
        let assignment = rank(WriteShard::from_index(1), &set, 3).expect("ranked");
        let domains: Vec<&str> = core::iter::once(assignment.primary.as_str())
            .chain(assignment.replicas.iter().map(String::as_str))
            .map(|id| {
                set.iter()
                    .find(|w| w.id == id)
                    .expect("known worker")
                    .failure_domain
                    .as_str()
            })
            .collect();
        let mut unique = domains.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), domains.len(), "domains repeated: {domains:?}");
    }

    #[test]
    fn too_few_failure_domains_is_an_error_not_a_co_located_set() {
        // Returning a co-located set would silently weaken durability while
        // still reporting the configured replication factor.
        let set = workers(&[("w0", "host-a"), ("w1", "host-a"), ("w2", "host-a")]);
        assert!(rank(WriteShard::from_index(1), &set, 3).is_err());
    }

    #[test]
    fn an_empty_worker_set_is_an_error() {
        assert!(rank(WriteShard::from_index(1), &[], 1).is_err());
    }

    #[test]
    fn a_transfer_always_increments_the_term() {
        // §9.3: neither session expiry nor an unhealthy snapshot grants
        // ownership by itself. Without the increment, a paused old owner's
        // writes would still carry a current term and be accepted.
        let current = descriptor();
        let desired = DesiredAssignment {
            shard: current.shard,
            primary: "w1".to_owned(),
            replicas: vec!["w2".to_owned(), "w3".to_owned()],
        };
        let proposed = propose_transfer(&current, &desired, "inc-9", ShardState::Draining);
        assert_eq!(proposed.term, FencingTerm::new(5));
        assert!(proposed.term > current.term);
    }

    #[test]
    fn a_transfer_retains_the_previous_acting_set() {
        // Recovery interrogates those workers for acknowledged writes. Dropping
        // the list would make the recovery quorum unanswerable.
        let current = descriptor();
        let desired = DesiredAssignment {
            shard: current.shard,
            primary: "w1".to_owned(),
            replicas: vec!["w2".to_owned(), "w3".to_owned()],
        };
        let proposed = propose_transfer(&current, &desired, "inc-9", ShardState::Draining);
        assert_eq!(proposed.previous_acting_set, current.acting_set);
    }

    #[test]
    fn a_stale_term_is_rejected() {
        // The fence itself. A partitioned old owner may not know it lost the
        // shard; what stops it is that its term is below the current one.
        let descriptor = descriptor();
        assert!(descriptor.accepts_term(FencingTerm::new(4)));
        assert!(descriptor.accepts_term(FencingTerm::new(5)));
        assert!(!descriptor.accepts_term(FencingTerm::new(3)));
    }

    #[test]
    fn the_recovery_quorum_intersects_every_acknowledged_write() {
        // W + Q > R is the property that makes recovery correct. With R=3, W=2:
        // Q=2, and 2+2 > 3.
        let descriptor = descriptor();
        let q = descriptor.recovery_quorum();
        assert_eq!(q, 2);
        assert!(
            descriptor.write_quorum + q > descriptor.replication_factor,
            "W + Q must exceed R or recovery can miss an acknowledged write"
        );
    }

    #[test]
    fn only_active_shards_accept_writes() {
        assert!(ShardState::Active.accepts_writes());
        for state in [
            ShardState::Draining,
            ShardState::Recovering,
            ShardState::Incomplete,
        ] {
            assert!(!state.accepts_writes(), "{state:?} must not accept writes");
        }
    }

    #[test]
    fn an_incomplete_shard_is_not_retryable() {
        // §9.8: an INCOMPLETE shard returns EIO "because retry alone cannot
        // prove that the acknowledged data is available". Marking it retryable
        // would spin a client until its deadline and then report a timeout,
        // hiding a durability problem behind a latency one.
        assert!(!ShardState::Incomplete.is_retryable());
        assert!(ShardState::Recovering.is_retryable());
    }
}
