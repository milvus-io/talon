# ADR 0002: Write-Cache Durability and Consistency Contract

- Status: Accepted
- Date: 2026-07-28
- Tracking issue: #274 (roadmap item 4)
- Decision issue: #361
- Mechanism (unwired): #358 / #360

## Context

Talon v1 is a read-through cache. The blob store is the durable source, the
cache is disposable, and that single fact is what buys RF=1, an eventually
consistent placement table, a rebuildable coordinator, and no write-ahead log.

The write path that exists today is strictly **write-through**:

1. `talon-fuse` buffers the whole object in client memory as it is written
   (`ops.rs`, capped at `DEFAULT_MAX_OBJECT_BYTES`).
2. At `flush`/`fsync` the client PUTs the assembled object to the object's
   owning worker.
3. `WorkerRuntime::write_object` uploads to the origin and, only on success,
   caches the bytes under the version the store assigned.

The backend upload *is* the durability point. Nothing is acknowledged before it,
so there is no worker-side dirty state and nothing to lose on a node failure.

Roadmap item 4 asks for a write cache with durability and consistency
guarantees. That request inverts the central v1 assumption, so this ADR fixes
the contract before any of it becomes reachable behavior. The local mechanism —
durable staging, crash recovery, a flush queue with per-object coalescing
(#360) — has deliberately landed **unwired**, so that shipped behavior does not
pre-empt the decision recorded here.

Two constraints frame the decision:

- **NVMe is not durability.** A write acknowledged from one node's local disk is
  lost when that node is lost. Any contract that acknowledges before the origin
  has the bytes is a promise about *replication*, not about disks.
- **ADR 0001 excluded consensus conditionally.** It rules out leader election
  and Raft "unless the coordinator later owns durable, non-rebuildable metadata
  such as write-back state." Write-back is precisely that trigger condition, so
  this ADR must either avoid the trigger or amend ADR 0001. It does the former.

## Decision

### 1. Write-through is the only durability contract Talon promises

Talon's supported write contract is:

> A write is durable when, and only when, the origin object store has
> acknowledged it. Talon never reports a write as complete before that.

The acknowledgement point is the client's `flush`/`fsync`/`close`, and it
returns only after the origin PUT succeeds. A failure surfaces to the
application as an error on that call — the operation the application already
has to check.

This is deliberately the *least* interesting choice of the three, and it is
correct for the current stage:

- It captures most of the user-visible value of a write cache. Applications work
  unmodified because POSIX writes work, which is what adoption actually
  requires.
- It carries essentially none of the risk. There is no un-flushed state, so node
  loss cannot lose acknowledged data, and the cache stays disposable.
- It preserves RF=1, the rebuildable coordinator, and the absence of a WAL —
  four simplifications that write-back would break simultaneously.

Write-around (bypass the cache, write straight to the origin) is not offered
separately: write-through with post-write caching is strictly better for the
read-after-write pattern Talon exists to serve, at the same durability.

### 2. Write-back is not offered until replication exists

Write-back — acknowledging on local durability and uploading later — is a
legitimate future contract, but it is only honest when the local copy survives
the loss of the node that holds it. This ADR therefore records write-back as
**deferred, with explicit entry conditions** rather than as rejected.

Write-back may only become available when all of the following hold:

1. **Replication before acknowledgement.** A write is acknowledged only after
   its bytes are durable on at least `W` distinct workers in distinct failure
   domains, with `W >= 2`. Single-replica write-back is not an acceptable
   configuration, not even opt-in, because no configuration flag makes "your
   data is on one disk" into durability.
2. **Bounded, observable dirty state.** A cap on un-flushed bytes per node and
   per cluster, with writes throttled or refused at the cap rather than growing
   without bound, and `dirty_bytes` alarmed on.
3. **Crash recovery proven by test.** Recovery from a mid-write interrupt, a
   mid-flush interrupt, and a node replacement, each covered by a test that
   kills a process rather than simulating the kill.
4. **A defined answer to permanent flush failure.** See §5.
5. **Its own ADR.** Enabling write-back changes the durability promise this ADR
   makes, so it supersedes this decision rather than extending it.

Until then, the flush queue and staging machinery in `talon-worker` stay
unreferenced by the serve path. Their value now is that they exist, are tested,
and constrain the design space — not that they run.

### 3. Read-your-writes is the consistency contract

Talon promises, for the write-through path:

- **Read-your-writes.** After a write acknowledges, any subsequent read through
  Talon — from the same client or another — observes that write or something
  newer. This follows from §1: an acknowledged write is already at the origin,
  so no reader can observe an older state without violating the origin's own
  consistency.
- **Monotonic reads.** A reader never observes an object move backwards. The
  version guard in `BlockId` provides this: blocks are keyed by the resolved
  ETag/generation, and `write_object` refreshes the version cache and evicts
  superseded blocks on commit, so the post-write version is what subsequent
  reads resolve.
- **No cross-client write atomicity beyond the origin's.** Concurrent writers to
  one object resolve exactly as the origin resolves them — last writer wins,
  under the origin's own semantics. Talon adds no locking, no ordering, and no
  merge. `put_if_match` is available for a caller that wants compare-and-swap,
  but the FUSE path does not impose it, because POSIX has no such semantic to
  map onto.

Explicitly **not** promised: byte-range atomicity for writes larger than one
object PUT, POSIX `O_APPEND` semantics across clients, and any visibility
guarantee for data that has been written to a file descriptor but not yet
flushed. The last is standard POSIX — unflushed data is not durable — but it is
worth stating because a whole-object buffering model makes the window larger
than a local filesystem's.

### 4. The kernel page cache is part of the contract

A correctness note that a worker-side contract alone does not cover: the FUSE
mount's page-cache policy determines whether a reader on the *same node* even
reaches Talon after a write elsewhere. Read-your-writes as stated in §3 is a
promise about reads that reach the mount's read path. Where the kernel may serve
a cached page across opens, the promise is bounded by that cache's lifetime.

This ADR does not resolve kernel-cache coherence; it records that the freshness
work (roadmap item 5) owns it, and that item 5's contract must be stated in the
same terms as §3 or the two will disagree.

### 5. Failure semantics

Under write-through, the failure matrix is small, which is the point:

| Failure | Behavior |
| --- | --- |
| Origin PUT fails | The error surfaces on `flush`/`fsync`/`close`. Nothing is cached; a failed write is never silently cached. |
| Worker unreachable at flush | Same: the write fails at the acknowledgement point. The application sees an error and owns the retry. |
| Client crashes before flush | Buffered data is lost, exactly as with an unflushed write to a local filesystem. No partial object is committed to the origin. |
| Node loss | No acknowledged data is at risk, because acknowledgement requires the origin. |
| Flush fails after a partial upload | The origin PUT is atomic per object; a failed PUT leaves the previous version intact. |

The retry question that would dominate a write-back design — "the flush to the
origin has failed 500 times, who is told, and when do we give up?" — does not
arise here, because there is no acknowledged-but-unflushed state to retry. That
absence is a substantial part of why §1 chooses write-through.

If write-back is later adopted, §2.4 requires answering it explicitly: a bounded
retry ladder, a terminal state that is visible in metrics and logs, and a
documented operator procedure for draining or abandoning undrainable dirty
state. "Retry forever" is not an answer, because it converts a write failure
into an unbounded capacity leak.

### 6. Interaction with fail-open

Roadmap item 1 makes any cache failure degrade to a direct origin read. That is
compatible with §1 without qualification: under write-through, the origin is
never behind Talon, so falling back to it can never serve data older than an
acknowledged write.

This compatibility is a **property of write-through, not a coincidence**, and it
is the second reason to sequence write-back last. Under write-back, "fall back
to the origin" and "there is data only in the cache" are contradictory, and
fail-open would have to be selectively disabled for objects with un-flushed
state — that is, the availability mechanism would have to be switched off
exactly for the objects most at risk. Any future write-back ADR must show how it
avoids that.

### 7. Interaction with data freshness

A locally-written object differs from a backend-sourced one: its version is
known exactly, from the origin's PUT response, rather than inferred from a
`HEAD`. The write path already exploits this — `write_object` stores the
returned version and evicts superseded blocks — so a write does not need
revalidation to be correct.

The freshness work must preserve this distinction rather than apply a uniform
revalidation TTL: a just-written object needs no `HEAD` to be known-fresh, and
paying one would be both a cost and a correctness regression if it raced with a
concurrent overwrite.

### 8. ADR 0001 is not amended

ADR 0001 conditions its exclusion of consensus on the coordinator owning
durable, non-rebuildable metadata "such as write-back state". Because §1 and §2
keep write-back out of the product, that condition is not met:

- No coordinator-side dirty state exists, so there is no non-rebuildable
  metadata.
- Coordinators remain stateless and disposable.
- Leader election and Raft remain correctly out of scope.

This ADR therefore **confirms** ADR 0001 rather than amending it. A future
write-back ADR must revisit ADR 0001 as part of its own scope — that
prerequisite is recorded here so it cannot be skipped later.

## Consequences

### Positive

- The durability promise is one sentence and is true without qualification,
  configuration caveats, or a failure-domain argument.
- v1's simplifying assumptions survive: RF=1, ephemeral cluster state, no WAL,
  disposable coordinators, disposable cache.
- Fail-open composes with the write path without special cases.
- POSIX write support — the actual adoption blocker — is delivered.
- The write-back mechanism is built and tested, so a future decision to enable
  it is a wiring and replication problem, not a from-scratch design problem.

### Costs and risks

- **Writes get no speedup.** A write costs an origin round trip; Talon
  accelerates reads only. Workloads that are write-latency-bound see no benefit,
  and this must be stated plainly in user-facing docs rather than left to be
  discovered.
- **Whole-object PUT is the commit granularity.** Small appends to a large
  object rewrite the whole object. This is an object-store property, not a Talon
  one, but Talon's POSIX surface makes it easy to hit unknowingly.
- **The client-side buffer bounds object size.** A whole object is buffered
  before the PUT, capped to keep an unprivileged `pwrite` at a large offset from
  exhausting host memory. Sparse and very large writes need a streaming or
  chunked representation (#347) independent of this ADR.
- **`flush` and `fsync` each PUT.** An application that calls `fsync` and then
  `close` uploads twice. Harmless for correctness, wasteful under write-heavy
  workloads; worth optimizing separately.

## Rejected alternatives

### Write-back now, replication later

Rejected. It promises durability that the system does not provide, and the gap
is invisible until a node is lost — the worst possible failure signature, since
nothing is wrong until data is gone. Shipping it with a warning in the docs does
not fix this; operators who enable a flag named "fast writes" do not read the
paragraph explaining that acknowledged data can vanish.

### Write-back behind an "unsafe/experimental" flag

Rejected for the same reason, one step removed. An experimental flag is a
support commitment with the safety properties removed, and it invites exactly
the benchmark-driven adoption ("we enabled the fast mode") that produces data
loss in production.

### Write-around (bypass the cache entirely)

Rejected as a separate mode. It has the same durability as write-through but
gives up the read-after-write hit for no compensating benefit. Write-through
with post-write caching dominates it.

### Coordinator-owned dirty-state tracking

Rejected in this iteration. It is the natural design once write-back exists —
some component must know which objects are un-flushed and where — but it turns
the coordinator into a durable metadata owner and pulls in consensus, which is
the ADR 0001 trigger. Deferring write-back defers this decision with it.

### Making read-your-writes configurable

Rejected. A weaker consistency mode would only be meaningful under write-back,
and a cache that can serve a stale version of an object the caller just wrote is
not usefully weaker — it is wrong in a way applications cannot compensate for.

## References

- Roadmap item 4: milvus-io/talon#274
- Write-back mechanism (unwired): milvus-io/talon#358, milvus-io/talon#360
- FUSE write path wiring: milvus-io/talon#232, milvus-io/talon#249
- Sparse/large-write representation: milvus-io/talon#347
- ADR 0001, on the consensus trigger condition:
  [`0001-management-plane-ha.md`](0001-management-plane-ha.md)
