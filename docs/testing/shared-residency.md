# Shared Worker residency metadata

The Worker should look up a block once and share its identity and runtime state
between residency, lifecycle and eviction. The motivating fixture has 1,934,887
blocks with one cached page each, small store disabled, and a 2-vCPU Worker.
The baseline restores in 19.29 s and uses 9.69 GiB RSS. These are checkpoint-based
startup measurements, not recovery from a long uncheckpointed journal.

The stack separates independently testable changes:

1. Share the block directory between BlockIndex and PageLifecycle.
2. Share page storage, preserving read guards and deletion generations.
3. Reuse resident objects for eviction membership; retain only necessary policy
   queues and secondary indexes.

The first layer stores optional residency and lifecycle components under one
canonical `Arc<BlockId>`. A pending fill can own lifecycle state without appearing
in residency lookups or byte/page counters. Removing residency does not replace
its mutation gate. Retirement removes the lifecycle component only when its page
state is empty, cleanup is complete, and no external owner retains the block.
Ordered shard traversal and access checkpoints continue to reference that object.

Residency replacement still retires the old index entry. Page updates that retained
that entry cannot mutate a new admission through an old metadata incarnation.
The current lock order is lifecycle registry -> block lifecycle -> index directory;
ordinary index reads do not acquire block lifecycle locks. Sharing page storage
must update this order before introducing those reads.

The journal, access snapshot and request formats are unchanged. Tests cover pending
fills excluded from residency, gate identity across removal/reinsertion, retirement
with external owners, and scanning abandoned fills. The first layer passed all
650 Worker library tests (8 ignored) and Clippy with warnings denied in the prescribed
container. The test container permits io_uring; Docker's default seccomp profile
otherwise rejects its creation with EPERM.

Re-run the same fixture after the complete stack and report startup, steady RSS,
restored counts and cached-content checks. A reduction below 1 GB is a target to
measure, not an outcome established by these structural changes.
