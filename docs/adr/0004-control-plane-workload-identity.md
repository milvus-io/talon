# ADR 0004: Coordinator-Worker Workload Identity and Authorization

- Status: Proposed
- Date: 2026-08-03
- Tracking issue: #434
- Blocks: #420 and ADR 0003 hard-link enablement
- Extends: ADR 0001 control-plane security and ADR 0003 section 5

## Context

ADR 0003 requires active coordinators to push namespace mapping revisions to
workers before a hard-link transition copies data. Every healthy
mutation-serving worker must acknowledge the revision and its current process
incarnation. The initiating coordinator counts those acknowledgements as the
distributed fence that excludes stale writes.

That exchange carries no object bytes or TMS credentials, but it is privileged.
A forged update can advance a worker's local guard and deny mutations. A forged
acknowledgement can be worse: it can let a transition proceed while a real
worker still accepts the old mapping revision.

The current coordinator-worker protocol cannot carry that authority safely:

- it uses plaintext TCP;
- worker control messages share the data-plane listener;
- a connection has no authenticated cluster, role, or node identity;
- registration fields are caller-supplied rather than bound to a credential;
- there is no namespace authorization decision at either endpoint.

The management HTTP listener's bearer token and reverse-proxy TLS do not solve
this problem. A shared bearer token does not identify an individual worker, and
the coordinator-worker protocol is raw framed TCP rather than HTTP. The
revision channel therefore needs its own workload-identity contract before the
hard-link capability can be enabled.

## Decision

### 1. Privileged service traffic uses a dedicated mTLS control channel

Coordinators and workers expose a control listener that is separate from the
worker data listener and the coordinator management HTTP listener. Every
connection on this listener uses TLS 1.3 with mutual certificate validation.
Plaintext fallback is not permitted on the same address.

The existing data listener may continue to serve ordinary range, PUT, DELETE,
`StatObject`, and `ListObjects` traffic during migration. It must reject mapping
revision updates, acknowledgements, transition commands, and future privileged
TMS operations. Network reachability to the data plane never grants control
authority.

Both endpoints trust an operator-configured CA bundle. Private keys and trust
bundles come from mounted files or an external workload-identity agent; they are
never accepted in TOML, command-line values, protocol messages, logs, metrics,
or management responses. Certificate and key files are reloaded atomically.
Reload failure retains the last valid material and reports not-ready for TMS
capabilities before its certificates expire.

### 2. URI SANs bind cluster, role, and node identity

Each leaf certificate has exactly one Talon workload URI SAN:

```text
spiffe://<trust-domain>/talon/<cluster-id>/<role>/<node-id>
```

`role` is `coordinator` or `worker`. Components compare the parsed SAN with
their configured trust domain and cluster ID. A coordinator accepts worker
registration or acknowledgements only from a `worker` SAN, and a worker accepts
revision updates only from a `coordinator` SAN. DNS names, source addresses,
common names, and caller-supplied message fields do not substitute for this
identity.

Cluster and node IDs are UTF-8 values encoded as canonical RFC 3986 path
segments. Reserved bytes use uppercase percent encoding; decoders reject
malformed, over-encoded, or non-canonical forms instead of treating two URI
spellings as the same identity.

The format is SPIFFE-compatible but does not require a SPIFFE deployment. An
operator may issue equivalent X.509 certificates with its existing private CA.
Deployments using SPIRE can supply the same URI SAN and rotate material through
the workload API or projected files.

The TLS identity is the authenticated node identity. A message containing a
different `cluster_id`, role, or `node_id` is rejected and audited. A worker
restart keeps its stable node ID but generates a new process incarnation ID;
the incarnation is status data bound to an authenticated node, not a second
credential.

### 3. Namespace authorization is explicit at both endpoints

Authentication answers which workload is connected; it does not grant every
workload access to every object-store namespace.

Each worker has an operator-configured set of canonical namespace grants. A
grant is a backend plus bucket/container and an optional path prefix. The worker
accepts a revision update or transition command only when the namespace is
within one of those grants.

Coordinators load the same policy and intersect it with the worker's
authoritative membership status. A coordinator sends an update only to healthy,
ready workers authorized to mutate that namespace, and counts an
acknowledgement only when the authenticated worker is in that set. Worker
self-reporting may narrow its grants for readiness, but cannot expand the
operator policy.

Policy changes are fail closed. Removing a grant immediately removes the worker
from the mutation-serving set for that namespace. Adding a grant does not admit
the worker until it has authenticated, reported its current incarnation, and
acknowledged the current mapping revision.

The initial policy is static configuration or mounted policy data. Dynamic
policy distribution may be added later, but must preserve the same local
enforcement and cannot make TMS a per-operation authorization dependency.

### 4. Revision messages are bound to the TLS session and incarnation

A revision update carries:

```text
cluster_id
namespace_id
mapping_revision
coordinator_id
coordinator_incarnation
```

The response on the same authenticated request carries:

```text
cluster_id
namespace_id
mapping_revision
worker_id
worker_incarnation
```

The receiver verifies message identity fields against the peer's URI SAN.
Coordinators additionally compare `worker_incarnation` with the authoritative
membership snapshot used to select the fence set. An acknowledgement from an
older or unknown incarnation is ignored and audited, even if the stable worker
ID matches.

Revision updates are monotonic and idempotent. A lower revision is ignored and
acknowledged with the worker's higher current revision; an equal revision
refreshes the guard deadline; a higher revision advances the guard. Because the
acknowledgement is the response to one authenticated request, it cannot be
detached and counted for a different worker or namespace. Replaying an old
request cannot move a guard backwards or satisfy a fence for a newer revision.

Acknowledgement sets remain transient coordinator state as required by ADR
0003. A replacement coordinator authenticates workers, reads the durable
transition, sends the revision again, and collects a fresh set.

### 5. Capability activation fails closed without this channel

The hard-link capability is advertised as usable only when all of the following
are true:

- the metadata backend supports the required transactions and is reachable;
- valid server and client workload certificates are loaded;
- namespace authorization policy is loaded;
- every healthy mutation-serving worker for the namespace has an authenticated
  control identity and a current guard acknowledgement.

A cluster may advertise that hard links are configured while reporting the
capability unavailable; clients then receive ADR 0003's retryable `EAGAIN`.
It must not report the capability reachable through a plaintext or
partially-authenticated channel.

Failure or absence of this channel does not change ordinary read-through or
write-through behavior in namespaces without TMS-backed features. Those paths
continue to use their existing availability contract. Privileged operations
fail closed rather than weakening authentication.

### 6. Rotation allows overlap, not identity ambiguity

Trust-bundle rotation uses an overlap window: deploy the new CA alongside the
old CA, rotate leaf certificates, then remove the old CA after every healthy
node reports the new issuer. Leaf rotation preserves the URI SAN identity and
does not change a worker incarnation.

Changing cluster, role, or node ID requires a new URI SAN and is an identity
change. A worker using the new identity must register and acknowledge guards as
a new member; acknowledgements from the previous identity do not transfer.

Expired certificates, unverifiable chains, missing URI SANs, multiple Talon URI
SANs, wrong EKUs, and identities outside the configured trust domain fail the
TLS or authorization check. They never degrade to plaintext. Rejections expose
bounded reason-labelled metrics and audit fields, but never certificate bodies,
private material, or bearer values.

## Consequences

- Revision propagation can satisfy ADR 0003 without giving workers TMS
  credentials or trusting data-plane clients.
- Coordinators and workers gain a separate listener, TLS configuration, policy
  loading, readiness signals, certificate reload, and rotation procedures.
- Existing deployments remain able to run ordinary cache workloads without
  configuring this channel, but cannot enable hard links or other privileged
  TMS features.
- A shared bearer token is intentionally insufficient for service identity.
- Namespace grants are duplicated at both ends. This is deliberate defense in
  depth: compromise or misconfiguration of one endpoint does not silently
  expand authority at the other.

## Rejected alternatives

### Reuse the plaintext data listener with network policy

Network policy is useful defense in depth but does not authenticate a process,
bind an acknowledgement to a worker incarnation, or prevent another workload
on an allowed network from forging a fence.

### Use one shared control-plane bearer token

A shared secret proves group membership, not node identity. Any compromised
worker could impersonate a coordinator or another worker and forge
acknowledgements. Per-node tokens would recreate certificate issuance,
distribution, and rotation with a less standard protocol.

### Terminate TLS at the management HTTP reverse proxy

The revision protocol is service-to-service framed TCP, and the worker also
needs to authenticate the coordinator. Routing it through the operator-facing
HTTP proxy would couple an internal correctness fence to the management UI
path and still require a separate workload identity handoff.

### Put TMS credentials on workers

This violates ADR 0003's management-tier boundary and expands the blast radius
of a worker compromise. Workers need only authenticated revision values and
bounded transition commands, not direct store access.

## Implementation sequence

1. Add TLS and identity configuration types with secret-redacting debug output.
2. Add dedicated coordinator and worker control listeners with mutual TLS.
3. Bind registration/status incarnation to the authenticated worker identity.
4. Add namespace policy loading and local authorization checks.
5. Add revision update/ack messages and the coordinator propagation loop.
6. Gate hard-link capability reachability on authenticated guard health.
7. Wire the ADR 0003 promotion/demotion path and run #420's pjdfstest gate.

Each item is independently reviewable. No item may enable hard links before
items 1 through 6 are complete.
