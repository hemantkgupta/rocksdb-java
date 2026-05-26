# ADR-0005: Per-host shared RateLimiter scope (process-wide; per-instance ceilings clamp downward)

- Status: accepted
- Date: 2026-05-27
- Phase: 3
- CP: 13

## Context

§4 of the FAST 2021 paper (Resource management) walks through the operational reality
that drove the shared-budget design. A single host commonly runs tens of RocksDB
instances — Tectonic-metadata shards, ZippyDB shards, Flink state stores — each with
its own compaction queue and foreground writes.

Per-instance RateLimiters sized for "this instance's share of host bandwidth" produce
two failure modes:
1. **Over-provisioning.** N × per-instance can exceed actual host bandwidth under
   correlated load (every instance compacting after a write spike). SSD FTL journal
   pressure stalls foreground writes.
2. **Under-utilisation.** Conservatively sized budgets leave an instance unable to
   borrow idle peers' headroom; aggregate throughput sits below SSD capacity.

The paper's solution: scope the RateLimiter at the host (process) level. One
bytes-per-second budget, shared. Busy instances borrow from idle ones; under load, the
budget enforces fair share. Per-instance overrides clamp *downward* only.

## Decision

The `RateLimiter` is **process-scope by default**. A `RocksDbHost` is constructed once
per JVM and holds one `TokenBucketRateLimiter` at `hostBytesPerSecond`. Every `RocksDb`
opened against that host shares it.

Per-instance overrides clamp downward only:
- `DbOptions.rateLimiterCeilingBytesPerSecond` caps *that* instance's draw; it cannot
  raise it above the shared host limit.
- A dedicated per-instance limiter requires explicit `RocksDb.useDedicatedRateLimiter()`
  opt-out, which the host logs as a divergence from the shared-budget contract.

The limiter has two priorities — `HIGH` (foreground writes) and `LOW` (compaction and
file deletion). LOW yields to HIGH when both are queued. Compaction and file-deletion
bytes both flow through `LOW`; the `FileDeletionScheduler` (CP 16) drains via
`request(N, LOW)`.

`rocksdb-rate-limiter` exposes it; `rocksdb-engine` wires it; `rocksdb-test-cluster.
RocksDbHost` shares one instance across in-JVM handles for integration tests.

## Rationale

The §4 contract is "many instances per host, shared budget, no instance starves the
others." That requires a token bucket that fairly multiplexes concurrent requesters,
yield-on-priority so LOW cannot block HIGH arbitrarily long, and a scope above the
instance — per-instance can't enforce a host-level SSD-bandwidth invariant.

Per-instance ceilings exist for the realistic case of one runaway tenant: cap its draw
without dropping the host budget for everyone. They clamp downward because clamping
upward would let a tenant override a host-level safety invariant. The dedicated-limiter
opt-out exists for testing and single-tenant deployments, but is an opt-out — the safe
default is shared.

## Consequences

Positive:
- Aggregate host bytes-per-second is bounded under all multi-instance configurations.
- An idle instance's headroom is automatically available to a busy peer.
- File-deletion bytes share the same budget as compaction bytes, preventing the
  TRIM-burst pattern the §4 paper text calls out as causing FTL-journal pressure.
- The integration test (`rocksdb-test-cluster`, CP 24) can construct N instances against
  one limiter and assert aggregate throughput ≤ host budget directly.

Negative:
- Cross-instance contention is real: a misbehaving instance can consume more than its
  fair share before the limiter pushes back. The fairness window is tens of ms —
  adequate for compaction, potentially coarse for very bursty workloads.
- Shared state forces a `RocksDbHost` construct that didn't exist in LevelDB. The
  runtime/HTTP demo and test cluster must be host-aware.
- Even single-instance tests must go through a host or wiring becomes inconsistent.
  `RocksDbHost.standalone()` is a one-liner that constructs an unrate-limited host.

## Alternatives considered

- **Per-instance RateLimiter only.** Rejected: cannot enforce a host-level invariant;
  either over-provisions or wastes peer headroom — the failure mode §4 rejects.
- **No RateLimiter at all.** Rejected: §4 documents that an unthrottled compactor
  saturates the SSD's write path and stalls foreground I/O.
- **Per-instance ceilings that raise as well as lower.** Rejected: defeats the
  host-level safety invariant.
- **Separate limiters for compaction vs file-deletion.** Rejected: both consume the
  same SSD write/erase budget; one limiter enforces the joint constraint, with priority
  levels handling foreground-vs-background ordering.

## References

- Implementation plan §5.6 (TokenBucketRateLimiter), §10 (ADR 0005),
  Phase 3 CP 13 + Phase 6 CP 24.
- FAST 2021 paper §4 (Resource management — tens of instances per host, shared budget,
  TRIM-induced FTL-journal pressure).
- Repo: `rocksdb-rate-limiter`, `rocksdb-test-cluster.RocksDbHost`.
- Original RocksDB source: `util/rate_limiter.cc`, `include/rocksdb/rate_limiter.h`.
