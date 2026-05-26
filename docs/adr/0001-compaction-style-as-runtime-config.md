# ADR-0001: Compaction style as runtime configuration

- Status: accepted
- Date: 2026-05-27
- Phase: 3
- CP: 14

## Context

The FAST 2021 paper opens (§2.2) by laying out the workload diversity RocksDB is asked
to absorb: Stream Processing, Logging/Queues, Index Services, and on-SSD Cache. Table 2
shows each category bottlenecks on a different resource — Logging is write-throughput
bound and tolerates space waste, Index Services are point-read bound and tolerate write
amplification for low read amp, Caches turn over data quickly and need fast deletion
rather than rewriting.

§3 (Resource amplification trade-offs) then makes the trade-off explicit. Table 3
quantifies it across the three classical compaction styles: Leveled gives the best
space-amp (low) and read-amp (low) at the cost of high write-amp (~16×); Tiered gives
the best write-amp (~5×) at the cost of much higher space-amp (~45%) and read-amp; FIFO
does no rewriting at all (write-amp ~2×) but provides no key-level retention guarantee.
There is no single point in this space that wins for every workload.

A single binary embedded in 30+ Meta applications and 39 ZippyDB deployments (§1) cannot
hard-wire one of these. The engine must carry all of them and let the embedding system
choose at open time, per column family.

## Decision

`CompactionStyle` is a per-CF runtime option with four values: `LEVELED`,
`DYNAMIC_LEVELED`, `TIERED`, `FIFO`. The engine ships all four picker implementations.
At CF open, the registry instantiates the picker that matches the configured style; the
compaction queue and worker pool are shared across CFs.

The four pickers live in three separate Gradle modules
(`rocksdb-compaction-leveled` covers both leveled and dynamic-leveled,
`rocksdb-compaction-tiered`, `rocksdb-compaction-fifo`), all depending on
`rocksdb-compaction-api`. Storage (`rocksdb-sstable-blockbased`) does not know which
picker consumes it. The style is also recorded in the MANIFEST via a
`SetCompactionStyle` edit so it survives restarts and can be inspected by
`manifest-dump`.

## Rationale

The paper's framing is that the engine is one component embedded in many
cluster-control-planes (§2 closing paragraph). Each adopter makes its own
RUM-conjecture trade-off; the engine must let them. Hard-coding leveled (as LevelDB did)
would lose the logging and caching workloads; hard-coding tiered would lose MyRocks.
Carrying all four costs four picker implementations — small relative to the engine — and
buys multi-tenant adoption.

The acyclic-DAG module layout (§3.2 of the plan) keeps the cost contained: each picker
is an independent compilation unit, testable in isolation
(`./gradlew :rocksdb-compaction-tiered:test`).

## Consequences

Positive:
- Same JAR serves OLTP, stream-state, and warm-cache workloads.
- A misconfigured CF can be re-opened with a different style (one MANIFEST edit) without
  re-formatting on-disk data — SSTables are picker-agnostic.
- Tests can exercise each picker against a synthetic workload that matches the row of
  Table 3 it targets.

Negative:
- Four pickers means four code paths to keep correct under range-tombstones, snapshots,
  and parallel L0 compaction. The compaction-api contract has to be tight enough that a
  picker bug doesn't corrupt the LSM.
- Cross-style behaviour (e.g., switching a CF from leveled to tiered mid-life) is not
  supported in v1 — the bottom-level layout assumptions differ. Style is set at CF
  creation.

## Alternatives considered

- **One style, hard-coded leveled** (LevelDB's choice). Rejected: loses the
  logging/queues workload class.
- **Build separate engines per style.** Rejected: defeats the §1 forcing function of one
  binary serving many tenants per host; multiplies operational surface.
- **Compaction style as a global engine option, not per-CF.** Rejected: CFs commonly
  represent different access patterns within one DB (an index CF next to a write-buffer
  CF), and Table 2 shows those patterns want different styles.

## References

- Implementation plan §6.3 (compaction protocol) and §10 (ADR 0001).
- FAST 2021 paper §2.2 (workload categories), §3 (Table 3 — write/read/space-amp by
  style), §1 (one engine, many embeddings).
- Repo: `rocksdb-compaction-api`, `rocksdb-compaction-leveled`,
  `rocksdb-compaction-tiered`, `rocksdb-compaction-fifo`.
- Original RocksDB source: `db/compaction/compaction_picker_*.cc`.
