# ADR-0004: Configurable WAL — Sync / Buffered / Disabled per WriteOptions

- Status: accepted
- Date: 2026-05-27
- Phase: 6
- CP: 23

## Context

§4 of the FAST 2021 paper frames the WAL choice sharply: the engine ships embedded in
systems whose durability story varies widely. OLTP (MyRocks) needs every write fsynced
before ack — the WAL *is* the durability boundary. A replicated KV with its own
consensus log (ZippyDB, LogDevice) already has cluster-level durability; the RocksDB
WAL is pure overhead, and disabling it can roughly double write throughput. A bulk
loader filling a fresh DB tolerates losing the last few hundred ms of ingest on crash.

A single hard-coded WAL policy can't serve all three. Only the embedder knows whether a
higher-level log already guarantees no-data-loss across the failure modes RocksDB itself
can no longer recover from.

## Decision

`WriteOptions.walDurability` is a sealed interface with three implementations, selected
per write (not per DB):

| Mode | Semantics | Typical caller |
|---|---|---|
| **Sync** | Append record to WAL → `FileChannel.force(true)` → ack | OLTP, single-node KVs without higher-level replication |
| **Buffered** | Append record to in-memory buffer → ack immediately; background thread fsyncs every `WAL_FLUSH_INTERVAL_MS` (default 100 ms) | Bulk load, applications that tolerate ≤ 100 ms data-loss window on crash |
| **Disabled** | No WAL write at all; the MemTable insert is the only durability point until the next flush | Replicated systems whose consensus log already provides durability (LogDevice, Paxos KVs) |

The knob is per-`WriteOptions` rather than per-DB so a single application can mix modes:
a transactional path can use Sync while a bulk-index-rebuild path on the same DB uses
Buffered. The mode is *not* recorded in the WAL itself — it's a process-side decision
that affects when ack returns, not what gets written.

`rocksdb-wal` owns the three writer paths; `rocksdb-engine` plumbs `WriteOptions` to the
right writer for each call.

## Rationale

The §4 framing is precise: **the engine ships the knob; the operator owns the
boundary**. Disabling WAL is correct only if the embedder's replication protocol
guarantees no-data-loss across every replica failure RocksDB itself can no longer
recover from. The engine cannot verify that property, so the right architecture is to
expose the knob with a clear contract.

Per-write (not per-DB) granularity matters because real applications mix durability
requirements: ZippyDB runs Disabled for most writes but Sync for control-plane
operations whose loss would corrupt the cluster's view of itself. Forcing one mode per
DB would push callers to either run two DBs or accept the wrong trade-off on every
write.

## Consequences

Positive:
- Same JAR serves OLTP, bulk-load, and replicated-with-own-log adopters without
  rebuilding.
- Per-call granularity lets one application mix durability levels — control writes Sync,
  bulk writes Buffered, replicated writes Disabled — against one DB.
- Disabled mode opens a clean throughput ceiling: without the per-write fsync, writes
  cap on the MemTable insert path rather than the disk.

Negative:
- Three code paths to keep correct under crash and concurrent-write scenarios. The
  Buffered background flusher in particular has subtle flush-on-close semantics — tests
  assert that graceful close drains the buffer before returning.
- The Disabled contract is sharp: any post-last-flush write is lost on crash. The
  `Disabled` constructor takes a documentation-only parameter acknowledging the
  contract, and the runtime logs a warning on first use per DB.
- Sync and Buffered WALs are byte-identical on disk; only the ack-return timing
  differs. A crashed Buffered DB's lost-data window is invisible to recovery.

## Alternatives considered

- **Sync-only WAL** (LevelDB's default). Rejected: leaves throughput on the table for
  replicated systems and bulk loaders.
- **WAL fully disabled.** Rejected: breaks OLTP adopters; makes RocksDB unsuitable for
  single-node deployment.
- **Per-DB WAL mode only.** Rejected: real applications mix durability requirements;
  per-DB forces two DBs or a wrong-mode compromise.
- **Group commit only.** Rejected: doesn't address the replication-already-durable case;
  it's an optimisation within Sync, which Buffered subsumes.

## References

- Implementation plan §7.1 (WAL durability modes) and §10 (ADR 0004).
- FAST 2021 paper §4 (WAL treatment, configurable durability).
- Repo: `rocksdb-wal` (three writer paths), `rocksdb-engine` (WriteOptions plumbing).
- Original RocksDB source: `include/rocksdb/options.h` (`WriteOptions::sync`,
  `WriteOptions::disableWAL`), `db/db_impl/db_impl_write.cc`.
