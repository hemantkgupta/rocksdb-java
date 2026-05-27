# ADR-0015: Snapshot as a sequence-number freeze, per-instance only

- Status: accepted
- Date: 2026-05-27
- Phase: 2
- CP: 6

## Context

The engine assigns a monotonically increasing 56-bit sequence number to every
WriteBatch (§5.1, §5.5; the layout matches production RocksDB's
`SequenceNumber`). Every value lives in the MemTable and in SSTables tagged
with the sequence number at which it was written, and every read resolves
versions by "highest sequence ≤ some bound." A `get()` with no snapshot reads
at `versions.lastSequence()` — i.e., as of right now; a `get()` against a
snapshot reads at the snapshot's frozen sequence number. The data structure
already pays the cost of per-version tagging for crash recovery and for
compaction's tombstone-collection rules; consistent snapshots fall out of it
almost for free.

The leveldb-implementation-plan §5.5 (the snapshot lifecycle on the sibling
LevelDB port) captures the same primitive: a snapshot is "just a sequence
number plus a reference-count entry in `activeSnapshots`." It holds no
resources beyond a small map entry, it requires no copy of MemTable or SSTable
state, and it works because the storage layer never overwrites a versioned
value in place — newer writes append, older versions are visible until
compaction sweeps them.

The asymmetric cost is on **compaction**: a held snapshot pins tombstones and
older value versions whose sequence number sits between the snapshot and the
current `lastSequence`. The §6.3 compaction loop must consult
`activeSnapshots` to find the *oldest live snapshot sequence* before dropping
anything; the §9 failure-modes table flags "snapshot held too long" as a real
space-amp risk specifically because tombstones can't be GC'd past it.

Cross-process and cross-shard snapshots are categorically different. A single
RocksDB instance owns its own sequence space; another instance on another
host has its own counter, and "snapshot at seq 1234" on instance A means
nothing on instance B. Coordinating snapshots across shards needs an
out-of-engine timestamp source (a hybrid logical clock, a wall-clock with
bounded skew, an external coordinator) — that is exactly what
**user-defined timestamps** (ADR-0007) provide. The engine ships one
primitive (per-instance freeze); the embedding system layers the other on top.

## Decision

A `Snapshot` is a sealed handle wrapping a 56-bit `SequenceNumber`. It is
created and released through the engine:

```java
public interface RocksDb {
    Snapshot newSnapshot();
    void releaseSnapshot(Snapshot s);
    Optional<byte[]> get(ColumnFamilyHandle cf, byte[] key, ReadOptions opts);
    // ReadOptions.snapshot is an Optional<Snapshot>
}
```

Implementation (§6.5):

1. **Create.** `newSnapshot()` reads `versions.lastSequence()`, allocates a
   `SnapshotImpl(seq)`, registers it in the engine's `activeSnapshots`
   set, and returns the handle. O(1). No state copy. No file pin. No
   block-cache reservation.
2. **Read at snapshot.** A `get` with `opts.snapshot = Some(s)` uses
   `s.seq` instead of `versions.lastSequence()` as the upper bound for the
   sequence-number walk in MemTable, frozen MemTables, and SSTables (§6.2 read
   path). Iterators do the same for their lifetime.
3. **Compaction pinning.** The §6.3 compaction loop computes
   `oldestLiveSnapshotSeq = min(activeSnapshots.seq, lastSequence)`. A
   tombstone may be dropped at the bottom level **only if** its sequence
   number is `≤ oldestLiveSnapshotSeq`. Older value versions may be dropped
   under the same rule. This is the load-bearing invariant: the moment a
   snapshot exists at seq N, every tombstone and stale value with seq > N is
   pinned until the snapshot is released.
4. **Release.** `releaseSnapshot(s)` removes the entry from
   `activeSnapshots`. The next compaction tick recomputes
   `oldestLiveSnapshotSeq` and may then collect the previously-pinned
   tombstones. The engine does not eagerly run a compaction on release —
   the natural compaction rhythm picks it up.

Scope is **per-instance**. The handle is meaningful only against the
`RocksDb` that created it; passing a snapshot from instance A to instance B
is a programming error (the API takes a typed handle, not a raw sequence
number). Cross-instance / cross-shard consistent reads are the embedding
system's concern and are served by user-defined timestamps (ADR-0007) when
the embedding system supplies a coordinated timestamp source.

## Rationale

Reusing the sequence-number machinery is the right primitive because the
engine already pays for it. Crash recovery needs per-write sequence tags;
compaction's tombstone GC needs the same tags. A snapshot that "freezes a
sequence number" adds an O(1) map entry plus a comparison in the read path —
nothing else.

Refusing to do cross-process snapshots in the engine is the §6 framing made
explicit. A distributed snapshot needs a coordinated clock, and the engine
deliberately does not own a clock. The §1.2 non-goals (no transactional layer
above LSM, no replication) cut here too: cross-shard consistency is a property
of the layer above. The engine ships user-defined timestamps so that layer
*has* a real handle to coordinate on; what it does not ship is the coordination
protocol itself.

The compaction-pinning rule is a real operational hazard, not a hidden detail.
The §9 failure-modes table calls it out and the engine exposes
`db.snapshots()` so an operator who sees space-amp climbing can identify the
holder. The contract is unambiguous: snapshots are cheap to take, expensive to
hold indefinitely.

## Consequences

Positive:
- Snapshots cost essentially nothing — one map entry — making them safe to
  take liberally for short-lived consistent reads (an iterator scan, a
  multi-step transaction-like read pattern at the application layer).
- Reads at a snapshot are correctness-equivalent to reads at the current
  sequence: same code path, different upper bound.
- The compaction-GC rule is the same one used for the no-snapshot case
  (drop at bottom level when no live reader can need the value) — snapshots
  just shift the "no live reader" boundary.
- Backup `Checkpoint`s (§6.7) compose with snapshots: a checkpoint takes a
  snapshot first, then hard-links the SSTables that snapshot references.

Negative:
- A leaked snapshot is a leak of *space*. Tombstones pinned past the snapshot
  accumulate at the bottom level; compactions still rewrite the files but
  cannot drop the entries. The §9 entry "snapshot held too long" is the
  documented failure path; ops tooling surfaces the open-snapshots list.
- Per-instance scope means a multi-shard distributed query cannot get a
  consistent snapshot from the engine alone — by design. Cross-shard
  consistency is ADR-0007's responsibility (user-defined timestamps) plus
  whatever the embedding layer adds (an HLC, a coordinator).
- The sequence-number space is 56 bits. At Meta's largest production rates
  (~10^8 writes/sec) that is decades of headroom; for this implementation it
  is effectively unbounded. Wrap-around handling is not implemented.

## Alternatives considered

- **Copy-on-snapshot MemTable / SSTable state.** Rejected: defeats the
  whole point — snapshots become O(state size) to create, useless for
  short-lived consistent reads. The sequence-tagging machinery already does
  the structural work; the snapshot just names a point in that history.
- **Cross-process / cross-shard snapshots in-engine.** Rejected as §1.2
  non-goal. The engine has no clock and no coordinator; building one
  internally would duplicate every distributed-systems primitive (Lamport
  clocks, vector clocks, external timestamp oracle) that the embedding
  system already has.
- **Time-based snapshots ("as of wall-clock T").** Rejected as the
  engine's primitive: wall-clock semantics belong to user-defined timestamps
  (ADR-0007). The engine ships the slot; the embedding layer picks the
  clock.
- **Auto-release snapshots after a deadline.** Rejected for v1: silently
  releasing a snapshot the application still holds would surface as
  "read returns inconsistent value" — a worse failure than "space-amp
  climbs and ops gets paged." The contract is explicit acquire / release.

## References

- Implementation plan §6.5 (Snapshot lifecycle), §6.2 (read at snapshot
  sequence), §6.3 (compaction tombstone-collection rule with
  `oldestLiveSnapshotSeq`), §9 (snapshot-held-too-long failure mode),
  Phase 2 CP 6, §10 (ADR 0015).
- leveldb-implementation-plan §5.5 (snapshot-as-sequence-number on the
  sibling LevelDB port — the same primitive at smaller scale).
- FAST 2021 paper §6 (KV interface — Versions and timestamps; the
  per-instance scope framing).
- Sibling ADR: ADR-0007 (user-defined timestamps as the cross-shard answer).
- Repo: `rocksdb-engine` (`Snapshot`, `SnapshotImpl`, `activeSnapshots`,
  compaction `oldestLiveSnapshotSeq` consultation).
- Original RocksDB source: `db/snapshot_impl.h`,
  `db/db_impl/db_impl.cc` (`GetSnapshot`, `ReleaseSnapshot`),
  `db/compaction/compaction_iterator.cc` (snapshot-aware GC).
