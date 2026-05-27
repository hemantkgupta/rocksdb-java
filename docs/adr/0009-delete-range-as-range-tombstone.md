# ADR-0009: DeleteRange writes a single range-tombstone, not N point tombstones

- Status: accepted
- Date: 2026-05-27
- Phase: 4
- CP: 17

## Context

§2.2 of the FAST 2021 paper documents `DeleteRange` as one of the 2016-era API
additions driven by Meta's workload patterns. The motivating use case is "delete all
keys with prefix `userid/`" — a logical operation against a contiguous user-key range
that, expressed as N point-`Delete`s, requires reading every key under the prefix
first (to know which point-deletes to issue), then writing one tombstone per key.
For a prefix with 10 K keys, that is 10 K reads followed by 10 K MemTable inserts and
10 K WAL records — an O(N) operation against a logically O(1) intent.

The expanded-tombstones approach also produces an *unbounded write amplification*:
during compaction, each of the N point tombstones travels through the LSM
independently. A 10 K-key prefix delete produces ~10 K extra entries per level until
each tombstone reaches the bottom level and can be dropped. For a prefix-delete-heavy
workload (multi-tenant cleanup, GDPR data deletion, partition reseat), that
amplification dominates write-amp.

The right primitive is to record the *intent* — a `[start, end)` range — as one entry
that iterators consult during reads and that compaction drops covered keys against.
The engine never enumerates the keys; the tombstone covers them by structural
inclusion.

## Decision

`DeleteRange(start, end)` writes **one** range-tombstone record per `WriteBatch` op,
not N point tombstones. The tombstone is stored separately from data, at every
layer:

- **MemTable** — a separate `ConcurrentSkipListMap<Slice, RangeTombstone>` keyed on
  `startUserKey`, alongside the data skiplist (§5.1 of the plan). Reads consult both
  maps: the data map for direct value/tombstone, the range-tombstone map for
  coverage. This separation is why one range tombstone stays O(1) — merging into
  one map would force per-key expansion.
- **WAL** — `opType = DELETE_RANGE` (§4.2). The `value` bytes encode the end key, so
  one record holds the full `[start, end)` range. One CRC, one length prefix, one
  fsync.
- **SSTable** — a dedicated range-tombstone section, written before the bloom-filter
  block (§4.1). Each entry is `(startKey, endKey, seq+type)`; entries are sorted by
  start key with an inline binary-searchable index. The section is *only* written
  when the SSTable contains at least one range tombstone.
- **Iterators** — a `MergingIterator` over `(MemTable iter, frozen iter, per-SSTable
  iter ×N)` tracks active range tombstones from each source alongside data entries.
  A data entry is suppressed if any source's tombstone covers `(userKey,
  asOfSequence)`.
- **Compaction** — the §6.3 compaction loop checks each key against active range
  tombstones. A key is dropped when (a) it is covered by a range tombstone with
  higher sequence, AND (b) the compaction output level is at or below the
  tombstone's bottom level (so no shallower file still needs the tombstone for read
  correctness). A range tombstone is itself dropped when all keys it covers have
  already been dropped.

The range tombstone never re-expands into point tombstones. It exists as one entry
from `DeleteRange` call to bottom-level compaction.

## Rationale

The "one record per logical intent" framing matches what the application meant. A
prefix delete is logically O(1); the API and the storage layout reflect that.

The separate-section storage in SSTables is the key correctness detail. If range
tombstones lived in the data block stream, every block reader would have to interpret
"this entry is a tombstone for a range I am not in" — sortable but expensive.
Putting range tombstones in a section at the head of the SSTable lets readers load
them once per opened file, hold them in the iterator's state, and consult them by
binary search per user key. The separate MemTable map applies the same logic to the
in-memory side.

The compaction-drop rule has to be careful: a range tombstone cannot be dropped
until *every* SSTable that could observe the covered keys has been compacted past it.
The bottom-level check in §6.3 is the structural guarantee — once the tombstone has
descended to `bottomLevel` and the compaction is producing a file at that level, no
shallower file can still reference a covered key (compaction is monotone in level).

## Consequences

Positive:
- Prefix deletes are O(1) at write time regardless of prefix cardinality. A
  10 K-key prefix delete is one WAL record, one MemTable insert, one tombstone.
- Iterator suppression is `O(log(R))` per data entry, where R is the count of
  active range tombstones in the merge — small in practice.
- Compaction drops covered keys cheaply: the tombstone is one entry to consult per
  user-key group, not N point comparisons.
- Bloom filters and indices are unaffected — range tombstones don't pollute the
  point-read fast path.

Negative:
- Range-tombstone state must be threaded through every iterator and every
  compaction worker. A bug that loses a range tombstone silently resurrects deleted
  data; tests at CP 17 explicitly cover the `DeleteRange + later Put at a covered
  key` interaction (the higher-seq Put wins).
- Until compaction descends past the tombstone, reads pay an iterator-overhead
  check per data entry against active tombstones. A compaction backlog leaves the
  tombstones active longer than they should be.
- The MemTable now has two sorted structures to maintain transactionally on every
  `WriteBatch` apply. Lock discipline (one structural lock for the batch) handles
  it but is a load-bearing invariant.

## Alternatives considered

- **Expand `DeleteRange` to N point `Delete`s at API entry.** Rejected: defeats the
  entire motivation — O(N) writes, O(N) WAL records, O(N) compaction amplification.
- **Store range tombstones inline in data blocks.** Rejected: every block reader
  pays the cost of skipping range-tombstone entries it doesn't care about; defeats
  binary-search-by-user-key in the data block.
- **Materialise covered point tombstones lazily at compaction time.** Rejected: pays
  the O(N) cost eventually anyway, just deferred; loses the iterator-suppression
  fast path that benefits reads before compaction runs.
- **Track only the tombstone count, not the bounds.** Rejected: incorrect — readers
  need to know which keys are covered, not just that something was deleted.

## References

- Implementation plan §5.1 (MemTable separate range-tombstone map),
  §4.1 (SSTable range-tombstone section), §4.2 (WAL `DELETE_RANGE` op),
  §6.3 (compaction drop rules), Phase 4 CP 17, §10 (ADR 0009).
- FAST 2021 paper §2.2 (2016-era API additions including `DeleteRange`).
- Repo: `rocksdb-memtable`, `rocksdb-sstable-blockbased`, `rocksdb-engine`.
- Original RocksDB source: `db/range_tombstone_fragmenter.cc`,
  `include/rocksdb/db.h` (`DeleteRange`).
