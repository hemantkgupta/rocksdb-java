# ADR-0007: User-defined timestamps as first-class CF metadata (not encoded in user key)

- Status: accepted
- Date: 2026-05-27
- Phase: 7
- CP: 27

## Context

§6 of the FAST 2021 paper documents the long-running cost of the older pattern of
encoding application-supplied timestamps into the user key (`<userKey> || <userTs>`)
to get point-in-time reads. The encoding works — comparators sort `(userKey, userTs)`
correctly, MVCC reads can scan from `(userKey, asOfTs)` backward — but it breaks the
bloom filter. Bloom probes operate on the full encoded key, so each `(userKey,
userTs_i)` is a *different* bloom-filter entry. A point read against a user key with
50 historical versions now needs the filter to be sized for 50 entries per logical
key, or it false-positives often enough to defeat its purpose.

Table 6 of the paper quantifies the impact: pulling timestamps out of the user key and
into a separate slot in the internal key delivers a **1.2–2.0× throughput improvement
on point-read workloads**, with the gain growing in the number of historical versions
per key. The mechanism is exactly the bloom-filter restoration: probes use only
`userKey`, so one user key is one filter entry regardless of how many timestamps it
has.

The change is also an API improvement. Callers no longer concatenate bytes; the engine
takes the timestamp as a distinct parameter and the comparator owns the ordering.
Cross-shard MVCC becomes possible because the timestamp space is meaningful outside
the engine (an application's wall-clock or hybrid-logical clock can synchronise
snapshots across shards in a way per-shard `SequenceNumber`s cannot — ADR-0015).

## Decision

User-defined timestamps are a **per-CF option** (`ColumnFamilyOptions.userTimestampSize`),
not a global engine mode. When `userTimestampSize > 0` for a CF:

1. The internal key layout extends from `(userKey, seq, type)` to
   `(userKey, userTs, seq, type)`. The comparator orders by `userKey` ASC, then
   `userTs` DESC, then `seq` DESC.
2. Write APIs take `userTs`:
   ```java
   void put(ColumnFamilyHandle cf, byte[] key, byte[] userTs, byte[] value, WriteOptions opts);
   void delete(ColumnFamilyHandle cf, byte[] key, byte[] userTs, WriteOptions opts);
   ```
3. Read APIs take `asOfTs`; `get` returns the most-recent entry with `userTs <= asOfTs`:
   ```java
   Optional<byte[]> get(ColumnFamilyHandle cf, byte[] key, byte[] asOfTs, ReadOptions opts);
   ```
4. The **bloom filter is keyed on `userKey` only**. This is the load-bearing detail —
   it is the bloom-filter restoration that delivers Table 6's gain.
5. Compaction retention by user-timestamp: a CF with
   `userTimestampRetention = Duration.ofDays(N)` drops entries with
   `userTs < (nowTs - N days)` at the bottom level.
6. WAL records carry the `userTs` slot when the target CF has UDTs enabled (§4.2 WAL
   framing — `userTs` present only when CF metadata indicates).

A CF without UDTs (`userTimestampSize == 0`, the default) uses the unchanged
`(userKey, seq, type)` layout. The two CF kinds coexist in one DB.

API rejection at the boundary: a write whose `userTs.length != cf.userTimestampSize`
is rejected before the WAL append, with a clear error. This is Phase 7 CP 27's
documented failure scenario.

## Rationale

The §6 framing is that the engine should *own* the timestamp slot rather than hide it
in user-key bytes, because the engine is the only component that knows the bloom
filter exists. Once the slot is owned, the filter can be probed on `userKey` alone,
recovering the 1.2–2.0× Table 6 gain "for free" — no work the application has to do.

Per-CF (not per-DB) granularity matches the ADR-0006 framing: many DBs have a mix of
versioned and non-versioned data (an MVCC payload CF next to a metadata CF that never
needs point-in-time reads). Forcing UDTs DB-wide would waste 8 bytes per KV in the
metadata CF for no benefit. Forcing UDTs off DB-wide would push versioned-data
adopters to encode timestamps in user keys again, undoing the win.

`userTs` ordering is **descending** within a user-key group so a `get(asOfTs)` walks
the head of the group and returns the first entry with `userTs <= asOfTs` — the same
trick the engine uses for sequence numbers, applied one slot up.

## Consequences

Positive:
- Point-read workloads with many historical versions per key reproduce Table 6's
  1.2–2.0× throughput gain. The §10 plan §7 stress test (CP 27) measures this.
- Cross-shard MVCC becomes a real option — the embedding system can supply a hybrid
  logical clock and get consistent snapshot reads across shards without engine
  coordination. ADR-0015 leans on this.
- Compaction can drop expired data without operator intervention, bounded by
  `userTimestampRetention`. A 7-day retention CF self-trims.

Negative:
- Every KV in a UDT-enabled CF pays 8 bytes for the timestamp slot, on top of the
  KV-checksum (ADR-0003) and the seq+type. Small-value workloads see meaningful
  overhead.
- Mixed-CF DBs require the read path to branch on whether the CF has UDTs — a tested
  branch but a branch nonetheless. The comparator is also per-CF.
- The bloom-filter restoration is conditional on disciplined call sites: any caller
  that probes the filter with the full internal-key bytes silently regresses to the
  pre-UDT behaviour. Tests assert filter calls use `userKey` only.

## Alternatives considered

- **Keep timestamps in the user key.** Rejected: forfeits the Table 6 1.2–2.0× gain;
  defeats the bloom filter for versioned data.
- **Global UDT mode (DB-wide).** Rejected: wastes 8 bytes per KV in CFs that don't
  need versioning; forces all CFs to share one retention policy.
- **Per-CF UDT but variable timestamp size (1 / 4 / 8 / 16 bytes).** Rejected for v1:
  one size (8 bytes) covers wall-clock millis, microsecond clocks, and 64-bit HLCs.
  The slot is sized in `ColumnFamilyOptions` so a future v2 could relax this without
  format change.
- **Engine-assigned timestamps (no `userTs` input).** Rejected: defeats the
  cross-shard MVCC use case, which requires the application to control the timestamp
  source.

## References

- Implementation plan §6.6 (User-defined timestamps API), §5.1 (MemTable Key
  comparator extension), §4.2 (WAL `userTs` slot), Phase 7 CP 27, §10 (ADR 0007).
- FAST 2021 paper §6 (User-defined timestamps), Table 6 (UDT vs encoded-timestamp
  throughput comparison).
- Repo: `rocksdb-common` (`Key` extension), `rocksdb-memtable`,
  `rocksdb-sstable-blockbased` (bloom-filter call sites), `rocksdb-engine`.
- Original RocksDB source: `include/rocksdb/options.h`
  (`ColumnFamilyOptions::comparator`, `kUserDefinedTimestampSize`),
  `db/dbformat.cc`.
