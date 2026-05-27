# ADR-0013: MultiGet parallelises across SSTables via a ForkJoinPool per call

- Status: accepted
- Date: 2026-05-27
- Phase: 7
- CP: 28

## Context

§6 of the FAST 2021 paper covers the KV-interface additions that fell out of
Meta's workload analysis (Table 2): the same engine serves stream-processing
state stores, logging ingestion, index services, and SSD caches, and several of
those workloads issue many point lookups per request. A search query resolves
hundreds of document IDs; an ML feature lookup resolves dozens of feature keys
for one inference call; a stream join probes both sides of the keyspace for
every event. Each lookup is a few microseconds when the block is cache-hot, but
tens of microseconds when it requires a disk read — and the per-call total
becomes the user-visible latency.

The sequential `get` API loses on two axes here. Per-key, it pays a separate
bloom-filter probe, a separate block-cache lookup, and a separate (possibly)
disk read. Across keys it cannot overlap any of those costs: the block cache
miss for key K_1 blocks the bloom probe for key K_2, even though they hit
disjoint SSTables and could trivially run in parallel.

Table 5 of the paper documents the production-fast-path win: a batched MultiGet
that submits per-SSTable work in parallel and overlaps bloom probes, block
reads, and CRC verification across keys delivers a measurable wall-time
reduction over the sequential equivalent, with the gain scaling in batch size
and in the fraction of keys that miss the block cache. The §3 framing — modern
multi-core servers with 64+ cores feeding SSDs that can sustain hundreds of K
random IOPS — is what makes the parallelism worth the API complexity.

## Decision

The engine exposes a batched `MultiGet` API:

```java
List<Optional<byte[]>> multiGet(ColumnFamilyHandle cf, List<byte[]> keys, ReadOptions opts);
```

Internal execution (§6.2):

1. **Snapshot resolution.** Compute `snapshotSeq` once for the whole batch from
   `opts.snapshot` or `versions.lastSequence()`. Every key in the batch reads at
   the same sequence number; results are mutually consistent.
2. **Per-SSTable bucketing.** Walk all overlapping SSTables across L0..L_max.
   For each SSTable, build a sublist of `(keyIndex, userKey)` pairs whose user
   keys fall within the file's `[smallestKey, largestKey]`. Skip files that
   bound-cull every key, and within a file run the bloom filter once per key to
   prune further. Bucketing groups keys by file before any I/O so each
   per-SSTable task reads one open file once and benefits from cache locality
   across its bucket.
3. **Parallel dispatch.** Submit one task per non-empty bucket to a per-call
   `ForkJoinPool` (size capped by `options.multiGetParallelism`, default
   matching the number of available cores). Each task: opens the file (cached
   handle), executes the bloom probes that survived bucketing, reads the
   required data blocks (going through the shared block cache; §5.5), runs
   block-CRC + KV-CRC verification (ADR-0003), and writes the resolved values
   into a per-key result slot.
4. **Merge.** Walk slots in input-key order. For each key, pick the value with
   the highest sequence number `≤ snapshotSeq`; respect tombstones and range
   tombstones identically to a single `get`.

Errors are aggregated, not propagated. A CRC mismatch on one SSTable fails the
keys that bucket needed and leaves the others unaffected; the call returns a
partial result plus an aggregate error indicating which key indices failed.
The §6.2 failure-paths note documents this as the v1 contract.

The §1.3 departures table is explicit that this is *not* the vectorised CRC32C
fast path the production engine uses; it is the algorithmic parallelism only.
The correctness is identical, the per-key CPU cost is the same as serial
`get`, and the win is the overlap of independent I/Os.

## Rationale

The §6 framing is that batch APIs let the engine see structure the per-key API
cannot. A list of 1000 keys hitting 10 SSTables is 10 disjoint per-file
workloads; the engine that knows the batch shape can saturate the available
core count and overlap whatever block-cache misses turn into disk reads.

Per-SSTable bucketing is the load-bearing detail. Without it, a naïve "submit
each key as its own task" implementation pays per-key overhead (file open
cache lookup, bloom-filter object dereference, block-handle lookup) N times and
never amortises the per-file fixed costs. Bucketing pays them once per file,
which is what makes the parallel path win on small-batch / many-file scenarios.

`ForkJoinPool` is the right primitive because tasks can themselves split (an
SSTable with 100 bucketed keys could fan out further), the work-stealing
scheduler handles skew (one SSTable with all the cold blocks doesn't block
others), and the API matches Java's structured-concurrency idioms.

## Consequences

Positive:
- Batched point-read workloads (search, ML features, stream joins) reproduce
  Table 5's wall-time reduction over sequential gets. The Phase 7 CP 28 test
  asserts a measurable wall-time gap on a 1000-key batch across 10 SSTables.
- Block-cache misses are overlapped across keys; the I/O subsystem sees the
  natural request rate of the workload rather than a serialised drip.
- Snapshot-consistency is automatic: the single `snapshotSeq` for the batch
  means all results are at the same logical point in time, regardless of
  scheduling order.

Negative:
- A `ForkJoinPool` per call has setup cost; very small batches (<8 keys) may
  not beat serial `get`. The engine falls back to sequential execution below
  a threshold (`MULTIGET_PARALLEL_THRESHOLD = 8`).
- Errors are partial, not all-or-nothing. Callers must handle the aggregate
  error format — a behavioural difference from `get` that the API documents
  explicitly.
- Per-call pools mean a burst of MultiGet requests can spawn many transient
  thread pools. The implementation uses a shared `ForkJoinPool` per engine
  instance with task-scoped result slots to avoid that.
- Without the production fast path's vectorised CRC32C, the per-key CPU cost
  is unchanged from serial; the win is purely from I/O overlap (ADR-0012's
  documented departure).

## Alternatives considered

- **Sequential MultiGet that just loops `get` internally.** Rejected: matches
  the API shape but delivers none of the Table 5 win; defeats the whole point
  of the batched call.
- **One thread per key.** Rejected: ignores per-SSTable locality; pays
  per-file fixed costs N times; spawns work for keys that bound-cull or
  bloom-prune to nothing.
- **Async / callback API instead of a sync batched call.** Rejected: the
  embedding system (a search engine, an ML feature server) wants the
  sync-batched shape; async pushes scheduling complexity to every caller.
- **Pre-sort the input by key to improve cache locality.** Rejected as a
  default: callers may depend on input-order results (the API guarantees
  result[i] corresponds to keys[i]); per-SSTable bucketing achieves the same
  locality without re-ordering the result vector.

## References

- Implementation plan §6.2 (MultiGet bucketing + ForkJoinPool dispatch),
  §1.3 (departures — algorithmic parallelism, not vectorised CRC),
  §5.5 (shared block cache used by per-bucket reads),
  Phase 7 CP 28, §10 (ADR 0013).
- FAST 2021 paper §6 (KV interface — MultiGet), Table 5 (MultiGet production
  fast-path measurements), §3 (CPU pivots motivating parallel I/O), Table 2
  (workload categories driving the batched API).
- Repo: `rocksdb-engine` (MultiGet implementation),
  `rocksdb-sstable-blockbased` (per-SSTable bucket execution),
  `rocksdb-block-cache` (shared cache surface).
- Original RocksDB source: `db/db_impl/db_impl_readonly.cc` (`MultiGet`
  internals), `table/block_based/block_based_table_reader.cc` (per-file
  read path).
