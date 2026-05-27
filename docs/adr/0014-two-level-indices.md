# ADR-0014: Two-level indices for SSTables larger than 32 MB

- Status: accepted
- Date: 2026-05-27
- Phase: 7
- CP: 29

## Context

A BlockBasedTable's index block holds one entry per data block — typically the
largest user key in each ~4 KB data block, paired with the block's `(offset,
length)` handle (§4.1). For a small SSTable this index is a few KB and is
cheap to keep fully in RAM; the per-file fixed cost is negligible. As SSTables
grow, the index grows linearly: a 64 MB SSTable with 4 KB blocks holds ~16 K
index entries; a 256 MB SSTable holds ~64 K. With typical user-key sizes the
single-level index runs from a few hundred KB to several MB per file.

§6 of the paper and the BlueStore-paper experience flagged the structural
issue: when an engine instance opens hundreds of SSTables — large CF, many
levels, dynamic-leveled compaction holding a fat L_max — the cumulative
per-file index memory becomes a meaningful slice of RAM, and that slice is
hot allocation that can't easily be paged out. Keeping every byte of every
index in heap also defeats the §1.3 commitment that block-cache accounting
captures the real memory cost; index memory sits outside the cache and is
invisible to the budget.

The read-amp math for large files makes the trade clear. A point read on a
256 MB SSTable with a fully-in-RAM index does one binary search in heap
(`log2(64K) ≈ 16` comparisons) plus one data-block read. A point read on the
same file with a two-level index does one binary search in a small top-level
index (`log2(256) ≈ 8` comparisons) to find the right bottom-level index
block, one cache-resident lookup or block-cache load of that bottom block
(~64 KB), one binary search inside it (`log2(256) ≈ 8` comparisons), plus
the data-block read. The extra cost is one block-cache lookup; the win is
that the bottom-level index pages are *cache-managed* and are paged in only
for the key ranges actually being read.

## Decision

When an SSTable exceeds `TWO_LEVEL_INDEX_THRESHOLD = 32 MB` (§12 config), its
index is written as a two-level structure (§4.1 SSTable layout):

1. **Bottom-level index blocks.** The single-level index is sharded into
   blocks of ~256 KB each (configurable; the same target as a large data block
   for cache-line consistency). Each bottom-level block is its own
   binary-searchable block with the same internal layout as a data block:
   sorted `(largestKey -> BlockHandle)` entries with a restart-point array and
   trailing type + CRC32C. The bottom-level blocks are written into the
   SSTable just before the footer, with the same on-disk framing as data
   blocks so they can be loaded through the shared block cache.
2. **Top-level index block.** A single small block, sized to hold one entry
   per bottom-level block (~`fileSize / 256KB / 256KB` entries — for a 256 MB
   file, ~16 entries). Each entry maps `largestKey` of a bottom-level block to
   that block's `BlockHandle`. The top-level block is small enough (a few KB
   at most) to load eagerly at file-open time and pin in the
   `BlockBasedTableReader`'s field directly.

Read path:
1. Bloom probe on `userKey`; if absent, return early (unchanged from
   single-level index path).
2. Binary search the in-memory top-level index for the bottom-level block
   whose `largestKey ≥ userKey`.
3. `blockCache.lookupOrLoad(file, bottomBlockHandle)` — cache hit if the key
   range is hot, cache miss + disk read if cold. Block CRC verified on load.
4. Binary search the bottom-level block for the data block.
5. `blockCache.lookupOrLoad(file, dataBlockHandle)` — same.
6. Binary search the data block for the entry; KV-CRC verified at exposure
   (ADR-0003).

Files at or below `TWO_LEVEL_INDEX_THRESHOLD` use the single-level index
unchanged — the two-level structure pays an extra cache lookup per read and
is only worth that overhead when the file is large enough that pinning the
whole index would be expensive. The 32 MB threshold is half the default SSTable
target size (`SST_FILE_TARGET_SIZE_BYTES = 64 MB`, §12), so by default most
non-L0 SSTables produced by compaction use two-level indices and L0 flushes
typically do not.

## Rationale

The §6 framing is that memory is a shared, finite resource the engine must
account for. Anything held in RAM that is not in the block cache is RAM the
operator did not budget for, and at the scale of "tens of instances per host"
(§4 resource-management discussion) the per-file index memory adds up to a
visible fraction. Pushing the bottom-level index into the cache puts that
memory back under the cache's LRU policy and the shared budget (ADR — block
cache scope).

The two-level split is correctness-neutral. A single binary search over 64 K
entries is the same logical operation as one binary search over 256 entries
followed by one binary search over 256 entries. The cost difference is only
the extra `Cache.lookupOrLoad` call on the bottom index; for cache-hot keys
that is a constant overhead in the tens of nanoseconds, and for cache-cold
keys the cost was going to be paid on the data-block read anyway.

The 32 MB threshold is the §6 + read-amp-math compromise: below 32 MB the
single-level index is small enough (~tens of KB to ~1 MB) that the per-file
RAM cost is acceptable; above 32 MB the cumulative cost across many files
becomes large enough that the cache-managed bottom-level approach wins.

## Consequences

Positive:
- Engine open cost is bounded regardless of how many large SSTables exist:
  each contributes only the small top-level index (a few KB) to fixed memory.
  The bottom-level indices are loaded on demand and paged out under cache
  pressure.
- Block-cache accounting captures all index memory for two-level files;
  ADR-0005's per-host shared budget tracks reality.
- Hot key ranges keep their bottom-level index blocks pinned naturally; cold
  ranges pay one extra block-cache miss on first access and then ride the LSR.

Negative:
- Reads on two-level files pay one extra `Cache.lookupOrLoad` per `get`. The
  cost is small (~tens of ns hot, one block read cold) but it is not zero;
  bench results (Phase 7 CP 29) document the per-op delta.
- The threshold is a single global tunable; workloads with many small files
  may benefit from a lower threshold and workloads with few large files may
  prefer single-level. Per-CF override is possible but not exposed in v1.
- Bottom-index block CRC mismatches now fail the read path with the same
  `BlockChecksumMismatchException` as data blocks (§9 failure modes,
  documented in CP 29). The KV checksum (ADR-0003) still catches above-IO
  corruption, but a corrupt bottom-index block fails the read at the lookup
  step.

## Alternatives considered

- **Single-level index always, with index memory accounted to the cache.**
  Rejected: the cache works in fixed-size blocks; a variable-size per-file
  index doesn't fit the eviction policy cleanly, and keeping the whole index
  pinned anyway defeats the memory-budget goal.
- **Three-level or deeper indices.** Rejected for v1: complexity does not pay
  for itself until SSTables reach into the GB range, which is outside the
  scope of `SST_FILE_TARGET_SIZE_BYTES = 64 MB` default. A future v2 could
  generalise to N-level if the file-target grows.
- **Always two-level, regardless of file size.** Rejected: small files pay
  an extra block-cache lookup per read for no win. Threshold is the right
  trade.
- **Mmap the SSTable and let the OS page-cache the index.** Rejected: ties
  accounting to the OS page cache, which is outside the engine's budget;
  conflicts with ADR-0012's documented choice to do explicit `read(ByteBuffer)`
  rather than mmap in v1.

## References

- Implementation plan §4.1 (SSTable two-level-index layout — "index-of-index
  block, two-level only"), §6.2 (read path — index block lookup via cache),
  §5.2 (`TwoLevelIndex` in `BlockBasedTableReader`), §12 config
  (`TWO_LEVEL_INDEX_THRESHOLD = 32 MB`), Phase 7 CP 29, §10 (ADR 0014),
  §9 (failure mode — bottom-index block CRC mismatch).
- FAST 2021 paper §6 (KV interface — index memory at scale), §3 (memory and
  CPU evolution).
- Repo: `rocksdb-sstable-blockbased` (`BlockBasedTableWriter`/`Reader`,
  `TwoLevelIndex`), `rocksdb-block-cache` (bottom-index residency).
- Original RocksDB source: `table/block_based/partitioned_index_reader.cc`,
  `table/block_based/index_builder.cc`.
