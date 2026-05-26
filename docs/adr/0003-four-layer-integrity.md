# ADR-0003: Four-layer integrity model (block / file / handoff / KV checksums)

- Status: accepted
- Date: 2026-05-27
- Phase: 5
- CP: 19-21

## Context

§5 of the FAST 2021 paper reports the empirical corruption rates Meta observed: roughly
one silent-corruption event per 100 PB of data written per three months, and — the
number that drove the design — **40% of those events had already propagated to replicas
before detection**. Replication does not catch corruption that originates above the
storage layer. A bit flipped in MemTable, in a CPU register, or in the block cache is
written *correctly* to disk and *correctly* replicated; downstream replicas all agree on
the wrong byte.

The implication is that a single checksum layer is insufficient. Each layer addresses a
different threat class, and replication only redundantly covers one of them (storage).
Figure 4 draws the threat surface explicitly.

## Decision

The engine implements four independent checksum layers, each computed and verified at
different points in the data path:

| Layer | Algorithm | Computed / Verified | Catches |
|---|---|---|---|
| **Block** | CRC32C | `BlockBuilder.finish()` / every block read | Disk bit rot, partial block writes |
| **File** | XXH64 streaming | SSTable finish (stored in `FileMetadata`) / `db-verify`, backup | Whole-file rot, file mis-attribution |
| **Handoff** | CRC32C per block | Engine before `fs.write(bytes, checksum)` / `LocalFs` on receipt | Bus errors, in-flight memory corruption to FS |
| **KV** | 4-byte CRC over `(seq, userKey, value)` | `MemTable.add()`, carried through flush/compaction / iterator/Get exposure | Corruption above file I/O: MemTable, cache, CPU register |

`rocksdb-integrity` is its own Gradle module, depending only on `rocksdb-common`. It
exposes the checksum primitives; storage and engine modules call into it. The KV
checksum is inline in the value bytes — appended at MemTable insert, carried verbatim
through freeze/flush/compaction (compaction never re-derives it; it only forwards
bytes), and verified on the read path before the value is handed to the caller.

A KV-checksum mismatch trips `ReadOnlyModeLatch` via the severity classifier
(`HARD_ERROR_READ_ONLY`).

## Rationale

The §5 "40% propagated" number is the load-bearing fact. If only ~60% of corruptions are
caught by inter-replica comparison, the engine itself must catch the remaining 40% by
carrying integrity *with the data* from authorship to consumption.

The four-layer split is not redundancy — each layer catches something the others can't:
Block CRC misses post-decompression cache corruption; File CRC requires a whole-file
read; Handoff CRC misses everything after the file lands; KV CRC misses index/footer
corruption. Stacking all four covers the surface end-to-end at ~4 bytes per KV and
~4 bytes per block — single-digit percent overhead.

## Consequences

Positive:
- Detects MemTable / cache / CPU-register corruption before exposing values. Reproduces
  the §5 promise.
- `db-verify` walks all four layers and identifies which failed, narrowing diagnosis.
- KV checksums carry across backup/restore — logical copies inherit the same contract.

Negative:
- Every KV pays ~4 bytes overhead even when values are small (an 8-byte counter pays
  50%). Acceptable: this is the layer that catches what replication doesn't.
- Compaction must treat the KV-checksum as opaque trailing bytes — forwarded
  byte-for-byte, never re-derived. A compactor that re-derives launders away in-memory
  corruption that occurred before the re-derivation. Tests enforce this invariant.
- Four layers mean four bug surfaces, each with dedicated byte-flip fault-injection
  tests.

## Alternatives considered

- **Block CRC only** (LevelDB's choice, RocksDB pre-2018). Rejected: misses the §5 "40%
  propagated to replicas" case entirely.
- **Rely on inter-replica reconciliation.** Rejected: that's the case the paper proves
  doesn't work — by the time replicas disagree, the disagreement has already propagated.
- **One unified checksum from MemTable to disk.** Rejected: different threats need
  different verification points. A single checksum verified only on Get can't catch
  partial-block disk rot before the rest of the block is also corrupted on read.

## References

- Implementation plan §7.5 and §10 (ADR 0003).
- FAST 2021 paper §5 (Failure handling), Figure 4, "1 corruption / 100 PB / 3 months,
  40% propagated to replicas."
- Repo: `rocksdb-integrity`; wired into `rocksdb-sstable-blockbased`,
  `rocksdb-memtable`, `rocksdb-block-cache`, `rocksdb-engine`.
- RocksDB source: `util/crc32c.cc`, `util/xxh3.cc`, `db/dbformat.h`.
