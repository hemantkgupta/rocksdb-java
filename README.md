# rocksdb-java

A faithful Java 17 implementation of post-FAST-2021 RocksDB. Layers Phases 3-7 of the RocksDB implementation plan on top of the [leveldb-java](https://github.com/hemantkgupta/leveldb-java) engine — multi-threaded compaction, tiered + FIFO + Dynamic Leveled compaction, four-layer integrity, configurable WAL, column families, user-defined timestamps, MultiGet, two-level indices.

This repo's commit history starts where leveldb-java stops: the initial commit ports leveldb-java's CPs 1-12 (= the RocksDB plan's Phases 1-2) into the RocksDB module shape, after which every subsequent CP is net-new work against the RocksDB plan.

## Target

| | |
|---|---|
| Reference | [`facebook/rocksdb`](https://github.com/facebook/rocksdb) post-FAST-2021 |
| Paper | Dong, Kryczka, Jin, Stumm — *Evolution of Development Priorities in Key-value Stores Serving Large-scale Applications* (FAST 2021) |
| Language | Java 17 (no JNI) |
| Build | Gradle multi-project |
| Test | JUnit 5 + AssertJ |
| Compression | JDK `Deflater` (pedagogical departure from native Snappy / Zstd / LZ4) |
| Namespace | `com.hkg.rocksdb.*` |
| Implementation plan | [`CSE-Raw/raw-blog/storage-engines/rocksdb-implementation-plan.md`](https://github.com/hemantkgupta/CSE-Raw/blob/main/raw-blog/storage-engines/rocksdb-implementation-plan.md) |
| Predecessor | [`hemantkgupta/leveldb-java`](https://github.com/hemantkgupta/leveldb-java) (covers Phases 1-2) |

## Module layout

```
rocksdb-java/
├── rocksdb-common               # Slice, InternalKey, SequenceNumber, MutationRecord, engine interface
├── rocksdb-memtable             # SkipListMemTable (concurrent skip-list)
├── rocksdb-wal                  # Write-ahead log (sync mode today; configurable per CP 23)
├── rocksdb-bloom                # Per-SSTable bloom filter (10 bits/key)
├── rocksdb-sstable-api          # SSTable interfaces (lifted-out in later CPs)
├── rocksdb-sstable-blockbased   # Block-based SSTable v1 (data/index/filter/footer)
├── rocksdb-manifest             # MANIFEST file + VersionEdit
├── rocksdb-block-cache          # Shared LRU block cache
├── rocksdb-compaction-api       # Compaction-style interface
├── rocksdb-compaction-leveled   # Leveled compaction (LevelDB-style today; Dynamic Leveled at CP 15)
├── rocksdb-compaction-tiered    # Tiered / Universal compaction (CP 11)
├── rocksdb-compaction-fifo      # FIFO compaction (CP 12)
├── rocksdb-rate-limiter         # Per-host shared RateLimiter (CP 13)
├── rocksdb-column-family        # ColumnFamilyRegistry (CP 14)
├── rocksdb-integrity            # Block / file / handoff / KV checksums (CPs 19-21)
├── rocksdb-error-handling       # SeverityClassifier + read-only mode latch (CP 22)
├── rocksdb-engine               # Assembled engine — composes all of the above
├── rocksdb-runtime              # HTTP demo wrapper
├── rocksdb-tools                # DbVerify + DbDump + ManifestDump
├── rocksdb-test-cluster         # In-process integration + stress fixture
└── rocksdb-cli                  # Command-line wrapper
```

Twelve of these are active right now (the ported leveldb-java code); the other nine are declared empty so the multi-project graph is stable and each CP lights up its module in place.

## Build

```bash
jenv local 17.0
./gradlew build
```

(Requires Java 17. If you use jenv, the included `.java-version` file pins it.)

## Phase status

| Phase | CPs | Status |
|---|---|---|
| Phase 1 — LevelDB-era foundation | 5 | _complete_ (ported from leveldb-java) |
| Phase 2 — LSM machinery | 4 | _complete_ (ported from leveldb-java) |
| Phase 3 — 2014-era write-amp pivot | 5 | _pending_ |
| Phase 4 — 2018-era space-amp pivot | 4 | _pending_ |
| Phase 5 — FAST 2021 §5 failure handling | 4 | _pending_ |
| Phase 6 — FAST 2021 §4 at-scale operations | 4 | _pending_ |
| Phase 7 — FAST 2021 §6 KV interface + tooling | 4 | _pending_ |

Phases 1+2 ship at 181 tests across 12 active modules — exactly where leveldb-java's CP 12 left off. Phase 3 onward is net-new RocksDB work.

## Relationship to leveldb-java

The on-disk format is **forward-compatible by design** with leveldb-java's SSTables, WAL, and MANIFEST. A DB written by leveldb-java will open in this engine: the bloom filter meta-block name `filter.leveldb.BuiltinBloomFilter2` is preserved verbatim, the SSTable footer magic is unchanged, and MANIFEST edit tags `0x10-0x15` still mean the same things. Phase 6 (CP 26) will add skip-unknown framing so future RocksDB-only extensions don't break LevelDB-era readers.

The diff that matters between the two repos:

- **leveldb-java**: single-threaded compaction, sync-only WAL, one compaction style (leveled K=10), block-CRC32 as the only integrity layer, no column families.
- **rocksdb-java** (target): N-threaded compaction; 3 WAL modes; 4 compaction styles; 4 integrity layers; column families; per-host RateLimiter; UDTs; MultiGet.

## License

MIT. The original `facebook/rocksdb` is GPLv2 / Apache-2.0 dual-licensed; nothing in this port reuses that code directly.
