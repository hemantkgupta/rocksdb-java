# Architecture — module dependency graph

```
rocksdb-common (no deps)
    ↑
    ├── rocksdb-memtable ─────────── (depends on common)
    │
    ├── rocksdb-wal ──────────────── (depends on common)
    │
    ├── rocksdb-bloom ────────────── (depends on common)
    │       ↑
    ├── rocksdb-sstable-blockbased ─ (depends on common, bloom, block-cache)
    │
    ├── rocksdb-manifest ─────────── (depends on common, wal)
    │
    ├── rocksdb-block-cache ──────── (depends on common)
    │
    ├── rocksdb-compaction-leveled ─ (depends on common, sstable-blockbased, manifest)
    │
    ├── rocksdb-engine ─────────────── (depends on all active modules above)
    │
    ├── rocksdb-tools ─────────────── (depends on engine)
    ├── rocksdb-test-cluster ──────── (depends on engine, tools)
    └── rocksdb-cli ────────────────── (depends on engine, tools)
```

The graph is acyclic. Storage modules (memtable, wal, sstable-blockbased, bloom) do not depend on each other except through the explicit composition in `rocksdb-engine`. `rocksdb-compaction-leveled` depends on `sstable-blockbased` + `manifest` because it composes both at the algorithm level.

## Stub modules

Declared in `settings.gradle` but empty, each reserved for a specific CP in the RocksDB plan:

- `rocksdb-sstable-api` — interface lift-out (later phases).
- `rocksdb-compaction-api` — Compaction / Picker interfaces.
- `rocksdb-compaction-tiered` — CP 11 (size-tiered / universal).
- `rocksdb-compaction-fifo` — CP 12 (FIFO).
- `rocksdb-rate-limiter` — CP 13 (shared TokenBucketRateLimiter).
- `rocksdb-column-family` — CP 14 (ColumnFamilyRegistry).
- `rocksdb-integrity` — CPs 19-21 (file + handoff + KV checksums).
- `rocksdb-error-handling` — CP 22 (SeverityClassifier + read-only mode latch).
- `rocksdb-runtime` — HTTP demo wrapper.

## Phase mapping

- **Phase 1+2 (CPs 1-9 of the RocksDB plan)** — _complete_, ported from leveldb-java. Lights up `common`, `memtable`, `wal`, `bloom`, `sstable-blockbased`, `manifest`, `compaction-leveled`, `block-cache`, `engine`, `tools`, `test-cluster`, `cli`.
- **Phase 3 (CPs 10-14)** — multi-threaded compaction, tiered, FIFO, rate limiter, column families.
- **Phase 4 (CPs 15-18)** — Dynamic Leveled, file-deletion rate limiter, DeleteRange, parallel L0/L1.
- **Phase 5 (CPs 19-22)** — file + handoff + KV checksums; severity classifier.
- **Phase 6 (CPs 23-26)** — configurable WAL; shared cache + per-instance overrides; data-format compatibility.
- **Phase 7 (CPs 27-30)** — UDTs; MultiGet; two-level indices; tooling + stress test.

`rocksdb-engine` is the integration module; it accumulates dependencies as phases land.
