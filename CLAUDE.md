# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A faithful pure-Java port of post-FAST-2021 RocksDB. The code traces an external implementation plan ("the plan") and a paper:

- Implementation plan: `CSE-Raw/raw-blog/storage-engines/rocksdb-implementation-plan.md` in the sibling repo (referenced in `README.md`). Plan section numbers (§4.1, §6.3, §10, etc.) appear in Javadoc and ADRs and are the canonical source of truth.
- Paper: Dong et al., FAST 2021. Cited as "the paper" or "§N of the paper" in ADRs.
- Predecessor: `hemantkgupta/leveldb-java`. The initial commit ports leveldb-java's CPs 1-12 (= the plan's Phases 1-2) into this repo's module shape; every commit after that is net-new RocksDB work.

Work is organised in numbered **Checkpoints (CPs)** grouped into seven **Phases**. The commit log is the phase ledger — see `git log --oneline` for current progress. Today (CP 30) all seven phases are landed.

## Build and test

JDK 17 required (pinned by `.java-version` for `jenv`). All builds go through the Gradle wrapper:

```bash
./gradlew build                                          # full build + all module tests
./gradlew :rocksdb-engine:test                           # one module's tests
./gradlew :rocksdb-engine:test --tests MultiGetTest      # one test class
./gradlew :rocksdb-engine:test --tests '*ColumnFamily*'  # pattern across the module
./gradlew clean                                          # wipe build/ in every module
```

No native toolchain. No code generators. `./gradlew build` is the whole CI.

## Module layout

The repo is a Gradle multi-project; each `rocksdb-*` directory is one subproject. Wiring lives in the root `build.gradle` (dependency edges) and `settings.gradle` (include list); both files have block comments explaining which modules are active vs reserved-as-stubs for future CPs. Architecture diagram and rationale: `docs/architecture.md` and `README.md`.

Three things to know before adding edges:

1. **The dependency graph is acyclic and explicitly enumerated** in the root `build.gradle`. Adding a new inter-module edge means editing that file (not just the consumer's `build.gradle`). Storage modules (`memtable`, `wal`, `sstable-blockbased`, `bloom`) do not depend on each other — they are composed in `rocksdb-engine`.
2. **Empty stub modules are intentional.** Several modules in `settings.gradle` (e.g., `rocksdb-runtime`, `rocksdb-sstable-api`, `rocksdb-compaction-api`) are declared empty so the multi-project graph stays stable as future CPs light them up in place. Do not delete or "consolidate" them.
3. **`rocksdb-engine` is the integration module.** It composes every active module and grows its `api project(...)` list as phases land. New algorithm/storage modules go in as siblings under it, not as nested children.

Namespace is `com.hkg.rocksdb.*`. Group is `com.hkg.rocksdb`, version `0.1.0-SNAPSHOT`.

## Architectural commitments (non-negotiable)

These are load-bearing across the whole codebase. Changing one of them is an ADR, not a refactor. Full reasoning lives in `docs/adr/` — read the relevant ADR before touching the corresponding area.

- **Pure Java, no JNI / no `Unsafe` / no Panama** (ADR-0012). No `org.rocksdb:rocksdbjni` dependency, no native libraries, no `System.loadLibrary`. Compression is JDK `Deflater` only (Snappy/Zstd/LZ4 enum values exist but the writer refuses them). I/O is `java.nio.file` + `FileChannel.force(true)` for fsync. The CRC32C primitive is `java.util.zip.CRC32C`; XXH64 is hand-rolled in `rocksdb-integrity`.
- **Hand-rolled binary framing for every on-disk and on-wire format** (ADR-0011). No protobuf, no JSON, no Java `Serializable`, no FlatBuffers. SSTable / WAL / MANIFEST / admin-wire frames are all encoded by code in `rocksdb-common` (`WireReader`/`WireWriter`/`VarInt`) and called explicitly from each format module. Tests assert byte-exact encodings.
- **Length-prefixed skip-unknown framing for forward + backward compatibility** (ADR-0010). The SSTable footer is fixed at 56 bytes forever; `formatVersion` gates new fields; MANIFEST edits use `editTag(1) | length-prefixed body`. **Never reuse a tag, never change a fixed offset.** Once `0x10` means `AddFile`, it means that forever. A field is a new tag or a new trailing field — not a repurposing. The on-disk format is forward-compatible by design with leveldb-java's SSTables/WAL/MANIFEST; do not change `filter.leveldb.BuiltinBloomFilter2`, the footer magic, or MANIFEST edit tags `0x10-0x15`.
- **Four-layer integrity (block / file / handoff / KV checksums)** (ADR-0003). Compaction treats the KV checksum as opaque trailing bytes — **forwarded byte-for-byte, never re-derived**. A compactor that re-derives launders away in-memory corruption that occurred before the re-derivation. Tests enforce this invariant.
- **Configuration is compile-time only.** All knobs live in `rocksdb-common/Constants.java` (Javadoc cites §12 of the plan). RocksDB famously has no runtime configurability for these defaults; changing them is a recompile.

## Documentation map

| Question | File |
|---|---|
| "What does the engine do?" | [`docs/spec/`](docs/spec/) — reference-grade spec, status-aware. Start at [`docs/spec/README.md`](docs/spec/README.md). |
| "Why was this designed this way?" | [`docs/adr/`](docs/adr/) — 15 architecture decision records. |
| "What depends on what?" | [`docs/architecture.md`](docs/architecture.md). |
| "What's a one-page intro?" | [`README.md`](README.md). |

The spec is **status-aware**: every section calls out where the current code diverges from the target ADR / plan claim. If a spec section and an ADR disagree, the spec describes today's behaviour and the ADR describes the destination.

## Read the ADRs before editing in these areas

The `docs/adr/` directory carries the architectural reasoning. When a change touches one of these areas, read the corresponding ADR first — they document not just the decision but the rejected alternatives, so a "why don't we just..." instinct is usually already answered:

| Area | ADR |
|---|---|
| Compaction style choice (Leveled / Dynamic Leveled / Tiered / FIFO) | 0001 |
| Default compaction style (Dynamic Leveled) | 0002 |
| Integrity layers — when to checksum, where to verify | 0003 |
| WAL durability modes (Sync / Buffered / Disabled, per-`WriteOptions`) | 0004 |
| RateLimiter scope (per-host, downward-clamp per instance) | 0005 |
| Column families (shared WAL, separate MemTables, per-CF level structure) | 0006 |
| User-defined timestamps (per-CF, bloom-keyed on userKey only) | 0007 |
| Severity classifier + `ReadOnlyModeLatch` | 0008 |
| DeleteRange as one range-tombstone (never expanded to point tombstones) | 0009 |
| Format compatibility (skip-unknown framing, tag immutability) | 0010 |
| Hand-rolled binary framing across all formats | 0011 |
| No JNI / pure Java | 0012 |
| MultiGet parallelism (per-SSTable bucketing, ForkJoinPool dispatch) | 0013 |
| Two-level indices (threshold `TWO_LEVEL_INDEX_THRESHOLD = 32 MB`) | 0014 |
| Snapshot semantics (per-instance sequence freeze; pins compaction) | 0015 |

## Commit message convention

The log uses one shape; match it exactly when committing:

```
rocksdb-java: Phase <N> CP <M>: <short summary> (<comma-separated module list>)
```

Examples from history:

```
rocksdb-java: Phase 7 CP 30: ManifestDump + CLI + Phase-7 stress test (rocksdb-tools, rocksdb-cli, rocksdb-test-cluster)
rocksdb-java: Phase 4 CP 17: DeleteRange + range tombstones (rocksdb-common, rocksdb-wal, rocksdb-memtable, rocksdb-sstable-blockbased, rocksdb-engine)
rocksdb-java: ADRs 0011-0015 (docs/adr) — completes the §10 set
```

Phase and CP numbers come from the implementation plan. Each commit corresponds to one CP (or one ADR set); split larger CPs into `CP <N>a`/`CP <N>b` (see `CP 14a`/`CP 14b` for column families).

## CLI

The CLI subcommands (`put`, `get`, `delete`, `scan`, `verify`, `dump`, `manifest-dump`, `compact`) are defined in `rocksdb-cli/src/main/java/com/hkg/rocksdb/cli/RocksDbCli.java`. The `rocksdb-cli` module is a plain `java-library` today — no `application` plugin, no distribution task. Drive it from `RocksDbCliTest` or by invoking `RocksDbCli.main` against a built classpath. Keys and values are treated as UTF-8 strings.
