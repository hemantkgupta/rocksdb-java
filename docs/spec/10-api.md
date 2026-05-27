# 10 — Public API

The engine's public surface is two classes (`RocksDb`, `ColumnFamilyDb`), the `KvEngine` interface they implement, three tools (`DbVerify`, `DbDump`, `ManifestDump`), and a CLI wrapper (`RocksDbCli`). Everything else in the modules is implementation detail subject to change without notice.

> **Implementation status — options objects:** ADR-0004 / ADR-0007 / ADR-0006 reference `WriteOptions`, `ReadOptions`, and rich `DbOptions` types that the engine consults per call. These types **do not exist** in the current codebase. Configuration is positional arguments to `RocksDb.open(...)` and `ColumnFamilyDb.open(...)`. The signatures below reflect today's reality.

## `KvEngine` interface

`com.hkg.rocksdb.common.KvEngine`. The minimal contract every engine implements.

```java
public interface KvEngine extends AutoCloseable {
    void                  put(Key key, Slice value);
    Optional<Slice>       get(Key key);
    Optional<Slice>       get(Key key, Snapshot snapshot);
    void                  delete(Key key);
    Snapshot              snapshot();
    void                  releaseSnapshot(Snapshot snapshot);
    Iterator<MutationRecord> scan(Key from, Key to);   // throws UnsupportedOperationException today
    void                  flush();
    @Override void        close();
}
```

`scan` throws — see [`01-data-model.md`](01-data-model.md) status callout. `ColumnFamilyDb` does **not** implement this interface (its methods take a `ColumnFamilyHandle`).

## `RocksDb` — single-CF engine

`com.hkg.rocksdb.engine.RocksDb`. The assembled engine.

### `open` overloads

```java
public static RocksDb open(Path dbDir) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache, int compactionThreads) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache, int compactionThreads, RateLimiter rateLimiter) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache, int compactionThreads, RateLimiter rateLimiter, boolean kvChecksumEnabled) throws IOException;
public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache, int compactionThreads, RateLimiter rateLimiter, boolean kvChecksumEnabled, int userTimestampSize) throws IOException;
```

| Parameter | Default (shorter overloads) | Notes |
|---|---|---|
| `dbDir` | — | Directory; created if missing. Contains `CURRENT`, `MANIFEST-*`, `*.log`, `*.sst`. |
| `compressOutput` | `true` | Apply JDK `Deflater` to flushed and compacted block payloads when smaller than raw. |
| `blockCache` | `new LruBlockCache(8 MiB)` | Shared across all `BlockBasedTableReader`s this engine opens. |
| `compactionThreads` | `1` | Size of the `CompactionExecutor` worker pool. |
| `rateLimiter` | `null` | Optional `LOW`-priority pacing of compaction output blocks. |
| `kvChecksumEnabled` | `false` | Opt-in 4-byte CRC32 wrap of every value (see [`07-integrity.md`](07-integrity.md)). |
| `userTimestampSize` | `0` | `0` = UDT off; `8` = UDT on; other values throw `IllegalArgumentException`. |

### Writes

```java
void put(Key key, Slice value);
void delete(Key key);
void deleteRange(Key startKey, Key endKey);     // endKey exclusive; startKey<endKey strictly
void putWithUserTs(Key key, UserTimestamp ts, Slice value);   // requires userTimestampSize > 0
```

All write entry points:

1. `readOnlyLatch.ensureWritable()` (throws `ReadOnlyModeException` if latched).
2. `synchronized (writeLock)`.
3. Allocate sequence (`nextSequence.getAndIncrement()`).
4. KV-wrap if `kvChecksumEnabled`.
5. WAL append (durability per `WalDurability` the writer was opened with).
6. MemTable insert.
7. `maybeFlush()`.

`MultiGetPartialResult` is the only non-runtime exception writes can throw besides `ReadOnlyModeException`.

### Reads

```java
Optional<Slice> get(Key key);
Optional<Slice> get(Key key, Snapshot snapshot);
Optional<Slice> getAsOf(Key key, UserTimestamp asOfTs);    // requires userTimestampSize > 0

Map<Key, Optional<Slice>> multiGet(List<Key> keys);
Map<Key, Optional<Slice>> multiGet(List<Key> keys, Snapshot snapshot);
```

`multiGet` may throw `RocksDb.MultiGetPartialResult` (a `RuntimeException`) — `partial()` carries the successfully-resolved keys, `getCause()` carries the first observed failure.

### Snapshots

```java
Snapshot snapshot();
void releaseSnapshot(Snapshot snapshot);
long oldestLiveSnapshotSequence();    // for compactor & operator monitoring
```

### Flush + compaction

```java
void   flush();                                    // foreground; runs under writeLock
boolean maybeCompact() throws IOException;         // single job, returns true if one ran
int    runCompactionPass(int maxParallel) throws IOException;
int    runParallelL0Compaction(int targetJobs) throws IOException;
```

### Status + diagnostics

```java
boolean isReadOnly();
String  readOnlyReason();
void    attemptResume();              // operator-driven clear of the read-only latch
boolean kvChecksumEnabled();
boolean userTimestampsEnabled();
int     compactionThreads();
long    lastSequence();
Version currentVersion();             // immutable Version snapshot
BlockCache blockCache();
RateLimiter rateLimiter();            // may be null
```

### Lifecycle

```java
void close();                  // flushes active MT, closes WAL + readers + manifest + executor
void closeWithoutFlush();      // test-only: simulates a crash (WAL replay recovers MT)
```

## `ColumnFamilyDb` — multi-CF wrapper

`com.hkg.rocksdb.engine.ColumnFamilyDb`. Owns N `RocksDb` instances, one per CF (today's layout — see [`06-column-families.md`](06-column-families.md)).

```java
public static ColumnFamilyDb open(Path dbDir) throws IOException;
public static ColumnFamilyDb open(Path dbDir, boolean compressOutput, long blockCacheBytes,
                                  int compactionThreads, RateLimiter rateLimiter) throws IOException;
```

### CF registry

```java
ColumnFamilyHandle defaultColumnFamily();
Optional<ColumnFamilyHandle> getColumnFamily(String name);
Collection<ColumnFamilyHandle> columnFamilies();
ColumnFamilyHandle createColumnFamily(String name, ColumnFamilyOptions options) throws IOException;
void dropColumnFamily(ColumnFamilyId id) throws IOException;
```

`defaultColumnFamily()` is always present — every fresh `ColumnFamilyDb` opens with a "default" CF at id `0`.

### Per-CF mutations and reads

```java
void put(ColumnFamilyHandle cf, Key key, Slice value);
void delete(ColumnFamilyHandle cf, Key key);
Optional<Slice> get(ColumnFamilyHandle cf, Key key);
Optional<Slice> get(ColumnFamilyHandle cf, Key key, Snapshot snapshot);
void flush(ColumnFamilyHandle cf);
boolean compact(ColumnFamilyHandle cf) throws IOException;
```

Each call dispatches to the underlying per-CF `RocksDb`. Passing a `ColumnFamilyHandle` from a different `ColumnFamilyDb` (or a dropped CF) throws `IllegalArgumentException`.

> **Implementation status:** `ColumnFamilyDb` does not yet expose `deleteRange`, `putWithUserTs`, `getAsOf`, `multiGet`, snapshots, or compaction-style-aware dispatch. These exist on `RocksDb` but the per-CF wrapper does not yet route them. Reaching the underlying `RocksDb` via internal access is currently the only path.

## Configuration constants (`Constants.java`)

Plan §12. All knobs are compile-time. Changing them is a recompile, not a runtime config.

| Constant | Value | Purpose |
|---|---|---|
| `MEMTABLE_FLUSH_THRESHOLD_BYTES` | 4 MiB | MemTable size at which a flush fires. |
| `BLOCK_SIZE_BYTES` | 4 KiB | Target data-block size. |
| `L0_FILE_COUNT_TRIGGER` | 4 | L0 file count → L0 score = 1.0. |
| `LEVEL_SIZE_BASE_BYTES` | 10 MiB | Target size of L1. |
| `LEVEL_SIZE_MULTIPLIER` | 10 | Per-level size fan-out. |
| `MAX_LEVEL_COUNT` | 7 | Levels in the LSM. |
| `BLOOM_BITS_PER_KEY` | 10 | ~1% FPR at default. |
| `BLOOM_FILTER_FPP` | 0.01 | Target false-positive rate. |
| `BLOCK_CACHE_DEFAULT_BYTES` | 8 MiB | `LruBlockCache` default capacity. |
| `WAL_SYNC_DEFAULT` | `true` | Default WAL durability is `Sync`. |
| `MAX_SEQUENCE_NUMBER` | 2^56 − 1 | Sequence number upper bound. |
| `MANIFEST_ROTATION_BYTES` | 4 MiB | MANIFEST size that triggers snapshot+rotate. |
| `SST_FILE_TARGET_SIZE_BYTES` | 2 MiB | Target output SSTable size during compaction. |
| `L0_SLOWDOWN_WRITES_TRIGGER` | 8 | L0 count above which writes throttle. (declared, not enforced today) |
| `L0_STOP_WRITES_TRIGGER` | 12 | L0 count above which writes stop. (declared, not enforced today) |
| `TWO_LEVEL_INDEX_THRESHOLD` | 32 MiB | SSTable size above which two-level index is emitted. |
| `INDEX_BLOCK_TARGET_BYTES` | 4 KiB | Bottom-level index-block target in two-level layout. |

## Tools

All three live in `com.hkg.rocksdb.tools` and operate against a DB directory without holding the engine open.

### `DbVerify`

```java
public static Report DbVerify.run(Path dbDir) throws IOException;

public record Report(boolean ok, int filesScanned, long blocksScanned,
                     int failures, List<FileResult> perFile) {
    public String render();
}
public record FileResult(FileNumber fileNumber, boolean pass, String detail);
```

Walks every SSTable in the active `Version`. Verifies block CRC32 (every block) and file XXH64 (against MANIFEST-stored value). Returns `ok=false` if any layer fails on any file.

### `DbDump`

```java
public static void DbDump.run(Path dbDir, PrintWriter out) throws IOException;
```

Hex-dumps every entry in every SSTable. Used by the CLI's `dump` and (fallback) `scan` subcommands.

### `ManifestDump`

```java
public static void ManifestDump.run(Path dbDir, PrintWriter out) throws IOException;
```

Human-readable replay of every MANIFEST edit batch. Output is one line per edit; the `NEW_FILE` edits include the stored file checksum (or `NO_CHECKSUM` for pre-CP-19 entries).

## CLI (`RocksDbCli`)

`com.hkg.rocksdb.cli.RocksDbCli` in `rocksdb-cli`. The CLI module is a plain `java-library` today (no `application` plugin), so there is no `installDist` task. Drive it from `RocksDbCliTest` or by invoking `RocksDbCli.main(String[])` against a built classpath:

```
put     <dbDir> <key> <value>       Insert or overwrite a key.
get     <dbDir> <key>               Print the value, or "(not found)" + exit 1.
delete  <dbDir> <key>               Tombstone a key.
scan    <dbDir> [limit]             Stream every key (today: falls back to DbDump).
verify  <dbDir>                     Block-CRC + file-XXH64 walk; exit 0 PASS, 1 FAIL.
dump    <dbDir>                     Hex dump of every SSTable entry.
manifest-dump <dbDir>               Human-readable replay of every MANIFEST edit.
compact <dbDir>                     Trigger one compaction pass and exit.
```

Keys and values are UTF-8 strings. The exit code is:

| Code | Meaning |
|---|---|
| `0` | success / PASS / value found |
| `1` | "not found" / verify reported failures |
| `2` | usage error or IOException |

## Module-level public surface (per-module quick reference)

| Module | Exports |
|---|---|
| `rocksdb-common` | `Key`, `Slice`, `InternalKey`, `InternalKeyCodec`, `ValueType`, `SequenceNumber`, `MutationRecord`, `RangeTombstone`, `UserTimestamp`, `Snapshot`, `KeyLookup`, `FileNumber`, `KvEngine`, `Constants` |
| `rocksdb-memtable` | `SkipListMemTable` |
| `rocksdb-wal` | `LogWriter`, `LogReader`, `MutationCodec`, `RecordType`, `WalConstants`, `WalDurability`, `WalCorruptionException` |
| `rocksdb-bloom` | `BloomFilter`, `BloomFilter.Builder` |
| `rocksdb-sstable-blockbased` | `BlockBasedTableWriter`, `BlockBasedTableReader`, `Footer`, `BlockHandle`, `BlockBuilder`, `Block`, `Compression`, `BloomBlock`, `RangeTombstoneBlock`, `VarInt`, `WriteHook`, `SsTableFormatException`, `BlockChecksumMismatchException` |
| `rocksdb-manifest` | `Manifest`, `VersionEdit`, `VersionEditCodec`, `VersionSet`, `Version`, `FileMetadata`, `ManifestCorruptionException` |
| `rocksdb-block-cache` | `BlockCache`, `LruBlockCache`, `CacheKey` |
| `rocksdb-compaction-leveled` | `LeveledCompactionPicker`, `DynamicLeveledCompactionPicker`, `Compactor`, `CompactionJob`, `CompactionExecutor`, `MergingIterator` |
| `rocksdb-compaction-tiered` | `TieredCompactionPicker`, `TieredCompactionJob` |
| `rocksdb-compaction-fifo` | `FifoCompactionPicker`, `FifoDeletionJob` |
| `rocksdb-rate-limiter` | `RateLimiter`, `TokenBucketRateLimiter`, `Priority`, `FileDeletionScheduler` |
| `rocksdb-column-family` | `ColumnFamilyId`, `ColumnFamilyHandle`, `ColumnFamilyOptions`, `ColumnFamilyRegistry`, `CompactionStyle` |
| `rocksdb-integrity` | `KvChecksumCodec`, `FileChecksum`, `XxHash64`, `KvChecksumMismatchException`, `FileChecksumMismatchException` |
| `rocksdb-error-handling` | `Severity`, `SeverityClassifier`, `ReadOnlyModeLatch`, `ReadOnlyModeException` |
| `rocksdb-engine` | `RocksDb`, `ColumnFamilyDb` |
| `rocksdb-tools` | `DbVerify`, `DbDump`, `ManifestDump` |
| `rocksdb-test-cluster` | `RocksDbHost` |
| `rocksdb-cli` | `RocksDbCli` (also `main(String[])`) |

## See also

- [`01-data-model.md`](01-data-model.md) — types referenced in signatures.
- [`03-write-path.md`](03-write-path.md), [`04-read-path.md`](04-read-path.md), [`05-compaction.md`](05-compaction.md) — call-graph semantics.
- [`08-error-handling.md`](08-error-handling.md) — exceptions and their classification.
- ADR-0004, ADR-0007 — `WriteOptions` / `ReadOptions` target (not yet a type in the code).
- ADR-0012 — pure-Java commitment (no JNI APIs in the surface).
- Source: every module's `src/main/java/com/hkg/rocksdb/<module>/` top-level types are the public surface; everything `package-private` or under `internal/` (if added later) is implementation detail.
