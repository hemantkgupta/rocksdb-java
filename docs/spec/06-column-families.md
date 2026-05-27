# 06 — Column families

Plan §3.3, §4.4, §6.1. ADR-0006. A **column family (CF)** is a logical sub-collection within one DB handle that shares the write path with other CFs but has its own storage policy (compaction style, options, level structure). Bigtable's "locality group" framing applied to a single-tenant LSM.

## What a CF is and isn't

| A CF **is** the unit of... | A CF **is not** the unit of... |
|---|---|
| Separate MemTable (flushed independently) | Independent WAL (shared, ideally — see status callout) |
| Separate level structure (per-CF L0..L_max) | Independent MANIFEST (target: one shared) |
| Per-CF compaction style + options (`bloomBitsPerKey`, `memtableBytes`, `userTimestampSize`, `fifoMaxBytes`) | Multi-tenant security isolation — corrupt MANIFEST takes down all CFs |
| Per-CF flush + compaction triggers | Independent block cache or rate limiter (process-shared by default) |

## Types

```java
record ColumnFamilyId(int value);
record ColumnFamilyHandle(ColumnFamilyId id, String name);
record ColumnFamilyOptions(CompactionStyle compactionStyle, long fifoMaxBytes);

enum CompactionStyle { LEVELED, DYNAMIC_LEVELED, TIERED, FIFO }
```

`ColumnFamilyOptions` is minimal — RocksDB's real version carries dozens of knobs; this engine ships only the two the code actually branches on today.

| Factory | Style | `fifoMaxBytes` |
|---|---|---|
| `ColumnFamilyOptions.defaults()` | `LEVELED` | 0 (unused) |
| `ColumnFamilyOptions.leveled()` | `LEVELED` | 0 |
| `ColumnFamilyOptions.tiered()` | `TIERED` | 0 |
| `ColumnFamilyOptions.fifo(maxBytes)` | `FIFO` | `maxBytes` |

> **Implementation status:** ADR-0002's target default is `DYNAMIC_LEVELED`. `defaults()` returns `LEVELED` today. A subsequent CP is expected to flip the default once parallel-L0 tests are confirmed stable against dynamic sizing.

## ColumnFamilyDb

`ColumnFamilyDb` is a wrapper around one `RocksDb` instance per CF. It exposes the standard mutation/lookup API parameterised by `ColumnFamilyHandle`:

```java
ColumnFamilyHandle h = cfDb.createColumnFamily("users", ColumnFamilyOptions.leveled());
cfDb.put(h, Key.of("alice"), Slice.of("..."));
Optional<Slice> v = cfDb.get(h, Key.of("alice"));
cfDb.flush(h);
cfDb.compact(h);
cfDb.dropColumnFamily(h);
```

### On-disk layout (today)

Each CF is a **self-contained sub-DB**. The default CF lives at `<dbDir>/` itself (so a single-CF DB written before column families existed still opens); every non-default CF gets `<dbDir>/cf-NNNNNN/` named by its `ColumnFamilyId` (zero-padded to 6 digits):

```
<dbDir>/
├── CFs                       sidecar: <id>,<name>,<style>,<fifoCap> per line
├── CURRENT                   default CF's CURRENT
├── MANIFEST-000001           default CF's MANIFEST
├── 000002.log                default CF's WAL
├── 000003.sst                default CF's L0 SSTable
├── cf-000001/                first non-default CF
│   ├── CURRENT
│   ├── MANIFEST-000001
│   ├── 000002.log
│   └── 000003.sst
└── cf-000002/                next non-default CF
    └── ...
```

The default-CF-at-root layout is the **backwards-compatibility hinge**: a single-CF DB written by `RocksDb.open(dbDir)` directly is recognised as a one-CF `ColumnFamilyDb` on first open — no migration required.

The sidecar `CFs` is the recovery anchor — `ColumnFamilyDb.open` reads it, recreates each `ColumnFamilyHandle`, and opens the underlying `RocksDb` against the CF's subdirectory (or `dbDir` for the default). Each line is `<id>,<name>,<style>,<fifoCap>`.

### Target layout (plan / ADR-0006)

```
<dbDir>/
├── CURRENT                   one CURRENT for the whole DB
├── MANIFEST-000001           one MANIFEST, with edits tagged by cfId
├── 000002.log                one shared WAL; records carry cfId-keyed op lists
├── 000003.sst                SSTables flat in dbDir; MANIFEST records which cfId owns each
└── 000004.sst
```

Target MANIFEST edit tags (not yet implemented):

| Tag | Edit | Purpose |
|---|---|---|
| `0x15` | `AddColumnFamily(cfId, name)` | Persistent registration of a new CF |
| `0x16` | `DropColumnFamily(cfId)` | Tombstone for dropped CF |
| `0x17` | `SetCompactionStyle(cfId, style)` | Per-CF style change |

Each `NewFile` / `DeleteFile` edit would gain a `cfId` field so the MANIFEST replay routes SSTables to the right CF's level structure.

> **Implementation status:** The current sub-DB-per-CF layout means:
> - **A WAL stall in one CF does NOT stall other CFs** (because each CF has its own WAL — opposite of the target's "shared WAL = one fsync covers all CFs").
> - **Cross-CF atomic writes are impossible** (each CF's WAL is independent).
> - **One MANIFEST per CF** means rotation and replay happen per CF.
> - **Adding `WAL` durability mode changes per-write** would need to be CF-aware in the target, but is engine-wide today.
>
> The API contract (`put(handle, key, value)`) is identical to the target; only the on-disk layout differs. A future CP migrates without changing API.

## Lifecycle

### Create

```java
ColumnFamilyHandle h = cfDb.createColumnFamily("users", ColumnFamilyOptions.leveled());
```

1. Allocate the next `ColumnFamilyId` (`registry.nextId()`).
2. Create `<dbDir>/cf-<id>/`.
3. Open a fresh `RocksDb` rooted at that subdirectory.
4. Append a line to `<dbDir>/CFs`: `<id>,<name>,<style>,<fifoCap>`.
5. Return the `ColumnFamilyHandle`.

The sidecar write is the persistence point; an interrupted create leaves an orphan subdirectory that the next open ignores (no `CFs` entry → not part of the registry → swept on next sweep). The empty subdir is harmless but not currently cleaned.

### Drop

```java
cfDb.dropColumnFamily(h);
```

1. Close the underlying `RocksDb` for that CF.
2. Remove the entry from the in-memory registry.
3. Rewrite `<dbDir>/CFs` without that line.
4. Recursively delete `<dbDir>/cf-<id>/`.

A crash between steps 3 and 4 leaves the subdirectory on disk; the next open sees the directory but no registry entry → orphan, ignored. The bytes are reclaimed only by manual cleanup today.

### Get / put / delete / flush / compact

Each operation dispatches to the per-CF `RocksDb`:

```java
public void put(ColumnFamilyHandle h, Key k, Slice v) {
    instances.get(h.id()).put(k, v);
}
```

The handle's `id` indexes the in-memory `Map<ColumnFamilyId, RocksDb>`. Wrong handle (CF dropped, or handle from a different `ColumnFamilyDb`) throws `IllegalArgumentException`.

### Default CF

On a fresh `ColumnFamilyDb.open(dbDir)`, the engine creates `default` as CF id `0` if no `CFs` sidecar exists. This matches RocksDB's convention and gives every DB a starting CF.

## Per-CF compaction style — current effect

`ColumnFamilyOptions.compactionStyle` is **persisted in the `CFs` sidecar and preserved across reopen**. But `RocksDb.runCompactionPass` hard-wires `LeveledCompactionPicker` regardless of the CF's recorded style. The consequences:

| CF style | What actually happens today |
|---|---|
| `LEVELED` | Works as expected — leveled compaction picker drives the per-CF level structure. |
| `DYNAMIC_LEVELED` | Falls back to plain leveled (the engine doesn't read the per-CF style flag). |
| `TIERED` | Picker exists in `rocksdb-compaction-tiered` and has its own tests; engine wiring not present. Calling `compact(h)` runs the leveled picker. |
| `FIFO` | Same — picker + `FifoDeletionJob` exist in `rocksdb-compaction-fifo`; engine wiring missing. |

A future CP routes `runCompactionPass` to the picker that matches `cfOptions.compactionStyle`. The `CFs` sidecar already persists everything needed; the change is confined to the engine's compaction dispatch.

## Per-CF user-defined timestamps

ADR-0007: UDT is a per-CF option (`userTimestampSize`). Today `RocksDb.open` takes a single `userTimestampSize` parameter (engine-wide). `ColumnFamilyDb` does not yet expose this per CF — every CF in a `ColumnFamilyDb` runs with the same UDT setting.

## See also

- ADR-0001 — compaction style as runtime config.
- ADR-0006 — column family per locality group (the load-bearing decisions).
- ADR-0007 — UDT as per-CF option (target).
- Source: `rocksdb-column-family/.../{ColumnFamilyOptions,ColumnFamilyHandle,ColumnFamilyId,ColumnFamilyRegistry,CompactionStyle}`; `rocksdb-engine/.../ColumnFamilyDb`.
- Tests: `rocksdb-column-family/src/test/java/com/hkg/rocksdb/columnfamily/ColumnFamilyRegistryTest.java`; `rocksdb-engine/src/test/java/com/hkg/rocksdb/engine/ColumnFamilyDbTest.java`.
