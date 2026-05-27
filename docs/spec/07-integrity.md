# 07 — Integrity

Plan §7.5. ADR-0003. The FAST 2021 §5 finding: **40% of silent-corruption events propagate to replicas before detection**. A single integrity layer catches the disk-bit-rot case but not the in-memory / cache / CPU-register case that gets correctly written and correctly replicated as wrong bytes. The engine ships independent checksums at multiple layers.

## Layers

ADR-0003 defines four layers. Three are implemented today; the fourth is documented in the ADR but not wired.

| # | Layer | Algorithm | Computed | Verified | Where in code | Status |
|---|---|---|---|---|---|---|
| 1 | **Block** | CRC32C (via JDK `java.util.zip.CRC32`) | `BlockBasedTableWriter.writeBlock` — over `(payload \|\| compType)` | Every block read in `BlockBasedTableReader.loadBlock` (via `BlockCache.lookupOrLoad` loader) | `rocksdb-sstable-blockbased` | ✅ implemented |
| 2 | **File** | XXH64 (hand-rolled in `XxHash64`) | After `BlockBasedTableWriter.finish()` and after `Compactor.run` per output; stored in `FileMetadata.fileChecksum` and persisted in MANIFEST | `DbVerify.run`; backup/restore (future) | `rocksdb-integrity` | ✅ implemented |
| 3 | **Handoff** | (planned: CRC32C per block at the FS boundary) | (planned: engine → `fs.write(bytes, checksum)`) | (planned: `LocalFs` on receipt) | n/a | ❌ **not implemented** — the engine writes via plain `FileChannel.write` |
| 4 | **KV** | CRC32 over `(userKey \|\| value)` (4 B LE trailer on the value) | `RocksDb.put` if `kvChecksumEnabled = true` | `RocksDb.get` (and `multiGet` results) via `KvChecksumCodec.unwrap` | `rocksdb-integrity/KvChecksumCodec` | ✅ implemented (opt-in) |

> **Implementation status — handoff layer:** ADR-0003 calls for a fourth layer that catches bus errors and in-flight memory corruption between the engine and the filesystem. The current engine has no `LocalFs` abstraction — it writes via direct `FileChannel.write` — so this layer is **not implemented**. The block + file + KV layers still catch every documented corruption scenario in the test suite; the gap is the bus-error case specifically (which is rare and the §5 paper documents as the minority of the propagated cases).

## Where each layer fires

```mermaid
sequenceDiagram
    participant App as Application
    participant Eng as RocksDb
    participant MT as MemTable
    participant WAL
    participant SST as SSTable (writer / reader)
    participant Cache as BlockCache
    participant Cmp as Compactor

    Note over App,Eng: WRITE
    App->>Eng: put(key, value)
    Eng->>Eng: KV checksum wrap (if enabled)\n   value || CRC32(userKey || value):4
    Eng->>WAL: append(MutationCodec.encode(Put))
    Note over WAL: WAL record CRC32 fragment-level
    Eng->>MT: put(key, storedValue, seq)

    Note over Eng,SST: FLUSH
    Eng->>SST: write each block
    SST->>SST: per-block CRC32 trailer
    SST->>SST: finish()
    Eng->>Eng: FileChecksum.compute(sstPath) → XXH64
    Eng->>Eng: NewFile edit carries the XXH64 in FileMetadata

    Note over App,Cache: READ
    App->>Eng: get(key)
    Eng->>SST: lookup(key, asOf)
    SST->>Cache: lookupOrLoad(block)
    Cache->>SST: on miss, load + verify per-block CRC32
    SST-->>Eng: KeyLookup.Found(storedValue)
    Eng->>Eng: KV checksum unwrap (verify + strip)
    Eng-->>App: Optional<value>

    Note over Cmp: COMPACT
    Cmp->>SST: read input blocks (per-block CRC verified)
    Cmp->>SST: write output blocks (per-block CRC trailer)
    Cmp->>Cmp: FileChecksum.compute(outputPath) per output
    Note over Cmp: KV checksums forwarded byte-for-byte; NEVER re-derived
```

## Block CRC32

`BlockBasedTableWriter.writeBlock`:

```java
crc.reset();
crc.update(body, 0, body.length);
crc.update(compType);                       // CRC is over (body || compType), not the length
int crcVal = (int) crc.getValue();
```

Trailer layout per on-disk block: `payload | compType:1 | crc32:4 LE`. The `BlockHandle` returned by the writer covers only the payload (not the trailer); the trailer length is implicit (5 bytes) and re-read by the reader.

On read, `BlockBasedTableReader`'s loader reads the payload + trailer, recomputes the CRC, compares; mismatch throws `BlockChecksumMismatchException`. The severity classifier matches the class name and returns `HARD_ERROR_READ_ONLY`, which trips `ReadOnlyModeLatch`.

## File checksum (XXH64)

`FileChecksum.compute(path)` streams the file through `XxHash64` in 64 KiB chunks and returns the digest. Used by:

- **`RocksDb.doFlush`** — computed after `writer.finish()`, embedded in the `NewFile(level=0, FileMetadata(..., checksum))` edit before MANIFEST append.
- **`Compactor.run`** — computed per output SSTable after `writer.finish()`, embedded in the resulting `FileMetadata`.
- **`DbVerify.run`** — re-streams every SSTable and compares against the MANIFEST-stored checksum. Mismatch is reported in the per-file `PASS/FAIL` line.

The MANIFEST `NewFile` codec uses the CP-19 backward-compat trick: a trailing `(hasChecksum, [checksum])` suffix. Older NewFile records without the suffix decode to `FileMetadata.fileChecksum = null`; `DbVerify` reports `NO_CHECKSUM` rather than failing them.

## KV checksum

Opt-in via `RocksDb.open(..., kvChecksumEnabled=true)`. ADR-0003 framing: the KV checksum is the layer that catches what block-CRC cannot — bit-flips in MemTable, cache poisoning, register corruption.

### Wrap

```java
wrapped = value || CRC32(userKey || value):4 LE
```

The sequence number is deliberately **not** in the CRC: the read path can verify without threading the original write's seq through every layer's lookup API. The trade-off is that two writes with the same `(userKey, value)` at different seqs hash identically — accepted because the goal is bit-flip detection, not replay detection.

### Flow contract

- WAL, MemTable, SSTable writer/reader, block cache, **and compaction** see opaque `byte[]` that happens to be 4 bytes longer.
- **Compaction never re-derives the KV checksum**. Re-derivation would launder away in-memory corruption that occurred between the original write and the compaction read.
- `RocksDb.get` / `multiGet` unwrap at the boundary; any mismatch trips the read-only latch.

```java
public static byte[] unwrap(byte[] userKey, byte[] wrapped) {
    int valueLen = wrapped.length - 4;
    int storedCrc = ByteBuffer.wrap(wrapped, valueLen, 4).order(LE).getInt();
    int actualCrc = CRC32(userKey || wrapped[0..valueLen]);
    if (storedCrc != actualCrc) throw new KvChecksumMismatchException(...);
    return Arrays.copyOf(wrapped, valueLen);
}
```

`KvChecksumMismatchException` matches the severity classifier's substring rule on its class name and triggers `HARD_ERROR_READ_ONLY`. The latch trips before the bad byte propagates to a replica — the central §5 promise.

## DbVerify

`rocksdb-tools/.../DbVerify.run(dbDir)` walks every SSTable referenced by the active `Version` and reports:

| Per-file outcome | Meaning |
|---|---|
| `PASS` | Every block CRC32 verified; file XXH64 matches MANIFEST. |
| `FAIL block CRC at offset N` | Per-block CRC mismatch — disk bit rot or partial write. |
| `FAIL file checksum (stored=X actual=Y)` | XXH64 mismatch — whole-file rot, mis-attribution, or truncation. |
| `NO_CHECKSUM` | Pre-CP-19 file; only block CRC was verified. |

`DbVerify.Report.ok()` is true only if every file reports `PASS` (or `NO_CHECKSUM`). The CLI's `verify` subcommand surfaces this as the process exit code.

## Operational notes

- **MemTable bit-flip without KV checksum** — the engine has no way to detect it. The corrupt value is written to the next L0 SSTable, where its block CRC and file XXH64 both correctly reflect the corrupt bytes. The corruption is permanent and propagates to replicas. KV checksum is the only defence.
- **Block cache poisoning** — same. The cache holds decoded block bytes; a bit-flip in cache memory is read back as "valid" by the per-block CRC (which was verified on the original disk read, not on every cache hit). KV checksum catches it on the next `get` because the unwrap recomputes the CRC32 fresh.
- **Footer CRC** — there is no footer CRC. The structural anchor is the magic. A footer bit-flip is caught by the file XXH64 if it lands inside the footer; a magic mismatch trips the FATAL classifier path (engine refuses to remain open).
- **WAL torn write** — handled at the framing layer, not the integrity layer (see [`02-on-disk-formats.md`](02-on-disk-formats.md) and [`08-error-handling.md`](08-error-handling.md)).
- **MANIFEST CRC** — every MANIFEST record uses the WAL fragment CRC32 (it reuses the WAL physical format). Mid-record corruption throws `ManifestCorruptionException`, classified as FATAL.

## See also

- ADR-0003 — four-layer integrity model.
- ADR-0008 — severity classification of integrity failures.
- Source: `rocksdb-integrity/.../{KvChecksumCodec,FileChecksum,XxHash64,KvChecksumMismatchException,FileChecksumMismatchException}`; `rocksdb-sstable-blockbased/.../{BlockBasedTableWriter:writeBlock,BlockChecksumMismatchException}`; `rocksdb-engine/.../RocksDb:{put,unwrapKvChecksumIfNeeded,doFlush}`; `rocksdb-compaction-leveled/.../Compactor:run`; `rocksdb-tools/.../DbVerify`.
- Tests: `rocksdb-integrity/src/test/java/com/hkg/rocksdb/integrity/{XxHash64Test,KvChecksumCodecTest,FileChecksumTest}.java`; `rocksdb-engine/src/test/java/com/hkg/rocksdb/engine/{KvChecksumEngineTest,SeverityIntegrationTest}.java`; `rocksdb-tools/src/test/java/com/hkg/rocksdb/tools/DbVerifyTest.java`.
