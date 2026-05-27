# 02 — On-disk formats

Plan §4. The engine writes three persistent format families plus one pointer file:

| File | Purpose | Encoder | Decoder |
|---|---|---|---|
| `MANIFEST-NNNNNN` | Versioned log of `VersionEdit`s — the truth about which SSTables exist | `Manifest` + `VersionEditCodec` | `Manifest.replay` |
| `NNNNNN.log` | WAL of `MutationRecord`s | `LogWriter` + `MutationCodec` | `LogReader` + `MutationCodec` |
| `NNNNNN.sst` | Block-based SSTable | `BlockBasedTableWriter` | `BlockBasedTableReader` |
| `CURRENT` | Plain-text pointer to the active MANIFEST | `VersionSet.writeCurrent` | `VersionSet.readCurrent` |
| `CFs` | Sidecar listing CFs (only present under `ColumnFamilyDb`) | `ColumnFamilyDb` | `ColumnFamilyDb` |

All multi-byte integers are **little-endian** (matches RocksDB).

## DB directory layout

```
<dbDir>/
├── CURRENT              text: "MANIFEST-000001\n"
├── MANIFEST-000001      append-only edit log
├── 000002.log           active WAL
├── 000003.sst           L0 SSTable
├── 000004.sst           ...
└── CFs                  (only under ColumnFamilyDb — see 06-column-families.md)
```

Under `ColumnFamilyDb`, the **default CF lives at `<dbDir>/`** (so leveldb-era single-CF DBs open unchanged) and every **non-default CF** gets its own `<dbDir>/cf-NNNNNN/` subdirectory:

```
<dbDir>/
├── CFs                     sidecar registry (see 06-column-families.md)
├── CURRENT                 default CF's pointer
├── MANIFEST-000001         default CF's MANIFEST
├── 000002.log              default CF's WAL
├── 000003.sst              default CF's L0
├── cf-000001/              first non-default CF, fully self-contained
│   ├── CURRENT
│   ├── MANIFEST-000001
│   ├── 000002.log
│   └── 000003.sst
└── cf-000002/
    └── ...
```

> **Implementation status:** ADR-0006's target is one shared WAL + one shared MANIFEST at `<dbDir>/` with `cfId`-tagged records. Today each CF has its own WAL + MANIFEST in a subdirectory and the registry sidecar (`CFs`) is the recovery anchor. The on-disk API is the same; the layout differs.

## WAL format (`.log` and `MANIFEST-*`)

`WalConstants`: block size **32 KiB**, header size **7 bytes**. The MANIFEST reuses this framing exactly — its records are `VersionEdit` payloads instead of `MutationRecord` payloads.

### Block + fragment layout

```
File := Block*

Block (32 KiB) := Fragment* [zero padding]
                  // If remaining block bytes < 7 (a header doesn't fit),
                  // the writer zero-pads to the block boundary and starts
                  // the next fragment at offset 0 of the next block.

Fragment := crc32(4 LE) | length(2 LE) | type(1) | data[length]

type ∈ {
    0x00 ZERO_PADDING  // never a real record; signals "skip rest of block"
    0x01 FULL          // self-contained
    0x02 FIRST         // first of a multi-fragment record
    0x03 MIDDLE        // middle
    0x04 LAST          // last
}
```

The CRC is computed over `type || data` (not the length field).

### Logical-record framing

A logical record larger than `BLOCK_SIZE - HEADER_SIZE = 32 761` bytes is split into `FIRST | MIDDLE* | LAST`. Smaller records are one `FULL` fragment.

### Truncation / torn-write tolerance

`LogReader` distinguishes three end-of-file situations:

| Situation | Detection | Behaviour |
|---|---|---|
| **Clean EOF after intact records** | No more bytes after the last fragment chain ends with `LAST`/`FULL` | Reader returns `Optional.empty()` — every committed record was delivered. |
| **Torn write at tail** | CRC mismatch OR truncated fragment OR unknown type byte at the final fragment of the file | Reader returns `Optional.empty()` (silent drop). This is how the engine survives a process kill mid-WAL-write. |
| **Corruption mid-file** | CRC mismatch with more valid fragments following | Reader throws `WalCorruptionException` — real damage. |

The MANIFEST reader wraps `WalCorruptionException` into `ManifestCorruptionException` so callers can distinguish "MANIFEST unreadable" from a generic I/O error.

### WAL payload: `MutationCodec`

A WAL record payload is one `MutationRecord` encoded by `MutationCodec`:

```
+------+--------+--------+-------+----------------------+
| type | seq    | keyLen | key   | [op-specific tail]   |
| 1 B  | 8 B LE | 4 B LE | keyLen bytes               |
+------+--------+--------+-------+----------------------+

op-specific tail:
  type=0x01 Put:           valLen(4 LE) | val[valLen]
  type=0x00 Delete:        — (no tail)
  type=0x04 DeleteRange:   endKeyLen(4 LE) | endKey[endKeyLen]
                           // The leading "key" field is the startKey for DeleteRange.
```

| WAL op byte | MutationRecord | Notes |
|---|---|---|
| `0x00` | `Delete` | |
| `0x01` | `Put` | |
| `0x04` | `DeleteRange` | CP 17 addition. |

An unknown leading byte throws `WalCorruptionException`. There is no skip-unknown discipline at this layer — every WAL writer and reader must agree on every op byte. (ADR-0011 motivates this; new mutation types are an ADR, not a refactor.)

> **Implementation status:** ADR-0011 / ADR-0006's target is one WAL record per `WriteBatch` containing a `cfId`-keyed list of per-CF op lists. Today each WAL record carries exactly one `MutationRecord` and there is no `cfId` field in the framing.

## SSTable format (`.sst`)

Plan §4.1. Files are append-only; readers find blocks via the trailing footer + index walk.

### File layout

```
+----------------------+
| data block 0         |
| data block 1         |
|   ...                |
| data block N-1       |
+----------------------+
| bloom filter block   |  meta-key: "filter.leveldb.BuiltinBloomFilter2"
+----------------------+
| range-tombstone block|  meta-key: "rocksdb.range_tombstones"   (omitted if no RTs)
+----------------------+
| bottom-level index 0 |  only when two-level index is in effect
| bottom-level index 1 |
|   ...                |
+----------------------+
| index block          |  single-level: entries map (largestKey -> dataBlockHandle)
                          two-level:    entries map (largestKey-of-bottom-block -> bottomBlockHandle)
+----------------------+
| meta-index block     |  sorted entries: bloom, [range-tombstones], [two-level-marker]
+----------------------+
| footer (48 B)        |
+----------------------+
```

### Block framing

Every on-disk block (data, bloom, range-tombstone, index, meta-index) is followed by a 1-byte compression tag and a 4-byte CRC32:

```
+----------------+--------+----------+
| block payload  | compTy | crc32 LE |
| variable       | 1 B    | 4 B      |
+----------------+--------+----------+

CRC32 is computed over: (block payload || compType).
A BlockHandle returned by writeBlock covers ONLY the payload bytes (not the
compType or crc trailer), so trailer length is implicit (5 bytes).
```

| `compType` | Codec |
|---|---|
| `0x00` | none |
| `0x01` | deflate (JDK `java.util.zip.Deflater`) |
| `0x02..0x04` | reserved (Snappy / Zstd / LZ4) — writer rejects with a clear error per ADR-0012 |

The writer only emits deflate if the compressed body is smaller than the raw payload; otherwise it falls back to `none`.

### Data-block payload framing

Plan §4.1; encoded by `BlockBuilder`. Each block holds a sorted run of `(internalKey, value)` pairs with prefix compression and restart points:

```
entry := varint(sharedBytes) | varint(unsharedBytes) | varint(valueLen)
       | unsharedKeyBytes
       | valueBytes

block payload := entry* | restartArray (4 B LE × R) | restartCount (4 B LE)
```

- `RESTART_INTERVAL = 16` — every 16th entry starts a new restart group with `shared = 0` (full key bytes inline). Restart points let readers binary-search across groups before linear-scanning within one.
- Keys must be added in ascending order. The builder does not verify ordering — passing unsorted keys produces an invalid block.
- The same framing is used for the index block and meta-index block; only the *meaning* of the value bytes differs (a `BlockHandle` instead of a user value).

### Footer

```
+---------------------------+---------------------------+--------------+
| metaIndexHandle           | indexHandle               | magic (8 LE) |
| varlong+varlong, padded   | varlong+varlong, padded   |              |
| to 20 B                   | to 20 B                   |              |
+---------------------------+---------------------------+--------------+

ENCODED_LENGTH = 48 bytes
MAGIC          = 0xDB4775248B80FB57   // lifted verbatim from RocksDB
```

A `BlockHandle` is `varlong(offset) | varlong(length)`. Each `BlockHandle` is zero-padded to its 20-byte maximum encoding so the footer's total size is constant.

The reader seeks to `fileSize - 48`, parses the footer, and walks `indexHandle` to load the index block. Magic mismatch throws `SsTableFormatException("footer magic mismatch: ...")`.

> **Implementation status:** ADR-0010's target layout is 56 bytes with `metaIdxHandle | indexHandle | fileSize | fileChecksum | formatVersion | reserved | magic`. The current footer is 48 bytes with only `metaIdxHandle | indexHandle | magic`. The file-level checksum lives in `FileMetadata` (MANIFEST) rather than the footer, and there is no `formatVersion` field on disk — so the skip-unknown forward-compat story is **not** implemented at the SSTable footer level. See ADR-0010 for the rationale and §`08-error-handling.md` for the failure-mode consequence.

### Meta-index keys

Sorted UTF-8 keys; values are encoded `BlockHandle`s:

| Key | Always present? | Value points to |
|---|---|---|
| `filter.leveldb.BuiltinBloomFilter2` | yes | bloom-filter block |
| `rocksdb.range_tombstones` | only if file contains range tombstones | range-tombstone block |
| `rocksdb.index.two_level` | only if file uses two-level index | top-level index block (same as `footer.indexHandle`) |

Older readers tolerate the absence of `rocksdb.range_tombstones` and `rocksdb.index.two_level` — they fall back to "no RTs" and "single-level index" respectively. The bloom meta-key string is preserved verbatim from LevelDB so leveldb-java SSTables open here unchanged.

### Two-level index

Active when `fileSize > TWO_LEVEL_INDEX_THRESHOLD` (default 32 MiB; see `Constants.java`). ADR-0014.

- Bottom-level index blocks each cap at `INDEX_BLOCK_TARGET_BYTES` (default 4 KiB).
- Top-level index block holds `(largestKey-of-bottom -> bottomBlockHandle)`; small enough to pin in the `BlockBasedTableReader`.
- Bottom-level blocks ride the shared `BlockCache` — they are demand-loaded the same way data blocks are.

Read cost vs. single-level: one extra `Cache.lookupOrLoad` per `get`; details in [`04-read-path.md`](04-read-path.md).

## MANIFEST format

`MANIFEST-NNNNNN` files reuse the WAL physical framing (32 KiB blocks, 7-byte CRC header, FULL/FIRST/MIDDLE/LAST chunking). Each record payload is one or more `VersionEdit`s, encoded by `VersionEditCodec`:

```
VersionEdit := tag(1) | per-tag-fields

tag = 0x10  NEW_FILE        : varlong(level), varlong(fileNum), varlong(sizeBytes),
                              varlong(smallestLen), smallestBytes,
                              varlong(largestLen),  largestBytes,
                              hasChecksum(1) | [if 1: checksum(8 LE)]  // CP 19 suffix
tag = 0x11  DELETE_FILE     : varlong(level), varlong(fileNum)
tag = 0x12  SET_LOG_NUMBER  : varlong(logNumber)
tag = 0x13  SET_NEXT_FILENUM: varlong(nextFileNumber)
tag = 0x14  SET_LAST_SEQ    : varlong(lastSequence)
```

The `smallestBytes` / `largestBytes` payloads are `InternalKeyCodec.encode(InternalKey)` — userKey bytes + 8-byte trailer.

### CP 19 backward-compat trick

`NEW_FILE` gained a trailing `(hasChecksum, checksum)` suffix in CP 19. The decoder detects whether the suffix is present by peeking the next byte after `largestBytes`:

- Byte is `0` or `1` → it's the CP-19 `hasChecksum` flag; consume it (and the 8-byte checksum if `1`).
- Byte is `0x10..0x14` → it's the next edit's tag; older NewFile with no checksum.
- No remaining bytes → end of record.

This works because all defined tags are `≥ 0x10`. A pre-CP-19 reader that encountered a CP-19 record would fail at the spurious tag `0x00`/`0x01`; rocksdb-java has not been deployed, so we accept that asymmetry.

### Unknown tags = hard error

An unknown `editTag` in the middle of a record throws `IllegalArgumentException("unknown VersionEdit tag: 0xNN")`, which `Manifest.replay` translates into `ManifestCorruptionException`. There is **no skip-unknown semantics** at the MANIFEST level today.

> **Implementation status:** ADR-0010 calls for length-prefixed skip-unknown framing on every MANIFEST edit so a v1 reader can tolerate a v2-written record. The current `VersionEditCodec` does not implement length-prefixed bodies (fields are inline varints) and does not skip unknown tags. Forward compatibility across MANIFEST edits is **not** available; an `AddColumnFamily` edit tag (target: `0x15`/`0x16`/`0x17` per ADR-0006) added by a future version would break older readers.

### `Version` snapshot on rotation

When the active MANIFEST grows past `Constants.MANIFEST_ROTATION_BYTES` (4 MiB), `VersionSet.rotateManifest` writes a new MANIFEST containing a complete snapshot of the current `Version` — every `NewFile` edit for every live SSTable across every level, plus `SetLogNumber`, `SetNextFileNumber`, `SetLastSequence`. The `CURRENT` file is atomically rewritten (`temp + rename`) to point at the new MANIFEST, then the old one is deleted.

## `CURRENT` file

Plain text, exactly one line:

```
MANIFEST-000123\n
```

`VersionSet.readCurrent` parses the suffix. Missing file or non-`MANIFEST-` prefix throws `ManifestCorruptionException`. Writes go through a `temp + ATOMIC_MOVE` so a crash mid-write cannot leave a partially-written pointer.

## Filename patterns

From `FileNumber`:

| Pattern | Type |
|---|---|
| `MANIFEST-%06d` | MANIFEST log |
| `%06d.log` | WAL |
| `%06d.sst` | SSTable |
| `%06d.sst.tmp` | In-flight SSTable (deleted by `RocksDb.sweepOrphans` on open) |
| `CURRENT` | Active MANIFEST pointer (no number) |
| `CFs` | CF registry sidecar (multi-CF only) |

## See also

- ADR-0010 — format-compat skip-unknown framing (target; partly unimplemented — see status callouts).
- ADR-0011 — hand-rolled binary framing (no protobuf, no JSON).
- ADR-0006 — column families and shared-WAL target.
- ADR-0014 — two-level indices.
- ADR-0009 — range tombstones as a dedicated SSTable section.
- Source: `rocksdb-wal/.../{LogWriter,LogReader,MutationCodec,RecordType,WalConstants}.java`; `rocksdb-sstable-blockbased/.../{BlockBasedTableWriter,BlockBasedTableReader,Footer,BlockBuilder,Block,BloomBlock,RangeTombstoneBlock,VarInt,Compression}.java`; `rocksdb-manifest/.../{Manifest,VersionEditCodec,VersionEdit,VersionSet,Version,FileMetadata}.java`.
- Tests: every format module has byte-exact round-trip + golden-vector tests; see `*Test.java` in each.
