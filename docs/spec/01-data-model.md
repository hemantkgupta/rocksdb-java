# 01 — Data model

Plan §5.1; Javadoc references `§5.1`.

The data-model types live in `rocksdb-common` and are the only types every other module shares. They are deliberately small and almost all `record`s; equality is value-based.

## Type catalog

| Type | Module | Shape | Purpose |
|---|---|---|---|
| `Slice` | common | `(backing: byte[], offset: int, length: int)` | A windowed view over a `byte[]`. Equality compares bytes. Mirrors RocksDB C++ `Slice`. |
| `Key` | common | wraps `Slice` | A user-supplied key. `Comparable<Key>` — unsigned-byte lexicographic. |
| `ValueType` | common | sealed: `Value(tag=0x01)` \| `Deletion(tag=0x00)` | Internal-key value-type tag. |
| `SequenceNumber` | common | `record(long value)` | 56-bit monotonic write counter. |
| `InternalKey` | common | `(userKey, sequence, type)` | RocksDB's per-version key. |
| `MutationRecord` | common | sealed: `Put` \| `Delete` \| `DeleteRange` | One WAL-level mutation. |
| `RangeTombstone` | common | `(startKey, endKey, sequence)` | `[start, end)` exclusive range deletion. |
| `UserTimestamp` | common | `record(long value)` | 8-byte BE application-supplied timestamp. |
| `Snapshot` | common | `record(SequenceNumber sequence)` | A frozen read-point. |
| `KeyLookup` | common | sealed: `Found(Slice)` \| `Tombstoned` \| `Absent` | Per-source lookup result; exhaustive `switch`. |
| `FileNumber` | common | `record(long value)` | Monotonic id; produces canonical filenames. |
| `KvEngine` | common | interface | Top-level engine surface implemented by `RocksDb`. |

## InternalKey

The RocksDB internal key combines a user key with a per-write `SequenceNumber` and a `ValueType` tag. **Ordering** is what makes a single-pass read work:

```
userKey  ASC
sequence DESC
type     DESC
```

Equivalent to comparing the packed 8-byte trailer `((seq << 8) | type)` in **descending** order. The DESC tie-break on `type` is load-bearing: a snapshot lookup at sequence `S` probes `(userKey, S, VALUE)` and the `ceilingEntry` walk correctly returns a same-sequence tombstone if one exists.

### Encoding (`InternalKeyCodec.encode`)

```
+----------------------+--------------------------------+
| userKey bytes        | packed trailer (8 B LE)        |
| variable             | (seq << 8) | type              |
+----------------------+--------------------------------+
```

`TRAILER_LEN = 8`. The 56-bit cap on `SequenceNumber` is exactly the space left for `seq` after the 1-byte type tag in the packed trailer.

| Helper | Purpose |
|---|---|
| `encode(InternalKey)` | Produce the on-disk byte form. |
| `decode(byte[])` | Parse back to `InternalKey`. |
| `compareInternalBytes(byte[], byte[])` | Compare encoded forms — must match `InternalKey.compareTo`. SSTable readers use this on raw block entries without re-allocating `InternalKey`. |
| `userKeyOf(byte[])` / `sequenceOf` / `tagOf` | Slice the encoded form. |

## ValueType

```
0x01 = VALUE     (Put — entry carries a live value)
0x00 = DELETION  (Delete — entry is a tombstone)
```

These two tags are the only ones the codec accepts; an unknown tag throws `IllegalArgumentException`. The `MutationCodec` (WAL) uses different op-type bytes (see [`02-on-disk-formats.md`](02-on-disk-formats.md)) — do not confuse the two.

## SequenceNumber

- Range: `[0, 2^56 - 1]`. `MAX = (1L << 56) - 1L`. `ZERO = new SequenceNumber(0L)`.
- Construction validates the range; out-of-range throws `IllegalArgumentException`.
- `next(prev)` returns `prev + 1` (throws if it would overflow `MAX`).
- The engine assigns sequences via an `AtomicLong nextSequence`. One sequence per `put` / `delete` / `deleteRange`. No batching today.

> **Implementation status:** the production RocksDB allocates one sequence per `WriteBatch` (so N ops in a batch share one number, recovered atomically). This engine assigns one per individual mutation — the `MutationRecord` sealed type does not model a multi-op batch. ADR-0011 / ADR-0006 describe the target shared-WAL framing with a per-record list of ops; today the WAL is one mutation per record.

## MutationRecord

Sealed; `switch` is exhaustively checked.

| Variant | Fields | WAL type byte |
|---|---|---|
| `Put(key, value, sequence)` | `Key`, `Slice`, `SequenceNumber` | `0x01` |
| `Delete(key, sequence)` | `Key`, `SequenceNumber` | `0x00` |
| `DeleteRange(startKey, endKey, sequence)` | `Key`, `Key`, `SequenceNumber` | `0x04` |

The shared `key()` accessor returns `startKey` for `DeleteRange` — code that needs range semantics must pattern-match on the concrete type.

## RangeTombstone

```
(startKey, endKey, sequence)    where startKey < endKey strictly
```

- `endKey` is exclusive (SQL convention).
- `covers(userKey)` returns true iff `startKey ≤ userKey < endKey`.
- Construction validates `startKey < endKey` strictly — equal or inverted bounds throw `IllegalArgumentException`.
- Visibility under a snapshot read at `asOf`:
  - A tombstone with `seq ≤ asOf` suppresses every `Put` for a covered key with `seq ≤ asOf AND seq < tombstone.seq`.
  - A `Put` with `seq > tombstone.seq` wins (later writer beats earlier deleter).

Range tombstones are stored separately from data — in the MemTable's secondary `CopyOnWriteArrayList<RangeTombstone>`, and as a dedicated SSTable meta-block. They are **never expanded into N point tombstones**; the whole point of ADR-0009 is that they stay as one record from API call to bottom-level compaction.

## UserTimestamp

8-byte big-endian application-supplied timestamp. The engine treats it as opaque — the application defines the meaning (Unix epoch ms, microsecond clock, 64-bit HLC, etc.).

- `ENCODED_LENGTH = 8` (the only supported size in v1).
- Storage layout: the user timestamp is appended to the user-key bytes at `put` time. The internal key is `(userKey || userTs, seq, type)` — i.e. `RocksDb` extends the stored user key, not the `InternalKey` shape.
- Read visibility: `entry.userTs ≤ asOfTs`.

> **Implementation status:** ADR-0007's target is for `InternalKey` itself to carry a separate `userTs` slot (`(userKey, userTs, seq, type)`) so the bloom filter probes on `userKey` alone. The current code appends `userTs` to the stored key bytes and rebuilds a per-engine in-memory floor index at open time. Both behaviours satisfy the API contract; the on-disk layout differs.

## Snapshot

```
record Snapshot(SequenceNumber sequence)
```

A `Snapshot` is just a wrapped sequence number. Its lifecycle lives in `RocksDb`:

- `snapshot()` reads `nextSequence`, registers `seq - 1` in `activeSnapshotSeqs` (a `ConcurrentHashMap.newKeySet()`), and returns the handle.
- `releaseSnapshot(s)` removes the entry.
- `oldestLiveSnapshotSequence()` returns the minimum of `activeSnapshotSeqs`, or `Compactor.NO_SNAPSHOTS_HELD = Long.MAX_VALUE` if none are held.
- The compactor consults this value to decide which tombstones it can drop at the bottom level.

A snapshot held forever is a space-amp leak — every tombstone with `seq > snapshot.seq` is pinned at the bottom level until release. ADR-0015 documents this as the operator's responsibility.

## KeyLookup

Three-way result returned by every per-source lookup (`SkipListMemTable.lookup`, `BlockBasedTableReader.lookup`):

```
Found(Slice value)   — live value; return it
Tombstoned()         — deletion tombstone; older sources MUST NOT be consulted; return absent
Absent()             — not present here; keep looking in older sources
```

Sealed so the engine's read path can `switch` exhaustively. `KeyLookup.ABSENT` and `KeyLookup.TOMBSTONED` are pre-allocated singletons.

## FileNumber

Monotonic 64-bit ids assigned to every on-disk file. Filename helpers:

| Method | Pattern | Example |
|---|---|---|
| `tableFileName()` | `%06d.sst` | `000123.sst` |
| `logFileName()` | `%06d.log` | `000123.log` |
| `manifestFileName()` | `MANIFEST-%06d` | `MANIFEST-000123` |

The `Version` tracks `nextFileNumber` so a recovered DB never reuses an id. Allocators bump the counter and emit a `SetNextFileNumber` edit so the high-water mark survives reopen.

## KvEngine interface

```java
interface KvEngine extends AutoCloseable {
    void put(Key key, Slice value);
    Optional<Slice> get(Key key);
    Optional<Slice> get(Key key, Snapshot snapshot);
    void delete(Key key);
    Snapshot snapshot();
    void releaseSnapshot(Snapshot snapshot);
    Iterator<MutationRecord> scan(Key from, Key to);
    void flush();
    void close();
}
```

Implemented by `RocksDb` (the single-CF engine) in `rocksdb-engine`. `ColumnFamilyDb` (multi-CF wrapper) does not implement `KvEngine`; its methods take a `ColumnFamilyHandle` as an extra parameter.

> **Implementation status:** `scan(from, to)` throws `UnsupportedOperationException("scan() lands in CP 10 (Phase 3)")`. Despite the comment, no public forward-iterator API has been added. The CLI's `scan` subcommand falls back to `DbDump.run`. Range iteration is a real gap.

## See also

- ADR-0007 — user-defined timestamps
- ADR-0009 — DeleteRange as one range-tombstone
- ADR-0015 — snapshot as sequence-number freeze
- Source: `rocksdb-common/.../{Slice,Key,InternalKey,InternalKeyCodec,ValueType,SequenceNumber,MutationRecord,RangeTombstone,UserTimestamp,Snapshot,KeyLookup,FileNumber,KvEngine}.java`
- Tests: `rocksdb-common/src/test/java/com/hkg/rocksdb/common/*Test.java`
