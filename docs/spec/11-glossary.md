# 11 — Glossary

Quick reference for terms used in this spec, the ADRs, and source Javadoc.

| Term | Definition |
|---|---|
| **ADR** | Architecture Decision Record. Documents context, decision, rationale, rejected alternatives for a load-bearing choice. Numbered records live in `docs/adr/`. |
| **`asOf`** | `SequenceNumber` upper bound used by a read: only versions with `seq ≤ asOf` are visible. Set to `nextSequence - 1` for unscoped reads, or `snapshot.sequence()` for snapshot reads. |
| **Backward compatibility** | A new reader (version N+1) can open data written by an older writer (version N). The engine achieves this by never reusing a tag or changing a fixed offset. |
| **Block** | A unit of SSTable I/O: ~4 KiB of sorted entries (data block) or one of bloom/index/range-tombstone/meta-index (other block types). Every block has a 1-byte compression-type tag + 4-byte CRC32 trailer. |
| **Block cache** | Process-wide LRU of decoded block payloads keyed by `(dbDir, fileNumber, blockHandle)`. See [`09-concurrency-and-resources.md`](09-concurrency-and-resources.md). |
| **Bloom filter** | Per-SSTable probabilistic membership test. False positives possible (~1% at 10 bits/key); false negatives never. Probed before any block read in the per-file `lookup` path. |
| **Bottom level** | The highest-numbered level in the LSM (default `L_max = L6`). Tombstone GC only fires here. |
| **CF** | Column family. A logical sub-collection inside one DB handle with its own MemTable + level structure + compaction style. See [`06-column-families.md`](06-column-families.md). |
| **`cfId`** | Per-CF identifier in the (target) shared-WAL framing. Today each CF has its own WAL and the field is unused. |
| **CP** | Checkpoint. A numbered milestone in the implementation plan; commits map 1:1 to CPs. Phases group CPs (Phase 1+2 = ported leveldb-java; Phase 3+ = net-new RocksDB work). |
| **`CURRENT` file** | Plain-text pointer to the active MANIFEST file. Atomically updated via temp + rename. |
| **DELETION** | A tombstone — value-type tag `0x00` in the internal key. Suppresses older versions of the user key during read. |
| **Dynamic Leveled** | Compaction style that derives per-level targets from the actual L_max size instead of from a fixed L1 base. ADR-0002. Worst-case space overhead ~13% vs ~90% for plain leveled. |
| **FIFO compaction** | Compaction style that never rewrites — drops the oldest L0 file once total L0 bytes exceed `fifoMaxBytes`. Cache-of-recent-data workloads. |
| **File checksum** | XXH64 over every byte of an SSTable file. Stored in `FileMetadata.fileChecksum` and persisted in MANIFEST. Verified by `DbVerify`. Layer #2 of ADR-0003. |
| **FileNumber** | Monotonic 64-bit id assigned to every on-disk file. Produces canonical filenames via `tableFileName()`, `logFileName()`, `manifestFileName()`. |
| **Footer** | Last 48 bytes of an SSTable: `metaIndexHandle (20) | indexHandle (20) | magic (8)`. Magic is `0xDB4775248B80FB57`. |
| **Forward compatibility** | An older reader (version N) can open data written by a newer writer (version N+1). Achieved (in the target) via skip-unknown framing; **not yet implemented** in this engine. |
| **Frozen MemTable** | A MemTable that has been frozen by `freeze()` and is being flushed to L0. Briefly visible to readers as the second priority source. |
| **Handoff checksum** | (Target, not implemented.) A per-block CRC at the engine ↔ filesystem boundary to catch bus errors and in-flight memory corruption. Layer #3 of ADR-0003. |
| **HARD_ERROR_READ_ONLY** | One of three `Severity` buckets. Trips `ReadOnlyModeLatch`; reads continue, writes throw `ReadOnlyModeException`; cleared only by `attemptResume()`. |
| **In-flight files** | The `Set<FileNumber>` of SSTables currently being read by a compaction worker. The picker excludes these from candidate jobs. |
| **InternalKey** | `(userKey, sequence, type)`. Sort order: userKey ASC, sequence DESC, type DESC. Encoded as `userKey || ((seq << 8) | type) as 8 B LE`. |
| **JNI** | Java Native Interface. **Not used** anywhere in this engine — ADR-0012. |
| **KV checksum** | Per-value CRC32 trailer over `(userKey || value)`. Wrapped at `RocksDb.put`, verified at `RocksDb.get`. Layer #4 of ADR-0003. Opt-in via `kvChecksumEnabled`. |
| **`KeyLookup`** | Three-way result from a per-source lookup: `Found(value)`, `Tombstoned()`, `Absent()`. Sealed for exhaustive `switch`. |
| **L0** | Level 0 of the LSM. Files may overlap; reads probe newest-first by file number. |
| **L1..L_max** | Higher levels. Each is non-overlapping (binary search finds at most one candidate file per level per key). |
| **LSM** | Log-Structured Merge tree. Writes go to a MemTable; flushes produce L0 SSTables; compactions migrate data downward through levels. |
| **`MAX_LEVEL_COUNT`** | 7. The total number of levels (L0 through L6). |
| **MANIFEST** | Append-only log of `VersionEdit`s. Reuses the WAL's 32-KiB-block / 7-byte-header framing. The truth about which SSTables exist. |
| **MemTable** | In-memory write buffer. Backed by `ConcurrentSkipListMap<InternalKey, byte[]>` plus a `CopyOnWriteArrayList<RangeTombstone>`. |
| **MultiGet** | Batched point-lookup API resolving N keys at one consistent `asOf`, with per-SSTable bucketing dispatched onto `ForkJoinPool.commonPool()`. ADR-0013. |
| **`MutationRecord`** | Sealed type: `Put` / `Delete` / `DeleteRange`. The WAL payload format. |
| **`NewFile`** | MANIFEST edit (tag `0x10`) registering a new SSTable at a level. Carries the `FileMetadata` including the (CP 19) file checksum. |
| **`oldestLiveSnapshotSeq`** | The minimum sequence number in `activeSnapshotSeqs`, or `Long.MAX_VALUE` if none. Compaction consults this to decide which tombstones it can drop at the bottom level. |
| **Picker** | The compaction-style-specific class that chooses *which* files to compact next. One per style: `LeveledCompactionPicker`, `DynamicLeveledCompactionPicker`, `TieredCompactionPicker`, `FifoCompactionPicker`. |
| **Range tombstone** | `[startKey, endKey)` exclusive range deletion. One record covers any number of user keys. ADR-0009. |
| **Read amplification** | Ratio of bytes read from disk per logical read. Bloom filters and the block cache reduce it. |
| **Read-only latch** | `ReadOnlyModeLatch` — one-way gate that turns the engine read-only after a HARD-severity error. ADR-0008. |
| **Restart point** | Position in a data block where prefix compression resets (every 16 entries by default). Lets readers binary-search across restart groups. |
| **`SequenceNumber`** | 56-bit monotonic per-write counter. Allocated by `AtomicLong nextSequence`. Packed with `ValueType` into the 8-byte internal-key trailer. |
| **Severity** | Three-bucket classification: `TRANSIENT_RETRYABLE` / `HARD_ERROR_READ_ONLY` / `FATAL_HALT`. ADR-0008. |
| **Skip-unknown framing** | Length-prefixed records that older readers can step over without breaking. ADR-0010's target; **not yet implemented** at the SSTable or MANIFEST level. |
| **Slice** | A windowed view over a `byte[]`. `(backing, offset, length)`. Equality compares bytes. |
| **Snapshot** | Wrapped `SequenceNumber`. `db.snapshot()` registers; `db.releaseSnapshot()` unregisters. ADR-0015. |
| **SSTable** | Sorted String Table. On-disk format: data blocks → bloom → range tombstones → index → meta-index → 48-byte footer. |
| **`SST_FILE_TARGET_SIZE_BYTES`** | 2 MiB. Compaction rolls a new output SSTable when the current one reaches this size. |
| **Target level size** | For leveled: `LEVEL_SIZE_BASE_BYTES × LEVEL_SIZE_MULTIPLIER^(L-1)`. For dynamic leveled: derived from `actualSize(L_max)` and shrunk downward. |
| **Tiered compaction** | Size-tiered / universal compaction. Merges adjacent runs of similar size. Best for write-heavy / logging workloads. |
| **TRIM storm** | A burst of file deletions that forces the SSD's FTL journal to rewrite, competing with foreground I/O. `FileDeletionScheduler` paces deletions through the rate limiter to prevent this. |
| **Two-level index** | SSTable index format for files larger than `TWO_LEVEL_INDEX_THRESHOLD` (32 MiB). Top-level pinned; bottom-level rides the block cache. ADR-0014. |
| **UDT** | User-defined timestamp. Application-supplied 8-byte BE value paired with each `put`. Visibility: `entry.userTs ≤ asOfTs`. ADR-0007. |
| **Universal compaction** | Synonym for tiered in some RocksDB literature. |
| **`Version`** | Immutable snapshot of which SSTables live at which level, plus `logNumber`, `lastSequence`, `nextFileNumber`. Held by `VersionSet`. |
| **`VersionEdit`** | One mutation to the `Version`: `NewFile`, `DeleteFile`, `SetLogNumber`, `SetNextFileNumber`, `SetLastSequence`. The MANIFEST is a log of these. |
| **WAL** | Write-Ahead Log. Every mutation is appended (and, under `Sync`, fsynced) here before the MemTable insert. Crash recovery replays WAL to rebuild the MemTable. |
| **WAL durability mode** | `Sync` / `Buffered` / `Disabled`. Per-`WriteOptions` in ADR-0004 target; per-engine in current code. |
| **Write amplification** | Ratio of bytes written to disk per logical write byte. Leveled compaction = ~16×; dynamic leveled = same; tiered = ~5×; FIFO = ~2×. |
| **`writeLock`** | The single `Object` lock every `RocksDb` write call holds. Compaction picker and apply phases also take it briefly. |
| **`WriteHook`** | Functional interface called by `BlockBasedTableWriter` before each block write. Used to install rate-limited pacing on compaction output. |
| **XXH64** | The file checksum algorithm — hand-rolled pure-Java in `rocksdb-integrity/XxHash64`. |
