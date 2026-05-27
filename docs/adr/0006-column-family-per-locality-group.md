# ADR-0006: Column family per locality group (shared WAL, separate MemTables, per-CF level structure)

- Status: accepted
- Date: 2026-05-27
- Phase: 3
- CP: 14

## Context

The FAST 2021 paper opens (§1) with the observation that one engine binary is embedded
in 30+ Meta applications and 39 ZippyDB deployments, where a single DB handle commonly
holds several logical sub-collections that share a write path but want different
storage policies — e.g., a write-buffer CF that wants `TIERED` next to an index CF
that wants `DYNAMIC_LEVELED`, or a metadata CF that wants UDTs next to a payload CF
that does not. Forcing each into its own DB would multiply WAL fsyncs, MANIFEST files,
recovery passes, and compaction queues per host.

The shape of the abstraction is the load-bearing decision. RocksDB inherits Bigtable's
**locality group** framing (Bigtable §6.4) rather than the "independent database"
framing: a CF is a *physical co-tenant* of other CFs on one write path, not a
fully-isolated DB. This matches how the §2.2 workload classes actually compose: Stream
Processing state stores typically hold one "state" CF plus one "metadata" CF in one
shared write-amp/durability budget.

## Decision

A column family is the unit of:

- **Separate MemTable** — one `SkipListMemTable` per CF, frozen and flushed
  independently. A flush of CF A does not flush CF B.
- **Separate level structure** — `Version.families[cfId].levels[]` is per-CF. L0
  triggers, level targets, and bottom-level computation are scoped to one CF.
- **Per-CF compaction style and options** — `ColumnFamilyOptions.compactionStyle` and
  the picker bound to it; `bloomBitsPerKey`; `userTimestampSize`;
  `userTimestampRetention`; `memtableBytes`.

A column family is **not** the unit of:

- **WAL** — one WAL is shared across all CFs in a DB. Each WAL `WRITE_BATCH` record
  carries a `cfId`-keyed list of per-CF edits (§4.2). One fsync per `WriteBatch` covers
  every CF the batch touched.
- **MANIFEST** — one MANIFEST per DB. `AddColumnFamily` / `DropColumnFamily` /
  `SetCompactionStyle` edits (§4.3 tags `0x15`/`0x16`/`0x17`) carry the per-CF metadata.
- **Block cache** and **RateLimiter** — process-shared by default per ADR-0005 and the
  block-cache resource discussion in §4 of the paper.

The directory layout follows: no per-CF subdirectory; all SSTables for all CFs live in
the DB directory (§4.4) and the MANIFEST is the truth about which CF owns each file.

## Rationale

A shared WAL is what makes a CF cheaper than a DB. The dominant cost of `Sync` writes
on flash (ADR-0004) is the fsync, not the bytes written; sharing one WAL across N CFs
means one fsync per atomic batch instead of N. That is what makes "use a CF" the right
default answer when an application wants logical separation.

Separate MemTables and level structures are what make a CF more useful than a
key-prefix convention inside one CF. They let the engine flush, compact, and tune each
CF on its own schedule — a hot CF can be flushed at 64 MiB while a cold CF accumulates
to 512 MiB; an index CF can run `DYNAMIC_LEVELED` (ADR-0002) while a logging CF runs
`TIERED` for the Table 3 write-amp win.

Bigtable's locality groups are the structural ancestor: groups inside one tablet share
the SSTable infrastructure but separate the on-disk column-family encoding so that
scans touch only the groups they need. RocksDB column families inherit the same
trade-off shape (cheap to add, expensive to fully isolate) and the same operational
guidance: use CFs for storage-policy diversity within one logical dataset, not for
multi-tenant isolation across teams.

## Consequences

Positive:
- One DB handle serves multiple storage policies — Table 2's workload classes can
  coexist in one process without multiplying WAL fsyncs.
- Per-CF compaction style enables the ADR-0001 runtime-configurability promise at a
  finer granularity than per-DB.
- Dropping a CF is a single `DropColumnFamily` MANIFEST edit plus background file
  deletion — orders of magnitude cheaper than dropping a DB.

Negative:
- A WAL stall (slow fsync) stalls *every* CF, not just the writer. Operators expecting
  per-CF isolation get surprised.
- The CF abstraction is not a security boundary — one corrupt MANIFEST takes down all
  CFs in the DB. Multi-tenant isolation still needs separate DBs.
- Recovery replays the shared WAL once but must route each batch's `cfId` edits to the
  correct per-CF MemTable; a `cfId` referencing a not-yet-created CF in mid-replay is a
  recoverable error (CP 14 covers it) but a code path that must be tested.

## Alternatives considered

- **One MemTable + WAL + level structure shared across all CFs (CF as key-prefix).**
  Rejected: loses per-CF compaction style and per-CF flush triggers; the whole point
  of CFs is per-CF storage policy.
- **One WAL per CF (full isolation).** Rejected: multiplies fsyncs per atomic batch by
  N; defeats the §4 host-budget framing; makes cross-CF atomic writes impossible.
- **Per-CF subdirectories on disk.** Rejected: complicates the MANIFEST-as-truth model;
  adds inode churn; SSTables are already self-identifying by file number.

## References

- Implementation plan §3.3 (`rocksdb-column-family`), §4.2 (WAL framing with `cfId`),
  §4.4 (column-family encoding on disk), §6.1 (write path through shared WAL),
  Phase 3 CP 14, §10 (ADR 0006).
- FAST 2021 paper §1 (multi-tenant embedding patterns), §2.2 (workload classes
  coexisting in one DB), §4 (resource sharing).
- Bigtable paper §6.4 (locality groups — the structural ancestor of CFs).
- Repo: `rocksdb-column-family`, `rocksdb-engine` (per-CF MemTable + level wiring).
- Original RocksDB source: `db/column_family.cc`, `include/rocksdb/options.h`
  (`ColumnFamilyOptions`).
