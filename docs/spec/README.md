# rocksdb-java specification

This directory is the **reference specification** for the rocksdb-java engine. It documents the data model, on-disk formats, runtime paths (read / write / compaction), public API, integrity model, error handling, concurrency, and operational resources at a level of detail that lets a reader — human or LLM — make a correct change without dropping into source code for byte-level questions.

## What this spec is (and isn't)

| Document | Purpose | Reader's question |
|---|---|---|
| `README.md` (root) | One-page repo intro | "What is this project?" |
| `docs/architecture.md` | Module dependency graph | "What depends on what?" |
| `docs/adr/*` | Decision records — context, decision, rationale, rejected alternatives | "Why was this designed this way?" |
| `docs/spec/*` (this) | Reference of how the engine works today | "How does the engine behave? What bytes does it write?" |
| `CLAUDE.md` | AI assistant onboarding | "What conventions must I follow when editing?" |

The spec is **self-contained**. Plan-section references in source Javadoc (`§4.1`, `§6.3`, etc.) point to an external implementation plan in a sibling repo; the relevant claims have been restated here so this repo stands alone.

The spec is **status-aware**. Each section explicitly distinguishes:

- **Implemented** — what the code does today.
- **Target** — what the ADRs / plan describe as the destination.
- **Gap** — where the two diverge. Look for the call-out:

> **Implementation status:** *what's actually there today vs. the target ADR claim.*

This matters because the README's phase table currently shows Phases 3-7 as "pending" while the commit log shows them landed; the spec is the authoritative reconciliation.

## How to navigate

| # | File | Contents |
|---|---|---|
| — | [`README.md`](README.md) | This index |
| 01 | [`01-data-model.md`](01-data-model.md) | `Slice`, `Key`, `InternalKey`, `ValueType`, `SequenceNumber`, `MutationRecord`, `RangeTombstone`, `UserTimestamp`, `Snapshot`, `KeyLookup`, `FileNumber` |
| 02 | [`02-on-disk-formats.md`](02-on-disk-formats.md) | DB directory layout; WAL framing; SSTable block / bloom / index / footer; MANIFEST edit tags; `CURRENT` |
| 03 | [`03-write-path.md`](03-write-path.md) | `put` / `delete` / `deleteRange` flow; WAL durability modes; MemTable insert; flush trigger |
| 04 | [`04-read-path.md`](04-read-path.md) | `get` walk (MemTable → frozen → L0 → L1..L_max); bloom + cache + two-level index; `MultiGet`; `getAsOf` |
| 05 | [`05-compaction.md`](05-compaction.md) | Picker styles; `Compactor` per-user-key rules; snapshot pinning; parallel L0→L1; file deletion |
| 06 | [`06-column-families.md`](06-column-families.md) | `ColumnFamilyDb`; per-CF storage policy; today's per-CF subdirs vs. target shared WAL+MANIFEST |
| 07 | [`07-integrity.md`](07-integrity.md) | Block / file / handoff / KV checksums; computed-vs-verified points; `DbVerify` |
| 08 | [`08-error-handling.md`](08-error-handling.md) | `SeverityClassifier`; `ReadOnlyModeLatch`; failure-modes catalog |
| 09 | [`09-concurrency-and-resources.md`](09-concurrency-and-resources.md) | Lock model; thread pools; block cache; rate limiter; `RocksDbHost`; memory budget |
| 10 | [`10-api.md`](10-api.md) | Public surface (`KvEngine`, `RocksDb`, `ColumnFamilyDb`, tools, CLI) |
| 11 | [`11-glossary.md`](11-glossary.md) | LSM / CP / CF / write-amp / etc. — quick lookup |

## Conventions used in the spec

- **Hex tables** show bytes left-to-right, little-endian where the engine encodes that way (matching RocksDB).
- **Mermaid** is used for sequence and state diagrams.
- **`§N` references** at the top of a section indicate which plan section the corresponding spec content covers.
- **Every section ends with `## See also`** linking to the relevant ADR, source files, and tests.
- **Code references use `module/relative/path:Class` notation** (e.g. `rocksdb-engine/.../RocksDb:put` rather than `rocksdb-engine/src/main/java/com/hkg/rocksdb/engine/RocksDb.java#L308`) — line numbers go stale.

## See also

- [`README.md`](../../README.md) — one-page intro and phase status.
- [`architecture.md`](../architecture.md) — module dependency graph.
- [`adr/`](../adr/) — 15 architecture decision records.
- [`CLAUDE.md`](../../CLAUDE.md) — AI-assistant conventions for editing this repo.
