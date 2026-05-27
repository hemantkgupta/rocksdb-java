# ADR-0008: Severity-based error handling — transient retried, hard read-only, fatal halt

- Status: accepted
- Date: 2026-05-27
- Phase: 5
- CP: 22

## Context

§5 of the FAST 2021 paper documents the evolution of RocksDB's error-handling
contract. The 2012-era inherited behaviour from LevelDB was **panic on any I/O
error**: a single transient `ENOSPC` would crash the embedding process. At Meta's
scale — tens of instances per host, hundreds of PB total, mixed workloads — that
contract was unworkable. Transient failures (full disk that the operator is freeing,
brief network blip to a remote storage layer) are *frequent*; permanent failures
(silent corruption detected by a checksum layer, MANIFEST unreadable) are *rare* but
must not be papered over.

The two failure classes need opposite responses. Transient errors want **patient
retry inside the engine** so the embedding system never sees the bump. Permanent
errors want **the engine to stop writing immediately** so a propagation-of-corruption
event (the ADR-0003 §5 "40% propagated" scenario) does not get worse while operators
investigate. A third class — corruption of the MANIFEST itself — wants the engine to
**halt entirely**, because the on-disk state is no longer interpretable.

## Decision

The engine classifies every thrown exception into one of three severities and acts
accordingly:

| Severity | Trigger examples | Engine response |
|---|---|---|
| **TRANSIENT_RETRYABLE** | `ENOSPC`, retryable network I/O to a remote `FileSystem`, brief lock contention | Retry inside the engine with backoff; surface to the caller only after N retries fail |
| **HARD_ERROR_READ_ONLY** | Block CRC mismatch, KV-checksum mismatch (ADR-0003), `EIO` from the FS, bloom-filter block CRC mismatch | Trip the `ReadOnlyModeLatch`; subsequent writes fail with `IOException("db is in read-only mode")`; reads continue; only `db.attemptResume()` clears it after operator investigation |
| **FATAL_HALT** | MANIFEST CRC mismatch, footer magic mismatch on the active SSTable index | Engine refuses to remain open; caller's process must restart with `forwardCompat`, manual `CURRENT` rewrite, or `db-verify --manifest-only` triage |

`SeverityClassifier.classify(Throwable)` is the single point that maps exceptions to
severities (`rocksdb-error-handling`, §5.7 of the plan). The classifier is small
enough to audit in one screen and is the only code path that mints a `Severity`.

`ReadOnlyModeLatch.trip(severity, cause)` is one-way: only `attemptResume()` clears
it. The `attemptResume()` API requires explicit operator action, not an internal
retry timer, because the §5 design intent is that a HARD error is the operator's
signal to investigate before more bytes touch the bad data path.

The classifier and latch are wired into:
- WAL append path (TRANSIENT on `ENOSPC`, HARD on `EIO`).
- SSTable read path (HARD on block CRC mismatch).
- KV-checksum verification at Get / iterator exposure (HARD).
- MANIFEST reader on startup (FATAL on CRC mismatch).

## Rationale

The §5 framing is precise: **the engine's response to an error encodes a hypothesis
about the error's cause**. Crash-on-anything (LevelDB) hypothesises "every error is
fatal," which is wrong — most are transient. Retry-everything hypothesises "every
error is transient," which is worse — it will retry a corrupted-MANIFEST read until
the heat death of the universe. The three-bucket split matches the three actual
populations of errors observed in production.

Read-only-on-hard-error rather than crash-on-hard-error is the load-bearing choice.
A crash takes down every CF and every embedding-system component sharing the JVM; a
read-only latch lets reads keep serving (the data is still readable, just no longer
*safe to extend*) while the operator investigates. This is also the only contract
under which `db-verify` can run against a live engine that has detected corruption —
it needs read access to triage.

Auto-resume after a timer is explicitly rejected: a hard error is by hypothesis a
real problem, and silent clearance defeats the §5 investigative intent. The
`attemptResume()` API is named to flag this — it *attempts* to clear and may fail if
the underlying condition is still present (e.g., the corrupt block is still cached).

## Consequences

Positive:
- Transient disk-full events do not crash embedding processes; the engine waits the
  operator out.
- A KV-checksum mismatch (ADR-0003) trips read-only before the bad byte propagates
  to a replica — exactly the §5 "40% propagated" scenario the integrity layers are
  designed to interrupt.
- MANIFEST corruption surfaces immediately on the next open rather than producing
  silently-wrong reads.

Negative:
- The classifier is a giant `instanceof` chain that grows as new exception types are
  added. A misclassification of a HARD as TRANSIENT silently masks corruption; a
  misclassification of TRANSIENT as HARD makes the engine unnecessarily fragile.
  Tested with fault-injection (Phase 5 CP 22 covers `ENOSPC`, `EIO`, MANIFEST
  corruption).
- Read-only mode is a global latch per DB. A corrupt block in one CF trips the latch
  for every CF in that DB. Operators wanting tighter blast radius need to split into
  multiple DBs.
- `attemptResume()` is an operator API; embedding-system code calling it on a timer
  defeats the design. The CLI exposes it (`rocksdb-cli` `resume` subcommand) and the
  runtime admin port requires explicit opt-in.

## Alternatives considered

- **Panic / abort on any I/O error** (LevelDB). Rejected: makes the engine
  unsuitable for shared-host deployment.
- **Retry every error indefinitely.** Rejected: launders silent corruption into
  silent data loss; defeats ADR-0003's integrity layers.
- **Two severities only (retry / fail).** Rejected: collapses HARD and FATAL, losing
  the read-only-but-still-serving-reads contract that production needs.
- **Auto-resume on a fixed timer.** Rejected: defeats the investigative intent;
  silently clears errors that operators need to see.
- **Severity declared by the caller** (caller picks retry policy). Rejected: the
  classifier knows things callers don't — that a `ManifestCorruptionException` is
  never transient, for example. Authoritative classification belongs in the engine.

## References

- Implementation plan §5.7 (`SeverityClassifier`, `ReadOnlyModeLatch`),
  §6.1 (write-path error paths), §9 (failure modes table), Phase 5 CP 22,
  §10 (ADR 0008).
- FAST 2021 paper §5 (Severity-based error handling — the three-bucket design and
  the read-only-latch evolution from panic-on-error).
- Repo: `rocksdb-error-handling`; wired in `rocksdb-wal`, `rocksdb-engine`,
  `rocksdb-sstable-blockbased`, `rocksdb-manifest`.
- Original RocksDB source: `db/error_handler.cc`, `include/rocksdb/status.h`
  (`Status::Severity`).
