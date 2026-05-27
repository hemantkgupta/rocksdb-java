# ADR-0010: Forward + backward data-format compatibility via length-prefixed skip-unknown framing

- Status: accepted
- Date: 2026-05-27
- Phase: 6
- CP: 26

## Context

§4 of the FAST 2021 paper frames the operational reality that drove the
format-compatibility contract: a binary embedded in 30+ Meta applications across many
ZippyDB / Tectonic-metadata / MyRocks deployments cannot roll out a new on-disk
format the way a single-service binary does. Rollouts proceed *gradually* — some
hosts run version N+1 while peers still run version N, sharing replicated state — and
must be **rollback-able**: a regression in version N+1 means rolling back to N
*without* re-formatting every DB on the cluster.

That requires two properties:

1. **Backward compatibility.** Version N+1 can read every byte version N wrote. This
   is mostly free if the format only ever *adds* fields; the load-bearing discipline
   is to never reuse a tag or change a fixed offset.
2. **Forward compatibility.** Version N can read a DB that version N+1 has written
   to. This is the harder direction: the older reader must be able to skip fields
   and records it does not recognise without corrupting its understanding of the
   rest of the file.

Both properties together let an operator roll out N+1, observe a regression, and
roll back to N — N opens the partially-N+1-written DB cleanly, ignores the N+1
extensions, and continues serving. Without this, the rollback would require taking
DBs out of service to reformat, which at Meta's scale is operationally infeasible.

## Decision

Every on-disk format that this engine writes uses **length-prefixed skip-unknown
framing**, applied at two places where the format is extensible:

**SSTable footer** (§4.1). Fixed at 56 bytes, with these fields:
```
metaIdxHandle | indexHandle | fileSize | fileChecksum | formatVersion | reserved | magic
   16 B           16 B          8 B          8 B           4 B            2 B       2 B
```
- `magic = 0x5354` is the structural anchor — readers refuse to open a file whose
  magic does not match. The footer layout itself never changes.
- `formatVersion` is the version field. Readers with `MAX_KNOWN_FORMAT = X` accept
  files with `formatVersion <= X` unconditionally. For `formatVersion > X`,
  behaviour depends on `options.forwardCompat`:
  - `false` (default): refuse to open with a clear error.
  - `true`: open the file using the footer (its layout is fixed), and skip any
    extension blocks the metaIdx references with unknown tags.

**MANIFEST `VersionEdit` records** (§4.3). Each edit is `editTag` + length-prefixed
body. The reader knows tags `0x10`–`0x19` in v1. An unknown `editTag` is silently
skipped after consuming its length-prefixed body — the reader stays aligned with the
record stream regardless of what tags the writer added.

Two disciplines back the framing:

1. **Never reuse a tag or change a field's offset.** Once `0x10` means `AddFile` in
   v1, it means `AddFile` forever; once `fileChecksum` lives at footer offset 40, it
   stays there. A new field is a *new* tag or a *new* trailing field, never a
   repurposing.
2. **Bounded forward-compat horizon: one year.** The engine guarantees that version
   N can read version N+1 for the duration of a typical rollout-and-soak window
   (roughly 12 months). Beyond that, the operator is expected to have rolled
   forward; engineering effort is not spent keeping a 2-year-old reader functional.

CP 26 of the plan validates both properties via a synthetic "v2" file: a footer with
`formatVersion = 0x0002` plus a MANIFEST with an `editTag = 0xFF` extension. A v1
reader with `forwardCompat=true` opens the DB, ignores the extension, and continues.

## Rationale

Length-prefixed skip-unknown is the smallest framing that supports both directions.
The length prefix lets a reader consume an unknown record's bytes without
interpreting them; the magic/version anchor catches structural mismatches that
length-prefixing would otherwise paper over (truncated file, totally-wrong file
type).

The fixed 56-byte footer is the structural guarantee that even a totally-unknown
file version can be located on disk. If the footer size grew with versions, an older
reader could not find it at `fileSize - footerSize` and would have to scan — which is
both slow and error-prone. Fixed-size is the right trade.

The one-year horizon is borrowed from the paper's §4 framing: rollouts complete in
weeks, soak in months, and re-rollouts within a year are operationally normal. Two
years of forward-compat means carrying every format addition twice, which is the
maintenance cost the paper says is not worth paying.

## Consequences

Positive:
- Rollouts of new on-disk fields are non-blocking. Version N+1 can be deployed
  without first migrating every host; version N peers ignore the new fields.
- Rollbacks are non-destructive. If N+1 regresses, operators downgrade hosts back to
  N and the partially-N+1-written DBs open cleanly.
- The MANIFEST extension story makes feature flags real: a new edit-tag can be
  introduced disabled-by-default, rolled out across N+1, then enabled in N+2 once
  every reader knows it.

Negative:
- The tag-immutability discipline must hold across every refactor. A code review
  that "renames" an edit-tag silently breaks backward compatibility for every DB on
  disk. Tests at CP 26 lock the tag values; format documentation calls them out
  explicitly.
- The fixed-size footer constrains the engine to ≤ 56 bytes of footer fields
  forever. New per-file metadata must live in extension blocks reached via
  `metaIdxHandle`, not in the footer itself.
- Forward-compat is opt-in (`forwardCompat=true`) because the safer default for
  unknown formats is "refuse." Operators rolling back must remember to set the
  flag; a rollout playbook documents this.

## Alternatives considered

- **Strict version checking with no forward-compat.** Rejected: makes every rollout
  irreversible without a reformat, which §4 documents is operationally infeasible at
  Meta's scale.
- **Self-describing format with full type metadata per field** (e.g., protobuf).
  Rejected: heavier than skip-unknown framing; conflicts with ADR-0011's
  hand-rolled-binary commitment; doesn't materially improve the rollback story.
- **Infinite forward-compat horizon.** Rejected: maintenance burden of carrying
  every format addition forever, with no operational gain beyond ~1 year.
- **Inline format versions per block instead of per file.** Rejected: pays the
  framing cost N times per file; the file-level granularity is sufficient because
  rollouts are per-binary, not per-block.

## References

- Implementation plan §4.1 (SSTable footer + formatVersion + skip-unknown),
  §4.3 (MANIFEST edit-tag skip-unknown), §7.6 (forward + backward data-format
  compatibility), Phase 6 CP 26, §9 (format-version-mismatch failure mode),
  §10 (ADR 0010).
- FAST 2021 paper §4 (Data format compatibility — rollback-able rollouts).
- Repo: `rocksdb-sstable-blockbased` (`FooterReader`), `rocksdb-manifest`
  (`ManifestReader`).
- Original RocksDB source: `table/format.cc` (footer layout, version checks),
  `db/version_edit.cc` (edit-tag handling).
