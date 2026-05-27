# ADR-0011: Hand-rolled binary framing in WAL, MANIFEST, SSTable, and admin wire

- Status: accepted
- Date: 2026-05-27
- Phase: 1 (cross-cutting)
- CP: 1

## Context

The engine has three on-disk formats (SSTable, WAL, MANIFEST) and one on-wire
admin protocol that together carry every byte the system persists or transmits.
§4 of the implementation plan lays each format out byte-by-byte: 56-byte fixed
SSTable footer, length+typeByte+lsn+payload+crc32c WAL record envelope,
edit-tag + length-prefixed body in the MANIFEST, magic-prefixed admin frames in
the runtime sub-protocol. None of these descriptions reference a third-party
schema language; the bytes are documented exactly as they appear.

The §1.2 non-goals call this out explicitly: "no protobuf, no JSON, no Java
serialization on disk or wire." The convention is inherited from the sibling
Bigtable plan and reflects the same pedagogical commitment: a reader of this
codebase should be able to point at a hex dump and explain every byte by reading
one file.

The real-world RocksDB makes the same choice — its WAL records, SSTable footer,
and `VersionEdit` log all use hand-rolled framings, not schema-generated
encoders. That choice is load-bearing for the §4 forward-compatibility story
(ADR-0010): length-prefixed skip-unknown only works when the framing is owned
end-to-end. A schema compiler that adds a hidden envelope, a varint length
field, or a wire-type tag would silently shift offsets and break the
fixed-footer guarantee.

## Decision

Every byte the engine writes or transmits is encoded by code that lives in
`rocksdb-common` (`WireReader`, `WireWriter`, `VarInt`) and is invoked
explicitly from each format module. No protobuf, no JSON, no Java
`Serializable`, no Kryo, no Jackson, no schema registry. Concretely:

- **SSTable** (`rocksdb-sstable-blockbased`): data block entries with explicit
  `sharedKey | unsharedKeyLen | valueLen | unsharedKeySuffix | seq+type | value`
  framing; restart-point array; 1-byte type + 4-byte CRC32C trailer; fixed
  56-byte footer with explicit field offsets (§4.1).
- **WAL** (`rocksdb-wal`): `length(4) | typeByte(1) | lsn(8) | payload(var) |
  crc32c(4)` records; payload is a varint-count list of CF edits, each a
  varint-count list of ops with explicit `opType | seq | userKey | (userTs) |
  valueLen | value` framing (§4.2).
- **MANIFEST** (`rocksdb-manifest`): the WAL envelope around a payload that is a
  sequence of `editTag(1) | length-prefixed body` records, with the body itself
  varint-and-length-prefix encoded (§4.3).
- **Admin wire** (`rocksdb-runtime`): a 4-byte magic + type + length + reqId
  header followed by a raw payload (§4.5).

Tests assert byte-exact encodings for every format: a known input produces a
known hex output, and the parsers reject malformed inputs at well-defined
offsets. The HTTP demo carries raw bytes in `application/octet-stream` bodies;
there is no JSON anywhere in the engine, runtime, or admin protocol.

## Rationale

Three reasons compound. First, **inspectability**. A binary that is broken in
production must be debuggable from a hex dump alone. With protobuf the same
dump requires the matching `.proto`, a decoder, and a tool to handle unknown
fields. With hand-rolled framing the format documentation in §4 is the decoder.

Second, **format-compatibility ownership**. ADR-0010's skip-unknown contract
hinges on length prefixes appearing at exactly the bytes the engine controls.
A schema compiler with its own wire-type tags would invalidate the fixed-footer
guarantee and force every forward-compat decision through the compiler's
discipline rather than the engine's.

Third, **pedagogical clarity**. The plan's central premise is that every
architectural commitment is traceable to source. A reader can grep
`WireWriter.writeVarInt` and find every byte ever varint-encoded by the system;
the same grep against a generated `parseDelimitedFrom` finds nothing useful.

The cost — manual encoder/decoder code per format — is bounded because the
formats are small (four total) and fixed (changing them is an ADR, not a
refactor).

## Consequences

Positive:
- Every byte on disk is explained by exactly one file in `rocksdb-common` plus
  the format module that uses it. Debugging a corrupt file requires only those
  two files and a hex viewer.
- ADR-0010's skip-unknown contract is implementable: the engine owns the length
  prefixes and tag spaces directly.
- No build-time codegen step. `./gradlew build` does not depend on a `.proto`
  compiler, a JSON-schema validator, or any other generator.
- KV-checksum (ADR-0003) carries inline with values without fighting a schema
  framework that wants to own value layout.

Negative:
- Encoder/decoder bugs are entirely on us. A misaligned offset or off-by-one
  varint length corrupts data with no schema runtime to catch it. Tests cover
  every format with golden-byte vectors plus property-based round-trip checks.
- Adding a new field requires editing the format documentation and the
  hand-written encoder together. A code reviewer who edits only one side
  silently breaks the format. The skip-unknown framing limits the blast radius
  but does not eliminate it.
- Inter-language interoperability is not a goal. A future Python or Rust
  reader of the on-disk format would have to re-implement the framing from §4.

## Alternatives considered

- **Protobuf for MANIFEST + WAL, hand-rolled SSTable.** Rejected: splits the
  convention; the MANIFEST is where format compatibility matters most and where
  ADR-0010 needs direct control of the length prefixes.
- **JSON for the admin protocol** (engine still binary). Rejected: introduces a
  second framing convention for no real benefit; raw-byte HTTP works for the
  demo and matches the engine's discipline.
- **Java built-in serialization.** Rejected: opaque, version-fragile, and
  unsafe; categorically inappropriate for an on-disk format.
- **FlatBuffers / Cap'n Proto.** Rejected: same inspectability and
  format-compatibility-ownership concerns as protobuf, with more build
  complexity.

## References

- Implementation plan §1.2 (non-goals — no protobuf/JSON/Java-serialization),
  §4 (all on-disk and on-wire formats laid out byte-by-byte), §4.1 (SSTable),
  §4.2 (WAL), §4.3 (MANIFEST), §4.5 (admin protocol), §10 (ADR 0011),
  Phase 1 CP 1 (`WireReader`/`WireWriter`/`VarInt` in `rocksdb-common`).
- Sibling: ADR-0010 (skip-unknown framing depends on hand-rolled framing
  ownership).
- Repo: `rocksdb-common` (wire primitives), `rocksdb-wal`, `rocksdb-manifest`,
  `rocksdb-sstable-blockbased`, `rocksdb-runtime`.
- Original RocksDB source: `db/log_writer.cc` (WAL framing),
  `db/version_edit.cc` (MANIFEST edit encoding), `table/format.cc` (footer).
