# ADR-0002: Dynamic Leveled Compaction as the default for new column families

- Status: accepted
- Date: 2026-05-27
- Phase: 3
- CP: 15

## Context

ADR-0001 establishes that the engine carries four compaction styles. This ADR picks the
default — the style a CF gets when `ColumnFamilyOptions.compactionStyle` is unset.

The classical LevelDB-derived leveled compaction picks `target(L_n)` bottom-up from a
fixed `L1_BASE` and a constant multiplier (typically 10×). The pathology this produces
is documented in §3 of the FAST 2021 paper (Space amplification): if the actual size of
the bottom level happens to land just above its target, the next-up level must be sized
for 1/10 of that target, but the *actual* data above will be ~1/10 of the *real* bottom,
producing a level that is effectively empty and 9× over-provisioned. In aggregate the
worst case approaches 90% space overhead — the LSM occupies nearly 2× the live data
size.

Table 4 quantifies this against the Dynamic Leveled scheme: under the same adversarial
workload, dynamic-leveled keeps worst-case space overhead at ~13%, an order of magnitude
better, with the same write-amp profile.

For tenants whose primary cost is SSD capacity — most of Meta's 30+ RocksDB applications
fall here, including ZippyDB and Tectonic-metadata — that 90%→13% reduction is the
single largest space lever the engine has.

## Decision

`ColumnFamilyOptions.compactionStyle` defaults to `DYNAMIC_LEVELED`. The
`DynamicLeveledCompactionPicker` is selected if no style is configured at CF creation
time. The legacy `LEVELED` style remains available behind an explicit opt-in for
workloads that must reproduce LevelDB-era behaviour (e.g., for paper-reproduction tests
or migration scenarios).

The default is set in `ColumnFamilyOptions` (in `rocksdb-column-family`); the picker
implementation lives in `rocksdb-compaction-leveled` alongside the classical leveled
picker (they share level-overlap and merge-iterator code).

## Rationale

Dynamic Leveled inverts the level-sizing computation: instead of fixing `target(L1)`
and growing upward, it fixes `target(L_max) = actualSize(L_max)` and shrinks downward by
the multiplier. Upper levels are always a small fraction of the actual bottom, so the
"over-provisioned empty level" pathology disappears.

Crucially, write amplification is unchanged. Each key still migrates through the level
hierarchy at the same rate; only the per-level size targets are computed differently.
The 1.2-2× space-amp win is therefore "free" relative to leveled — there is no
operational reason to prefer the LevelDB scheme as the default.

The paper makes this the recommended setting for new deployments. The plan follows.

## Consequences

Positive:
- A CF created with no explicit options runs at ~13% worst-case space overhead instead
  of ~90%.
- Tests in `rocksdb-test-cluster` that fill a DB and measure
  `du(dbDir) / sum(liveValueBytes)` pass with the bound set at ~1.2× rather than ~2×.
- Aligns the Java port with current RocksDB upstream defaults; readers reproducing the
  paper's Table 4 see the predicted numbers.

Negative:
- Picker logic is slightly more complex: `target(L_n)` is re-derived on every compaction
  pick rather than being a constant. Adds a ~10-line computation in the picker.
- Behaviour changes across DB growth: as `actualSize(L_max)` shifts, upper-level targets
  shift with it, which can make "expected behaviour at level X" harder to reason about
  in tests. Mitigated by a `LEVEL_SIZE_BASE_BYTES` floor that prevents micro-levels.

## Alternatives considered

- **Classical leveled as default.** Rejected: 90% worst-case space overhead is no longer
  acceptable on SSD-bound deployments; Table 4 makes the case unambiguous.
- **Tiered as default.** Rejected: write-amp wins (~5× vs ~16×) come with read-amp and
  space-amp losses (~45%) that most workloads don't tolerate; tiered is the right pick
  for logging workloads but not the right default.
- **No default — force every CF to set the style explicitly.** Rejected: violates the
  principle that the safe choice is the easy choice; encourages copy-paste of whatever
  someone's example used.

## References

- Implementation plan §6.3 (DynamicLeveledCompactionPicker logic) and §10 (ADR 0002).
- FAST 2021 paper §3 (Space amplification, Table 4 — dynamic-leveled vs leveled
  worst-case space overhead).
- Repo: `rocksdb-compaction-leveled` (both pickers); `rocksdb-column-family`
  (`ColumnFamilyOptions.compactionStyle` default).
- Original RocksDB source: `db/compaction/compaction_picker_level.cc`,
  particularly the `level_compaction_dynamic_level_bytes` option.
