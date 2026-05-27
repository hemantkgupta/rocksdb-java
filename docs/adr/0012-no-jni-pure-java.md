# ADR-0012: No JNI; pure Java implementation

- Status: accepted
- Date: 2026-05-27
- Phase: 1 (cross-cutting)
- CP: 1

## Context

Production RocksDB is C++ and links against native compression libraries
(Snappy, Zstd, LZ4) through tightly-tuned in-process call paths. A Java binding
exists (`rocksdbjni`) but it is a JNI wrapper around the same C++ engine — the
storage logic, the compaction loops, the bloom filter, the block cache all run
in native code, with JNI marshalling at the API boundary.

This implementation is a **port**, not a binding. The §1 framing of the plan is
explicit: the goal is a faithful re-implementation in Java that a reader can
trace end-to-end without leaving the JVM. §1.2 lists JNI and embedded RocksDB
as explicit non-goals; §1.3's faithful-to-paper-departures table accepts a
pedagogical CPU efficiency loss in exchange for that traceability.

Native compression is the load-bearing case. Snappy / Zstd / LZ4 each have
production-grade pure-Java ports, but each is a non-trivial codebase, each
introduces a dependency the rest of the plan does not need, and each would
require its own ADR for codec lineage. The plan's compromise is to ship the
*format slot* (a per-block compression byte; §4.1) without shipping the
codecs — the layout is faithful, the bytes are correctly tagged, and a future
v2 can plug in a JNI-free codec without changing on-disk format.

## Decision

The engine and all its modules are **pure JDK 17**. No JNI, no native libraries,
no `System.loadLibrary`, no `sun.misc.Unsafe`, no Project Panama / FFI calls.
Concretely:

- **No embedded RocksDB.** The `rocksdb-engine` module does not declare a
  Gradle dependency on `org.rocksdb:rocksdbjni` or any equivalent. Validated by
  a build-time check on the resolved classpath.
- **Compression knob: `none` (default) or `deflate`.** `deflate` is the only
  non-trivial codec the engine supports in v1, and it is implemented via
  `java.util.zip.Deflater` (JDK built-in, no native library load required —
  the JDK's zlib is statically embedded). `snappy`, `zstd`, `lz4` are reserved
  enum values that the writer refuses with a clear error message.
- **All cryptographic / hashing primitives are JDK or hand-rolled.** CRC32C
  uses `java.util.zip.CRC32C` (JDK 9+). XXH64 (file checksum, ADR-0003) is a
  hand-rolled pure-Java implementation in `rocksdb-integrity`.
- **File I/O via `java.nio.file` + `java.nio.channels.FileChannel`.** `fsync`
  is `FileChannel.force(true)`. No `posix_fadvise`, no `O_DIRECT`, no
  io_uring — the durability contract is preserved (§7.1), the micro-tuning is
  not.

The §1.3 departures table records this as a deliberate trade: the engine
preserves every architectural decision in the FAST 2021 paper (compaction
styles, integrity layers, rate limiter scopes, MultiGet parallelism, two-level
indices) while accepting that compression and some I/O fast paths are stubbed
or absent.

## Rationale

Three reasons compound. First, **traceability**. A reader can open this repo
and follow any architectural claim from §10 of the plan to a single Java file
without traversing JNI stubs, native build scripts, or platform-specific
binaries. The whole point of the port is to make RocksDB's design
understandable by reading code; JNI defeats that.

Second, **build simplicity**. Pure Java means `./gradlew build` works on any
JDK 17 host without a C++ toolchain, without `cmake`, without per-platform
shared-library packaging. The CI matrix is one row, not nine.

Third, **scope discipline**. Implementing Snappy/Zstd/LZ4 well in Java is a
sustained engineering effort that does not advance the paper's lessons. The
paper is about LSM-tree evolution and at-scale operational concerns — not
about codec performance. Stubbing compression lets the plan reach §6 (KV
interface) and §5 (failure handling) in finite calendar time.

The departure is honest. A production deployment of this codebase would
re-add native compression (and probably native CRC32C SIMD) as a separate
optimisation pass with its own ADRs; that work is out of scope for v1.

## Consequences

Positive:
- Single-language codebase; no native build chain; one JDK version is the
  whole toolchain.
- Every line of behaviour is debuggable from the JVM (jstack, async-profiler,
  JFR) without dropping into `gdb` for the native side.
- Reproducible cross-platform behaviour. Bugs that depend on `glibc` version
  or kernel version simply do not exist here.
- ADR-0011's hand-rolled framing is well-matched: both choices reinforce
  "every byte is owned by Java code we wrote."

Negative:
- **No native compression.** The §4.1 per-block compression byte exists in the
  format but reads as `0x00` (none) in v1. A SSTable written by production
  RocksDB with Snappy blocks is not readable by this engine; conversely, an
  SSTable written here is not directly competitive on space-amp with the
  paper's measurements.
- **Slower CRC32C.** `java.util.zip.CRC32C` is JIT-compiled but not SIMD-vectorised
  the way the production C++ path is. The §1.3 table accepts this; the §6.2
  read path is correctness-equivalent, not throughput-equivalent.
- **No memory-mapped file fast path.** `FileChannel.map` is available but the
  v1 engine reads via explicit `read(ByteBuffer)`; this keeps the block-cache
  accounting (ADR controlled by `rocksdb-block-cache`) honest. A future v2
  could opt into mmap for cold-block reads.
- A reader expecting JNI-style raw-pointer KV APIs will not find them; all
  values are `byte[]` and pay an allocation cost.

## Alternatives considered

- **JNI to embedded RocksDB.** Rejected: that is `rocksdbjni`; this project's
  purpose is to *re-implement*, not to wrap.
- **Pure-Java Snappy / Zstd ports in v1.** Rejected for v1 scope; the
  compression byte slot is preserved so a v2 can add them without format
  change.
- **Project Panama / FFI for native compression.** Rejected: still introduces
  a native dependency, still complicates the build, still defeats the
  traceability argument. Deferred to a hypothetical v2.
- **`sun.misc.Unsafe` for off-heap block storage.** Rejected: ties to a
  deprecated API; on-heap `byte[]` with the block cache size-accounted in
  bytes is the right v1 trade.

## References

- Implementation plan §1.2 (non-goals — no JNI; no embedded RocksDB; no native
  compression), §1.3 (faithful-to-paper departures table), §4.1 (compression
  byte slot preserved in block layout), §7.1 (durability via
  `FileChannel.force(true)`), §10 (ADR 0012), Phase 1 CP 1 bootstrap.
- Sibling: ADR-0011 (hand-rolled framing reinforces the pure-Java commitment).
- Repo: every Gradle module; build-time classpath check in the root
  `build.gradle`.
- Original RocksDB source for contrast: `util/compression.h` (native codec
  dispatch), `port/port_posix.cc` (native I/O paths) — both absent here.
