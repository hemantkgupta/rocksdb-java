package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.KeyLookup;
import com.hkg.rocksdb.common.KvEngine;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.Snapshot;
import com.hkg.rocksdb.common.UserTimestamp;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.compaction.CompactionExecutor;
import com.hkg.rocksdb.compaction.CompactionJob;
import com.hkg.rocksdb.compaction.Compactor;
import com.hkg.rocksdb.compaction.LeveledCompactionPicker;
import com.hkg.rocksdb.errorhandling.ReadOnlyModeLatch;
import com.hkg.rocksdb.errorhandling.Severity;
import com.hkg.rocksdb.errorhandling.SeverityClassifier;
import com.hkg.rocksdb.integrity.KvChecksumCodec;
import com.hkg.rocksdb.ratelimiter.Priority;
import com.hkg.rocksdb.ratelimiter.RateLimiter;
import com.hkg.rocksdb.sstable.WriteHook;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import com.hkg.rocksdb.manifest.VersionEdit;
import com.hkg.rocksdb.manifest.VersionSet;
import com.hkg.rocksdb.memtable.SkipListMemTable;
import com.hkg.rocksdb.sstable.BlockBasedTableReader;
import com.hkg.rocksdb.sstable.BlockBasedTableWriter;
import com.hkg.rocksdb.wal.LogReader;
import com.hkg.rocksdb.wal.LogWriter;
import com.hkg.rocksdb.wal.MutationCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The assembled RocksDb engine. Composes a MemTable, a synchronous WAL, a
 * MANIFEST-tracked Version, and a set of open SSTable readers — exposing the
 * standard KvEngine surface (put / get / delete / snapshot / flush).
 *
 * <p>Phase 2 scope (CP 8 + CP 9): single-instance, single-engine, no
 * background compaction thread. The {@link #flush()} call freezes the
 * active MemTable, writes an L0 SSTable, and applies a NewFile VersionEdit
 * atomically. Compactions can be triggered manually via
 * {@link #maybeCompact()}; full background-thread automation lands in
 * Phase 3.
 *
 * <p>Concurrency: writes serialise on {@link #writeLock}. Reads probe
 * volatile MemTable / frozen-MemTable references and the current Version
 * (whose levels are immutable lists) without locking.
 */
public final class RocksDb implements KvEngine {

    private final Path dbDir;
    private final VersionSet versions;

    private volatile SkipListMemTable activeMemTable;
    private volatile SkipListMemTable frozenMemTable;

    private volatile LogWriter walWriter;
    private volatile FileNumber walNumber;

    private final AtomicLong nextSequence;
    private final ConcurrentHashMap<FileNumber, BlockBasedTableReader> openTables;
    private final Set<Long> activeSnapshotSeqs = ConcurrentHashMap.newKeySet();
    private final Object writeLock = new Object();
    private final boolean compressOutput;
    private final BlockCache blockCache;
    private final CompactionExecutor compactionExecutor;
    /** File numbers currently being read by a compaction worker; consulted by the picker to avoid overlap. */
    private final Set<FileNumber> inFlightFiles = ConcurrentHashMap.newKeySet();
    /** Optional shared rate limiter. When non-null, compaction I/O is paced at block granularity. */
    private final RateLimiter rateLimiter;
    /**
     * CP 21 — when true, every value stored is wrapped with a 4-byte KV
     * CRC32 over (seq, userKey, value) at put time and verified + stripped
     * at get time. Catches above-block-CRC corruptions (RAM bit rot, cache
     * poisoning) that disk-level integrity layers cannot.
     */
    private final boolean kvChecksumEnabled;
    /** CP 22 — engine-wide read-only mode latch tripped on HARD-severity errors. */
    private final ReadOnlyModeLatch readOnlyLatch = new ReadOnlyModeLatch();
    /** CP 22 — maps thrown exceptions to severity buckets; HARDs trip the latch. */
    private final SeverityClassifier severityClassifier = new SeverityClassifier();
    /**
     * CP 27 — user-defined-timestamp size in bytes. {@code 0} disables UDT (LevelDB-era
     * behaviour). When non-zero (currently only {@link com.hkg.rocksdb.common.UserTimestamp#ENCODED_LENGTH}
     * = 8 is supported), {@link #putWithUserTs} / {@link #getAsOf} are available and the
     * legacy {@link #put} path is rejected with {@code IllegalStateException}.
     */
    private final int userTimestampSize;
    /**
     * CP 27 — per-userKey set of timestamps written under UDT mode. Rebuilt at open
     * time by iterating every SSTable; updated on put. {@code null} when UDT is off.
     */
    private final java.util.concurrent.ConcurrentHashMap<Key, java.util.concurrent.ConcurrentSkipListSet<Long>> udtIndex;

    private RocksDb(Path dbDir, VersionSet versions, SkipListMemTable mt,
                     LogWriter walWriter, FileNumber walNumber,
                     ConcurrentHashMap<FileNumber, BlockBasedTableReader> tables,
                     AtomicLong nextSequence, boolean compressOutput,
                     BlockCache blockCache, CompactionExecutor compactionExecutor,
                     RateLimiter rateLimiter, boolean kvChecksumEnabled,
                     int userTimestampSize) {
        this.dbDir = dbDir;
        this.versions = versions;
        this.activeMemTable = mt;
        this.walWriter = walWriter;
        this.walNumber = walNumber;
        this.openTables = tables;
        this.nextSequence = nextSequence;
        this.compressOutput = compressOutput;
        this.blockCache = blockCache;
        this.compactionExecutor = compactionExecutor;
        this.rateLimiter = rateLimiter;
        this.kvChecksumEnabled = kvChecksumEnabled;
        this.userTimestampSize = userTimestampSize;
        this.udtIndex = userTimestampSize > 0
            ? new java.util.concurrent.ConcurrentHashMap<>() : null;
    }

    /**
     * Open the engine on {@code dbDir}, creating a fresh DB if no CURRENT
     * pointer exists. Replays the MANIFEST to reconstruct the Version and
     * opens readers for every referenced SSTable. WAL replay (recovering
     * the in-memory MemTable contents from a prior crash) is handled
     * separately by {@link #recoverWal(Path)} — wired up in CP 9.
     */
    public static RocksDb open(Path dbDir) throws IOException {
        return open(dbDir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, false, 0);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput) throws IOException {
        return open(dbDir, compressOutput, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, false, 0);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, 1, null, false, 0);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, compactionThreads, null, false, 0);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads, RateLimiter rateLimiter)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, compactionThreads, rateLimiter, false, 0);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads, RateLimiter rateLimiter,
                                boolean kvChecksumEnabled)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, compactionThreads, rateLimiter,
            kvChecksumEnabled, 0);
    }

    /**
     * Open the engine with an explicit {@link BlockCache}, a fixed-size pool
     * of compaction worker threads, an optional {@link RateLimiter} for
     * compaction I/O at {@link Priority#LOW}, and an opt-in KV checksum.
     *
     * <p>With {@code kvChecksumEnabled = true}, every value flowing through
     * the engine is wrapped at put time with a 4-byte CRC32 over (seq,
     * userKey, value); the wrap is verified + stripped at get time. This is
     * the FAST 2021 §5 end-to-end checksum: catches above-block corruption
     * (RAM bit rot, cache poisoning) that the block-CRC layer cannot.
     * Storage layers (WAL, MemTable, SSTable, block cache, compaction) are
     * unmodified — they see opaque bytes that happen to be 4 bytes longer.
     */
    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads, RateLimiter rateLimiter,
                                boolean kvChecksumEnabled, int userTimestampSize)
        throws IOException {
        if (userTimestampSize != 0 && userTimestampSize != UserTimestamp.ENCODED_LENGTH) {
            throw new IllegalArgumentException(
                "userTimestampSize must be 0 (off) or 8 (on), got " + userTimestampSize);
        }
        Files.createDirectories(dbDir);
        boolean freshDb = !Files.exists(dbDir.resolve("CURRENT"));
        VersionSet vs = freshDb ? VersionSet.createFresh(dbDir) : VersionSet.open(dbDir);

        // Defensive sweep: remove .sst.tmp and any orphan .sst files not referenced by the Version.
        sweepOrphans(dbDir, vs.current());

        ConcurrentHashMap<FileNumber, BlockBasedTableReader> tables = new ConcurrentHashMap<>();
        for (FileNumber fn : vs.current().allFileNumbers()) {
            tables.put(fn, BlockBasedTableReader.open(dbDir.resolve(fn.tableFileName()), fn, blockCache, dbDir.toString()));
        }

        // WAL replay: rebuild the MemTable from the prior WAL recorded in MANIFEST.
        SkipListMemTable recoveredMt = new SkipListMemTable();
        long maxSeq = vs.current().lastSequence();
        long priorLogNum = vs.current().logNumber();
        Path priorWalPath = (priorLogNum > 0)
            ? dbDir.resolve(new FileNumber(priorLogNum).logFileName())
            : null;
        boolean replayed = false;
        if (priorWalPath != null && Files.exists(priorWalPath)) {
            replayed = true;
            try (LogReader reader = LogReader.open(priorWalPath)) {
                Iterator<byte[]> it = reader.records();
                while (it.hasNext()) {
                    MutationRecord mr = MutationCodec.decode(it.next());
                    if (mr instanceof MutationRecord.Put put) {
                        recoveredMt.put(put.key(), put.value(), put.sequence());
                    } else if (mr instanceof MutationRecord.Delete del) {
                        recoveredMt.delete(del.key(), del.sequence());
                    } else if (mr instanceof MutationRecord.DeleteRange dr) {
                        recoveredMt.deleteRange(dr.startKey(), dr.endKey(), dr.sequence());
                    }
                    long s = mr.sequence().value();
                    if (s > maxSeq) maxSeq = s;
                }
            }
        }

        // Allocate a new active WAL for subsequent writes.
        long walNum = vs.current().nextFileNumber();
        Path walPath = dbDir.resolve(new FileNumber(walNum).logFileName());
        LogWriter wal = LogWriter.open(walPath, true);
        vs.apply(List.of(
            new VersionEdit.SetNextFileNumber(walNum + 1L),
            new VersionEdit.SetLogNumber(walNum),
            new VersionEdit.SetLastSequence(maxSeq)));

        AtomicLong seq = new AtomicLong(maxSeq + 1L);
        CompactionExecutor executor = new CompactionExecutor(compactionThreads);
        RocksDb db = new RocksDb(dbDir, vs, recoveredMt, wal,
            new FileNumber(walNum), tables, seq, compressOutput, blockCache, executor,
            rateLimiter, kvChecksumEnabled, userTimestampSize);
        if (userTimestampSize > 0) {
            db.rebuildUdtIndex();
        }

        // If we recovered any in-memory state, persist it as an L0 SSTable so the old WAL
        // can be safely deleted. doFlush() opens a fresh WAL of its own and deletes the
        // one we just allocated; net result is one new L0 file + a clean WAL on disk.
        if (replayed && recoveredMt.size() > 0) {
            db.flush();
        }
        if (replayed && Files.exists(priorWalPath)) {
            Files.deleteIfExists(priorWalPath);
        }

        return db;
    }

    /** Remove .sst.tmp orphans + any .sst files not referenced by the recovered Version. */
    private static void sweepOrphans(Path dbDir, Version v) throws IOException {
        java.util.Set<Long> validNums = new java.util.HashSet<>();
        for (FileNumber fn : v.allFileNumbers()) validNums.add(fn.value());
        try (var stream = Files.list(dbDir)) {
            for (Path p : stream.toList()) {
                String name = p.getFileName().toString();
                if (name.endsWith(".sst.tmp")) {
                    Files.deleteIfExists(p);
                } else if (name.endsWith(".sst")) {
                    String base = name.substring(0, name.length() - 4);
                    try {
                        long n = Long.parseLong(base);
                        if (!validNums.contains(n)) {
                            Files.deleteIfExists(p);
                        }
                    } catch (NumberFormatException ignored) {
                        // Unrelated file; leave it alone.
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------- writes

    @Override
    public void put(Key key, Slice value) {
        readOnlyLatch.ensureWritable();
        synchronized (writeLock) {
            try {
                long seq = nextSequence.getAndIncrement();
                // CP 21 — wrap with a KV checksum if enabled. The wrapped bytes flow opaque
                // through WAL, MemTable, SSTable, block cache, compaction; unwrapped at get().
                Slice storedValue = kvChecksumEnabled
                    ? Slice.of(KvChecksumCodec.wrap(key.bytes(), value.toBytes()))
                    : value;
                MutationRecord.Put put = new MutationRecord.Put(key, storedValue, new SequenceNumber(seq));
                walWriter.append(MutationCodec.encode(put));
                activeMemTable.put(key, storedValue, new SequenceNumber(seq));
                maybeFlush();
            } catch (IOException e) {
                tripIfHard(e);
                throw new UncheckedIOException(e);
            } catch (RuntimeException e) {
                tripIfHard(e);
                throw e;
            }
        }
    }

    @Override
    public void delete(Key key) {
        readOnlyLatch.ensureWritable();
        synchronized (writeLock) {
            try {
                long seq = nextSequence.getAndIncrement();
                MutationRecord.Delete del = new MutationRecord.Delete(key, new SequenceNumber(seq));
                walWriter.append(MutationCodec.encode(del));
                activeMemTable.delete(key, new SequenceNumber(seq));
                maybeFlush();
            } catch (IOException e) {
                tripIfHard(e);
                throw new UncheckedIOException(e);
            } catch (RuntimeException e) {
                tripIfHard(e);
                throw e;
            }
        }
    }

    /**
     * CP 17 — DeleteRange API. Every user key in {@code [startKey, endKey)} is
     * logically deleted as of the assigned sequence. End key is exclusive.
     *
     * <p>The range tombstone is appended to the WAL (durable before return) and
     * added to the active MemTable. Subsequent flushes persist it as a meta-block
     * in the resulting L0 SSTable; the read path consults RTs across MemTable +
     * frozen MemTable + every SSTable and chooses the winner by sequence number.
     */
    public void deleteRange(Key startKey, Key endKey) {
        Objects.requireNonNull(startKey, "startKey");
        Objects.requireNonNull(endKey, "endKey");
        if (startKey.compareTo(endKey) >= 0) {
            throw new IllegalArgumentException("startKey must be strictly less than endKey");
        }
        readOnlyLatch.ensureWritable();
        synchronized (writeLock) {
            try {
                long seq = nextSequence.getAndIncrement();
                MutationRecord.DeleteRange dr = new MutationRecord.DeleteRange(
                    startKey, endKey, new SequenceNumber(seq));
                walWriter.append(MutationCodec.encode(dr));
                activeMemTable.deleteRange(startKey, endKey, new SequenceNumber(seq));
                maybeFlush();
            } catch (IOException e) {
                tripIfHard(e);
                throw new UncheckedIOException(e);
            } catch (RuntimeException e) {
                tripIfHard(e);
                throw e;
            }
        }
    }

    /**
     * CP 22 — classify a throwable and trip the read-only latch if the
     * classifier returns {@link Severity#HARD_ERROR_READ_ONLY}. TRANSIENT
     * leaves the latch untouched (the engine retries / surfaces); FATAL
     * also leaves the latch untouched but the caller will propagate up.
     * Returns the assigned severity for callers that want to act on it.
     */
    private Severity tripIfHard(Throwable t) {
        Severity sev = severityClassifier.classify(t);
        if (sev == Severity.HARD_ERROR_READ_ONLY) {
            readOnlyLatch.trip(t.getClass().getSimpleName() + ": " + t.getMessage());
        }
        return sev;
    }

    // -------------------------------------------------------------------- reads

    @Override
    public Optional<Slice> get(Key key) {
        long current = nextSequence.get();
        SequenceNumber asOf = current > 0 ? new SequenceNumber(current - 1) : SequenceNumber.ZERO;
        return getWithSeverity(key, asOf);
    }

    @Override
    public Optional<Slice> get(Key key, Snapshot snapshot) {
        return getWithSeverity(key, snapshot.sequence());
    }

    /**
     * Wraps the read path with CP 22 severity classification: any HARD-severity
     * exception trips the read-only latch (in particular, KV-checksum
     * mismatches and block-CRC mismatches). The exception still propagates;
     * the latch ensures subsequent writes are rejected with
     * {@code ReadOnlyModeException} until the operator calls
     * {@link #attemptResume()}.
     */
    private Optional<Slice> getWithSeverity(Key key, SequenceNumber asOf) {
        try {
            return readAt(key, asOf).map(v -> unwrapKvChecksumIfNeeded(key, v));
        } catch (RuntimeException e) {
            tripIfHard(e);
            throw e;
        }
    }

    /**
     * CP 21 — if KV checksum is enabled, verify and strip the trailing 4-byte
     * CRC32 before exposing the value to the application. A mismatch throws
     * {@link com.hkg.rocksdb.integrity.KvChecksumMismatchException} — in CP 22
     * this will be classified as HARD_ERROR_READ_ONLY and trip the engine's
     * read-only latch.
     */
    private Slice unwrapKvChecksumIfNeeded(Key key, Slice wrapped) {
        if (!kvChecksumEnabled) return wrapped;
        byte[] stripped = KvChecksumCodec.unwrap(key.bytes(), wrapped.toBytes());
        return Slice.of(stripped);
    }

    // -------------------------------------------------------------------- multiGet (CP 28)

    /**
     * CP 28 — batched point lookup. Resolves N keys at a single, consistent
     * snapshot (the current sequence at call time) and returns a map keyed by
     * the input Keys; absent keys map to {@link Optional#empty()}.
     *
     * <p>Execution (per ADR-0013): every input key is matched against the
     * current {@link Version} to determine which SSTables it may hit (L0
     * candidates via range overlap; L1..L_max via per-level binary search),
     * then keys are <em>bucketed by SSTable</em>. Each non-empty bucket is
     * dispatched to {@link ForkJoinPool#commonPool()} as one task that runs
     * every bucketed key through that SSTable's {@code lookup}. After all
     * tasks finish, results are merged per-key in the standard read-path
     * priority order (active MemTable -&gt; frozen MemTable -&gt; L0 newest-first
     * -&gt; L1 -&gt; ... -&gt; L_max). The MemTable + frozen MemTable probes run inline
     * on the calling thread &mdash; no I/O to parallelise there.
     *
     * <p>If any sub-lookup throws, the FIRST exception observed is rethrown
     * (after all in-flight tasks settle). {@link Severity#HARD_ERROR_READ_ONLY}
     * exceptions trip the engine's read-only latch via {@link #tripIfHard}.
     * The returned map BEFORE the throw carries results for every key whose
     * candidate buckets all completed cleanly; this matches ADR-0013's
     * "partial results plus first error" v1 contract. Implementation: the
     * exception is thrown via a {@link MultiGetPartialResult} carrying the
     * partial map alongside the cause; callers that want the partial view
     * can catch and inspect.
     *
     * <p>If KV checksum is enabled, every returned value is unwrapped via
     * {@link #unwrapKvChecksumIfNeeded} on the calling thread.
     *
     * <p>Duplicate keys in the input list collapse into the same map entry
     * &mdash; the returned map's size is the count of <em>distinct</em> keys.
     */
    public Map<Key, Optional<Slice>> multiGet(List<Key> keys) {
        Objects.requireNonNull(keys, "keys");
        long current = nextSequence.get();
        SequenceNumber asOf = current > 0 ? new SequenceNumber(current - 1) : SequenceNumber.ZERO;
        return multiGetAt(keys, asOf);
    }

    /**
     * CP 28 &mdash; snapshot-aware batched lookup. Every key is resolved at
     * {@code snapshot.sequence()}; writes appended after the snapshot was
     * taken are invisible.
     */
    public Map<Key, Optional<Slice>> multiGet(List<Key> keys, Snapshot snapshot) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(snapshot, "snapshot");
        return multiGetAt(keys, snapshot.sequence());
    }

    /**
     * Exception thrown by {@link #multiGet} when at least one bucket task
     * failed. Carries the partial result map (keys whose lookups completed
     * cleanly) alongside the underlying cause. The first observed sub-lookup
     * failure is the wrapped cause; subsequent failures are dropped.
     */
    public static final class MultiGetPartialResult extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final transient Map<Key, Optional<Slice>> partial;

        public MultiGetPartialResult(Map<Key, Optional<Slice>> partial, Throwable cause) {
            super("multiGet completed with partial results; first error: " + cause, cause);
            this.partial = partial;
        }

        /** Snapshot of results for keys whose buckets all completed cleanly. */
        public Map<Key, Optional<Slice>> partial() {
            return partial;
        }
    }

    private Map<Key, Optional<Slice>> multiGetAt(List<Key> keys, SequenceNumber asOf) {
        Map<Key, Optional<Slice>> result = new LinkedHashMap<>();
        if (keys.isEmpty()) return result;

        // De-duplicate while preserving order: identical Keys collapse to one logical
        // lookup; both input entries share the same returned value.
        List<Key> unique = new ArrayList<>();
        for (Key k : keys) {
            if (!result.containsKey(k)) {
                result.put(k, Optional.empty()); // placeholder; overwritten below
                unique.add(k);
            }
        }

        try {
            BatchResolution res = resolveBatch(unique, asOf);
            // Populate result for every key whose lookup completed (failed[i] == false).
            for (int i = 0; i < unique.size(); i++) {
                Key k = unique.get(i);
                if (res.failed[i]) {
                    // Leave Optional.empty() placeholder; the throw carries the partial map.
                    continue;
                }
                KeyLookup r = res.resolved[i];
                if (r instanceof KeyLookup.Found f) {
                    result.put(k, Optional.of(unwrapKvChecksumIfNeeded(k, f.value())));
                } else {
                    result.put(k, Optional.empty());
                }
            }
            if (res.firstError != null) {
                // CP 22 — propagate HARD-severity exceptions to the read-only latch
                // before throwing the partial-result wrapper.
                tripIfHard(res.firstError);
                throw new MultiGetPartialResult(result, res.firstError);
            }
            return result;
        } catch (MultiGetPartialResult e) {
            throw e;
        } catch (RuntimeException e) {
            tripIfHard(e);
            throw e;
        }
    }

    /** Aggregated outcome of one batch resolution. */
    private static final class BatchResolution {
        final KeyLookup[] resolved;
        final boolean[] failed;
        final Throwable firstError;
        BatchResolution(KeyLookup[] r, boolean[] f, Throwable err) {
            this.resolved = r; this.failed = f; this.firstError = err;
        }
    }

    /**
     * Bucket {@code uniqueKeys} by SSTable, dispatch per-bucket lookups to
     * {@link ForkJoinPool#commonPool()}, and merge results in priority order.
     *
     * <p>Per-SSTable bucketing strategy (ADR-0013 §6.2 point 2): for each
     * input key, walk the snapshotted Version's L0 (newest-first by file
     * number) collecting every file whose [smallestKey, largestKey] range
     * covers the key; then for L1..L_max use {@link #findInLevel} to pick
     * the single covering file per level. Each covered file is added to a
     * bucket keyed by FileNumber. One ForkJoin task per bucket runs every
     * key in that bucket through the open reader's {@code lookup}, opening
     * the file handle once and reusing the bloom filter / index probe for
     * cache locality across the bucket.
     */
    private BatchResolution resolveBatch(List<Key> uniqueKeys, SequenceNumber asOf) {
        int n = uniqueKeys.size();

        // 1. Inline probe of active + frozen MemTables (no I/O to parallelise).
        KeyLookup[] resolved = new KeyLookup[n];
        boolean[] done = new boolean[n];
        boolean[] failed = new boolean[n];
        SkipListMemTable active = activeMemTable;
        SkipListMemTable frozen = frozenMemTable;
        for (int i = 0; i < n; i++) {
            KeyLookup r = active.lookup(uniqueKeys.get(i), asOf);
            if (!(r instanceof KeyLookup.Absent)) {
                resolved[i] = r;
                done[i] = true;
                continue;
            }
            if (frozen != null) {
                r = frozen.lookup(uniqueKeys.get(i), asOf);
                if (!(r instanceof KeyLookup.Absent)) {
                    resolved[i] = r;
                    done[i] = true;
                    continue;
                }
            }
            resolved[i] = new KeyLookup.Absent();
        }

        // 2. Snapshot the current Version once for the whole batch.
        Version v = versions.current();

        // 3. Per-key candidate file list (priority order: L0 newest -> ... -> L_max).
        List<FileMetadata> l0 = new ArrayList<>(v.level(0));
        l0.sort(Comparator.comparingLong((FileMetadata fm) -> fm.fileNumber().value()).reversed());

        @SuppressWarnings("unchecked")
        List<FileNumber>[] candidates = (List<FileNumber>[]) new List[n];
        LinkedHashMap<FileNumber, List<Integer>> bucket = new LinkedHashMap<>();

        for (int i = 0; i < n; i++) {
            if (done[i]) {
                candidates[i] = List.of();
                continue;
            }
            List<FileNumber> cand = new ArrayList<>();
            Key k = uniqueKeys.get(i);
            for (FileMetadata fm : l0) {
                if (userKeyInRange(k, fm)) {
                    cand.add(fm.fileNumber());
                    bucket.computeIfAbsent(fm.fileNumber(), x -> new ArrayList<>()).add(i);
                }
            }
            for (int level = 1; level < Constants.MAX_LEVEL_COUNT; level++) {
                FileMetadata fm = findInLevel(v.level(level), k);
                if (fm == null) continue;
                cand.add(fm.fileNumber());
                bucket.computeIfAbsent(fm.fileNumber(), x -> new ArrayList<>()).add(i);
            }
            candidates[i] = cand;
        }

        if (bucket.isEmpty()) {
            return new BatchResolution(resolved, failed, null);
        }

        // 4. Dispatch one ForkJoin task per bucket.
        ForkJoinPool pool = ForkJoinPool.commonPool();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        // Track which key indices a given file's task FAILED to produce a result for.
        // (Empty map entry == every bucketed key for that file got a KeyLookup.)
        Map<FileNumber, Map<Integer, KeyLookup>> perFile = new HashMap<>();
        Map<FileNumber, Set<Integer>> perFileFailed = new HashMap<>();
        List<ForkJoinTask<TaskResult>> tasks = new ArrayList<>(bucket.size());
        List<FileNumber> orderedFiles = new ArrayList<>(bucket.keySet());

        for (FileNumber fn : orderedFiles) {
            List<Integer> idxs = bucket.get(fn);
            BlockBasedTableReader reader = openTables.get(fn);
            tasks.add(pool.submit(() -> {
                Map<Integer, KeyLookup> out = new HashMap<>();
                Set<Integer> taskFailed = new HashSet<>();
                if (reader == null) {
                    // File closed / reopened away from us — treat every bucketed key as Absent.
                    return new TaskResult(out, taskFailed);
                }
                for (int idx : idxs) {
                    try {
                        KeyLookup r = reader.lookup(uniqueKeys.get(idx), asOf);
                        out.put(idx, r);
                    } catch (IOException e) {
                        firstError.compareAndSet(null, new UncheckedIOException(e));
                        taskFailed.add(idx);
                    } catch (RuntimeException e) {
                        firstError.compareAndSet(null, e);
                        taskFailed.add(idx);
                    }
                }
                return new TaskResult(out, taskFailed);
            }));
        }

        // 5. Collect results (always wait for every task — even after observing a failure —
        //    so we can correctly populate partial results).
        for (int t = 0; t < tasks.size(); t++) {
            FileNumber fn = orderedFiles.get(t);
            try {
                TaskResult tr = tasks.get(t).get();
                perFile.put(fn, tr.results);
                perFileFailed.put(fn, tr.failed);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, new RuntimeException("interrupted during multiGet", ie));
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                firstError.compareAndSet(null, cause);
            }
        }

        // 6. Per-key, walk candidates in priority order to pick the winner.
        for (int i = 0; i < n; i++) {
            if (done[i]) continue;
            boolean anyTaskFailedForThisKey = false;
            for (FileNumber fn : candidates[i]) {
                Set<Integer> tf = perFileFailed.get(fn);
                if (tf != null && tf.contains(i)) {
                    anyTaskFailedForThisKey = true;
                    break; // can't trust subsequent priority levels — this key's value is unknown
                }
                Map<Integer, KeyLookup> m = perFile.get(fn);
                if (m == null) continue;
                KeyLookup r = m.get(i);
                if (r == null) continue;
                if (r instanceof KeyLookup.Found || r instanceof KeyLookup.Tombstoned) {
                    resolved[i] = r;
                    break;
                }
            }
            if (anyTaskFailedForThisKey) failed[i] = true;
        }

        return new BatchResolution(resolved, failed, firstError.get());
    }

    /** Per-bucket task output: KeyLookup per resolved key + the set of keys that threw. */
    private static final class TaskResult {
        final Map<Integer, KeyLookup> results;
        final Set<Integer> failed;
        TaskResult(Map<Integer, KeyLookup> r, Set<Integer> f) { this.results = r; this.failed = f; }
    }

    private Optional<Slice> readAt(Key key, SequenceNumber asOf) {
        // 1. Active MemTable.
        SkipListMemTable active = activeMemTable;
        KeyLookup r = active.lookup(key, asOf);
        if (r instanceof KeyLookup.Found f) return Optional.of(f.value());
        if (r instanceof KeyLookup.Tombstoned) return Optional.empty();

        // 2. Frozen MemTable (being flushed).
        SkipListMemTable frozen = frozenMemTable;
        if (frozen != null) {
            r = frozen.lookup(key, asOf);
            if (r instanceof KeyLookup.Found f) return Optional.of(f.value());
            if (r instanceof KeyLookup.Tombstoned) return Optional.empty();
        }

        Version v = versions.current();

        // 3. L0 — files may overlap; probe newest first by file number.
        List<FileMetadata> l0 = new ArrayList<>(v.level(0));
        l0.sort(Comparator.comparingLong((FileMetadata fm) -> fm.fileNumber().value()).reversed());
        for (FileMetadata fm : l0) {
            if (!userKeyInRange(key, fm)) continue;
            BlockBasedTableReader reader = openTables.get(fm.fileNumber());
            if (reader == null) continue;
            try {
                r = reader.lookup(key, asOf);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (r instanceof KeyLookup.Found f) return Optional.of(f.value());
            if (r instanceof KeyLookup.Tombstoned) return Optional.empty();
        }

        // 4. L1..L_max — non-overlapping per level. Binary search for the candidate file.
        for (int level = 1; level < Constants.MAX_LEVEL_COUNT; level++) {
            FileMetadata fm = findInLevel(v.level(level), key);
            if (fm == null) continue;
            BlockBasedTableReader reader = openTables.get(fm.fileNumber());
            if (reader == null) continue;
            try {
                r = reader.lookup(key, asOf);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (r instanceof KeyLookup.Found f) return Optional.of(f.value());
            if (r instanceof KeyLookup.Tombstoned) return Optional.empty();
        }

        return Optional.empty();
    }

    private static boolean userKeyInRange(Key key, FileMetadata fm) {
        byte[] uk = key.bytes();
        byte[] smallest = fm.smallestKey().userKey().bytes();
        byte[] largest = fm.largestKey().userKey().bytes();
        return lexCompare(uk, smallest) >= 0 && lexCompare(uk, largest) <= 0;
    }

    /** Binary-search a non-overlapping level for the file whose range covers {@code userKey}. */
    static FileMetadata findInLevel(List<FileMetadata> level, Key userKey) {
        if (level.isEmpty()) return null;
        byte[] uk = userKey.bytes();
        int lo = 0, hi = level.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            byte[] largest = level.get(mid).largestKey().userKey().bytes();
            if (lexCompare(largest, uk) < 0) lo = mid + 1;
            else hi = mid;
        }
        if (lo == level.size()) return null;
        FileMetadata fm = level.get(lo);
        byte[] smallest = fm.smallestKey().userKey().bytes();
        if (lexCompare(uk, smallest) < 0) return null;
        return fm;
    }

    static int lexCompare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xff) - (b[i] & 0xff);
            if (d != 0) return d;
        }
        return a.length - b.length;
    }

    // -------------------------------------------------------------------- snapshots

    @Override
    public Snapshot snapshot() {
        long s = nextSequence.get();
        long seq = s > 0 ? s - 1 : 0;
        activeSnapshotSeqs.add(seq);
        return new Snapshot(new SequenceNumber(seq));
    }

    @Override
    public void releaseSnapshot(Snapshot snapshot) {
        activeSnapshotSeqs.remove(snapshot.sequence().value());
    }

    /**
     * Oldest sequence currently visible to at least one active snapshot,
     * or {@link Compactor#NO_SNAPSHOTS_HELD} if none are held.
     */
    public long oldestLiveSnapshotSequence() {
        if (activeSnapshotSeqs.isEmpty()) {
            return Compactor.NO_SNAPSHOTS_HELD;
        }
        return new TreeSet<>(activeSnapshotSeqs).first();
    }

    // -------------------------------------------------------------------- flush + compaction

    @Override
    public void flush() {
        synchronized (writeLock) {
            try {
                doFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void maybeFlush() throws IOException {
        if (activeMemTable.approximateBytes() >= Constants.MEMTABLE_FLUSH_THRESHOLD_BYTES) {
            doFlush();
        }
    }

    /** Freeze the active MemTable, write it as an L0 SSTable, record VersionEdit. */
    private void doFlush() throws IOException {
        // No data AND no range tombstones to persist → nothing to do.
        // CP 17 limitation: an RT-only MemTable (deleteRange calls but no put/delete)
        // is NOT flushed here — the RT bounds would not yield a valid
        // (smallestKey, largestKey) for FileMetadata. The RTs stay in MemTable until
        // a subsequent point write triggers a normal flush; in the meantime they're
        // durable in the WAL so a crash recovery replays them.
        if (activeMemTable.size() == 0) return;

        SkipListMemTable toFlush = activeMemTable;
        toFlush.freeze();
        frozenMemTable = toFlush;
        activeMemTable = new SkipListMemTable();

        // Open a new WAL for the new active MemTable.
        long newWalNum = versions.current().nextFileNumber();
        Path newWalPath = dbDir.resolve(new FileNumber(newWalNum).logFileName());
        walWriter.close();
        walWriter = LogWriter.open(newWalPath, true);
        FileNumber oldWal = walNumber;
        walNumber = new FileNumber(newWalNum);

        // Write the frozen MemTable as a new L0 SSTable.
        long tableNum = newWalNum + 1L;
        FileNumber tableFn = new FileNumber(tableNum);
        Path tablePath = dbDir.resolve(tableFn.tableFileName());
        InternalKey smallest = null, largest = null;
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(tablePath, compressOutput)) {
            Iterator<SkipListMemTable.MemTableEntry> it = toFlush.iterator();
            while (it.hasNext()) {
                SkipListMemTable.MemTableEntry e = it.next();
                if (smallest == null) smallest = e.internalKey();
                largest = e.internalKey();
                w.add(e.internalKey(), e.value());
            }
            // CP 17 — persist range tombstones into the SSTable's meta-block.
            for (com.hkg.rocksdb.common.RangeTombstone rt : toFlush.rangeTombstones()) {
                w.addRangeTombstone(rt);
            }
            w.finish();
        }
        long fileSize = Files.size(tablePath);
        // CP 19 — compute the §5 file-level XXH64 over every byte of the
        // freshly written SSTable. Catches whole-file corruption that the
        // per-block CRC cannot see (footer bit-flip, truncation, mis-attribution).
        // Computed AFTER w.finish() and BEFORE we record the VersionEdit so the
        // checksum in MANIFEST always describes the bytes that are on disk now.
        com.hkg.rocksdb.integrity.FileChecksum fileCk =
            com.hkg.rocksdb.integrity.FileChecksum.compute(tablePath);
        FileMetadata fm = new FileMetadata(tableFn, fileSize, smallest, largest, fileCk);

        // Apply VersionEdits: bump nextFileNumber past tableNum, register the new L0 file,
        // set the new active WAL, persist lastSequence.
        long lastSeq = nextSequence.get() - 1L;
        versions.apply(List.of(
            new VersionEdit.SetNextFileNumber(tableNum + 1L),
            new VersionEdit.SetLogNumber(newWalNum),
            new VersionEdit.SetLastSequence(lastSeq),
            new VersionEdit.NewFile(0, fm)));

        // Open the new SSTable for reads — wired into the shared block cache.
        openTables.put(tableFn, BlockBasedTableReader.open(tablePath, tableFn, blockCache, dbDir.toString()));

        // Discard the frozen MemTable.
        frozenMemTable = null;

        // Delete the old WAL.
        Files.deleteIfExists(dbDir.resolve(oldWal.logFileName()));
    }

    /**
     * Run one compaction pass if the picker selects a job. Returns true if
     * a compaction ran. Phase 2's compactor is foreground; Phase 3 wires it
     * onto a background thread.
     */
    public boolean maybeCompact() throws IOException {
        return runCompactionPass(1) > 0;
    }

    /**
     * CP 18 — run a parallel L0→L1 compaction pass. The
     * {@link LeveledCompactionPicker#partitionL0 picker} splits the L0 backlog
     * into up to {@code targetJobs} disjoint sub-range jobs; the engine
     * dispatches each to a worker. Returns the number of jobs that actually
     * ran (may be {@literal <} {@code targetJobs} when L0 ranges or L1
     * overlaps force merging).
     *
     * <p>Use this when the engine wants to maximise L0 drain throughput —
     * e.g. when the L0 file count is near {@link Constants#L0_STOP_WRITES_TRIGGER}
     * and the next-largest level can absorb writes in parallel. For ordinary
     * scoring-based compaction (any level), call {@link #runCompactionPass}
     * instead — it handles the general case.
     */
    public int runParallelL0Compaction(int targetJobs) throws IOException {
        if (targetJobs < 1) {
            throw new IllegalArgumentException("targetJobs must be >= 1, got " + targetJobs);
        }
        List<CompactionJob> jobs;
        long oldestLive;
        AtomicLong allocCounter;
        synchronized (writeLock) {
            jobs = new LeveledCompactionPicker().partitionL0(versions.current(), targetJobs);
            if (jobs.isEmpty()) return 0;
            // Filter out jobs that conflict with in-flight files (rare but possible if a regular
            // compaction is already underway when this is called).
            List<CompactionJob> kept = new ArrayList<>();
            Set<FileNumber> banned = new HashSet<>(inFlightFiles);
            for (CompactionJob j : jobs) {
                boolean clean = true;
                for (FileMetadata fm : j.allInputs()) {
                    if (banned.contains(fm.fileNumber())) { clean = false; break; }
                }
                if (clean) {
                    kept.add(j);
                    for (FileMetadata fm : j.allInputs()) banned.add(fm.fileNumber());
                }
            }
            jobs = kept;
            if (jobs.isEmpty()) return 0;

            oldestLive = oldestLiveSnapshotSequence();
            allocCounter = new AtomicLong(versions.current().nextFileNumber());
            for (CompactionJob job : jobs) {
                for (FileMetadata fm : job.allInputs()) inFlightFiles.add(fm.fileNumber());
            }
        }

        List<Future<List<FileMetadata>>> futures = new ArrayList<>(jobs.size());
        Compactor.ReaderOpener opener = fn -> openTables.computeIfAbsent(fn, k -> {
            try {
                return BlockBasedTableReader.open(dbDir.resolve(k.tableFileName()), k, blockCache, dbDir.toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        WriteHook compactionHook = rateLimiterHookFor(Priority.LOW);
        for (CompactionJob job : jobs) {
            futures.add(compactionExecutor.submit(job, oldestLive,
                Constants.SST_FILE_TARGET_SIZE_BYTES, dbDir,
                () -> new FileNumber(allocCounter.getAndIncrement()),
                opener,
                compressOutput,
                compactionHook));
        }

        int completed = 0;
        try {
            for (int i = 0; i < jobs.size(); i++) {
                CompactionJob job = jobs.get(i);
                List<FileMetadata> outputs;
                try {
                    outputs = futures.get(i).get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting for compaction worker", ie);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof IOException io) throw io;
                    if (cause instanceof RuntimeException re) throw re;
                    throw new IOException("compaction worker failed", cause);
                }
                applyCompactionResult(job, outputs, allocCounter);
                completed++;
            }
        } finally {
            for (CompactionJob job : jobs) {
                for (FileMetadata fm : job.allInputs()) inFlightFiles.remove(fm.fileNumber());
            }
        }
        return completed;
    }

    /**
     * Run one compaction scheduling pass: pick up to {@code maxParallel}
     * non-overlapping jobs, dispatch each to a worker on the
     * {@link CompactionExecutor}, wait for them all to finish, and apply the
     * resulting {@link VersionEdit}s (sequentially per job, under
     * {@link #writeLock}). Returns the number of jobs that actually executed.
     *
     * <p>The writeLock is held only while picking, allocating file numbers,
     * and applying edits — NOT while a worker is merging SSTables. This is
     * the CP 10 win: foreground writes proceed concurrently with compaction
     * work, and two non-overlapping compactions overlap in time.
     *
     * <p>If {@code maxParallel} exceeds the executor's worker count, the
     * extra jobs queue inside the executor and execute as workers free up.
     */
    public int runCompactionPass(int maxParallel) throws IOException {
        if (maxParallel < 1) {
            throw new IllegalArgumentException("maxParallel must be >= 1, got " + maxParallel);
        }
        List<CompactionJob> jobs;
        long oldestLive;
        AtomicLong allocCounter;
        synchronized (writeLock) {
            jobs = new LeveledCompactionPicker().pickMultiple(
                versions.current(), new HashSet<>(inFlightFiles), maxParallel);
            if (jobs.isEmpty()) return 0;
            oldestLive = oldestLiveSnapshotSequence();
            // One shared FileNumber allocator across all parallel jobs: every output gets
            // a distinct number, and SetNextFileNumber after all jobs apply captures the high-water mark.
            allocCounter = new AtomicLong(versions.current().nextFileNumber());
            for (CompactionJob job : jobs) {
                for (FileMetadata fm : job.allInputs()) {
                    inFlightFiles.add(fm.fileNumber());
                }
            }
        }

        // Submit all jobs to the executor — they may run concurrently.
        List<Future<List<FileMetadata>>> futures = new ArrayList<>(jobs.size());
        Compactor.ReaderOpener opener = fn -> openTables.computeIfAbsent(fn, k -> {
            try {
                return BlockBasedTableReader.open(dbDir.resolve(k.tableFileName()), k, blockCache, dbDir.toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        WriteHook compactionHook = rateLimiterHookFor(Priority.LOW);
        for (CompactionJob job : jobs) {
            futures.add(compactionExecutor.submit(job, oldestLive,
                Constants.SST_FILE_TARGET_SIZE_BYTES, dbDir,
                () -> new FileNumber(allocCounter.getAndIncrement()),
                opener,
                compressOutput,
                compactionHook));
        }

        // Wait for each job to finish, then apply its VersionEdits + cleanup.
        int completed = 0;
        try {
            for (int i = 0; i < jobs.size(); i++) {
                CompactionJob job = jobs.get(i);
                List<FileMetadata> outputs;
                try {
                    outputs = futures.get(i).get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting for compaction worker", ie);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof IOException io) throw io;
                    if (cause instanceof RuntimeException re) throw re;
                    throw new IOException("compaction worker failed", cause);
                }
                applyCompactionResult(job, outputs, allocCounter);
                completed++;
            }
        } finally {
            for (CompactionJob job : jobs) {
                for (FileMetadata fm : job.allInputs()) {
                    inFlightFiles.remove(fm.fileNumber());
                }
            }
        }
        return completed;
    }

    /** Apply the VersionEdits for one finished compaction + clean up obsolete files. */
    private void applyCompactionResult(CompactionJob job, List<FileMetadata> outputs,
                                         AtomicLong allocCounter) throws IOException {
        synchronized (writeLock) {
            List<VersionEdit> edits = new ArrayList<>();
            edits.add(new VersionEdit.SetNextFileNumber(allocCounter.get()));
            for (FileMetadata fm : job.inputs()) {
                edits.add(new VersionEdit.DeleteFile(job.inputLevel(), fm.fileNumber()));
            }
            for (FileMetadata fm : job.overlapping()) {
                edits.add(new VersionEdit.DeleteFile(job.outputLevel(), fm.fileNumber()));
            }
            for (FileMetadata out : outputs) {
                edits.add(new VersionEdit.NewFile(job.outputLevel(), out));
            }
            versions.apply(edits);

            for (FileMetadata out : outputs) {
                openTables.computeIfAbsent(out.fileNumber(), k -> {
                    try {
                        return BlockBasedTableReader.open(dbDir.resolve(k.tableFileName()), k, blockCache, dbDir.toString());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            for (FileMetadata fm : job.inputs()) {
                closeAndDelete(fm.fileNumber());
            }
            for (FileMetadata fm : job.overlapping()) {
                closeAndDelete(fm.fileNumber());
            }
        }
    }

    private void closeAndDelete(FileNumber fn) throws IOException {
        BlockBasedTableReader r = openTables.remove(fn);
        if (r != null) r.close();
        Files.deleteIfExists(dbDir.resolve(fn.tableFileName()));
    }

    // -------------------------------------------------------------------- misc

    public Version currentVersion() {
        return versions.current();
    }

    /** Shared block cache used by every SSTable reader this engine owns. */
    public BlockCache blockCache() {
        return blockCache;
    }

    /** Rate limiter pacing compaction I/O, or {@code null} if no throttling is configured. */
    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    /** True iff this engine wraps stored values with a KV checksum (CP 21). */
    public boolean kvChecksumEnabled() {
        return kvChecksumEnabled;
    }

    /**
     * CP 22 — true iff the engine has tripped its read-only latch in response
     * to a HARD-severity error (block-CRC mismatch, KV-checksum mismatch,
     * file-checksum mismatch). When tripped, writes throw
     * {@code ReadOnlyModeException}; reads continue.
     */
    public boolean isReadOnly() {
        return readOnlyLatch.isTripped();
    }

    /**
     * CP 22 — last-known reason the read-only latch was tripped, or null
     * if it hasn't been. Surface this to operators (logs, an admin
     * endpoint) so they know what to investigate before calling
     * {@link #attemptResume()}.
     */
    public String readOnlyReason() {
        return readOnlyLatch.reason();
    }

    /**
     * CP 22 — clear the read-only latch after the operator has investigated
     * the cause of the HARD error. Writes resume; if the underlying issue
     * remains, the next failing read or write will trip the latch again.
     */
    public void attemptResume() {
        readOnlyLatch.attemptResume();
    }

    // -------------------------------------------------------------------- CP 27 — user-defined timestamps

    /** True iff this engine was opened with {@code userTimestampSize > 0}. */
    public boolean userTimestampsEnabled() {
        return userTimestampSize > 0;
    }

    /**
     * CP 27 — write a key/value at an application-supplied user timestamp.
     * Only valid when {@link #userTimestampsEnabled()} is true. The user
     * timestamp is appended (8 bytes BE) to the user key bytes at storage
     * time, and tracked in a per-engine in-memory index that
     * {@link #getAsOf} consults to find the floor ts.
     */
    public void putWithUserTs(Key key, UserTimestamp ts, Slice value) {
        if (!userTimestampsEnabled()) {
            throw new IllegalStateException("putWithUserTs requires userTimestampSize > 0");
        }
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(ts, "ts");
        Objects.requireNonNull(value, "value");
        byte[] storedKeyBytes = appendTs(key.bytes(), ts.value());
        put(Key.of(storedKeyBytes), value);
        udtIndex.computeIfAbsent(key,
            k -> new java.util.concurrent.ConcurrentSkipListSet<>()).add(ts.value());
    }

    /**
     * CP 27 — point read at a user timestamp. Returns the value of the entry
     * for {@code key} whose user timestamp is the largest value
     * {@code ≤ asOfTs} (the "floor" timestamp), or {@link Optional#empty()}
     * if no such entry exists.
     *
     * <p>Note the asymmetry with {@link #get(Key, Snapshot)}: snapshots are
     * sequence-number-based and ordered by engine wall-clock; UDT visibility
     * uses application-supplied timestamps and ignores sequence.
     */
    public Optional<Slice> getAsOf(Key key, UserTimestamp asOfTs) {
        if (!userTimestampsEnabled()) {
            throw new IllegalStateException("getAsOf requires userTimestampSize > 0");
        }
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(asOfTs, "asOfTs");
        java.util.concurrent.ConcurrentSkipListSet<Long> tss = udtIndex.get(key);
        if (tss == null || tss.isEmpty()) return Optional.empty();
        Long floor = tss.floor(asOfTs.value());
        if (floor == null) return Optional.empty();
        byte[] storedKeyBytes = appendTs(key.bytes(), floor);
        return get(Key.of(storedKeyBytes));
    }

    /** Append an 8-byte BE-encoded ts to a user-key. */
    private static byte[] appendTs(byte[] userKey, long ts) {
        byte[] out = new byte[userKey.length + UserTimestamp.ENCODED_LENGTH];
        System.arraycopy(userKey, 0, out, 0, userKey.length);
        for (int i = 0; i < UserTimestamp.ENCODED_LENGTH; i++) {
            out[userKey.length + i] = (byte) (ts >>> (56 - 8 * i));
        }
        return out;
    }

    /** Reverse of {@link #appendTs}: split a stored key into (userKey, ts). */
    private static Object[] splitStoredKey(byte[] storedKey) {
        if (storedKey.length < UserTimestamp.ENCODED_LENGTH) return null;
        int userLen = storedKey.length - UserTimestamp.ENCODED_LENGTH;
        byte[] userKey = new byte[userLen];
        System.arraycopy(storedKey, 0, userKey, 0, userLen);
        long ts = 0L;
        for (int i = 0; i < UserTimestamp.ENCODED_LENGTH; i++) {
            ts = (ts << 8) | (storedKey[userLen + i] & 0xFFL);
        }
        return new Object[] {userKey, ts};
    }

    /**
     * CP 27 — at open time (or after recovery), walk every SSTable referenced
     * by the current Version, split each entry's stored key into
     * (userKey, ts), and populate {@link #udtIndex}. Also walks the active
     * MemTable. This rebuilds the in-memory floor-lookup structure UDT reads
     * depend on.
     *
     * <p>Memory cost: O(unique-user-keys × unique-timestamps). Acceptable for
     * the pedagogical scope; production UDT persists a per-CF index sidecar
     * to avoid the full scan on open.
     */
    private void rebuildUdtIndex() {
        if (udtIndex == null) return;
        // Walk every SSTable.
        for (BlockBasedTableReader r : openTables.values()) {
            try {
                Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries();
                while (it.hasNext()) {
                    BlockBasedTableReader.SsTableEntry e = it.next();
                    Object[] split = splitStoredKey(e.internalKey().userKey().bytes());
                    if (split == null) continue;
                    byte[] uk = (byte[]) split[0];
                    long ts = (Long) split[1];
                    udtIndex.computeIfAbsent(Key.of(uk),
                        k -> new java.util.concurrent.ConcurrentSkipListSet<>()).add(ts);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // Walk the active memtable (post-WAL-replay).
        Iterator<SkipListMemTable.MemTableEntry> mit = activeMemTable.iterator();
        while (mit.hasNext()) {
            SkipListMemTable.MemTableEntry e = mit.next();
            Object[] split = splitStoredKey(e.internalKey().userKey().bytes());
            if (split == null) continue;
            byte[] uk = (byte[]) split[0];
            long ts = (Long) split[1];
            udtIndex.computeIfAbsent(Key.of(uk),
                k -> new java.util.concurrent.ConcurrentSkipListSet<>()).add(ts);
        }
    }

    /**
     * Build a {@link WriteHook} that requests {@code bytes} tokens from this
     * engine's {@link #rateLimiter} at {@code priority}. Returns
     * {@link WriteHook#NO_OP} when no limiter is configured — the un-throttled
     * path the LevelDB-era stress test exercises.
     */
    private WriteHook rateLimiterHookFor(Priority priority) {
        if (rateLimiter == null) return WriteHook.NO_OP;
        return bytes -> {
            try {
                rateLimiter.request(bytes, priority);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for rate limiter", ie);
            }
        };
    }

    public long lastSequence() {
        return nextSequence.get() - 1L;
    }

    @Override
    public Iterator<MutationRecord> scan(Key from, Key to) {
        throw new UnsupportedOperationException("scan() lands in CP 10 (Phase 3)");
    }

    /** Number of compaction worker threads this engine is using. */
    public int compactionThreads() {
        return compactionExecutor.threadCount();
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            try {
                if (activeMemTable.size() > 0) {
                    doFlush();
                }
                walWriter.close();
                for (BlockBasedTableReader r : openTables.values()) {
                    r.close();
                }
                openTables.clear();
                versions.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        compactionExecutor.close();
    }

    /**
     * Close file handles WITHOUT flushing the active MemTable. The WAL is
     * already fsynced (synchronous mode) so every acked write is durable;
     * the MemTable contents will be recovered via WAL replay on the next
     * {@link #open(Path)}. Used by the integration test suite to simulate
     * a process crash.
     */
    public void closeWithoutFlush() {
        synchronized (writeLock) {
            try {
                walWriter.close();
                for (BlockBasedTableReader r : openTables.values()) {
                    r.close();
                }
                openTables.clear();
                versions.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        compactionExecutor.close();
    }
}
