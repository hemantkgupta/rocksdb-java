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
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.compaction.CompactionExecutor;
import com.hkg.rocksdb.compaction.CompactionJob;
import com.hkg.rocksdb.compaction.Compactor;
import com.hkg.rocksdb.compaction.LeveledCompactionPicker;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

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

    private RocksDb(Path dbDir, VersionSet versions, SkipListMemTable mt,
                     LogWriter walWriter, FileNumber walNumber,
                     ConcurrentHashMap<FileNumber, BlockBasedTableReader> tables,
                     AtomicLong nextSequence, boolean compressOutput,
                     BlockCache blockCache, CompactionExecutor compactionExecutor,
                     RateLimiter rateLimiter) {
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
    }

    /**
     * Open the engine on {@code dbDir}, creating a fresh DB if no CURRENT
     * pointer exists. Replays the MANIFEST to reconstruct the Version and
     * opens readers for every referenced SSTable. WAL replay (recovering
     * the in-memory MemTable contents from a prior crash) is handled
     * separately by {@link #recoverWal(Path)} — wired up in CP 9.
     */
    public static RocksDb open(Path dbDir) throws IOException {
        return open(dbDir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput) throws IOException {
        return open(dbDir, compressOutput, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, 1, null);
    }

    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads)
        throws IOException {
        return open(dbDir, compressOutput, blockCache, compactionThreads, null);
    }

    /**
     * Open the engine with an explicit {@link BlockCache}, a fixed-size pool
     * of compaction worker threads, and an optional {@link RateLimiter} that
     * paces every compaction worker's block writes at {@link Priority#LOW}.
     *
     * <p>With {@code compactionThreads = 1} the engine behaves like LevelDB
     * 2011 (one job at a time); with {@code compactionThreads > 1} the
     * {@link com.hkg.rocksdb.compaction.LeveledCompactionPicker#pickMultiple
     * picker} chooses several non-overlapping jobs that workers execute in
     * parallel. With {@code rateLimiter = null}, compaction I/O runs
     * un-throttled (LevelDB-era behaviour).
     *
     * <p>The cache, executor, and rate limiter are per-engine; cross-instance
     * sharing of the rate limiter (RocksDB plan CP 24) is out of scope here.
     */
    public static RocksDb open(Path dbDir, boolean compressOutput, BlockCache blockCache,
                                int compactionThreads, RateLimiter rateLimiter)
        throws IOException {
        Files.createDirectories(dbDir);
        boolean freshDb = !Files.exists(dbDir.resolve("CURRENT"));
        VersionSet vs = freshDb ? VersionSet.createFresh(dbDir) : VersionSet.open(dbDir);

        // Defensive sweep: remove .sst.tmp and any orphan .sst files not referenced by the Version.
        sweepOrphans(dbDir, vs.current());

        ConcurrentHashMap<FileNumber, BlockBasedTableReader> tables = new ConcurrentHashMap<>();
        for (FileNumber fn : vs.current().allFileNumbers()) {
            tables.put(fn, BlockBasedTableReader.open(dbDir.resolve(fn.tableFileName()), fn, blockCache));
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
            new FileNumber(walNum), tables, seq, compressOutput, blockCache, executor, rateLimiter);

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
        synchronized (writeLock) {
            try {
                long seq = nextSequence.getAndIncrement();
                MutationRecord.Put put = new MutationRecord.Put(key, value, new SequenceNumber(seq));
                walWriter.append(MutationCodec.encode(put));
                activeMemTable.put(key, value, new SequenceNumber(seq));
                maybeFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void delete(Key key) {
        synchronized (writeLock) {
            try {
                long seq = nextSequence.getAndIncrement();
                MutationRecord.Delete del = new MutationRecord.Delete(key, new SequenceNumber(seq));
                walWriter.append(MutationCodec.encode(del));
                activeMemTable.delete(key, new SequenceNumber(seq));
                maybeFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    // -------------------------------------------------------------------- reads

    @Override
    public Optional<Slice> get(Key key) {
        long current = nextSequence.get();
        SequenceNumber asOf = current > 0 ? new SequenceNumber(current - 1) : SequenceNumber.ZERO;
        return readAt(key, asOf);
    }

    @Override
    public Optional<Slice> get(Key key, Snapshot snapshot) {
        return readAt(key, snapshot.sequence());
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
            w.finish();
        }
        long fileSize = Files.size(tablePath);
        FileMetadata fm = new FileMetadata(tableFn, fileSize, smallest, largest);

        // Apply VersionEdits: bump nextFileNumber past tableNum, register the new L0 file,
        // set the new active WAL, persist lastSequence.
        long lastSeq = nextSequence.get() - 1L;
        versions.apply(List.of(
            new VersionEdit.SetNextFileNumber(tableNum + 1L),
            new VersionEdit.SetLogNumber(newWalNum),
            new VersionEdit.SetLastSequence(lastSeq),
            new VersionEdit.NewFile(0, fm)));

        // Open the new SSTable for reads — wired into the shared block cache.
        openTables.put(tableFn, BlockBasedTableReader.open(tablePath, tableFn, blockCache));

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
                return BlockBasedTableReader.open(dbDir.resolve(k.tableFileName()), k, blockCache);
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
                        return BlockBasedTableReader.open(dbDir.resolve(k.tableFileName()), k, blockCache);
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
