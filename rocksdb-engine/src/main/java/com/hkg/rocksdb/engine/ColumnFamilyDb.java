package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.columnfamily.ColumnFamilyHandle;
import com.hkg.rocksdb.columnfamily.ColumnFamilyId;
import com.hkg.rocksdb.columnfamily.ColumnFamilyOptions;
import com.hkg.rocksdb.columnfamily.ColumnFamilyRegistry;
import com.hkg.rocksdb.columnfamily.CompactionStyle;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.ratelimiter.RateLimiter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * CP 14 engine — exposes the multi-column-family API on top of one
 * {@link RocksDb} per column family. Each CF lives in its own subdirectory
 * under {@code dbDir}; the registry's contents are persisted in a sidecar
 * {@code CFs} file (one record per CF: {@code <id>,<name>,<style>,<fifoCap>}).
 *
 * <p>What this commit DOES land:
 * <ul>
 *   <li>{@code createColumnFamily} / {@code dropColumnFamily} / {@code get}.</li>
 *   <li>Per-CF MemTable + level structure + openTables (isolated per
 *       sub-{@link RocksDb}).</li>
 *   <li>Per-CF compaction-style choice carried in {@link ColumnFamilyOptions}
 *       and preserved across reopen.</li>
 *   <li>{@code put}/{@code get}/{@code delete}/{@code flush}/{@code compact}
 *       accept a {@link ColumnFamilyHandle}.</li>
 *   <li>Drop deletes the CF's subdirectory and all SSTables under it.</li>
 * </ul>
 *
 * <p>What this commit DOES NOT land (deferred to a follow-on CP):
 * <ul>
 *   <li>SHARED WAL across CFs with cfId tagging. Today each CF has its own
 *       WAL inside its subdir; the plan §4.4 calls for one WAL at the DB
 *       root with records carrying cfId. The on-disk format is the only
 *       difference; the API stays the same.</li>
 *   <li>SHARED MANIFEST with {@code AddColumnFamily}/{@code DropColumnFamily}
 *       edits + cfId-tagged {@code NewFile}/{@code DeleteFile}. Today each
 *       CF has its own MANIFEST in its subdir; the registry is recovered
 *       from the sidecar.</li>
 *   <li>Per-CF compaction style for TIERED / FIFO / DYNAMIC_LEVELED. The
 *       option is preserved per CF; the sibling pickers exist (CP 11/CP 12);
 *       but {@link RocksDb#runCompactionPass} still hard-wires
 *       LeveledCompactionPicker. Calling {@code compact} on a non-LEVELED CF
 *       runs leveled compaction; switching that to consult the CF's style
 *       lands when the picker wiring is generalised.</li>
 * </ul>
 *
 * <p>The pedagogical trade-off is explicit: this scope ships the application-
 * facing CF semantics (isolation, drop, reopen, options) without doing the
 * on-disk format refactor in the same commit.
 */
public final class ColumnFamilyDb implements AutoCloseable {

    private static final String SIDECAR_NAME = "CFs";

    private final Path dbDir;
    private final boolean compressOutput;
    private final long blockCacheBytes;
    private final int compactionThreads;
    private final RateLimiter rateLimiter;
    private final ColumnFamilyRegistry registry;
    private final Map<ColumnFamilyId, RocksDb> engines = new HashMap<>();

    private ColumnFamilyDb(Path dbDir, boolean compressOutput, long blockCacheBytes,
                            int compactionThreads, RateLimiter rateLimiter,
                            ColumnFamilyRegistry registry) {
        this.dbDir = dbDir;
        this.compressOutput = compressOutput;
        this.blockCacheBytes = blockCacheBytes;
        this.compactionThreads = compactionThreads;
        this.rateLimiter = rateLimiter;
        this.registry = registry;
    }

    /** Open with default options (no rate limit, one compaction thread, 8 MiB cache). */
    public static ColumnFamilyDb open(Path dbDir) throws IOException {
        return open(dbDir, true, Constants.BLOCK_CACHE_DEFAULT_BYTES, 1, null);
    }

    public static ColumnFamilyDb open(Path dbDir, boolean compressOutput, long blockCacheBytes,
                                       int compactionThreads, RateLimiter rateLimiter)
        throws IOException {
        Files.createDirectories(dbDir);
        ColumnFamilyRegistry registry = new ColumnFamilyRegistry();
        loadSidecar(dbDir, registry);

        ColumnFamilyDb db = new ColumnFamilyDb(dbDir, compressOutput, blockCacheBytes,
            compactionThreads, rateLimiter, registry);

        // Open the default CF unconditionally; open every previously-registered CF too.
        for (ColumnFamilyHandle cf : registry.all()) {
            db.engines.put(cf.id(), db.openEngineFor(cf));
        }
        // Persist sidecar so the file exists on first open (idempotent if already present).
        writeSidecar(dbDir, registry);
        return db;
    }

    private RocksDb openEngineFor(ColumnFamilyHandle cf) throws IOException {
        Path cfDir = cfDirectoryFor(cf);
        Files.createDirectories(cfDir);
        BlockCache cache = new LruBlockCache(blockCacheBytes);
        return RocksDb.open(cfDir, compressOutput, cache, compactionThreads, rateLimiter);
    }

    private Path cfDirectoryFor(ColumnFamilyHandle cf) {
        if (cf.id().equals(ColumnFamilyId.DEFAULT)) {
            // The default CF lives at the DB root so existing single-CF DBs
            // (leveldb-java format) open here without surgery.
            return dbDir;
        }
        return dbDir.resolve(String.format("cf-%06d", cf.id().value()));
    }

    // -------------------------------------------------------------------- registry surface

    public ColumnFamilyHandle defaultColumnFamily() {
        return registry.defaultColumnFamily();
    }

    public Optional<ColumnFamilyHandle> getColumnFamily(String name) {
        return registry.get(name);
    }

    public Collection<ColumnFamilyHandle> columnFamilies() {
        return registry.all();
    }

    public synchronized ColumnFamilyHandle createColumnFamily(String name, ColumnFamilyOptions options)
        throws IOException {
        ColumnFamilyHandle cf = registry.create(name, options);
        engines.put(cf.id(), openEngineFor(cf));
        writeSidecar(dbDir, registry);
        return cf;
    }

    public synchronized void dropColumnFamily(ColumnFamilyId id) throws IOException {
        ColumnFamilyHandle h = registry.drop(id);
        RocksDb engine = engines.remove(h.id());
        if (engine != null) {
            engine.close();
        }
        Path cfDir = cfDirectoryFor(h);
        // Drop the subdirectory and everything under it. Idempotent — a partial drop
        // (engine closed but rm failed) is recoverable on next open via the sidecar.
        deleteRecursively(cfDir);
        writeSidecar(dbDir, registry);
    }

    // -------------------------------------------------------------------- per-CF operations

    public void put(ColumnFamilyHandle cf, Key key, Slice value) {
        engineFor(cf).put(key, value);
    }

    public void delete(ColumnFamilyHandle cf, Key key) {
        engineFor(cf).delete(key);
    }

    public Optional<Slice> get(ColumnFamilyHandle cf, Key key) {
        return engineFor(cf).get(key);
    }

    public void flush(ColumnFamilyHandle cf) {
        engineFor(cf).flush();
    }

    /**
     * Run one compaction pass on the named CF. Today the picker is always
     * leveled regardless of {@code cf.options().compactionStyle()} — the
     * style is preserved as metadata, but generalising the picker selection
     * is a follow-on CP.
     */
    public int compact(ColumnFamilyHandle cf, int maxParallel) throws IOException {
        ensureLeveledOrTolerable(cf);
        return engineFor(cf).runCompactionPass(maxParallel);
    }

    /** Shortcut: leveled only; returns true if a job ran. */
    public boolean maybeCompact(ColumnFamilyHandle cf) throws IOException {
        return compact(cf, 1) > 0;
    }

    /** Convenience accessor for tests: the underlying single-CF engine for {@code cf}. */
    public RocksDb engineFor(ColumnFamilyHandle cf) {
        RocksDb engine = engines.get(cf.id());
        if (engine == null) {
            throw new IllegalStateException("ColumnFamily not open: id=" + cf.id().value()
                + " name=" + cf.name());
        }
        return engine;
    }

    private static void ensureLeveledOrTolerable(ColumnFamilyHandle cf) {
        // Non-leveled styles silently degrade to leveled compaction for now; the picker
        // generalisation lands in the same CP series that wires TieredCompactionPicker
        // / FifoCompactionPicker into the engine.
        CompactionStyle style = cf.options().compactionStyle();
        if (style != CompactionStyle.LEVELED) {
            // No-op — preserved for tracing in test output if needed.
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (RocksDb engine : engines.values()) {
            try { engine.close(); } catch (RuntimeException re) {
                if (first == null) first = new IOException(re);
            }
        }
        engines.clear();
    }

    // -------------------------------------------------------------------- sidecar

    /**
     * Sidecar format: one line per CF, comma-separated, trailing newline. The
     * implicit default CF is included for completeness even though
     * {@link ColumnFamilyRegistry} seeds it automatically.
     *
     * <pre>
     *   id,name,compactionStyle,fifoMaxBytes
     *   0,default,LEVELED,0
     *   1,metrics,TIERED,0
     *   2,cache,FIFO,67108864
     * </pre>
     */
    private static void writeSidecar(Path dbDir, ColumnFamilyRegistry registry) throws IOException {
        Path sidecar = dbDir.resolve(SIDECAR_NAME);
        StringBuilder sb = new StringBuilder();
        for (ColumnFamilyHandle cf : registry.all()) {
            sb.append(cf.id().value()).append(',')
              .append(cf.name()).append(',')
              .append(cf.options().compactionStyle().name()).append(',')
              .append(cf.options().fifoMaxBytes()).append('\n');
        }
        Files.write(sidecar, sb.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE);
    }

    private static void loadSidecar(Path dbDir, ColumnFamilyRegistry registry) throws IOException {
        Path sidecar = dbDir.resolve(SIDECAR_NAME);
        if (!Files.exists(sidecar)) return;
        for (String line : Files.readAllLines(sidecar, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] parts = line.split(",", 4);
            if (parts.length != 4) {
                throw new IOException("malformed CFs sidecar line: " + line);
            }
            int id = Integer.parseInt(parts[0]);
            String name = parts[1];
            CompactionStyle style = CompactionStyle.valueOf(parts[2]);
            long fifoCap = Long.parseLong(parts[3]);
            if (id == ColumnFamilyId.DEFAULT.value()) {
                // Default is auto-registered by the registry constructor; skip.
                continue;
            }
            registry.registerExisting(new ColumnFamilyId(id), name,
                new ColumnFamilyOptions(style, fifoCap));
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;
        // Walk depth-first so directories are emptied before removal.
        List<Path> paths;
        try (Stream<Path> s = Files.walk(root)) {
            paths = new ArrayList<>(s.toList());
        }
        // Reverse to delete children before parents.
        for (int i = paths.size() - 1; i >= 0; i--) {
            Files.deleteIfExists(paths.get(i));
        }
    }
}
