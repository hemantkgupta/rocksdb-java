package com.hkg.rocksdb.testcluster;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.ratelimiter.RateLimiter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Phase 6 CP 24 — a minimal "shared host" handle for opening N {@link RocksDb}
 * instances against ONE process-wide {@link RateLimiter} (and optionally one
 * shared {@link BlockCache}).
 *
 * <p>ADR-0005 calls for the RateLimiter to be process-scope by default: every
 * RocksDb opened against a host shares one bytes-per-second budget so an idle
 * instance's headroom is available to a busy peer, and so aggregate compaction
 * I/O is bounded regardless of how many instances run. {@code RocksDbHost} is
 * the in-JVM realisation of that contract for tests — production code would
 * keep the same shape but add per-instance ceiling overrides and lifecycle.
 *
 * <p>This is intentionally tiny. It exists so the CP 24 (shared RateLimiter)
 * and CP 25 (shared BlockCache) integration tests can express "open N
 * instances against one limiter / one cache" in a single line, and so the
 * sibling agent writing the CP 25 cache test can re-use the same construct
 * without divergent helpers springing up.
 */
public final class RocksDbHost implements AutoCloseable {

    private final List<RocksDb> instances;
    private final RateLimiter sharedRateLimiter;
    private final BlockCache sharedBlockCache;

    private RocksDbHost(List<RocksDb> instances,
                        RateLimiter sharedRateLimiter,
                        BlockCache sharedBlockCache) {
        this.instances = instances;
        this.sharedRateLimiter = sharedRateLimiter;
        this.sharedBlockCache = sharedBlockCache;
    }

    /**
     * Open {@code count} {@link RocksDb} instances rooted at
     * {@code baseDir/inst-0/}, {@code baseDir/inst-1/}, ... each wired to the
     * same {@code sharedRateLimiter} and {@code sharedBlockCache}. Each
     * instance runs with a single compaction worker so the test can reason
     * about per-instance request counts without picker non-determinism
     * inflating the per-pass byte total.
     *
     * <p>If any instance fails to open, the partially-opened ones are closed
     * before the {@link IOException} is re-thrown so the test doesn't leak
     * file descriptors.
     */
    public static RocksDbHost openN(int count,
                                    Path baseDir,
                                    RateLimiter sharedRateLimiter,
                                    BlockCache sharedBlockCache) throws IOException {
        if (sharedBlockCache == null) {
            throw new NullPointerException("sharedBlockCache");
        }
        return openN(count, baseDir, sharedRateLimiter, i -> sharedBlockCache);
    }

    /**
     * Open {@code count} instances where each gets the cache returned by
     * {@code cacheFor.apply(i)}. Use this when the caller wants per-instance
     * caches (e.g. CP 24's rate-limiter tests, which deliberately isolate
     * the cache so each instance's file numbers — which restart from 1 on
     * every fresh DB — don't collide in a shared cache's keys).
     */
    public static RocksDbHost openN(int count,
                                    Path baseDir,
                                    RateLimiter sharedRateLimiter,
                                    IntFunction<BlockCache> cacheFor) throws IOException {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0, got " + count);
        }
        if (baseDir == null) {
            throw new NullPointerException("baseDir");
        }
        if (cacheFor == null) {
            throw new NullPointerException("cacheFor");
        }
        Files.createDirectories(baseDir);

        List<RocksDb> opened = new ArrayList<>(count);
        BlockCache cache0 = null;
        try {
            for (int i = 0; i < count; i++) {
                Path instDir = baseDir.resolve("inst-" + i);
                Files.createDirectories(instDir);
                BlockCache cache = cacheFor.apply(i);
                if (i == 0) cache0 = cache;
                // compactionThreads=1 so the test can pin the per-instance LOW
                // byte attribution at exactly the bytes the picker chose for
                // each runCompactionPass(1) call. The shared RateLimiter is
                // what fans concurrent compactions across instances.
                opened.add(RocksDb.open(instDir, true, cache, 1, sharedRateLimiter));
            }
        } catch (IOException | RuntimeException e) {
            for (RocksDb db : opened) {
                try {
                    db.close();
                } catch (RuntimeException suppress) {
                    e.addSuppressed(suppress);
                }
            }
            throw e;
        }
        return new RocksDbHost(opened, sharedRateLimiter, cache0);
    }

    /** Live, unmodifiable view of the underlying instances. */
    public List<RocksDb> instances() {
        return Collections.unmodifiableList(instances);
    }

    /** The instance at index {@code i}; convenience for tests. */
    public RocksDb instance(int i) {
        return instances.get(i);
    }

    /** Count of open instances on this host. */
    public int size() {
        return instances.size();
    }

    /** The RateLimiter every instance was opened against. May be {@code null}. */
    public RateLimiter sharedRateLimiter() {
        return sharedRateLimiter;
    }

    /**
     * The BlockCache instance 0 was opened against. When all instances were
     * opened against the same cache (the simple {@code openN(...,
     * BlockCache)} overload), this is the one shared cache. With the
     * per-instance {@code IntFunction<BlockCache>} overload, this is
     * instance 0's cache — callers wanting per-instance access should reach
     * through {@link #instance(int)} instead.
     */
    public BlockCache sharedBlockCache() {
        return sharedBlockCache;
    }

    /**
     * Close every instance, collecting suppressed exceptions so a failure on
     * one instance still attempts to close the rest. Closing an
     * already-closed host is a no-op (the underlying instance list is cleared
     * on the first call to {@code closeAll}).
     */
    public void closeAll() {
        RuntimeException first = null;
        for (RocksDb db : instances) {
            try {
                db.close();
            } catch (RuntimeException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        instances.clear();
        if (first != null) {
            throw new RuntimeException("RocksDbHost.closeAll failed", first);
        }
    }

    @Override
    public void close() {
        closeAll();
    }
}
