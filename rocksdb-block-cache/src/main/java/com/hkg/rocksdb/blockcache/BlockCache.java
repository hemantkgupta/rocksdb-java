package com.hkg.rocksdb.blockcache;

import java.io.IOException;
import java.util.Optional;

/**
 * Process-wide LRU cache for uncompressed SSTable blocks. The default
 * implementation is {@link LruBlockCache}, which bounds itself by total byte
 * size (not entry count). All SSTable readers in one {@code RocksDb} engine
 * share a single instance.
 */
public interface BlockCache {

    /** Returns the cached payload if present, else {@link Optional#empty()}. */
    Optional<byte[]> lookup(CacheKey key);

    /** Inserts a payload under {@code key}. May evict older entries to stay in budget. */
    void insert(CacheKey key, byte[] payload);

    /**
     * Cache-aware load: if {@code key} is present, return it; otherwise call
     * {@code loader}, insert the result, and return it. Implementations
     * should serialise the load only as much as their concurrency model
     * requires.
     */
    byte[] lookupOrLoad(CacheKey key, Loader loader) throws IOException;

    /** Current cache occupancy in bytes (sum of payload lengths). */
    long usageBytes();

    /** Maximum cache occupancy in bytes (compile-time configured). */
    long capacityBytes();

    /** Number of cached entries. Useful for tests/diagnostics. */
    int size();

    /** Loader callback for {@link #lookupOrLoad}. */
    @FunctionalInterface
    interface Loader {
        byte[] load() throws IOException;
    }
}
