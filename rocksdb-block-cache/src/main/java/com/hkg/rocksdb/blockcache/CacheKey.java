package com.hkg.rocksdb.blockcache;

import com.hkg.rocksdb.common.FileNumber;

import java.util.Objects;

/**
 * Identifies a single SSTable block in the block cache.
 *
 * <p>The triple {@code (dbId, file, blockOffset)} is what makes
 * cross-instance cache sharing safe (CP 25): each {@link
 * com.hkg.rocksdb.engine.RocksDb} mints its own DB-scoped id from its
 * {@code dbDir} path at open time, so two engines whose fresh file
 * numbers both start at 1 don't collide in a shared cache.
 *
 * <p>{@code dbId} is nullable for backward compat with pre-CP-25 callers
 * (single-instance engines that don't need namespacing); a null id
 * compares equal only to another null. Use a non-null id whenever the
 * cache might be shared across multiple engine instances.
 */
public record CacheKey(String dbId, FileNumber file, long blockOffset) {

    public CacheKey {
        Objects.requireNonNull(file, "file");
        if (blockOffset < 0L) {
            throw new IllegalArgumentException("blockOffset must be non-negative, got " + blockOffset);
        }
    }

    /** Backward-compat ctor for single-instance engines (no dbId namespacing). */
    public CacheKey(FileNumber file, long blockOffset) {
        this(null, file, blockOffset);
    }
}
