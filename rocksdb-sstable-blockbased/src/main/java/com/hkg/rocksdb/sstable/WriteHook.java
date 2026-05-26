package com.hkg.rocksdb.sstable;

import java.io.IOException;

/**
 * Block-flush hook called by {@link BlockBasedTableWriter} immediately before
 * each on-disk block write. The single {@code int} argument is the number of
 * bytes (payload + 1-byte compression-type + 4-byte CRC trailer) about to flow
 * to the channel. Implementations MAY block — the writer thread is paused
 * until the call returns — so a rate limiter can pace SSTable I/O at
 * block granularity.
 *
 * <p>The {@link #NO_OP} hook does nothing; pass it (or null, via the
 * convenience overloads on {@link BlockBasedTableWriter#open}) for the
 * un-throttled path. The rate-limited path lives in the engine: a
 * {@link com.hkg.rocksdb.ratelimiter.RateLimiter} is wrapped as a hook and
 * threaded through {@link com.hkg.rocksdb.compaction.Compactor} so every
 * compaction worker's writes are paced.
 *
 * <p>Why a hook and not a direct {@code RateLimiter} dependency: the
 * sstable-blockbased module must not depend on rate-limiter (the SSTable
 * format has nothing to do with throttling, and we want to keep
 * rocksdb-sstable-blockbased reusable from tools / verification paths that
 * have no engine). The hook is the seam.
 */
@FunctionalInterface
public interface WriteHook {

    /** Called immediately before {@code bytes} are written to the channel. */
    void beforeBlockWrite(int bytes) throws IOException;

    /** Convenience no-op hook used when rate limiting is disabled. */
    WriteHook NO_OP = bytes -> { };
}
