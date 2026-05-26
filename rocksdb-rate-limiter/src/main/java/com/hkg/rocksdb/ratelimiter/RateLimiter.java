package com.hkg.rocksdb.ratelimiter;

/**
 * Process-wide byte-budget rate limiter. Implementations enforce a configured
 * {@code bytesPerSecond} ceiling across all callers that share one instance.
 *
 * <p>RocksDB uses a single {@code RateLimiter} per host (see FAST 2021 §5.1):
 * compaction workers across all open databases and column families call
 * {@link #request} with {@link Priority#LOW} before issuing the disk write;
 * foreground WAL flushes and bulk loaders call with {@link Priority#HIGH}. The
 * limiter blocks the caller until the requested bytes are available, taking
 * fewer tokens per loop iteration where necessary so a single huge request
 * cannot starve the bucket.
 *
 * <p>Implementations MUST honour the priority preemption contract: while a
 * {@link Priority#HIGH} call is waiting, any concurrent {@link Priority#LOW}
 * call should refund its reservation and re-queue so the HIGH caller acquires
 * tokens first.
 */
public interface RateLimiter {

    /**
     * Block until {@code bytes} tokens have been deducted from the bucket under
     * the priority discipline. A request of zero bytes returns immediately
     * without acquiring any lock-protected state.
     *
     * @param bytes    number of bytes the caller intends to write; must be {@code >= 0}.
     * @param priority {@link Priority#HIGH} preempts queued {@link Priority#LOW} waiters.
     * @throws InterruptedException if the calling thread is interrupted while waiting.
     */
    void request(long bytes, Priority priority) throws InterruptedException;
}
