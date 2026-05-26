package com.hkg.rocksdb.ratelimiter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Classic token-bucket {@link RateLimiter} with two-priority preemption.
 *
 * <p>The bucket holds at most {@code burstBytes = bytesPerSecond *
 * refillIntervalMillis / 1000} tokens; tokens accumulate continuously at
 * {@code bytesPerSecond} (measured against {@link System#nanoTime()}, so wall
 * clock changes do not perturb the schedule). A {@link #request} call deducts
 * tokens in a loop: at each pass it consumes {@code min(available, remaining)}
 * tokens and, if more are still needed, waits on a {@link Condition} until the
 * next refill arrives. A single request larger than {@code burstBytes} is
 * therefore served as a sequence of chunked drains rather than rejected — the
 * caller blocks for as long as it takes the bucket to deliver them.
 *
 * <h2>Priority discipline</h2>
 *
 * <p>The limiter publishes the count of currently-waiting {@link Priority#HIGH}
 * callers via {@code highWaiting}. The rules:
 * <ul>
 *   <li>A {@code HIGH} caller takes tokens whenever any are available and
 *       wakes itself on each refill.
 *   <li>A {@code LOW} caller is allowed to acquire tokens only while
 *       {@code highWaiting == 0}. If a {@code HIGH} caller arrives while a
 *       {@code LOW} caller is mid-loop, the {@code LOW} caller stops draining
 *       on its next pass through the wait barrier — effectively refunding the
 *       remaining bandwidth — and re-queues behind the {@code HIGH} caller.
 * </ul>
 *
 * <p>This matches the discipline named in RocksDB §5.1 / FAST 2021: foreground
 * writes never wait behind compaction once they enter the limiter, while a
 * lightly-loaded host still lets compaction consume the full budget.
 *
 * <h2>Concurrency</h2>
 *
 * <p>One {@link ReentrantLock} guards {@code tokensAvailable},
 * {@code lastRefillNanos}, and {@code highWaiting}. Waiters block on the
 * shared {@link Condition} {@code tokensAvailableCondition}; every refill
 * broadcasts ({@code signalAll}) so any combination of {@code HIGH} and
 * {@code LOW} callers can re-evaluate fairly.
 */
public final class TokenBucketRateLimiter implements RateLimiter {

    private final long bytesPerSecond;
    private final long refillIntervalMillis;
    private final long burstBytes;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition tokensAvailableCondition = lock.newCondition();

    /** Currently available tokens (bytes). Capped at {@link #burstBytes}. */
    private long tokensAvailable;

    /** Monotonic-clock timestamp of the last refill. */
    private long lastRefillNanos;

    /** Number of {@link Priority#HIGH} callers currently blocked in {@link #request}. */
    private int highWaiting;

    /**
     * @param bytesPerSecond       sustained byte budget; must be {@code > 0}.
     * @param refillIntervalMillis nominal refill quantum in millis; sets {@code burstBytes}
     *                             to {@code bytesPerSecond * refillIntervalMillis / 1000}.
     *                             Must be {@code > 0}; typical value is {@code 100}.
     */
    public TokenBucketRateLimiter(long bytesPerSecond, long refillIntervalMillis) {
        if (bytesPerSecond <= 0L) {
            throw new IllegalArgumentException("bytesPerSecond must be > 0, got " + bytesPerSecond);
        }
        if (refillIntervalMillis <= 0L) {
            throw new IllegalArgumentException(
                    "refillIntervalMillis must be > 0, got " + refillIntervalMillis);
        }
        this.bytesPerSecond = bytesPerSecond;
        this.refillIntervalMillis = refillIntervalMillis;
        // Round up so very low bytesPerSecond * very small intervals still allow at
        // least one byte through per refill window.
        long burst = Math.max(1L, bytesPerSecond * refillIntervalMillis / 1000L);
        this.burstBytes = burst;
        // Start the bucket full so the first request does not block on a cold start.
        this.tokensAvailable = burst;
        this.lastRefillNanos = System.nanoTime();
    }

    @Override
    public void request(long bytes, Priority priority) throws InterruptedException {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be >= 0, got " + bytes);
        }
        if (bytes == 0L) {
            // Match leveldb / rocksdb: a zero-byte request is a no-op; do not
            // perturb the bucket or wake anyone.
            return;
        }
        if (priority == null) {
            throw new NullPointerException("priority");
        }

        long remaining = bytes;
        lock.lock();
        try {
            boolean countedHigh = false;
            try {
                if (priority == Priority.HIGH) {
                    highWaiting++;
                    countedHigh = true;
                    // Wake any LOW caller currently mid-sleep so it can re-check
                    // the priority gate and yield this iteration.
                    tokensAvailableCondition.signalAll();
                }
                while (remaining > 0L) {
                    refillIfDue();
                    // LOW priority must yield while any HIGH caller is waiting.
                    boolean mayDrain = (priority == Priority.HIGH) || (highWaiting == 0);
                    if (mayDrain && tokensAvailable > 0L) {
                        long take = Math.min(tokensAvailable, remaining);
                        tokensAvailable -= take;
                        remaining -= take;
                        if (remaining == 0L) {
                            // Wake peers in case our partial drain freed up nothing for
                            // them, but our exit re-arms the next refill window check.
                            tokensAvailableCondition.signalAll();
                            break;
                        }
                    }
                    awaitNextRefill();
                }
            } finally {
                if (countedHigh) {
                    highWaiting--;
                    // A departing HIGH caller may unblock LOW waiters who were
                    // gated only on highWaiting == 0.
                    tokensAvailableCondition.signalAll();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Sustained byte budget configured at construction. */
    public long bytesPerSecond() {
        return bytesPerSecond;
    }

    /** Refill quantum in millis. */
    public long refillIntervalMillis() {
        return refillIntervalMillis;
    }

    /** Maximum tokens the bucket can hold (i.e. burst capacity). */
    public long burstBytes() {
        return burstBytes;
    }

    /** Snapshot of currently-available tokens (after an on-demand refill). */
    public long tokensAvailable() {
        lock.lock();
        try {
            refillIfDue();
            return tokensAvailable;
        } finally {
            lock.unlock();
        }
    }

    // ----------------------------------------------------------------------------
    // Internals — caller MUST hold {@code lock}.
    // ----------------------------------------------------------------------------

    private void refillIfDue() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;
        if (elapsedNanos <= 0L) {
            return;
        }
        // tokens to add = bytesPerSecond * elapsed / 1e9; use the long form to
        // avoid floating-point drift across long-running processes.
        long added = bytesPerSecond * elapsedNanos / 1_000_000_000L;
        if (added <= 0L) {
            return;
        }
        long newTokens = tokensAvailable + added;
        if (newTokens > burstBytes || newTokens < 0L /* overflow guard */) {
            newTokens = burstBytes;
        }
        tokensAvailable = newTokens;
        // Advance lastRefillNanos by exactly the consumed elapsed slice so any
        // sub-byte remainder is not lost — important for very low byte rates.
        long consumedNanos = added * 1_000_000_000L / bytesPerSecond;
        lastRefillNanos += consumedNanos;
        tokensAvailableCondition.signalAll();
    }

    private void awaitNextRefill() throws InterruptedException {
        // Sleep just long enough that at least one more token should be ready.
        // We cap by refillIntervalMillis so HIGH arrivals also get a chance to
        // wake LOW callers even if no tokens accumulated.
        long sleepMillis = refillIntervalMillis;
        if (bytesPerSecond > 0L) {
            // Time to refill exactly one byte, in nanos; clamp to >= 1 ms.
            long oneByteNanos = 1_000_000_000L / bytesPerSecond;
            long oneByteMillis = Math.max(1L, oneByteNanos / 1_000_000L);
            sleepMillis = Math.min(sleepMillis, oneByteMillis);
            sleepMillis = Math.max(1L, sleepMillis);
        }
        tokensAvailableCondition.await(sleepMillis, TimeUnit.MILLISECONDS);
    }
}
