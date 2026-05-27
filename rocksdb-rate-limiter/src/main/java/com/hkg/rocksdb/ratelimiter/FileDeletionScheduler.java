package com.hkg.rocksdb.ratelimiter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Background queue that drains SSTable deletions at a configured
 * {@code fileDeletionBytesPerSecond}, throttled through a shared
 * {@link RateLimiter} at {@link Priority#LOW}.
 *
 * <p>RocksDB §4 / FAST 2021 documents that the SSD's flash-translation-layer
 * journal is the hidden victim of bulk file deletion: when a giant compaction
 * obsoletes thousands of SSTables at once, the resulting TRIM storm forces the
 * FTL to rewrite its metadata journal, which competes with foreground reads
 * and writes for the same flash channels. The countermeasure is to spread the
 * TRIM activity over time. This scheduler accepts files immediately (so the
 * compaction can return) and serialises their actual {@code unlink()} through
 * the rate limiter, charging each file's logical size against the bucket.
 *
 * <h2>Operational model</h2>
 *
 * <ul>
 *   <li>{@link #enqueue(Path, long)} hands a file off to the queue and returns
 *       immediately. Multiple producers may enqueue concurrently.
 *   <li>A single daemon worker thread ({@code rocksdb-deletion-scheduler})
 *       drains the queue head-first. For each entry it calls
 *       {@code limiter.request(sizeBytes, Priority.LOW)} — blocking until the
 *       bucket grants the budget — then issues
 *       {@link Files#deleteIfExists(Path)}.
 *   <li>Enqueueing a path that no longer exists is a no-op: the underlying
 *       {@code deleteIfExists} returns {@code false} and the entry is dropped.
 *       This matches the §7.2 recovery path where a crashed engine may
 *       re-enqueue files at startup that were already unlinked.
 *   <li>I/O failures during deletion are swallowed (the file is logically gone
 *       from the {@code Version} either way) and the worker continues with the
 *       next entry — a single bad path must not poison the queue.
 * </ul>
 *
 * <h2>Lifecycle</h2>
 *
 * <p>{@link #start()} spins up the worker exactly once; subsequent calls are
 * no-ops. {@link #close()} signals shutdown, waits for the queue to drain, and
 * gives up after {@link #DRAIN_TIMEOUT_MILLIS} so a stuck filesystem cannot
 * hang JVM exit indefinitely.
 *
 * <h2>Concurrency</h2>
 *
 * <p>A single {@link ReentrantLock} guards the queue, pending byte counter,
 * and lifecycle flags. Producers signal the worker via a {@link Condition};
 * the worker signals waiters in {@link #close()} via the same condition when
 * the queue drains.
 */
public final class FileDeletionScheduler implements AutoCloseable {

    /** How long {@link #close()} will wait for the queue to drain before returning. */
    public static final long DRAIN_TIMEOUT_MILLIS = 30_000L;

    private static final String WORKER_THREAD_NAME = "rocksdb-deletion-scheduler";

    private final RateLimiter limiter;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();
    private final Condition queueDrained = lock.newCondition();

    /** Pending deletions; FIFO drained by the worker. */
    private final ArrayDeque<Entry> queue = new ArrayDeque<>();

    /** Sum of {@code sizeBytes} across queued entries — exposed via {@link #pendingBytes()}. */
    private long pendingBytes;

    /** Lifecycle: {@code true} once {@link #start()} has spawned the worker. */
    private boolean started;

    /** Lifecycle: {@code true} once {@link #close()} has been invoked. */
    private boolean closed;

    /** The worker thread; {@code null} until {@link #start()}. */
    private Thread worker;

    /** Cumulative count of successful deletions — diagnostics only. */
    private final AtomicLong deletedCount = new AtomicLong(0L);

    /**
     * @param limiter shared {@link RateLimiter} this scheduler will charge each
     *                deletion against at {@link Priority#LOW}. Must not be
     *                {@code null}. The scheduler does not own the limiter and
     *                will not close it.
     */
    public FileDeletionScheduler(RateLimiter limiter) {
        this.limiter = Objects.requireNonNull(limiter, "limiter");
    }

    /**
     * Spawns the daemon worker thread. Subsequent calls are no-ops. Calling
     * after {@link #close()} throws {@link IllegalStateException} — a closed
     * scheduler is not restartable.
     */
    public void start() {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("scheduler already closed");
            }
            if (started) {
                return;
            }
            started = true;
            worker = new Thread(this::runWorkerLoop, WORKER_THREAD_NAME);
            worker.setDaemon(true);
            worker.start();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Append a file to the deletion queue. Returns immediately; the actual
     * {@code unlink()} happens on the worker thread after the rate limiter
     * grants {@code sizeBytes}.
     *
     * <p>Enqueueing after {@link #close()} throws {@link IllegalStateException}.
     * Enqueueing a non-existent path is safe (the worker calls
     * {@link Files#deleteIfExists(Path)}); enqueueing the same path twice is
     * safe (the second deletion is a no-op).
     *
     * @param file       path to delete; must not be {@code null}.
     * @param sizeBytes  logical size charged to the rate limiter; must be
     *                   {@code >= 0}. Use the on-disk size at enqueue time —
     *                   the limiter does not stat the file.
     */
    public void enqueue(Path file, long sizeBytes) {
        Objects.requireNonNull(file, "file");
        if (sizeBytes < 0L) {
            throw new IllegalArgumentException("sizeBytes must be >= 0, got " + sizeBytes);
        }
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("scheduler is closed");
            }
            queue.addLast(new Entry(file, sizeBytes));
            pendingBytes += sizeBytes;
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Number of files currently queued for deletion. */
    public int pendingCount() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Sum of {@code sizeBytes} across queued entries (not including any in-flight deletion). */
    public long pendingBytes() {
        lock.lock();
        try {
            return pendingBytes;
        } finally {
            lock.unlock();
        }
    }

    /** Cumulative count of completed deletion attempts (diagnostics). */
    public long deletedCount() {
        return deletedCount.get();
    }

    /**
     * Signal shutdown and block until the queue is fully drained, the worker
     * exits, or {@link #DRAIN_TIMEOUT_MILLIS} elapses — whichever comes first.
     * Idempotent. Safe to invoke from any thread other than the worker itself.
     *
     * <p>If the timeout elapses with the worker still mid-deletion, the worker
     * is interrupted; we do not {@code join} forever because a stuck
     * filesystem call must not hang JVM exit.
     */
    @Override
    public void close() {
        Thread workerSnapshot;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DRAIN_TIMEOUT_MILLIS);
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            // Wake the worker so it observes `closed` and exits after drain.
            workAvailable.signalAll();
            workerSnapshot = worker;
            if (workerSnapshot == null) {
                // Never started — nothing to drain.
                return;
            }
            // Wait for queue to drain, but don't deadlock waiting forever.
            while (!queue.isEmpty()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    break;
                }
                try {
                    queueDrained.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            lock.unlock();
        }

        if (workerSnapshot == null) {
            return;
        }
        // Give the worker a chance to exit cleanly post-drain.
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
                Math.max(0L, deadlineNanos - System.nanoTime()));
        if (remainingMillis > 0L) {
            try {
                workerSnapshot.join(remainingMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (workerSnapshot.isAlive()) {
            // Last resort: interrupt a stuck filesystem call so JVM exit is not blocked.
            workerSnapshot.interrupt();
        }
    }

    // ----------------------------------------------------------------------------
    // Worker loop
    // ----------------------------------------------------------------------------

    private void runWorkerLoop() {
        while (true) {
            Entry next;
            lock.lock();
            try {
                while (queue.isEmpty() && !closed) {
                    try {
                        workAvailable.await();
                    } catch (InterruptedException e) {
                        // Re-assert interrupt and exit promptly.
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (queue.isEmpty()) {
                    // closed && empty -> we're done.
                    queueDrained.signalAll();
                    return;
                }
                // PEEK rather than poll — keep the entry visible in pendingCount /
                // pendingBytes until the actual on-disk delete completes. Otherwise
                // the test (and any caller polling pending=0) sees the queue go empty
                // and may observe the file still on disk for the brief window between
                // signal-and-delete.
                next = queue.peekFirst();
            } finally {
                lock.unlock();
            }

            // Charge the rate limiter OUTSIDE the lock: limiter.request() may
            // block for seconds at low byte budgets, and producers must be
            // able to enqueue while we wait.
            try {
                limiter.request(next.sizeBytes, Priority.LOW);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            try {
                Files.deleteIfExists(next.file);
            } catch (IOException ignored) {
                // The file is already absent from the live Version; an IO
                // error here means orphaned bytes on disk, not a correctness
                // failure. A future operator-tool sweep can reap it.
            } catch (RuntimeException ignored) {
                // Same disposition — never let one bad path stop the worker.
            } finally {
                deletedCount.incrementAndGet();
            }

            // Now that the on-disk delete is durable, remove the entry from the queue
            // and update the byte counter. Callers polling pendingCount / pendingBytes
            // see the decrement only after the file is gone.
            lock.lock();
            try {
                Entry removed = queue.pollFirst();
                // Defensive: the head should still be `next`, but in case of
                // unexpected interleaving (e.g. an enqueue happens to land at the head
                // somehow), only subtract what we actually deleted.
                long charge = (removed != null) ? removed.sizeBytes : next.sizeBytes;
                pendingBytes -= charge;
                if (queue.isEmpty()) {
                    queueDrained.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /** Immutable queue entry. */
    private static final class Entry {
        final Path file;
        final long sizeBytes;

        Entry(Path file, long sizeBytes) {
            this.file = file;
            this.sizeBytes = sizeBytes;
        }
    }
}
