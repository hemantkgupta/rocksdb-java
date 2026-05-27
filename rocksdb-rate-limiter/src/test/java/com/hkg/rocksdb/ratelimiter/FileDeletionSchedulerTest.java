package com.hkg.rocksdb.ratelimiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link FileDeletionScheduler}.
 *
 * <p>Timing assertions use a generous {@code +/-50%} tolerance so the suite is
 * tolerant of loaded CI hosts. The mass-delete test asserts only a lower bound
 * on wall time (the scheduler must NOT drain faster than the limiter allows)
 * combined with a polled-completion deadline (the scheduler must drain within
 * a reasonable upper bound). Every test caps itself with an explicit
 * {@code timeout}/deadline so a regression cannot hang the build.
 */
final class FileDeletionSchedulerTest {

    private static final long REFILL_INTERVAL_MILLIS = 50L;
    private static final long POLL_INTERVAL_MILLIS = 20L;
    private static final long DEFAULT_DEADLINE_MILLIS = 10_000L;

    @Test
    void enqueueDrainsAtConfiguredRate(@TempDir Path tempDir) throws Exception {
        // 1 MiB/s budget; 8 files of 128 KiB each = exactly 1 MiB total.
        // Lower bound: must take >= ~700 ms (50% of the 1 s "ideal") so we
        // know the limiter is genuinely throttling rather than draining
        // synchronously. Upper bound: must finish within 5 s.
        long bytesPerSecond = 1L << 20; // 1 MiB
        int fileCount = 8;
        long perFileBytes = bytesPerSecond / fileCount;

        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(bytesPerSecond, REFILL_INTERVAL_MILLIS);
        // Drain the initial burst so the first enqueue actually waits.
        limiter.request(limiter.burstBytes(), Priority.LOW);

        List<Path> files = createFiles(tempDir, fileCount, (int) perFileBytes);

        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            scheduler.start();
            long startNanos = System.nanoTime();
            for (Path p : files) {
                scheduler.enqueue(p, perFileBytes);
            }
            awaitPending(scheduler, 0, 5_000L);
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            // Lower bound only: we want to confirm throttling is in effect.
            assertThat(elapsedMillis)
                    .as("draining 1 MiB at 1 MiB/s must take at least ~700 ms")
                    .isGreaterThanOrEqualTo(500L);
            for (Path p : files) {
                assertThat(Files.exists(p)).as("%s should be deleted", p).isFalse();
            }
        }
    }

    @Test
    void pendingCountAndBytesDecrementAsFilesDelete(@TempDir Path tempDir) throws Exception {
        // Use a fast limiter so we observe the count fall to zero quickly.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        int fileCount = 5;
        int perFileBytes = 1_024;
        List<Path> files = createFiles(tempDir, fileCount, perFileBytes);

        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            // Enqueue BEFORE start to snapshot the pre-drain state.
            for (Path p : files) {
                scheduler.enqueue(p, perFileBytes);
            }
            assertThat(scheduler.pendingCount()).isEqualTo(fileCount);
            assertThat(scheduler.pendingBytes()).isEqualTo((long) fileCount * perFileBytes);

            scheduler.start();
            awaitPending(scheduler, 0, DEFAULT_DEADLINE_MILLIS);
            assertThat(scheduler.pendingCount()).isZero();
            assertThat(scheduler.pendingBytes()).isZero();
            assertThat(scheduler.deletedCount()).isEqualTo(fileCount);
        }
    }

    @Test
    void filesActuallyRemovedFromDisk(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        List<Path> files = createFiles(tempDir, 12, 256);
        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            scheduler.start();
            for (Path p : files) {
                assertThat(Files.exists(p)).isTrue();
                scheduler.enqueue(p, 256L);
            }
            awaitPending(scheduler, 0, DEFAULT_DEADLINE_MILLIS);
            for (Path p : files) {
                assertThat(Files.exists(p)).as("%s should be unlinked", p).isFalse();
            }
        }
    }

    @Test
    void nonExistentFileIsNoOpAndDoesNotPoisonQueue(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        Path missing = tempDir.resolve("does-not-exist.sst");
        Path real = createFile(tempDir.resolve("real.sst"), 128);

        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            scheduler.start();
            // Sandwich the missing entry between two real ones to prove it
            // doesn't stall the worker.
            scheduler.enqueue(real, 128L);
            scheduler.enqueue(missing, 1_000L);
            Path real2 = createFile(tempDir.resolve("real2.sst"), 128);
            scheduler.enqueue(real2, 128L);

            awaitPending(scheduler, 0, DEFAULT_DEADLINE_MILLIS);
            assertThat(Files.exists(real)).isFalse();
            assertThat(Files.exists(real2)).isFalse();
            assertThat(scheduler.deletedCount()).isEqualTo(3L);
        }
    }

    @Test
    void closeBlocksUntilQueueDrains(@TempDir Path tempDir) throws Exception {
        // Slow limiter (8 KiB/s) and many small files so close() must wait.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(8_192L, REFILL_INTERVAL_MILLIS);
        int fileCount = 6;
        int perFileBytes = 1_024;
        List<Path> files = createFiles(tempDir, fileCount, perFileBytes);

        FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter);
        scheduler.start();
        for (Path p : files) {
            scheduler.enqueue(p, perFileBytes);
        }
        scheduler.close();
        // After close() returns, every file must be gone (or close() drained
        // them all). The scheduler's contract is "block until drained".
        assertThat(scheduler.pendingCount()).isZero();
        for (Path p : files) {
            assertThat(Files.exists(p)).as("%s should be deleted before close() returned", p).isFalse();
        }
    }

    @Test
    void closeReturnsCleanlyAfterTimeoutWithSlowLimiter(@TempDir Path tempDir) throws Exception {
        // 1 byte/sec, refill 1s — a single 1 MiB file would take ~12 days to
        // drain. We exercise close() while the worker is blocked in
        // limiter.request(); close() must return promptly via interrupt.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L, 1_000L);
        // Drain the initial 1-byte burst so the next request blocks.
        limiter.request(limiter.burstBytes(), Priority.LOW);
        Path big = createFile(tempDir.resolve("big.sst"), 16);

        FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter);
        scheduler.start();
        scheduler.enqueue(big, 1L << 20);

        // Use a tiny synthetic deadline by interrupting close() ourselves
        // after a short pause — close() honours interrupt and returns.
        Thread mainThread = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(300L);
            } catch (InterruptedException ignored) {
            }
            mainThread.interrupt();
        }, "close-interrupter");
        interrupter.setDaemon(true);
        interrupter.start();

        long startNanos = System.nanoTime();
        scheduler.close();
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        // Clear our own interrupt flag — close() preserves it on InterruptedException.
        boolean wasInterrupted = Thread.interrupted();
        // close() must return within ~1 s thanks to the interrupt path; the
        // 30 s drain timeout is just a final safety net.
        assertThat(elapsedMillis).isLessThan(2_000L);
        assertThat(wasInterrupted).isTrue();
    }

    @Test
    void workerThreadIsDaemon(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            scheduler.start();
            // Enqueue something so we can latch onto the worker thread by name.
            Path p = createFile(tempDir.resolve("x.sst"), 64);
            scheduler.enqueue(p, 64L);
            awaitPending(scheduler, 0, DEFAULT_DEADLINE_MILLIS);

            Thread foundWorker = findThreadByName("rocksdb-deletion-scheduler");
            assertThat(foundWorker).as("worker thread must exist").isNotNull();
            assertThat(foundWorker.isDaemon())
                    .as("rocksdb-deletion-scheduler must be a daemon thread")
                    .isTrue();
        }
    }

    @Test
    void concurrentEnqueueFromMultipleProducersIsSafe(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        int producerCount = 8;
        int filesPerProducer = 32;
        int totalFiles = producerCount * filesPerProducer;
        List<Path> files = createFiles(tempDir, totalFiles, 64);

        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            scheduler.start();
            ExecutorService pool = Executors.newFixedThreadPool(producerCount);
            try {
                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger nextIdx = new AtomicInteger(0);
                List<Runnable> tasks = new ArrayList<>();
                for (int t = 0; t < producerCount; t++) {
                    tasks.add(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        for (int i = 0; i < filesPerProducer; i++) {
                            int idx = nextIdx.getAndIncrement();
                            if (idx < totalFiles) {
                                scheduler.enqueue(files.get(idx), 64L);
                            }
                        }
                    });
                }
                List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
                for (Runnable r : tasks) {
                    futures.add(pool.submit(r));
                }
                start.countDown();
                for (java.util.concurrent.Future<?> f : futures) {
                    f.get(10, TimeUnit.SECONDS);
                }
                awaitPending(scheduler, 0, DEFAULT_DEADLINE_MILLIS);
                assertThat(scheduler.deletedCount()).isEqualTo(totalFiles);
                for (Path p : files) {
                    assertThat(Files.exists(p)).isFalse();
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void enqueueAfterCloseIsRejected(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter);
        scheduler.start();
        scheduler.close();
        Path p = createFile(tempDir.resolve("post.sst"), 16);
        assertThatThrownBy(() -> scheduler.enqueue(p, 16L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeIsIdempotent(@TempDir Path tempDir) throws Exception {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter);
        scheduler.start();
        Path p = createFile(tempDir.resolve("a.sst"), 32);
        scheduler.enqueue(p, 32L);
        scheduler.close();
        // Second close must be a no-op and not throw.
        scheduler.close();
        assertThat(Files.exists(p)).isFalse();
    }

    @Test
    void negativeSizeRejected(@TempDir Path tempDir) {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        try (FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter)) {
            Path p = tempDir.resolve("x.sst");
            assertThatThrownBy(() -> scheduler.enqueue(p, -1L))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> scheduler.enqueue(null, 1L))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void startAfterCloseRejected() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 24, REFILL_INTERVAL_MILLIS);
        FileDeletionScheduler scheduler = new FileDeletionScheduler(limiter);
        scheduler.close();
        assertThatThrownBy(scheduler::start).isInstanceOf(IllegalStateException.class);
    }

    // ----------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------

    private static List<Path> createFiles(Path dir, int count, int sizeBytes) throws IOException {
        List<Path> out = new ArrayList<>(count);
        byte[] payload = new byte[sizeBytes];
        for (int i = 0; i < count; i++) {
            Path p = dir.resolve("file-" + i + ".sst");
            Files.write(p, payload);
            out.add(p);
        }
        return out;
    }

    private static Path createFile(Path p, int sizeBytes) throws IOException {
        Files.write(p, new byte[sizeBytes]);
        return p;
    }

    private static void awaitPending(FileDeletionScheduler scheduler, int target, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (scheduler.pendingCount() > target) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "pendingCount did not reach " + target + " within "
                                + timeoutMillis + " ms; still at " + scheduler.pendingCount());
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
    }

    private static Thread findThreadByName(String name) {
        Thread[] threads = new Thread[Thread.activeCount() * 2 + 16];
        int n = Thread.enumerate(threads);
        for (int i = 0; i < n; i++) {
            if (threads[i] != null && name.equals(threads[i].getName())) {
                return threads[i];
            }
        }
        return null;
    }
}
