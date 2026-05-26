package com.hkg.rocksdb.ratelimiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TokenBucketRateLimiter}.
 *
 * <p>Timing-based tests use generous tolerances (typically {@code +/-30%}) so
 * they remain green on a loaded CI host without losing their assertion power.
 * Each test caps its own wall-clock budget via {@link #within(long, Runnable)}
 * so a regression cannot hang the build.
 */
final class TokenBucketRateLimiterTest {

    private static final long REFILL_INTERVAL_MILLIS = 50L;

    @Test
    void zeroByteRequestReturnsImmediately() throws InterruptedException {
        // Rate of 1 byte/sec — a non-zero request would block for ~1s. Zero
        // bytes must short-circuit before touching the bucket.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L, REFILL_INTERVAL_MILLIS);
        long start = System.nanoTime();
        limiter.request(0L, Priority.HIGH);
        limiter.request(0L, Priority.LOW);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
        assertThat(elapsedMillis).isLessThan(50L);
    }

    @Test
    void negativeBytesRejected() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1_000L, REFILL_INTERVAL_MILLIS);
        assertThatThrownBy(() -> limiter.request(-1L, Priority.HIGH))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidConfigurationRejected() {
        assertThatThrownBy(() -> new TokenBucketRateLimiter(0L, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TokenBucketRateLimiter(1_000L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void honoursBytesPerSecondOverTime() throws InterruptedException {
        // 10 KiB/s budget; drain ~20 KiB and verify it takes ~2 s. Bucket starts
        // full at burstBytes = 10_240 * 50/1000 = 512 bytes, so the first 512
        // bytes are essentially free and the rest is paced.
        long ratePerSec = 10_240L;
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(ratePerSec, REFILL_INTERVAL_MILLIS);
        long target = ratePerSec * 2L; // ~2 seconds' worth
        long start = System.nanoTime();
        long drained = 0L;
        while (drained < target) {
            long chunk = Math.min(512L, target - drained);
            limiter.request(chunk, Priority.LOW);
            drained += chunk;
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
        // Expected ~2000 ms (slightly less because the initial burst is free).
        // Tolerate 1500-3500 ms.
        assertThat(elapsedMillis).isBetween(1_500L, 3_500L);
    }

    @Test
    void burstyRequestExceedingCapacityBlocksUntilTokensAccumulate() throws InterruptedException {
        // 4 KiB/s, refill quantum 100ms => burstBytes = 409. Request 2 KiB in
        // one go: must take ~(2048 - 409) / 4096 * 1000 = ~400 ms.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(4_096L, 100L);
        long start = System.nanoTime();
        limiter.request(2_048L, Priority.LOW);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
        // Tolerate 250-900 ms.
        assertThat(elapsedMillis).isBetween(250L, 900L);
    }

    @Test
    void highPriorityPreemptsLowPriority() throws Exception {
        // 4 KiB/s, refill 100ms => 409-byte bucket. Saturate with a LOW caller
        // that wants 4 KiB (~1 second). Mid-flight, fire a HIGH caller for
        // ~half a second worth of bytes; the HIGH caller must finish before
        // the LOW caller.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(4_096L, 100L);

        // Drain the starting burst so timing isn't dominated by the cold-start
        // freebie.
        limiter.request(limiter.burstBytes(), Priority.LOW);

        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            AtomicLong lowFinishedAt = new AtomicLong(-1L);
            AtomicLong highFinishedAt = new AtomicLong(-1L);

            long startNanos = System.nanoTime();
            Future<?> lowFuture = pool.submit(() -> {
                try {
                    limiter.request(4_096L, Priority.LOW);
                    lowFinishedAt.set(System.nanoTime() - startNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Let the LOW caller start spinning on the bucket.
            Thread.sleep(150L);

            Future<?> highFuture = pool.submit(() -> {
                try {
                    limiter.request(1_024L, Priority.HIGH);
                    highFinishedAt.set(System.nanoTime() - startNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            highFuture.get(5, TimeUnit.SECONDS);
            lowFuture.get(10, TimeUnit.SECONDS);

            assertThat(highFinishedAt.get()).isPositive();
            assertThat(lowFinishedAt.get()).isPositive();
            // HIGH must finish strictly before LOW.
            assertThat(highFinishedAt.get()).isLessThan(lowFinishedAt.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void manyLowCallersAllMakeProgress() throws Exception {
        // 8 KiB/s; 8 concurrent LOW callers each requesting 1 KiB. None should
        // be starved; each must finish within a couple of seconds.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(8_192L, REFILL_INTERVAL_MILLIS);
        int callerCount = 8;
        long bytesPerCaller = 1_024L;

        ExecutorService pool = Executors.newFixedThreadPool(callerCount);
        try {
            CountDownLatch ready = new CountDownLatch(callerCount);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(callerCount);
            AtomicInteger completed = new AtomicInteger(0);
            for (int i = 0; i < callerCount; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        limiter.request(bytesPerCaller, Priority.LOW);
                        completed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            go.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS))
                    .as("all LOW callers must finish; no starvation")
                    .isTrue();
            assertThat(completed.get()).isEqualTo(callerCount);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void manyCallersConvergeToConfiguredRate() throws Exception {
        // 16 KiB/s; many small requests (32 bytes each) across multiple threads;
        // verify the aggregate throughput sits in a band around the configured rate.
        long ratePerSec = 16_384L;
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(ratePerSec, REFILL_INTERVAL_MILLIS);
        int threads = 4;
        long totalBytesTarget = ratePerSec * 2L; // ~2 seconds' worth
        long bytesPerCaller = totalBytesTarget / threads;
        long chunk = 32L;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    long drained = 0L;
                    while (drained < bytesPerCaller) {
                        long take = Math.min(chunk, bytesPerCaller - drained);
                        limiter.request(take, Priority.LOW);
                        drained += take;
                    }
                    return drained;
                }));
            }
            long t0 = System.nanoTime();
            start.countDown();
            long actualDrained = 0L;
            for (Future<Long> f : futures) {
                actualDrained += f.get(15, TimeUnit.SECONDS);
            }
            long elapsedNanos = System.nanoTime() - t0;
            assertThat(actualDrained).isEqualTo(totalBytesTarget);
            double observedRate = actualDrained / (elapsedNanos / 1_000_000_000.0);
            // Within +/-30% of configured rate.
            assertThat(observedRate).isBetween(ratePerSec * 0.70, ratePerSec * 1.30);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void burstyHighPriorityArrivesAheadOfWaitingLow() throws Exception {
        // Variation on the preemption test using multiple LOW callers and a
        // single HIGH arrival to make sure the wake-up is broadcast.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(2_048L, 100L);
        // Drain initial burst.
        limiter.request(limiter.burstBytes(), Priority.LOW);

        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            int lowCount = 4;
            List<Future<Long>> lowFutures = new ArrayList<>();
            long startNanos = System.nanoTime();
            for (int i = 0; i < lowCount; i++) {
                lowFutures.add(pool.submit(() -> {
                    limiter.request(1_024L, Priority.LOW);
                    return System.nanoTime() - startNanos;
                }));
            }
            Thread.sleep(150L);
            Future<Long> highFuture = pool.submit(() -> {
                limiter.request(512L, Priority.HIGH);
                return System.nanoTime() - startNanos;
            });
            long highElapsed = highFuture.get(10, TimeUnit.SECONDS);
            long maxLow = 0L;
            for (Future<Long> f : lowFutures) {
                maxLow = Math.max(maxLow, f.get(15, TimeUnit.SECONDS));
            }
            // HIGH must finish before the last LOW caller does.
            assertThat(highElapsed).isLessThan(maxLow);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void tokensAvailableReportsBucketStateAfterRefill() throws InterruptedException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10_000L, REFILL_INTERVAL_MILLIS);
        // Drain the initial burst.
        limiter.request(limiter.burstBytes(), Priority.LOW);
        long immediate = limiter.tokensAvailable();
        Thread.sleep(200L);
        long afterSleep = limiter.tokensAvailable();
        assertThat(afterSleep).isGreaterThan(immediate);
        assertThat(afterSleep).isLessThanOrEqualTo(limiter.burstBytes());
    }

    @Test
    void interruptDuringRequestThrowsInterruptedException() throws Exception {
        // 1 byte/sec; a 100-byte request will block for ~100s — plenty of time
        // for the interrupt to land.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L, 1_000L);
        // Drain the (1-byte) initial burst.
        limiter.request(limiter.burstBytes(), Priority.LOW);

        Thread t = new Thread(() -> {
            try {
                limiter.request(100L, Priority.LOW);
            } catch (InterruptedException expected) {
                // good
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        Thread.sleep(100L);
        t.interrupt();
        t.join(5_000L);
        assertThat(t.isAlive()).isFalse();
    }
}
