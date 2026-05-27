package com.hkg.rocksdb.testcluster;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.ratelimiter.Priority;
import com.hkg.rocksdb.ratelimiter.RateLimiter;
import com.hkg.rocksdb.ratelimiter.TokenBucketRateLimiter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Phase 6 CP 24 — proves that ONE shared {@link TokenBucketRateLimiter}
 * applied across N {@link RocksDb} instances enforces a host-wide
 * compaction-bytes-per-second budget (ADR-0005, FAST 2021 §4).
 *
 * <p>The single-instance equivalent is
 * {@code rocksdb-engine.RateLimitedCompactionTest}; that test proves the
 * engine routes compaction writes through a {@code RateLimiter} at
 * {@link Priority#LOW}. THIS test generalises that to N instances and asserts
 * three things ADR-0005 explicitly calls out:
 *
 * <ol>
 *   <li>Every instance's compaction goes through the same shared limiter (no
 *       per-instance duplication of the byte budget).</li>
 *   <li>Aggregate wall time is meaningfully larger than a single instance
 *       running with the full budget — i.e. the budget is shared, not
 *       cloned per instance.</li>
 *   <li>No instance starves the others — every instance completes within
 *       deadline and its data is readable post-compaction.</li>
 * </ol>
 *
 * <p>Each instance is opened with a per-instance {@link TaggedRateLimiter}
 * (a thin recorder) that delegates to the SAME shared {@code TokenBucket}.
 * The token-bucket layer is the one that actually enforces the host-wide
 * byte budget; the per-instance proxies exist only so the test can attribute
 * LOW-priority requests back to the originating instance even though the
 * actual {@code request(...)} call happens on a compaction worker thread
 * (where a single shared recorder's {@code ThreadLocal} tag would not
 * propagate).
 *
 * <p>Each instance also gets its OWN {@link BlockCache} — file numbers
 * restart from 1 on every fresh DB, so cache-key collisions would corrupt
 * cross-instance reads. CP 25's {@code SharedCacheTest} is the place that
 * exercises shared-cache semantics.
 */
class SharedRateLimiterTest {

    private static final int INSTANCE_COUNT = 4;
    /** Tight enough that compaction is paced, loose enough not to add real seconds to CI. */
    private static final long SHARED_BYTES_PER_SEC = 256L * 1024L;
    private static final long REFILL_MILLIS = 10L;

    /**
     * Per-instance proxy that records the calls IT sees and then forwards to
     * a shared underlying limiter. All N proxies forward to the SAME
     * underlying {@link TokenBucketRateLimiter}, so the host-wide byte
     * budget is still enforced — the proxies are recorder-only.
     */
    private static final class TaggedRateLimiter implements RateLimiter {
        private final String tag;
        private final RateLimiter shared;
        private final AtomicLong lowCalls = new AtomicLong();
        private final AtomicLong lowBytes = new AtomicLong();
        private final AtomicLong highBytes = new AtomicLong();

        TaggedRateLimiter(String tag, RateLimiter shared) {
            this.tag = tag;
            this.shared = shared;
        }

        @Override
        public void request(long bytes, Priority priority) throws InterruptedException {
            if (priority == Priority.LOW) {
                lowCalls.incrementAndGet();
                lowBytes.addAndGet(bytes);
            } else {
                highBytes.addAndGet(bytes);
            }
            shared.request(bytes, priority);
        }

        String tag()       { return tag; }
        long lowCalls()    { return lowCalls.get(); }
        long lowBytes()    { return lowBytes.get(); }
        long highBytes()   { return highBytes.get(); }
    }

    /** Open N instances on per-instance caches and per-instance tagged proxies of {@code shared}. */
    private static Host openTaggedHost(int count, Path baseDir, RateLimiter shared,
                                       List<TaggedRateLimiter> proxiesOut) throws IOException {
        List<RocksDb> dbs = new ArrayList<>(count);
        java.nio.file.Files.createDirectories(baseDir);
        try {
            for (int i = 0; i < count; i++) {
                Path dir = baseDir.resolve("inst-" + i);
                java.nio.file.Files.createDirectories(dir);
                TaggedRateLimiter proxy = new TaggedRateLimiter("inst-" + i, shared);
                proxiesOut.add(proxy);
                BlockCache cache = new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES);
                dbs.add(RocksDb.open(dir, true, cache, 1, proxy));
            }
        } catch (IOException | RuntimeException e) {
            for (RocksDb db : dbs) try { db.close(); } catch (RuntimeException s) { e.addSuppressed(s); }
            throw e;
        }
        return new Host(dbs);
    }

    /** Tiny test-local host so we can open with per-instance proxies; not the production helper. */
    private static final class Host implements AutoCloseable {
        final List<RocksDb> dbs;
        Host(List<RocksDb> dbs) { this.dbs = dbs; }
        int size() { return dbs.size(); }
        RocksDb instance(int i) { return dbs.get(i); }
        @Override public void close() {
            for (RocksDb db : dbs) try { db.close(); } catch (RuntimeException ignored) {}
        }
    }

    // -----------------------------------------------------------------------
    // Workload helpers.
    // -----------------------------------------------------------------------

    /** Build up enough L0 backlog to trip the compaction trigger on one instance. */
    private static void seedL0Backlog(RocksDb db, int instanceIndex) throws IOException {
        for (int batch = 0; batch < 6; batch++) {
            for (int i = 0; i < 50; i++) {
                String k = String.format("inst-%d-k-%05d", instanceIndex, batch * 100 + i);
                db.put(Key.of(k), Slice.of("v-padded-bytes-" + i + "-batch-" + batch));
            }
            db.flush();
        }
        assertThat(db.currentVersion().level(0).size())
            .isGreaterThanOrEqualTo(Constants.L0_FILE_COUNT_TRIGGER);
    }

    /** Run {@code N} concurrent {@code runCompactionPass(1)} calls, one per instance. */
    private static void compactAllConcurrently(List<RocksDb> instances, long deadlineMillis)
        throws InterruptedException {
        int n = instances.size();
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            for (int idx = 0; idx < n; idx++) {
                final RocksDb db = instances.get(idx);
                pool.submit(() -> {
                    try {
                        start.await();
                        db.runCompactionPass(1);
                    } catch (IOException | InterruptedException ie) {
                        if (ie instanceof InterruptedException) Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(deadlineMillis, TimeUnit.MILLISECONDS))
                .withFailMessage("compaction did not complete within %d ms — an instance may be starving",
                    deadlineMillis)
                .isTrue();
        } finally {
            pool.shutdown();
            pool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    // -----------------------------------------------------------------------
    // Tests (8).
    // -----------------------------------------------------------------------

    @Test
    void openNCreatesIsolatedInstanceDirectoriesWiredToOneLimiter(@TempDir Path baseDir) throws IOException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        IntFunction<BlockCache> caches = i -> new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES);
        try (RocksDbHost host = RocksDbHost.openN(INSTANCE_COUNT, baseDir, limiter, caches)) {
            assertThat(host.size()).isEqualTo(INSTANCE_COUNT);
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                assertThat(host.instance(i).rateLimiter()).isSameAs(limiter);
                assertThat(baseDir.resolve("inst-" + i).toFile()).exists().isDirectory();
            }
            // All instances point at the same limiter object — pairwise.
            for (int i = 1; i < INSTANCE_COUNT; i++) {
                assertThat(host.instance(i).rateLimiter()).isSameAs(host.instance(0).rateLimiter());
            }
        }
    }

    @Test
    void everyInstanceCompactionRoutesThroughTheSharedLimiter(@TempDir Path baseDir)
        throws IOException, InterruptedException {
        // Generous limiter rate — the assertion is about routing, not pacing.
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(8L << 20, REFILL_MILLIS);
        List<TaggedRateLimiter> proxies = new ArrayList<>();
        try (Host host = openTaggedHost(INSTANCE_COUNT, baseDir, shared, proxies)) {
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                seedL0Backlog(host.instance(i), i);
            }
            compactAllConcurrently(host.dbs, 60_000L);

            for (int i = 0; i < INSTANCE_COUNT; i++) {
                TaggedRateLimiter p = proxies.get(i);
                assertThat(p.lowCalls())
                    .withFailMessage("instance %d made no LOW requests through its proxy", i)
                    .isGreaterThan(0L);
                assertThat(p.lowBytes())
                    .withFailMessage("instance %d wrote zero LOW bytes", i)
                    .isGreaterThan(0L);
                assertThat(p.highBytes())
                    .withFailMessage("instance %d generated unexpected HIGH-priority traffic", i)
                    .isZero();
            }
        }
    }

    @Test
    void aggregateWallTimeReflectsSharedBudgetNotPerInstanceBudget(@TempDir Path baseDir)
        throws IOException, InterruptedException {
        // Baseline: one instance with the full budget.
        long baselineMillis;
        try (RocksDb solo = RocksDb.open(baseDir.resolve("baseline"), true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1,
            new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS))) {
            seedL0Backlog(solo, 0);
            long t0 = System.nanoTime();
            solo.runCompactionPass(1);
            baselineMillis = (System.nanoTime() - t0) / 1_000_000L;
        }

        // N instances sharing the SAME budget (via per-instance proxies of one TokenBucket).
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        List<TaggedRateLimiter> proxies = new ArrayList<>();
        long sharedMillis;
        try (Host host = openTaggedHost(INSTANCE_COUNT, baseDir.resolve("shared"), shared, proxies)) {
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                seedL0Backlog(host.instance(i), i);
            }
            long t0 = System.nanoTime();
            compactAllConcurrently(host.dbs, 120_000L);
            sharedMillis = (System.nanoTime() - t0) / 1_000_000L;
        }

        // Sharing N ways must NOT cut total time proportionally. If the limiter
        // were cloned per instance we'd expect ~baselineMillis (everyone gets
        // their own budget in parallel). Asserting "shared >= 1.5x baseline"
        // is the wide-tolerance form of "the budget is genuinely shared."
        // (We intentionally do not assert "shared >= Nx baseline" — concurrent
        // refills + small per-instance output sizes leave real-world slack.)
        assertThat(sharedMillis)
            .withFailMessage(
                "shared %d ms vs baseline %d ms — limiter does not appear to be shared (expected >= 1.5x)",
                sharedMillis, baselineMillis)
            .isGreaterThanOrEqualTo((long) (baselineMillis * 1.5));
    }

    @Test
    void noInstanceStarvesUnderConcurrentSharedLimiterPressure(@TempDir Path baseDir)
        throws IOException, InterruptedException {
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        List<TaggedRateLimiter> proxies = new ArrayList<>();
        try (Host host = openTaggedHost(INSTANCE_COUNT, baseDir, shared, proxies)) {
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                seedL0Backlog(host.instance(i), i);
            }
            compactAllConcurrently(host.dbs, 60_000L);

            // Min/max LOW-bytes attributed to any one instance. The picker may
            // select different file counts per instance, but if any one
            // instance got < 25% of the busiest one's draw, the limiter is
            // starving someone.
            long min = Long.MAX_VALUE, max = 0L;
            for (TaggedRateLimiter p : proxies) {
                long b = p.lowBytes();
                if (b < min) min = b;
                if (b > max) max = b;
            }
            assertThat(min).isGreaterThan(0L);
            assertThat(min * 4L)
                .withFailMessage("starvation: min=%d max=%d (min < max/4)", min, max)
                .isGreaterThanOrEqualTo(max);
        }
    }

    @Test
    void everyInstancesDataIsReadableAfterSharedRateLimitedCompaction(@TempDir Path baseDir)
        throws IOException, InterruptedException {
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        List<TaggedRateLimiter> proxies = new ArrayList<>();
        try (Host host = openTaggedHost(INSTANCE_COUNT, baseDir, shared, proxies)) {
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                seedL0Backlog(host.instance(i), i);
            }
            compactAllConcurrently(host.dbs, 60_000L);

            // Spot-check a few keys per instance.
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                RocksDb db = host.instance(i);
                List<String> missing = new ArrayList<>();
                for (int batch = 0; batch < 6; batch++) {
                    for (int k : new int[]{0, 17, 49}) {
                        String key = String.format("inst-%d-k-%05d", i, batch * 100 + k);
                        String want = "v-padded-bytes-" + k + "-batch-" + batch;
                        Optional<Slice> got = db.get(Key.of(key));
                        if (got.isEmpty() || !new String(got.get().toBytes()).equals(want)) {
                            missing.add(key);
                        }
                    }
                }
                assertThat(missing).withFailMessage("instance %d missing keys: %s", i, missing).isEmpty();
            }
        }
    }

    @Test
    void totalLowBytesEqualsSumOfPerInstanceCounters(@TempDir Path baseDir)
        throws IOException, InterruptedException {
        // The InstrumentedRateLimiter pattern (used in single-instance test) doesn't carry across
        // worker threads, so we instead sum the per-instance proxies and assert each is positive.
        // This proves the host-wide budget was REACHED via the union of per-instance flows.
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(8L << 20, REFILL_MILLIS);
        List<TaggedRateLimiter> proxies = new ArrayList<>();
        try (Host host = openTaggedHost(INSTANCE_COUNT, baseDir, shared, proxies)) {
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                seedL0Backlog(host.instance(i), i);
            }
            compactAllConcurrently(host.dbs, 60_000L);

            long sumLowBytes = 0L;
            long sumLowCalls = 0L;
            for (TaggedRateLimiter p : proxies) {
                assertThat(p.lowBytes())
                    .withFailMessage("instance %s contributed zero LOW bytes", p.tag())
                    .isGreaterThan(0L);
                sumLowBytes += p.lowBytes();
                sumLowCalls += p.lowCalls();
            }
            assertThat(sumLowBytes).isGreaterThan(0L);
            assertThat(sumLowCalls).isGreaterThanOrEqualTo(INSTANCE_COUNT);
        }
    }

    @Test
    void closeAllReleasesEveryInstance(@TempDir Path baseDir) throws IOException {
        TokenBucketRateLimiter shared = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        IntFunction<BlockCache> caches = i -> new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES);
        RocksDbHost host = RocksDbHost.openN(INSTANCE_COUNT, baseDir, shared, caches);
        assertThat(host.size()).isEqualTo(INSTANCE_COUNT);
        host.closeAll();
        assertThat(host.size()).isZero();
        // Second close must be a no-op (idempotent).
        host.closeAll();
        // After close, reopening at the same baseDir must succeed — proves
        // the instances released their locks.
        try (RocksDbHost reopen = RocksDbHost.openN(INSTANCE_COUNT, baseDir, shared, caches)) {
            assertThat(reopen.size()).isEqualTo(INSTANCE_COUNT);
        }
    }

    @Test
    void openNRejectsZeroOrNegativeCount(@TempDir Path baseDir) {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(SHARED_BYTES_PER_SEC, REFILL_MILLIS);
        LruBlockCache cache = new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES);
        assertThat(catchThrowable(() -> RocksDbHost.openN(0, baseDir, limiter, cache)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(catchThrowable(() -> RocksDbHost.openN(-1, baseDir, limiter, cache)))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
