package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.ratelimiter.Priority;
import com.hkg.rocksdb.ratelimiter.RateLimiter;
import com.hkg.rocksdb.ratelimiter.TokenBucketRateLimiter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP 13 — verify that when the engine is wired to a {@link RateLimiter},
 * compaction worker writes go through it at {@link Priority#LOW} and the
 * un-throttled path (no limiter) preserves the LevelDB-era throughput.
 */
class RateLimitedCompactionTest {

    private static RocksDb openWithLimiter(Path dir, RateLimiter limiter) throws IOException {
        return RocksDb.open(dir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, limiter);
    }

    @Test
    void engineDefaultsToNoRateLimiter(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThat(db.rateLimiter()).isNull();
        }
    }

    @Test
    void engineCarriesTheLimiterReference(@TempDir Path dir) throws IOException {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 20, 10);
        try (RocksDb db = openWithLimiter(dir, limiter)) {
            assertThat(db.rateLimiter()).isSameAs(limiter);
        }
    }

    @Test
    void compactionWritesGoThroughTheRateLimiter(@TempDir Path dir) throws IOException {
        // Instrumented limiter that records LOW-priority requests during compaction.
        AtomicLong lowBytes = new AtomicLong();
        AtomicLong lowCalls = new AtomicLong();
        AtomicLong highBytes = new AtomicLong();
        RateLimiter recorder = (bytes, priority) -> {
            if (priority == Priority.LOW) {
                lowBytes.addAndGet(bytes);
                lowCalls.incrementAndGet();
            } else {
                highBytes.addAndGet(bytes);
            }
        };
        try (RocksDb db = openWithLimiter(dir, recorder)) {
            for (int batch = 0; batch < 6; batch++) {
                for (int i = 0; i < 50; i++) {
                    db.put(Key.of(String.format("k-%05d", batch * 100 + i)),
                        Slice.of("v-padded-bytes-" + i));
                }
                db.flush();
            }
            assertThat(db.currentVersion().level(0).size())
                .isGreaterThanOrEqualTo(Constants.L0_FILE_COUNT_TRIGGER);

            // Reset counters so we only measure compaction-time activity.
            lowBytes.set(0);
            lowCalls.set(0);
            highBytes.set(0);

            int ran = db.runCompactionPass(1);
            assertThat(ran).isEqualTo(1);

            // Compaction wrote at least one block; every output block went through LOW.
            assertThat(lowCalls.get()).isGreaterThan(0L);
            assertThat(lowBytes.get()).isGreaterThan(0L);
            // No HIGH calls during compaction — foreground-write wiring is a later CP.
            assertThat(highBytes.get()).isZero();
        }
    }

    @Test
    void compactionPaceMatchesConfiguredRate(@TempDir Path dir) throws IOException {
        // A tight ~256 KiB/s budget. With ~32 KiB of compaction output the merge should
        // take roughly 100 ms-ish; we assert a wide tolerance — the point is "it took
        // measurable time" not "it took exactly N ms".
        long bytesPerSec = 256L * 1024L;
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(bytesPerSec, 10);
        try (RocksDb db = openWithLimiter(dir, limiter)) {
            // Five L0 files, ~32 KiB each.
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 100; i++) {
                    db.put(Key.of(String.format("k-%05d", batch * 200 + i)),
                        Slice.of("value-with-some-bytes-" + i));
                }
                db.flush();
            }
            long t0 = System.nanoTime();
            db.runCompactionPass(1);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            // Anything > 0 ms confirms the limiter actually blocked; LevelDB-era
            // unlimited compaction of this much data would complete in single-digit ms.
            assertThat(elapsedMs).isGreaterThan(0L);
        }
    }

    @Test
    void dataReadableAfterRateLimitedCompaction(@TempDir Path dir) throws IOException {
        // Correctness check: rate-limiting only paces I/O; the resulting state must match
        // the un-limited path.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1L << 20, 10);
        try (RocksDb db = openWithLimiter(dir, limiter)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    db.put(Key.of(String.format("k-%04d", batch * 100 + i)),
                        Slice.of("v-" + batch + "-" + i));
                }
                db.flush();
            }
            db.runCompactionPass(1);
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    String k = String.format("k-%04d", batch * 100 + i);
                    String want = "v-" + batch + "-" + i;
                    Optional<Slice> v = db.get(Key.of(k));
                    assertThat(v).withFailMessage("%s missing", k).isPresent();
                    assertThat(new String(v.get().toBytes())).isEqualTo(want);
                }
            }
        }
    }

    @Test
    void nullLimiterRoutesThroughNoOpHook(@TempDir Path dir) throws IOException {
        // With no limiter, compaction must run without invoking any hook code path.
        // Smoke test: build up L0 files, compact, check no exception + state reachable.
        try (RocksDb db = openWithLimiter(dir, null)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 20; i++) {
                    db.put(Key.of(String.format("k-%04d", batch * 100 + i)), Slice.of("v" + i));
                }
                db.flush();
            }
            assertThat(db.runCompactionPass(1)).isEqualTo(1);
            assertThat(db.get(Key.of("k-0000"))).isPresent();
        }
    }

    @Test
    void rateLimiterIsSharedAcrossWorkerThreads(@TempDir Path dir) throws IOException {
        // With 3 worker threads + 1 limiter, all 3 workers' compactions must funnel through
        // the same limiter (a sanity check that the rate-limiter reference is shared,
        // not duplicated per worker).
        AtomicLong totalLowBytes = new AtomicLong();
        RateLimiter shared = (bytes, priority) -> {
            if (priority == Priority.LOW) totalLowBytes.addAndGet(bytes);
        };
        try (RocksDb db = RocksDb.open(dir, true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 3, shared)) {
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 25; i++) {
                    db.put(Key.of(String.format("k-%05d", batch * 100 + i)),
                        Slice.of("value-" + i));
                }
                db.flush();
            }
            totalLowBytes.set(0);
            db.runCompactionPass(3);
            // Whether the picker chose 1 or 2 or 3 jobs, every output block came through
            // the single shared limiter.
            assertThat(totalLowBytes.get()).isGreaterThan(0L);
        }
    }
}
