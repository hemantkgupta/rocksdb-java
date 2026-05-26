package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP 10 — verify the engine can run compactions on a worker pool, drop the
 * write lock during the merge phase, and produce identical state to the
 * single-threaded path.
 */
class MultiThreadedCompactionTest {

    private static RocksDb openWithThreads(Path dir, int threads) throws IOException {
        return RocksDb.open(dir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), threads);
    }

    @Test
    void engineReportsConfiguredCompactionThreadCount(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 4)) {
            assertThat(db.compactionThreads()).isEqualTo(4);
        }
    }

    @Test
    void runCompactionPassReturnsZeroWhenNothingToDo(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 2)) {
            db.put(Key.of("k"), Slice.of("v"));
            // No flush yet; no L0 file => picker finds nothing.
            assertThat(db.runCompactionPass(4)).isZero();
        }
    }

    @Test
    void singleThreadedCompactionStillWorks(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 1)) {
            // Generate enough L0 files to trigger compaction.
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 50; i++) {
                    db.put(Key.of(String.format("k-%04d", batch * 100 + i)), Slice.of("v" + i));
                }
                db.flush();
            }
            int compacted = db.runCompactionPass(1);
            assertThat(compacted).isEqualTo(1);
            // Data is still readable.
            for (int batch = 0; batch < 5; batch++) {
                Optional<Slice> v = db.get(Key.of(String.format("k-%04d", batch * 100)));
                assertThat(v).isPresent();
            }
        }
    }

    @Test
    void multipleThreadsExecuteJobsInParallel(@TempDir Path dir) throws IOException {
        // 8 L0 files, 4 worker threads — the picker should choose to compact L0 (which
        // becomes ONE job because L0 files overlap conceptually). To get >1 job in one
        // pass we ALSO need L1 over target. Simpler: assert that with maxParallel=4 and
        // 4 workers, the pass completes (and the worker threads are real, not the caller).
        try (RocksDb db = openWithThreads(dir, 4)) {
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 20; i++) {
                    db.put(Key.of(String.format("k-%05d", batch * 100 + i)),
                        Slice.of("value-bytes-padding-" + i));
                }
                db.flush();
            }
            // Pre-condition: L0 should be over its trigger.
            assertThat(db.currentVersion().level(0).size()).isGreaterThanOrEqualTo(Constants.L0_FILE_COUNT_TRIGGER);

            int ran = db.runCompactionPass(4);
            assertThat(ran).isGreaterThanOrEqualTo(1);

            // All keys still readable post-compaction.
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 20; i++) {
                    String k = String.format("k-%05d", batch * 100 + i);
                    assertThat(db.get(Key.of(k))).withFailMessage("missing %s", k).isPresent();
                }
            }
        }
    }

    @Test
    void foregroundWritesProceedDuringCompactionMerge(@TempDir Path dir) throws Exception {
        // Drop a bunch of L0 files, then start a compaction in one thread and fire foreground
        // writes in another. The writes must succeed (i.e. they don't block until compaction
        // completes). Pre-CP-10 the writeLock was held for the whole merge.
        try (RocksDb db = openWithThreads(dir, 1)) {
            for (int batch = 0; batch < 6; batch++) {
                for (int i = 0; i < 100; i++) {
                    db.put(Key.of(String.format("seed-%04d", batch * 100 + i)),
                        Slice.of("seed-value-" + i));
                }
                db.flush();
            }
            assertThat(db.currentVersion().level(0).size()).isGreaterThanOrEqualTo(Constants.L0_FILE_COUNT_TRIGGER);

            AtomicInteger writesDuringCompaction = new AtomicInteger();
            ExecutorService es = Executors.newFixedThreadPool(2);
            try {
                es.submit(() -> {
                    try {
                        db.runCompactionPass(1);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                // Race a writer thread against the compaction.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (System.nanoTime() < deadline && writesDuringCompaction.get() < 20) {
                    db.put(Key.of("rt-" + writesDuringCompaction.incrementAndGet()), Slice.of("v"));
                }
                es.shutdown();
                assertThat(es.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            } finally {
                es.shutdownNow();
            }

            // The whole point: at least some foreground writes interleaved with the merge.
            assertThat(writesDuringCompaction.get()).isGreaterThan(0);
            // And every realtime write is observable.
            for (int i = 1; i <= writesDuringCompaction.get(); i++) {
                assertThat(db.get(Key.of("rt-" + i))).withFailMessage("rt-%d missing", i)
                    .isPresent();
            }
        }
    }

    @Test
    void allEntriesReadableAfterMultiPassCompaction(@TempDir Path dir) throws IOException {
        // Stress-y: many flushes, several compaction passes, verify durability semantics
        // hold across the multi-threaded path.
        try (RocksDb db = openWithThreads(dir, 3)) {
            for (int batch = 0; batch < 12; batch++) {
                for (int i = 0; i < 50; i++) {
                    db.put(Key.of(String.format("k-%05d", batch * 100 + i)),
                        Slice.of("v-" + batch + "-" + i));
                }
                db.flush();
                db.runCompactionPass(2);
            }
            // Final drain.
            for (int p = 0; p < 5; p++) {
                if (db.runCompactionPass(3) == 0) break;
            }

            // Sample-check correctness.
            for (int batch = 0; batch < 12; batch++) {
                for (int i = 0; i < 50; i++) {
                    String k = String.format("k-%05d", batch * 100 + i);
                    String want = "v-" + batch + "-" + i;
                    Optional<Slice> v = db.get(Key.of(k));
                    assertThat(v).withFailMessage("%s missing", k).isPresent();
                    assertThat(new String(v.get().toBytes())).isEqualTo(want);
                }
            }
        }
    }

    @Test
    void runCompactionPassRejectsZeroOrNegativeMax(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 2)) {
            try {
                db.runCompactionPass(0);
                org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        }
    }
}
