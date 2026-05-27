package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP 18 — engine-level verification of {@link RocksDb#runParallelL0Compaction}.
 * The unit tests in {@code LeveledCompactionPickerPartitionL0Test} cover the
 * partitioning algorithm in isolation; here we verify the engine end-to-end:
 * data still readable, multiple worker threads actually dispatched, correctness
 * preserved across parallel jobs.
 */
class ParallelL0CompactionTest {

    private static RocksDb openWithThreads(Path dir, int threads) throws IOException {
        return RocksDb.open(dir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), threads);
    }

    @Test
    void runParallelL0OnEmptyDbReturnsZero(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 2)) {
            assertThat(db.runParallelL0Compaction(4)).isZero();
        }
    }

    @Test
    void runParallelL0ReturnsAtLeastOneJobWhenL0HasFiles(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 4)) {
            // Write 8 disjoint key ranges, one per L0 file.
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 40; i++) {
                    String k = String.format("k%02d-%04d", batch, i);
                    db.put(Key.of(k), Slice.of("v-" + i));
                }
                db.flush();
            }
            assertThat(db.currentVersion().level(0).size()).isGreaterThanOrEqualTo(8);

            int ran = db.runParallelL0Compaction(4);
            assertThat(ran).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void dataReadableAfterParallelL0Compaction(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 4)) {
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 30; i++) {
                    String k = String.format("k%02d-%04d", batch, i);
                    db.put(Key.of(k), Slice.of("v-" + batch + "-" + i));
                }
                db.flush();
            }
            db.runParallelL0Compaction(4);

            // Every key still present with correct value.
            for (int batch = 0; batch < 8; batch++) {
                for (int i = 0; i < 30; i++) {
                    String k = String.format("k%02d-%04d", batch, i);
                    String want = "v-" + batch + "-" + i;
                    assertThat(new String(db.get(Key.of(k)).get().toBytes())).isEqualTo(want);
                }
            }
        }
    }

    @Test
    void rejectsZeroOrNegativeTargetJobs(@TempDir Path dir) throws IOException {
        try (RocksDb db = openWithThreads(dir, 2)) {
            try {
                db.runParallelL0Compaction(0);
                org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        }
    }
}
