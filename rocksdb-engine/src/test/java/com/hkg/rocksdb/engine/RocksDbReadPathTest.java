package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDbReadPathTest {

    @Test
    void putAndGetSingleEntry(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("hello"), Slice.of("world"));
            Optional<Slice> v = db.get(Key.of("hello"));
            assertThat(v).isPresent();
            assertThat(new String(v.get().toBytes())).isEqualTo("world");
        }
    }

    @Test
    void overwriteReturnsLatestValue(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v1"));
            db.put(Key.of("k"), Slice.of("v2"));
            assertThat(new String(db.get(Key.of("k")).get().toBytes())).isEqualTo("v2");
        }
    }

    @Test
    void deleteSuppressesPreviousValue(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.delete(Key.of("k"));
            assertThat(db.get(Key.of("k"))).isEmpty();
        }
    }

    @Test
    void missingKeyReturnsEmpty(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("a"), Slice.of("1"));
            assertThat(db.get(Key.of("missing"))).isEmpty();
        }
    }

    @Test
    void readsFromL0AfterFlush(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of("k" + i), Slice.of("v" + i));
            }
            db.flush();
            // Now all data is in L0.
            for (int i = 0; i < 100; i++) {
                Optional<Slice> v = db.get(Key.of("k" + i));
                assertThat(v).isPresent();
                assertThat(new String(v.get().toBytes())).isEqualTo("v" + i);
            }
        }
    }

    @Test
    void readSpansMemTableAndL0(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of("k" + i), Slice.of("old" + i));
            }
            db.flush();
            // Overwrite half of them post-flush.
            for (int i = 0; i < 50; i++) {
                db.put(Key.of("k" + i), Slice.of("new" + i));
            }
            for (int i = 0; i < 50; i++) {
                assertThat(new String(db.get(Key.of("k" + i)).get().toBytes())).isEqualTo("new" + i);
            }
            for (int i = 50; i < 100; i++) {
                assertThat(new String(db.get(Key.of("k" + i)).get().toBytes())).isEqualTo("old" + i);
            }
        }
    }

    @Test
    void newerTombstoneShadowsOlderL0Value(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.flush();
            db.delete(Key.of("k"));
            // Even though L0 still has the Put, the MemTable tombstone shadows it.
            assertThat(db.get(Key.of("k"))).isEmpty();
            db.flush();
            // Now both the tombstone and the put are in L0; the newer (higher seq) tombstone still wins.
            assertThat(db.get(Key.of("k"))).isEmpty();
        }
    }

    @Test
    void snapshotReadSeesOldValueAfterOverwrite(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v1"));
            Snapshot snap = db.snapshot();
            db.put(Key.of("k"), Slice.of("v2"));
            // Latest read sees v2.
            assertThat(new String(db.get(Key.of("k")).get().toBytes())).isEqualTo("v2");
            // Snapshot read sees v1.
            assertThat(new String(db.get(Key.of("k"), snap).get().toBytes())).isEqualTo("v1");
            db.releaseSnapshot(snap);
        }
    }

    @Test
    void afterFlushExistingDataReadable(@TempDir Path dir) throws IOException {
        Path subdir = dir.resolve("db");
        try (RocksDb db = RocksDb.open(subdir)) {
            db.put(Key.of("persisted"), Slice.of("payload"));
            db.flush();
        }
        // Reopen — MANIFEST replay should resurrect the L0 file.
        try (RocksDb db = RocksDb.open(subdir)) {
            assertThat(new String(db.get(Key.of("persisted")).get().toBytes())).isEqualTo("payload");
        }
    }

    @Test
    void manualCompactionMovesL0ToL1(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            // Create more than L0_FILE_COUNT_TRIGGER L0 files.
            for (int file = 0; file < 5; file++) {
                for (int i = 0; i < 50; i++) {
                    db.put(Key.of(String.format("k-%03d-%03d", file, i)),
                        Slice.of("v" + i));
                }
                db.flush();
            }
            assertThat(db.currentVersion().level(0)).hasSize(5);
            boolean ran = db.maybeCompact();
            assertThat(ran).isTrue();
            // After compaction L0 should shrink; outputs went to L1.
            assertThat(db.currentVersion().level(1)).isNotEmpty();
            // All values still readable.
            for (int file = 0; file < 5; file++) {
                for (int i = 0; i < 50; i++) {
                    Optional<Slice> v = db.get(Key.of(String.format("k-%03d-%03d", file, i)));
                    assertThat(v).as("file=%d i=%d", file, i).isPresent();
                    assertThat(new String(v.get().toBytes())).isEqualTo("v" + i);
                }
            }
        }
    }
}
