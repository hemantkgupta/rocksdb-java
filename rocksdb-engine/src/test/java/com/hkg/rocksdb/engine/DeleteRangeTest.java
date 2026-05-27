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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CP 17 — DeleteRange end-to-end. Covers MemTable RTs, WAL replay,
 * SSTable persistence, and the cross-layer read path. Verifies the FAST
 * 2021 §6.6 failure-mode contract that a later Put at a covered key wins
 * over an earlier range tombstone.
 */
class DeleteRangeTest {

    @Test
    void deleteRangeMakesCoveredKeysAbsent(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of(String.format("k-%05d", i)), Slice.of("v" + i));
            }
            db.deleteRange(Key.of("k-00020"), Key.of("k-00080"));

            // Outside the range: still present.
            assertThat(db.get(Key.of("k-00010"))).isPresent();
            assertThat(db.get(Key.of("k-00090"))).isPresent();
            // Inside the range: absent.
            assertThat(db.get(Key.of("k-00020"))).isEmpty(); // start inclusive
            assertThat(db.get(Key.of("k-00050"))).isEmpty();
            assertThat(db.get(Key.of("k-00079"))).isEmpty();
            // End exclusive: not deleted.
            assertThat(db.get(Key.of("k-00080"))).isPresent();
        }
    }

    @Test
    void laterPutAtCoveredKeyBeatsRangeTombstone(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v1"));
            db.deleteRange(Key.of("k"), Key.of("l"));
            assertThat(db.get(Key.of("k"))).isEmpty();
            // Re-Put — the later sequence wins.
            db.put(Key.of("k"), Slice.of("v2"));
            assertThat(new String(db.get(Key.of("k")).get().toBytes())).isEqualTo("v2");
        }
    }

    @Test
    void deleteRangeSurvivesWalReplay(@TempDir Path dir) throws IOException {
        // Write some keys + a DR, abandon without close().
        RocksDb db = RocksDb.open(dir);
        for (int i = 0; i < 50; i++) {
            db.put(Key.of(String.format("k-%04d", i)), Slice.of("v" + i));
        }
        db.deleteRange(Key.of("k-0010"), Key.of("k-0040"));
        db.closeWithoutFlush();

        try (RocksDb recovered = RocksDb.open(dir)) {
            assertThat(recovered.get(Key.of("k-0005"))).isPresent();
            assertThat(recovered.get(Key.of("k-0020"))).isEmpty();
            assertThat(recovered.get(Key.of("k-0030"))).isEmpty();
            assertThat(recovered.get(Key.of("k-0045"))).isPresent();
        }
    }

    @Test
    void deleteRangeSurvivesFlushAcrossL0(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                db.put(Key.of(String.format("k-%05d", i)), Slice.of("v" + i));
            }
            db.deleteRange(Key.of("k-00050"), Key.of("k-00150"));
            db.flush();  // Forces RT into L0 SSTable.

            // Read every key — covered range must come back empty.
            for (int i = 0; i < 200; i++) {
                Key k = Key.of(String.format("k-%05d", i));
                Optional<Slice> v = db.get(k);
                if (i >= 50 && i < 150) {
                    assertThat(v).withFailMessage("k-%05d should be deleted", i).isEmpty();
                } else {
                    assertThat(v).withFailMessage("k-%05d should be present", i).isPresent();
                }
            }
        }
    }

    @Test
    void deleteRangeAcrossMemTableAndL0(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            // Batch 1: into L0 via flush.
            for (int i = 0; i < 50; i++) {
                db.put(Key.of(String.format("k-%04d", i)), Slice.of("L0-" + i));
            }
            db.flush();
            // Batch 2: into the active MemTable.
            for (int i = 50; i < 100; i++) {
                db.put(Key.of(String.format("k-%04d", i)), Slice.of("mem-" + i));
            }
            // DeleteRange covering keys from both batches.
            db.deleteRange(Key.of("k-0040"), Key.of("k-0060"));

            assertThat(db.get(Key.of("k-0030"))).isPresent();   // outside
            assertThat(db.get(Key.of("k-0045"))).isEmpty();     // in L0, covered
            assertThat(db.get(Key.of("k-0055"))).isEmpty();     // in MemTable, covered
            assertThat(db.get(Key.of("k-0065"))).isPresent();   // outside
        }
    }

    @Test
    void putAfterDeleteRangeWinsAcrossFlushBoundary(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("a"), Slice.of("old"));
            db.deleteRange(Key.of("a"), Key.of("z"));
            db.flush();  // RT goes to L0.

            // Now put — newer seq than the RT in L0.
            db.put(Key.of("a"), Slice.of("new"));
            assertThat(new String(db.get(Key.of("a")).get().toBytes())).isEqualTo("new");
        }
    }

    @Test
    void snapshotBeforeDeleteRangeSeesOldData(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 20; i++) {
                db.put(Key.of(String.format("k-%03d", i)), Slice.of("v" + i));
            }
            Snapshot snap = db.snapshot();
            db.deleteRange(Key.of("k-005"), Key.of("k-015"));

            // Live read: in the range → empty.
            assertThat(db.get(Key.of("k-010"))).isEmpty();
            // Snapshot read: still present (snapshot's seq < RT's seq).
            assertThat(db.get(Key.of("k-010"), snap)).isPresent();
            assertThat(new String(db.get(Key.of("k-010"), snap).get().toBytes())).isEqualTo("v10");

            db.releaseSnapshot(snap);
        }
    }

    @Test
    void deleteRangeCovering10KKeys(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 10_000; i++) {
                db.put(Key.of(String.format("k-%07d", i)), Slice.of("v"));
            }
            db.deleteRange(Key.of("k-0001000"), Key.of("k-0009000"));
            db.flush();
            db.runCompactionPass(1);

            // Spot-check.
            assertThat(db.get(Key.of("k-0000500"))).isPresent();
            assertThat(db.get(Key.of("k-0001000"))).isEmpty();
            assertThat(db.get(Key.of("k-0005000"))).isEmpty();
            assertThat(db.get(Key.of("k-0008999"))).isEmpty();
            assertThat(db.get(Key.of("k-0009000"))).isPresent();
            assertThat(db.get(Key.of("k-0009500"))).isPresent();
        }
    }

    @Test
    void emptyOrInvertedRangeRejected(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThatThrownBy(() -> db.deleteRange(Key.of("z"), Key.of("a")))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.deleteRange(Key.of("a"), Key.of("a")))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void multipleOverlappingRangesNewerWins(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v1"));
            db.deleteRange(Key.of("k"), Key.of("l")); // RT1: seq S1
            db.put(Key.of("k"), Slice.of("v2"));            // Put: seq S2 > S1
            db.deleteRange(Key.of("k"), Key.of("l")); // RT2: seq S3 > S2

            // RT2 is newest → empty.
            assertThat(db.get(Key.of("k"))).isEmpty();
            // Put after RT2 wins.
            db.put(Key.of("k"), Slice.of("v3"));
            assertThat(new String(db.get(Key.of("k")).get().toBytes())).isEqualTo("v3");
        }
    }
}
