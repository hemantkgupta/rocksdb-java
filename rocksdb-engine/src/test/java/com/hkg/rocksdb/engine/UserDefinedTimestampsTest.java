package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.UserTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CP 27 — user-defined timestamp end-to-end. Per-engine opt-in via
 * {@code userTimestampSize = 8}; legacy put/get are unaffected when off.
 */
class UserDefinedTimestampsTest {

    private static RocksDb openUdt(Path dir) throws IOException {
        return RocksDb.open(dir, true, new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES),
            1, null, false, UserTimestamp.ENCODED_LENGTH);
    }

    @Test
    void engineDefaultsToUdtDisabled(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThat(db.userTimestampsEnabled()).isFalse();
        }
    }

    @Test
    void udtOpenSetsTheFlag(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            assertThat(db.userTimestampsEnabled()).isTrue();
        }
    }

    @Test
    void putAndGetAsOfRoundtrip(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            db.putWithUserTs(Key.of("k"), new UserTimestamp(100L), Slice.of("v1"));
            assertThat(db.getAsOf(Key.of("k"), new UserTimestamp(100L)))
                .hasValueSatisfying(v -> assertThat(new String(v.toBytes())).isEqualTo("v1"));
        }
    }

    @Test
    void getAsOfReturnsFloorTimestamp(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            db.putWithUserTs(Key.of("k"), new UserTimestamp(100L), Slice.of("at-100"));
            db.putWithUserTs(Key.of("k"), new UserTimestamp(200L), Slice.of("at-200"));
            db.putWithUserTs(Key.of("k"), new UserTimestamp(300L), Slice.of("at-300"));

            assertReturns(db, "k", 99L,  Optional.empty());
            assertReturns(db, "k", 100L, Optional.of("at-100"));
            assertReturns(db, "k", 150L, Optional.of("at-100"));   // floor of 150 is 100
            assertReturns(db, "k", 200L, Optional.of("at-200"));
            assertReturns(db, "k", 250L, Optional.of("at-200"));
            assertReturns(db, "k", 300L, Optional.of("at-300"));
            assertReturns(db, "k", 999L, Optional.of("at-300"));   // floor of 999 is 300
        }
    }

    @Test
    void asOfBeforeAnyTsReturnsEmpty(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            db.putWithUserTs(Key.of("k"), new UserTimestamp(100L), Slice.of("v"));
            assertThat(db.getAsOf(Key.of("k"), new UserTimestamp(50L))).isEmpty();
        }
    }

    @Test
    void unknownKeyReturnsEmpty(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            db.putWithUserTs(Key.of("k"), new UserTimestamp(100L), Slice.of("v"));
            assertThat(db.getAsOf(Key.of("missing"), new UserTimestamp(999L))).isEmpty();
        }
    }

    @Test
    void udtSurvivesEngineReopen(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            for (int i = 0; i < 20; i++) {
                db.putWithUserTs(Key.of("k-" + i), new UserTimestamp(100L + i),
                    Slice.of("v-" + i));
            }
            db.flush();
        }
        // Reopen — rebuildUdtIndex walks every SSTable + active MemTable to repopulate the
        // floor-lookup structure.
        try (RocksDb db = openUdt(dir)) {
            for (int i = 0; i < 20; i++) {
                Optional<Slice> v = db.getAsOf(Key.of("k-" + i), new UserTimestamp(999L));
                assertThat(v).withFailMessage("k-%d missing after reopen", i).isPresent();
                assertThat(new String(v.get().toBytes())).isEqualTo("v-" + i);
            }
        }
    }

    @Test
    void putWithUserTsRejectedWhenUdtOff(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThatThrownBy(() ->
                db.putWithUserTs(Key.of("k"), new UserTimestamp(1L), Slice.of("v")))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void getAsOfRejectedWhenUdtOff(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThatThrownBy(() -> db.getAsOf(Key.of("k"), new UserTimestamp(1L)))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void multipleKeysIndependentFloors(@TempDir Path dir) throws IOException {
        try (RocksDb db = openUdt(dir)) {
            db.putWithUserTs(Key.of("a"), new UserTimestamp(100L), Slice.of("a-100"));
            db.putWithUserTs(Key.of("b"), new UserTimestamp(200L), Slice.of("b-200"));
            db.putWithUserTs(Key.of("a"), new UserTimestamp(300L), Slice.of("a-300"));

            assertReturns(db, "a", 150L, Optional.of("a-100"));
            assertReturns(db, "a", 999L, Optional.of("a-300"));
            assertReturns(db, "b", 250L, Optional.of("b-200"));
            assertReturns(db, "b", 50L,  Optional.empty());
        }
    }

    @Test
    void rejectsInvalidUserTsSize(@TempDir Path dir) throws IOException {
        assertThatThrownBy(() -> RocksDb.open(dir, true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, false, 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RocksDb.open(dir, true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, false, 16))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertReturns(RocksDb db, String key, long asOfTs, Optional<String> expected) {
        Optional<Slice> got = db.getAsOf(Key.of(key), new UserTimestamp(asOfTs));
        if (expected.isEmpty()) {
            assertThat(got).withFailMessage("key=%s asOf=%d expected empty", key, asOfTs).isEmpty();
        } else {
            assertThat(got).withFailMessage("key=%s asOf=%d expected %s", key, asOfTs, expected.get())
                .isPresent();
            assertThat(new String(got.get().toBytes()))
                .withFailMessage("key=%s asOf=%d", key, asOfTs)
                .isEqualTo(expected.get());
        }
    }
}
