package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.errorhandling.ReadOnlyModeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CP 22 main — verify the engine's read-only mode latch trips on HARD
 * severity errors (block-CRC mismatch, KV-checksum mismatch), rejects
 * subsequent writes, and clears via {@link RocksDb#attemptResume()}.
 */
class SeverityIntegrationTest {

    @Test
    void freshEngineIsWritable(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            assertThat(db.isReadOnly()).isFalse();
            assertThat(db.readOnlyReason()).isNull();
            db.put(Key.of("k"), Slice.of("v"));
            assertThat(new String(db.get(Key.of("k")).get().toBytes())).isEqualTo("v");
        }
    }

    @Test
    void blockCorruptionTripReadOnlyOnRead(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of(String.format("k-%04d", i)),
                    Slice.of("v-with-some-padding-bytes-" + i));
            }
            db.flush();
        }
        // Corrupt a byte inside the L0 SSTable.
        Path sst = firstSstable(dir);
        flipByteAt(sst, 64);

        try (RocksDb db = RocksDb.open(dir)) {
            assertThat(db.isReadOnly()).isFalse();
            // Trigger reads — one of them lands in the corrupt block.
            Throwable caught = null;
            for (int i = 0; i < 100 && caught == null; i++) {
                try {
                    db.get(Key.of(String.format("k-%04d", i)));
                } catch (Throwable t) {
                    caught = t;
                }
            }
            assertThat(caught).isNotNull();
            // Latch tripped — subsequent writes rejected.
            assertThat(db.isReadOnly()).isTrue();
            assertThat(db.readOnlyReason()).contains("BlockChecksumMismatchException");
        }
    }

    @Test
    void writesRejectedAfterTrip(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of(String.format("k-%04d", i)),
                    Slice.of("vvvvvvvvvvvvvvv-" + i));
            }
            db.flush();
        }
        flipByteAt(firstSstable(dir), 64);

        try (RocksDb db = RocksDb.open(dir)) {
            // Force a read to trip.
            for (int i = 0; i < 100 && !db.isReadOnly(); i++) {
                try { db.get(Key.of(String.format("k-%04d", i))); } catch (Throwable ignored) {}
            }
            assertThat(db.isReadOnly()).isTrue();

            assertThatThrownBy(() -> db.put(Key.of("new"), Slice.of("v")))
                .isInstanceOf(ReadOnlyModeException.class);
            assertThatThrownBy(() -> db.delete(Key.of("k-0000")))
                .isInstanceOf(ReadOnlyModeException.class);
            assertThatThrownBy(() -> db.deleteRange(Key.of("a"), Key.of("z")))
                .isInstanceOf(ReadOnlyModeException.class);
        }
    }

    @Test
    void attemptResumeClearsLatch(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of(String.format("k-%04d", i)),
                    Slice.of("vvvvvvvvvvvvvvv-" + i));
            }
            db.flush();
        }
        flipByteAt(firstSstable(dir), 64);

        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 100 && !db.isReadOnly(); i++) {
                try { db.get(Key.of(String.format("k-%04d", i))); } catch (Throwable ignored) {}
            }
            assertThat(db.isReadOnly()).isTrue();

            db.attemptResume();
            assertThat(db.isReadOnly()).isFalse();
            // Writes resume.
            db.put(Key.of("post-resume"), Slice.of("v"));
            assertThat(new String(db.get(Key.of("post-resume")).get().toBytes())).isEqualTo("v");
        }
    }

    @Test
    void readsContinueAfterTrip(@TempDir Path dir) throws IOException {
        // Corruption affects ONE block; reads of keys NOT in that block continue.
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                db.put(Key.of(String.format("k-%05d", i)),
                    Slice.of("v-padding-padding-padding-" + i));
            }
            db.flush();
        }
        Path sst = firstSstable(dir);
        // Pick a byte well into the file — likely inside one data block.
        flipByteAt(sst, 1024);

        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                try { db.get(Key.of(String.format("k-%05d", i))); } catch (Throwable ignored) {}
                if (db.isReadOnly()) break;
            }
            assertThat(db.isReadOnly()).isTrue();
            // Reads still work for keys we haven't touched (or in different blocks).
            // The point: read API doesn't throw "read-only" — only writes do.
            try {
                db.get(Key.of("definitely-missing-key"));
                // success — reads aren't blocked.
            } catch (Throwable t) {
                // It's also OK if the corrupted-block read throws again; what's NOT OK is a
                // ReadOnlyModeException on a READ. Assert that didn't happen.
                assertThat(t).isNotInstanceOf(ReadOnlyModeException.class);
            }
        }
    }

    @Test
    void kvChecksumMismatchTripsLatchToo(@TempDir Path dir) throws IOException {
        // With KV checksum enabled, a value-byte flip surfaces as a KvChecksumMismatchException.
        // CP 22 classifies that as HARD and trips the latch.
        try (RocksDb db = RocksDb.open(dir, true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, true)) {
            for (int i = 0; i < 100; i++) {
                db.put(Key.of(String.format("k-%04d", i)),
                    Slice.of("padded-value-bytes-" + i));
            }
            db.flush();
        }
        // Corrupt a byte deep inside an SSTable to flip a value byte
        // (which the KV checksum will catch on read).
        flipByteAt(firstSstable(dir), 256);

        try (RocksDb db = RocksDb.open(dir, true,
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES), 1, null, true)) {
            for (int i = 0; i < 100 && !db.isReadOnly(); i++) {
                try { db.get(Key.of(String.format("k-%04d", i))); } catch (Throwable ignored) {}
            }
            assertThat(db.isReadOnly()).isTrue();
            // The trip reason should mention one of: KvChecksum, BlockChecksum.
            assertThat(db.readOnlyReason()).satisfiesAnyOf(
                r -> assertThat(r).contains("KvChecksum"),
                r -> assertThat(r).contains("BlockChecksum"));
        }
    }

    private static Path firstSstable(Path dir) throws IOException {
        try (var s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".sst"))
                .sorted()
                .findFirst()
                .orElseThrow();
        }
    }

    private static void flipByteAt(Path file, long offset) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b, offset);
            b.flip();
            ch.write(ByteBuffer.wrap(new byte[] {(byte) (b.get(0) ^ 0xFF)}), offset);
        }
    }
}
