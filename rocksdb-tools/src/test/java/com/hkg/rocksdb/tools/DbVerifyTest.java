package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DbVerifyTest {

    @Test
    void emptyDbPassesVerify(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            // No writes — no SSTables.
        }
        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isTrue();
        assertThat(r.filesScanned()).isZero();
        assertThat(r.failures()).isZero();
    }

    @Test
    void populatedDbPassesVerify(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                db.put(Key.of("k-" + i), Slice.of("v-" + i));
            }
            db.flush();
            db.put(Key.of("late"), Slice.of("late-v"));
            db.flush();
        }
        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isTrue();
        assertThat(r.filesScanned()).isGreaterThanOrEqualTo(2);
        assertThat(r.blocksScanned()).isGreaterThan(0L);
        for (DbVerify.FileResult fr : r.perFile()) {
            assertThat(fr.ok()).isTrue();
        }
    }

    @Test
    void corruptedSstableIsDetected(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                db.put(Key.of(String.format("k-%05d", i)), Slice.of("value-bytes-" + i));
            }
            db.flush();
        }
        // Flip one byte inside the first SSTable's data area (well before the footer).
        Path victim = firstSstable(dir);
        corruptByteAt(victim, 64);

        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isFalse();
        assertThat(r.failures()).isGreaterThanOrEqualTo(1);
        DbVerify.FileResult failed = r.perFile().stream()
            .filter(f -> !f.ok())
            .findFirst()
            .orElseThrow();
        assertThat(failed.detail()).containsIgnoringCase("crc");
    }

    @Test
    void reportRenderIsHumanReadable(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.flush();
        }
        DbVerify.Report r = DbVerify.run(dir);
        String rendered = r.render();
        assertThat(rendered).contains("PASS").contains("scanned=").contains("OK");
    }

    private static Path firstSstable(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".sst"))
                .sorted()
                .findFirst()
                .orElseThrow(() -> new AssertionError("no .sst file present"));
        }
    }

    private static void corruptByteAt(Path file, long offset) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b, offset);
            b.flip();
            byte flipped = (byte) (b.get(0) ^ 0xFF);
            ch.write(ByteBuffer.wrap(new byte[] {flipped}), offset);
        }
    }
}
