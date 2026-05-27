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

    // ---------- CP 19: file-level XXH64 checksum ----------

    @Test
    void fileChecksumMismatchDetectedWhenBlocksRemainClean(@TempDir Path dir) throws IOException {
        // Footer.ENCODED_LENGTH = 48; magic occupies the last 8 bytes; the
        // ~10 bytes immediately before magic are zero-padding inside the
        // indexHandle area (varlong+varlong followed by zeros to 20 B).
        // Flipping a byte there leaves block CRCs untouched, leaves the
        // footer magic untouched, and leaves the index/metaIndex handles
        // untouched — yet the XXH64 over the whole file changes. That's
        // precisely the §5 above-block corruption the file checksum exists
        // to catch.
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 200; i++) {
                db.put(Key.of(String.format("k-%05d", i)), Slice.of("v-" + i));
            }
            db.flush();
        }
        Path victim = firstSstable(dir);
        long size = Files.size(victim);
        corruptByteAt(victim, size - 12L);

        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isFalse();
        DbVerify.FileResult failed = r.perFile().stream()
            .filter(f -> !f.ok())
            .findFirst()
            .orElseThrow();
        assertThat(failed.detail()).containsIgnoringCase("file checksum mismatch");
        assertThat(failed.detail()).contains("stored=").contains("actual=");
    }

    @Test
    void fileChecksumPassesOnCleanDb(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 50; i++) {
                db.put(Key.of("clean-" + i), Slice.of("v-" + i));
            }
            db.flush();
        }
        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isTrue();
        for (DbVerify.FileResult fr : r.perFile()) {
            assertThat(fr.detail()).isEqualTo("PASS");
        }
    }

    @Test
    void legacyFileMetadataWithoutChecksumStillVerifiesClean(@TempDir Path dir) throws IOException {
        // Synthesize an SSTable, then rewrite its MANIFEST so the NewFile
        // edit carries no file-checksum (pre-CP-19 layout). The verifier
        // must skip the file-checksum pass for that entry and still PASS.
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 30; i++) {
                db.put(Key.of("legacy-" + i), Slice.of("v-" + i));
            }
            db.flush();
        }
        stripFileChecksumFromManifest(dir);

        DbVerify.Report r = DbVerify.run(dir);
        assertThat(r.ok()).isTrue();
        assertThat(r.filesScanned()).isGreaterThanOrEqualTo(1);
    }

    /**
     * Rewrite every MANIFEST record so NewFile edits drop their
     * {@link com.hkg.rocksdb.integrity.FileChecksum} suffix — simulating a
     * pre-CP-19 MANIFEST.
     */
    private static void stripFileChecksumFromManifest(Path dir) throws IOException {
        Path current = dir.resolve("CURRENT");
        String manifestName = Files.readString(current).trim();
        Path manifestPath = dir.resolve(manifestName);

        java.util.List<com.hkg.rocksdb.manifest.VersionEdit> all = new java.util.ArrayList<>();
        try (com.hkg.rocksdb.wal.LogReader reader =
                 com.hkg.rocksdb.wal.LogReader.open(manifestPath)) {
            java.util.Optional<byte[]> next;
            while ((next = reader.readRecord()).isPresent()) {
                all.addAll(com.hkg.rocksdb.manifest.VersionEditCodec.decode(next.get()));
            }
        }
        java.util.List<com.hkg.rocksdb.manifest.VersionEdit> stripped = new java.util.ArrayList<>();
        for (com.hkg.rocksdb.manifest.VersionEdit e : all) {
            if (e instanceof com.hkg.rocksdb.manifest.VersionEdit.NewFile nf) {
                com.hkg.rocksdb.manifest.FileMetadata fm = nf.metadata();
                stripped.add(new com.hkg.rocksdb.manifest.VersionEdit.NewFile(
                    nf.level(),
                    new com.hkg.rocksdb.manifest.FileMetadata(
                        fm.fileNumber(), fm.sizeBytes(),
                        fm.smallestKey(), fm.largestKey(), null)));
            } else {
                stripped.add(e);
            }
        }
        try (com.hkg.rocksdb.wal.LogWriter w =
                 com.hkg.rocksdb.wal.LogWriter.open(manifestPath, true)) {
            // LogWriter.open truncates on existing — overwrites the original
            // MANIFEST with our rewritten (no-checksum) edit stream.
            w.append(com.hkg.rocksdb.manifest.VersionEditCodec.encode(stripped));
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
