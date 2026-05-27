package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.manifest.ManifestCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ManifestDump}. Each test opens a real engine, exercises a
 * different shape of write workload, and asserts that the dumped MANIFEST
 * contains the expected tagged edit lines.
 */
class ManifestDumpTest {

    @Test
    void emptyDbDumpsAtLeastTheInitialBootstrapEdits(@TempDir Path dir) throws IOException {
        // Just opening the engine writes SetLogNumber/SetNextFileNumber/SetLastSequence,
        // both at createFresh and again at first open.
        try (RocksDb db = RocksDb.open(dir)) {
            // no writes
        }
        String text = dumpToString(dir);
        assertThat(text).isNotBlank();
        assertThat(text).contains("SetLogNumber");
        assertThat(text).contains("SetNextFileNumber");
        assertThat(text).contains("SetLastSequence");
    }

    @Test
    void putsAndFlushProduceNewFileEdits(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 32; i++) {
                db.put(Key.of("k-" + i), Slice.of("v-" + i));
            }
            db.flush();
        }
        String text = dumpToString(dir);
        assertThat(text).contains("NewFile L0");
        assertThat(text).contains("smallest=");
        assertThat(text).contains("largest=");
        assertThat(text).contains("checksum=0x");
    }

    @Test
    void compactionProducesDeleteFileEdits(@TempDir Path dir) throws IOException {
        // Force several L0 files, then compact — the result should include
        // at least one DeleteFile edit referencing the inputs.
        try (RocksDb db = RocksDb.open(dir)) {
            for (int batch = 0; batch < 6; batch++) {
                for (int i = 0; i < 50; i++) {
                    db.put(Key.of(String.format("k-%04d-%04d", batch, i)),
                        Slice.of("v-padded-bytes-" + i));
                }
                db.flush();
            }
            // Compact until there's nothing to do.
            int passes = 0;
            while (db.maybeCompact() && passes < 16) passes++;
        }
        String text = dumpToString(dir);
        assertThat(text).contains("NewFile L0");
        assertThat(text).contains("DeleteFile L0");
    }

    @Test
    void everyLineStartsWithAKnownTag(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 8; i++) db.put(Key.of("k" + i), Slice.of("v" + i));
            db.flush();
        }
        String text = dumpToString(dir);
        for (String line : text.split("\\R")) {
            if (line.isBlank()) continue; // batch separator
            assertThat(line).matches(
                "^(NewFile|DeleteFile|SetLogNumber|SetNextFileNumber|SetLastSequence|UnknownEdit) .*");
        }
    }

    @Test
    void missingCurrentThrowsManifestCorruption(@TempDir Path dir) {
        // No CURRENT yet — never opened the engine here.
        assertThatThrownBy(() -> dumpToString(dir))
            .isInstanceOf(ManifestCorruptionException.class);
    }

    private static String dumpToString(Path dir) throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ManifestDump.run(dir, pw);
        pw.flush();
        return sw.toString();
    }
}
