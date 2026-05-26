package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DbDumpTest {

    @Test
    void dumpEmitsHexKeyAndValuePerEntry(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("alpha"), Slice.of("ALPHA"));
            db.put(Key.of("beta"), Slice.of("BETA"));
            db.flush();
        }
        String text = dumpToString(dir);
        // hex of "alpha" = 616c706861; hex of "ALPHA" = 414c504841
        assertThat(text).contains("616c706861");
        assertThat(text).contains("414c504841");
        assertThat(text).contains("type=value");
        assertThat(text).contains("seq=");
    }

    @Test
    void dumpIncludesTombstoneEntries(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.delete(Key.of("k"));
            db.flush();
        }
        String text = dumpToString(dir);
        assertThat(text).contains("type=deletion");
    }

    @Test
    void dumpEmptyDbProducesNoLines(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            // No writes.
        }
        String text = dumpToString(dir);
        assertThat(text).isEmpty();
    }

    @Test
    void dumpEveryLineIsParseable(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            for (int i = 0; i < 32; i++) {
                db.put(Key.of("key-" + i), Slice.of("val-" + i));
            }
            db.flush();
        }
        String text = dumpToString(dir);
        for (String line : text.split("\\R")) {
            if (line.isBlank()) continue;
            // Format: L<level> <file>.sst <hexKey> <hexValue> seq=<n> type=<value|deletion>
            String[] parts = line.split(" ");
            assertThat(parts).withFailMessage("malformed: %s", line).hasSize(6);
            assertThat(parts[0]).startsWith("L");
            assertThat(parts[1]).endsWith(".sst");
            assertThat(parts[4]).startsWith("seq=");
            assertThat(parts[5]).startsWith("type=");
        }
    }

    private static String dumpToString(Path dir) throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        DbDump.run(dir, pw);
        pw.flush();
        return sw.toString();
    }
}
