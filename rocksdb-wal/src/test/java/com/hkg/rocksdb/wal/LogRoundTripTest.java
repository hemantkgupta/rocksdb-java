package com.hkg.rocksdb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LogRoundTripTest {

    @Test
    void emptyFile_readerReturnsNoRecords(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal-empty.log");
        Files.createFile(log);
        try (LogReader reader = LogReader.open(log)) {
            assertThat(reader.readRecord()).isEmpty();
        }
    }

    @Test
    void singleSmallRecord_roundTrip(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("hello".getBytes());
        }
        try (LogReader reader = LogReader.open(log)) {
            assertThat(reader.readRecord()).map(String::new).contains("hello");
            assertThat(reader.readRecord()).isEmpty();
        }
    }

    @Test
    void manySmallRecords_roundTripInOrder(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, false)) {
            for (int i = 0; i < 100; i++) {
                writer.append(("record-" + i).getBytes());
            }
            writer.sync();
        }
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).hasSize(100);
            for (int i = 0; i < 100; i++) {
                assertThat(seen.get(i)).isEqualTo("record-" + i);
            }
        }
    }

    @Test
    void emptyPayload_singleRoundTrip(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append(new byte[0]);
        }
        try (LogReader reader = LogReader.open(log)) {
            byte[] got = reader.readRecord().orElseThrow();
            assertThat(got).isEmpty();
        }
    }

    @Test
    void writerAppendsToExistingFile(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("first".getBytes());
        }
        try (LogWriter writer = LogWriter.openForAppend(log, true)) {
            writer.append("second".getBytes());
        }
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).containsExactly("first", "second");
        }
    }
}
