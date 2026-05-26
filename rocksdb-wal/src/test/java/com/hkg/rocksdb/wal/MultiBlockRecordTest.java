package com.hkg.rocksdb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class MultiBlockRecordTest {

    @Test
    void recordLargerThanOneBlock_fragmentsAndReassembles(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        byte[] big = new byte[WalConstants.BLOCK_SIZE * 3 + 100]; // 3+ blocks
        Random r = new Random(42L);
        r.nextBytes(big);

        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append(big);
        }

        try (LogReader reader = LogReader.open(log)) {
            byte[] got = reader.readRecord().orElseThrow();
            assertThat(got).containsExactly(big);
            assertThat(reader.readRecord()).isEmpty();
        }
    }

    @Test
    void recordExactlyOneBlockMinusHeader_singleFullFragment(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        byte[] payload = new byte[WalConstants.BLOCK_SIZE - WalConstants.HEADER_SIZE];
        new Random(7L).nextBytes(payload);

        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append(payload);
        }

        try (LogReader reader = LogReader.open(log)) {
            byte[] got = reader.readRecord().orElseThrow();
            assertThat(got).containsExactly(payload);
        }
        // File should be exactly one block.
        assertThat(Files.size(log)).isEqualTo(WalConstants.BLOCK_SIZE);
    }

    @Test
    void manyMidSizedRecords_packingAndPadding(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        // Each record is 20 KiB — two records fit per 32 KiB block with leftover < HEADER_SIZE,
        // forcing zero-padding to next block.
        int recordSize = 20 * 1024;
        byte[] payload = new byte[recordSize];
        for (int i = 0; i < recordSize; i++) payload[i] = (byte) (i & 0xff);

        int numRecords = 20;
        try (LogWriter writer = LogWriter.open(log, false)) {
            for (int i = 0; i < numRecords; i++) {
                payload[0] = (byte) i; // distinguishable per-record
                writer.append(payload);
            }
            writer.sync();
        }

        try (LogReader reader = LogReader.open(log)) {
            int seen = 0;
            byte[] expected = new byte[recordSize];
            for (int i = 0; i < recordSize; i++) expected[i] = (byte) (i & 0xff);
            while (true) {
                byte[] got = reader.readRecord().orElse(null);
                if (got == null) break;
                expected[0] = (byte) seen;
                assertThat(got).containsExactly(expected);
                seen++;
            }
            assertThat(seen).isEqualTo(numRecords);
        }
    }
}
