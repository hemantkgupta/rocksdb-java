package com.hkg.rocksdb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CrcCorruptionTest {

    @Test
    void midFileCorruption_throws(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("first".getBytes());
            writer.append("second".getBytes());
            writer.append("third".getBytes());
        }

        // Flip a byte inside the payload of the first record (after the 7-byte header).
        try (RandomAccessFile raf = new RandomAccessFile(log.toFile(), "rw")) {
            raf.seek(WalConstants.HEADER_SIZE + 1); // first record's payload, second byte
            int b = raf.read();
            raf.seek(WalConstants.HEADER_SIZE + 1);
            raf.write(b ^ 0xff);
        }

        // The reader is reading at a position with more valid records after — so CRC mismatch
        // is real corruption, not torn-write. Must throw.
        try (LogReader reader = LogReader.open(log)) {
            assertThatThrownBy(reader::readRecord)
                .isInstanceOf(WalCorruptionException.class)
                .hasMessageContaining("CRC mismatch");
        }
    }
}
