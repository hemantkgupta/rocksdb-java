package com.hkg.rocksdb.integrity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class FileChecksumTest {

    @Test
    void computeMatchesSingleShotHashOfFileBytes(@TempDir Path dir) throws IOException {
        byte[] data = "the quick brown fox jumps over the lazy dog".getBytes();
        Path f = dir.resolve("data.bin");
        Files.write(f, data);

        FileChecksum fc = FileChecksum.compute(f);
        assertThat(fc.value()).isEqualTo(XxHash64.hash(data));
    }

    @Test
    void computeStreamsFilesLargerThanBufferCorrectly(@TempDir Path dir) throws IOException {
        // 400 KiB — exceeds the 64 KiB internal read buffer, must accumulate
        // across multiple read() calls without resetting state.
        byte[] data = new byte[400 * 1024];
        new Random(0xDEADBEEFL).nextBytes(data);
        Path f = dir.resolve("big.bin");
        Files.write(f, data);

        FileChecksum fc = FileChecksum.compute(f);
        assertThat(fc.value()).isEqualTo(XxHash64.hash(data));
    }

    @Test
    void emptyFileMatchesEmptyHash(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("empty.bin");
        Files.write(f, new byte[0]);

        FileChecksum fc = FileChecksum.compute(f);
        assertThat(fc.value()).isEqualTo(XxHash64.hash(new byte[0]));
    }

    @Test
    void toStringContainsHexValue() {
        FileChecksum fc = new FileChecksum(0xCAFEL);
        assertThat(fc.toString()).contains("cafe");
    }

    @Test
    void mismatchExceptionMessagePropagates() {
        FileChecksumMismatchException ex =
            new FileChecksumMismatchException("stored=1 actual=2");
        assertThat(ex.getMessage()).contains("stored=").contains("actual=");
    }
}
