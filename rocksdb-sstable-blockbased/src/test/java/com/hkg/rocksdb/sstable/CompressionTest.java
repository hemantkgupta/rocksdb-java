package com.hkg.rocksdb.sstable;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressionTest {

    @Test
    void deflateRoundTrip() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("the quick brown fox jumps over the lazy dog ");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Compression.compressDeflate(data);
        assertThat(compressed.length).isLessThan(data.length);
        byte[] restored = Compression.decompressDeflate(compressed);
        assertThat(restored).isEqualTo(data);
    }

    @Test
    void decodeNoneIsIdentity() {
        byte[] data = "raw".getBytes();
        assertThat(Compression.decode(Compression.TYPE_NONE, data)).isEqualTo(data);
    }

    @Test
    void unknownCompressionTypeThrows() {
        assertThatThrownBy(() -> Compression.decode((byte) 0x77, new byte[] {0}))
            .isInstanceOf(SsTableFormatException.class);
    }
}
