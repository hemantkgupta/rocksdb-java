package com.hkg.rocksdb.sstable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FooterTest {

    @Test
    void encodeDecodeRoundTrip() {
        Footer original = new Footer(
            new BlockHandle(1024L, 256L),
            new BlockHandle(2048L, 512L));
        byte[] bytes = original.encode();
        assertThat(bytes).hasSize(Footer.ENCODED_LENGTH);
        Footer decoded = Footer.decode(bytes);
        assertThat(decoded.metaIndexHandle()).isEqualTo(original.metaIndexHandle());
        assertThat(decoded.indexHandle()).isEqualTo(original.indexHandle());
    }

    @Test
    void magicMismatchThrows() {
        Footer original = new Footer(new BlockHandle(1L, 1L), new BlockHandle(2L, 2L));
        byte[] bytes = original.encode();
        // Corrupt magic.
        bytes[bytes.length - 1] ^= 0x01;
        assertThatThrownBy(() -> Footer.decode(bytes))
            .isInstanceOf(SsTableFormatException.class)
            .hasMessageContaining("magic");
    }

    @Test
    void wrongLengthThrows() {
        assertThatThrownBy(() -> Footer.decode(new byte[10]))
            .isInstanceOf(SsTableFormatException.class);
    }
}
