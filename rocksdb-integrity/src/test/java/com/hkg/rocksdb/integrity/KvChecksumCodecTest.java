package com.hkg.rocksdb.integrity;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KvChecksumCodecTest {

    @Test
    void wrapThenUnwrapRoundtripsValue() {
        byte[] key = "alpha".getBytes();
        byte[] value = "the quick brown fox".getBytes();
        byte[] wrapped = KvChecksumCodec.wrap(key, value);
        byte[] back = KvChecksumCodec.unwrap(key, wrapped);
        assertThat(back).isEqualTo(value);
        assertThat(wrapped.length).isEqualTo(value.length + 4);
    }

    @Test
    void wrappedEmptyValueRoundtrips() {
        byte[] key = "k".getBytes();
        byte[] empty = new byte[0];
        byte[] wrapped = KvChecksumCodec.wrap(key, empty);
        assertThat(KvChecksumCodec.unwrap(key, wrapped)).isEqualTo(empty);
    }

    @Test
    void byteFlipInValueDetected() {
        byte[] key = "k".getBytes();
        byte[] value = "some-bytes".getBytes();
        byte[] wrapped = KvChecksumCodec.wrap(key, value);
        // Flip a bit in the value region.
        wrapped[3] ^= 0x40;
        assertThatThrownBy(() -> KvChecksumCodec.unwrap(key, wrapped))
            .isInstanceOf(KvChecksumMismatchException.class)
            .hasMessageContaining("stored=");
    }

    @Test
    void byteFlipInTrailingChecksumDetected() {
        byte[] key = "k".getBytes();
        byte[] value = "some-bytes".getBytes();
        byte[] wrapped = KvChecksumCodec.wrap(key, value);
        // Flip a bit in the trailing CRC.
        wrapped[wrapped.length - 1] ^= 0x01;
        assertThatThrownBy(() -> KvChecksumCodec.unwrap(key, wrapped))
            .isInstanceOf(KvChecksumMismatchException.class);
    }

    @Test
    void differentKeyMismatches() {
        byte[] wrapped = KvChecksumCodec.wrap("right-key".getBytes(), "v".getBytes());
        assertThatThrownBy(() -> KvChecksumCodec.unwrap("wrong-key".getBytes(), wrapped))
            .isInstanceOf(KvChecksumMismatchException.class);
    }

    @Test
    void shortBufferRejected() {
        // 3 bytes < CHECKSUM_BYTES (4) → must throw.
        assertThatThrownBy(() -> KvChecksumCodec.unwrap("k".getBytes(), new byte[3]))
            .isInstanceOf(KvChecksumMismatchException.class)
            .hasMessageContaining("too short");
    }

    @Test
    void nullWrappedRejected() {
        assertThatThrownBy(() -> KvChecksumCodec.unwrap("k".getBytes(), null))
            .isInstanceOf(KvChecksumMismatchException.class);
    }

    @Test
    void largeValueRoundtrips() {
        byte[] key = "k".getBytes();
        byte[] big = new byte[1 << 16];
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i & 0xFF);
        byte[] wrapped = KvChecksumCodec.wrap(key, big);
        assertThat(KvChecksumCodec.unwrap(key, wrapped)).isEqualTo(big);
    }

    @Test
    void sameKeyAndValueProduceSameChecksum() {
        // CRC32 is deterministic — same (key, value) always yields same wrapped bytes.
        byte[] w1 = KvChecksumCodec.wrap("k".getBytes(), "v".getBytes());
        byte[] w2 = KvChecksumCodec.wrap("k".getBytes(), "v".getBytes());
        assertThat(w1).isEqualTo(w2);
    }
}
