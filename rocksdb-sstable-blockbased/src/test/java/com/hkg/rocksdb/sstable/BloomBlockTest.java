package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.bloom.BloomFilter;
import com.hkg.rocksdb.common.Key;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BloomBlockTest {

    @Test
    void encodeDecodeRoundTrip() {
        BloomFilter.Builder builder = new BloomFilter.Builder();
        for (int i = 0; i < 100; i++) {
            builder.add(Key.of("k-" + i));
        }
        BloomFilter original = builder.build();
        byte[] encoded = BloomBlock.encode(original);
        BloomFilter decoded = BloomBlock.decode(encoded);

        // Every key originally added must still test positive.
        for (int i = 0; i < 100; i++) {
            assertThat(decoded.mightContain(Key.of("k-" + i))).isTrue();
        }
        assertThat(decoded.numBits()).isEqualTo(original.numBits());
        assertThat(decoded.numHashes()).isEqualTo(original.numHashes());
        assertThat(decoded.rawBits()).isEqualTo(original.rawBits());
    }
}
