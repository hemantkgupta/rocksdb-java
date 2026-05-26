package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConstantsTest {

    @Test
    void memtableThreshold4MiB() {
        assertThat(Constants.MEMTABLE_FLUSH_THRESHOLD_BYTES).isEqualTo(4 * 1024 * 1024);
    }

    @Test
    void blockSize4KiB() {
        assertThat(Constants.BLOCK_SIZE_BYTES).isEqualTo(4 * 1024);
    }

    @Test
    void levelFanOutIsTen() {
        assertThat(Constants.LEVEL_SIZE_MULTIPLIER).isEqualTo(10);
    }

    @Test
    void l0Trigger4Files() {
        assertThat(Constants.L0_FILE_COUNT_TRIGGER).isEqualTo(4);
    }

    @Test
    void bloomBitsPerKey10() {
        assertThat(Constants.BLOOM_BITS_PER_KEY).isEqualTo(10);
    }

    @Test
    void blockCacheDefault8MiB() {
        assertThat(Constants.BLOCK_CACHE_DEFAULT_BYTES).isEqualTo(8L * 1024 * 1024);
    }

    @Test
    void maxSequenceNumberIs56Bits() {
        assertThat(Constants.MAX_SEQUENCE_NUMBER).isEqualTo((1L << 56) - 1L);
    }

    @Test
    void walSyncDefaultsTrue() {
        assertThat(Constants.WAL_SYNC_DEFAULT).isTrue();
    }

    @Test
    void compressionIsDeflate() {
        assertThat(Constants.COMPRESSION_TYPE_DEFLATE).isEqualTo("deflate");
    }
}
