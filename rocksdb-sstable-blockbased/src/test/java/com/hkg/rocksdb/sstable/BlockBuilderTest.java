package com.hkg.rocksdb.sstable;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class BlockBuilderTest {

    @Test
    void roundTripsSingleEntry() {
        BlockBuilder b = new BlockBuilder();
        b.add("hello".getBytes(StandardCharsets.UTF_8), "world".getBytes(StandardCharsets.UTF_8));
        byte[] raw = b.finish();
        Block block = new Block(raw);
        assertThat(block.size()).isEqualTo(1);
        assertThat(block.get(0).key()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(block.get(0).value()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void roundTripsManyEntriesWithSharedPrefix() {
        BlockBuilder b = new BlockBuilder();
        for (int i = 0; i < 100; i++) {
            byte[] k = String.format("key-%05d", i).getBytes(StandardCharsets.UTF_8);
            byte[] v = String.format("val-%05d", i).getBytes(StandardCharsets.UTF_8);
            b.add(k, v);
        }
        Block block = new Block(b.finish());
        assertThat(block.size()).isEqualTo(100);
        for (int i = 0; i < 100; i++) {
            String expectedKey = String.format("key-%05d", i);
            String expectedVal = String.format("val-%05d", i);
            assertThat(new String(block.get(i).key(), StandardCharsets.UTF_8)).isEqualTo(expectedKey);
            assertThat(new String(block.get(i).value(), StandardCharsets.UTF_8)).isEqualTo(expectedVal);
        }
    }

    @Test
    void seekIndexBinarySearch() {
        BlockBuilder b = new BlockBuilder();
        for (int i = 0; i < 32; i++) {
            byte[] k = new byte[] {(byte) (2 * i)};
            byte[] v = new byte[] {(byte) i};
            b.add(k, v);
        }
        Block block = new Block(b.finish());
        // Look for an even key that exists.
        int idx = block.seekIndex(new byte[] {10}, BlockBuilderTest::lex);
        assertThat(idx).isEqualTo(5);
        // Look for an odd key — should round up to the next even.
        idx = block.seekIndex(new byte[] {11}, BlockBuilderTest::lex);
        assertThat(block.get(idx).key()[0]).isEqualTo((byte) 12);
        // Look for a key past the end.
        idx = block.seekIndex(new byte[] {(byte) 100}, BlockBuilderTest::lex);
        assertThat(idx).isEqualTo(-1);
    }

    @Test
    void emptyBuilderFinishProducesValidBlock() {
        BlockBuilder b = new BlockBuilder();
        byte[] raw = b.finish();
        Block block = new Block(raw);
        assertThat(block.size()).isEqualTo(0);
    }

    @Test
    void crossesRestartIntervalBoundary() {
        BlockBuilder b = new BlockBuilder();
        // Push past the RESTART_INTERVAL = 16 boundary.
        int n = BlockBuilder.RESTART_INTERVAL * 3 + 5;
        for (int i = 0; i < n; i++) {
            byte[] k = String.format("k%06d", i).getBytes(StandardCharsets.UTF_8);
            b.add(k, new byte[] {(byte) i});
        }
        Block block = new Block(b.finish());
        assertThat(block.size()).isEqualTo(n);
        for (int i = 0; i < n; i++) {
            assertThat(new String(block.get(i).key(), StandardCharsets.UTF_8))
                .isEqualTo(String.format("k%06d", i));
            assertThat(block.get(i).value()[0]).isEqualTo((byte) i);
        }
    }

    static int lex(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xff) - (b[i] & 0xff);
            if (d != 0) return d;
        }
        return a.length - b.length;
    }
}
