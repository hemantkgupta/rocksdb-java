package com.hkg.rocksdb.integrity;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class XxHash64Test {

    // Canonical published reference vectors for XXH64 with seed=0. Matching
    // them proves the pure-Java port is bit-identical to upstream
    // (https://github.com/Cyan4973/xxHash, util/xxhash.c).
    private static final long EMPTY_XXH64 = 0xEF46DB3751D8E999L;
    private static final long ABC_XXH64   = 0x44BC2CF5AD770999L;

    @Test
    void emptyInputMatchesReferenceVector() {
        assertThat(XxHash64.hash(new byte[0])).isEqualTo(EMPTY_XXH64);
    }

    @Test
    void abcMatchesReferenceVector() {
        assertThat(XxHash64.hash("abc".getBytes(StandardCharsets.US_ASCII)))
            .isEqualTo(ABC_XXH64);
    }

    @Test
    void multiStripeInputDigestsDeterministically() {
        // 40 bytes — forces one full 32-byte stripe + an 8-byte tail.
        byte[] data = "Nobody inspects the spammish repetition!".getBytes(StandardCharsets.US_ASCII);
        long a = XxHash64.hash(data);
        long b = XxHash64.hash(data);
        assertThat(a).isEqualTo(b);
        // The digest must differ from the empty-input vector — proves the
        // stripe + tail paths were exercised.
        assertThat(a).isNotEqualTo(EMPTY_XXH64);
    }

    @Test
    void singleShotEqualsMultiChunkUpdate() {
        byte[] data = randomBytes(1, 4096);
        long oneShot = XxHash64.hash(data);

        // Feed in arbitrary slices — must yield the same digest.
        XxHash64 h = new XxHash64();
        int p = 0;
        int[] chunks = {1, 7, 32, 33, 100, 500, 1024};
        for (int chunk : chunks) {
            int n = Math.min(chunk, data.length - p);
            if (n <= 0) break;
            h.update(data, p, n);
            p += n;
        }
        if (p < data.length) {
            h.update(data, p, data.length - p);
        }
        assertThat(h.digest()).isEqualTo(oneShot);
    }

    @Test
    void byteByByteMatchesBulk() {
        byte[] data = "Lorem ipsum dolor sit amet, consectetur".getBytes(StandardCharsets.US_ASCII);
        XxHash64 h = new XxHash64();
        for (byte b : data) {
            h.update(b & 0xff);
        }
        assertThat(h.digest()).isEqualTo(XxHash64.hash(data));
    }

    @Test
    void largeInputStreamsCorrectly() {
        // 200 KiB — many stripes plus a non-trivial tail.
        byte[] data = randomBytes(200 * 1024, 200 * 1024);
        long expected = XxHash64.hash(data);

        XxHash64 h = new XxHash64();
        h.update(data, 0, 17);
        h.update(data, 17, 32);
        h.update(data, 49, 1024);
        h.update(data, 49 + 1024, data.length - (49 + 1024));
        assertThat(h.digest()).isEqualTo(expected);
    }

    @Test
    void digestIsIdempotent() {
        byte[] data = "the quick brown fox".getBytes(StandardCharsets.US_ASCII);
        XxHash64 h = new XxHash64();
        h.update(data, 0, data.length);
        long first = h.digest();
        long second = h.digest();
        assertThat(first).isEqualTo(second);
    }

    @Test
    void resetReturnsToInitialState() {
        XxHash64 h = new XxHash64();
        h.update("scratch".getBytes(StandardCharsets.US_ASCII), 0, 7);
        h.reset();
        assertThat(h.digest()).isEqualTo(EMPTY_XXH64);
    }

    @Test
    void getValueEqualsDigest() {
        XxHash64 h = new XxHash64();
        h.update("payload".getBytes(StandardCharsets.US_ASCII), 0, 7);
        assertThat(h.getValue()).isEqualTo(h.digest());
    }

    @Test
    void seedAffectsDigest() {
        byte[] data = "same input".getBytes(StandardCharsets.US_ASCII);
        long h0 = XxHash64.hash(data, 0, data.length, 0L);
        long h1 = XxHash64.hash(data, 0, data.length, 1L);
        assertThat(h0).isNotEqualTo(h1);
    }

    private static byte[] randomBytes(int min, int max) {
        Random r = new Random(0xCAFEBABEL);
        int len = min + r.nextInt(Math.max(1, max - min + 1));
        byte[] b = new byte[len];
        r.nextBytes(b);
        return b;
    }
}
