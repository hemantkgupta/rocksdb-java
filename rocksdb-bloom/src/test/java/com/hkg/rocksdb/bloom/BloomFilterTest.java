package com.hkg.rocksdb.bloom;

import com.hkg.rocksdb.common.Key;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BloomFilterTest {

    @Test
    void emptyFilter_neverContains() {
        BloomFilter bf = new BloomFilter.Builder().build();
        assertThat(bf.mightContain(Key.of("anything"))).isFalse();
    }

    @Test
    void addedKey_alwaysMightContain() {
        BloomFilter.Builder b = new BloomFilter.Builder();
        for (int i = 0; i < 1000; i++) {
            b.add(Key.of("key-" + i));
        }
        BloomFilter bf = b.build();
        for (int i = 0; i < 1000; i++) {
            assertThat(bf.mightContain(Key.of("key-" + i)))
                .as("key-%d added", i)
                .isTrue();
        }
    }

    @Test
    void falsePositiveRate_approximately1Percent_atDefault10BitsPerKey() {
        // Build with 10,000 keys; query 100,000 keys that are NOT in the filter.
        // At 10 bits/key default, FPR should be ~1%.
        int numAdded = 10_000;
        int numProbes = 100_000;
        BloomFilter.Builder b = new BloomFilter.Builder();
        for (int i = 0; i < numAdded; i++) {
            b.add(Key.of("present-" + i));
        }
        BloomFilter bf = b.build();

        int falsePositives = 0;
        for (int i = 0; i < numProbes; i++) {
            if (bf.mightContain(Key.of("absent-" + i))) {
                falsePositives++;
            }
        }
        double fpr = (double) falsePositives / numProbes;
        // Allow some slack — the FPR for FNV-1a + double-hashing won't be exactly 1%
        // but should be in the same order of magnitude.
        assertThat(fpr).isLessThan(0.05); // 5% slack
        assertThat(fpr).isGreaterThan(0.001); // confirm the filter isn't trivially perfect
    }

    @Test
    void higherBitsPerKey_lowerFPR() {
        int numAdded = 5_000;
        int numProbes = 50_000;
        BloomFilter lowQuality = buildAndProbe(5,  numAdded, numProbes);
        BloomFilter highQuality = buildAndProbe(20, numAdded, numProbes);
        double lowFpr  = measureFpr(lowQuality, numAdded, numProbes);
        double highFpr = measureFpr(highQuality, numAdded, numProbes);
        assertThat(highFpr).isLessThan(lowFpr);
    }

    @Test
    void differentKeysProduceDifferentHashes() {
        // Sanity test the hash function: at least some bits differ between two distinct keys.
        int h1 = BloomFilter.hash32("foo".getBytes());
        int h2 = BloomFilter.hash32("bar".getBytes());
        assertThat(h1).isNotEqualTo(h2);
    }

    @Test
    void builder_rejectsInvalidBitsPerKey() {
        assertThatThrownBy(() -> new BloomFilter.Builder(0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BloomFilter.Builder(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void numHashesCappedAt30() {
        BloomFilter bf = new BloomFilter.Builder(1000).add(Key.of("k")).build();
        assertThat(bf.numHashes()).isLessThanOrEqualTo(30);
    }

    @Test
    void serialisedRoundTrip_viaRawBits() {
        BloomFilter.Builder b = new BloomFilter.Builder();
        Set<String> keys = new HashSet<>();
        Random r = new Random(123L);
        for (int i = 0; i < 200; i++) {
            String k = "key-" + r.nextInt();
            keys.add(k);
            b.add(Key.of(k));
        }
        BloomFilter bf = b.build();

        // "Serialise": just grab rawBits + numBits + numHashes.
        byte[] bits = bf.rawBits();
        int numBits = bf.numBits();
        int numHashes = bf.numHashes();

        // "Deserialise": reconstruct via package-private constructor.
        BloomFilter rebuilt = new BloomFilter(bits, numBits, numHashes);
        for (String k : keys) {
            assertThat(rebuilt.mightContain(Key.of(k))).isTrue();
        }
    }

    // ---------------- helpers ----------------

    private static BloomFilter buildAndProbe(int bitsPerKey, int numAdded, int numProbes) {
        BloomFilter.Builder b = new BloomFilter.Builder(bitsPerKey);
        for (int i = 0; i < numAdded; i++) {
            b.add(Key.of("p-" + bitsPerKey + "-" + i));
        }
        return b.build();
    }

    private static double measureFpr(BloomFilter bf, int numAdded, int numProbes) {
        int fp = 0;
        for (int i = 0; i < numProbes; i++) {
            // Query keys not added (different prefix).
            if (bf.mightContain(Key.of("absent-" + i))) {
                fp++;
            }
        }
        return (double) fp / numProbes;
    }
}
