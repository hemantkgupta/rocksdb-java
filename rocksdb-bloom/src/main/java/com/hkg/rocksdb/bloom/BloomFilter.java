package com.hkg.rocksdb.bloom;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;

import java.util.Objects;

/**
 * Per-SSTable bloom filter — RocksDb's read-amp mitigation for absent-key
 * lookups. A space-efficient probabilistic membership test; never false
 * negatives, occasional false positives. Default 10 bits/key gives ~1% FPR.
 *
 * <p>The implementation mirrors RocksDb's: a single bit array probed by
 * {@code k} hash functions derived from a single 32-bit hash by
 * double-hashing (RocksDb calls this technique "Kirsch–Mitzenmacher").
 *
 * <p>Filters are sized once and serialised as part of an SSTable's filter
 * meta-block. They are not mutable after creation; the writer accumulates
 * keys via {@link Builder#add(Key)} then calls {@link Builder#build()}.
 */
public final class BloomFilter {

    private final byte[] bits;       // bit array; (bits.length * 8) total bits
    private final int numBits;       // m
    private final int numHashes;     // k

    BloomFilter(byte[] bits, int numBits, int numHashes) {
        this.bits = Objects.requireNonNull(bits, "bits");
        this.numBits = numBits;
        this.numHashes = numHashes;
        if (numBits > bits.length * 8) {
            throw new IllegalArgumentException("numBits exceeds bit array capacity");
        }
        if (numHashes < 1 || numHashes > 30) {
            throw new IllegalArgumentException("numHashes must be in [1, 30]; got " + numHashes);
        }
    }

    /** True if the filter MAY contain the key; false if it definitely does not. */
    public boolean mightContain(Key key) {
        Objects.requireNonNull(key, "key");
        byte[] keyBytes = key.bytes();
        int hash = hash32(keyBytes);
        // double-hashing: derive numHashes positions from a single 32-bit hash
        int delta = (hash >>> 17) | (hash << 15); // rotate by 17
        int h = hash;
        for (int i = 0; i < numHashes; i++) {
            int bitPos = Math.floorMod(h, numBits);
            if ((bits[bitPos >>> 3] & (1 << (bitPos & 7))) == 0) {
                return false;
            }
            h += delta;
        }
        return true;
    }

    /** Number of bits in the filter (its capacity, not the count of set bits). */
    public int numBits() { return numBits; }

    /** Number of hash probes per query. */
    public int numHashes() { return numHashes; }

    /** Underlying bit array, not defensively copied — callers must not mutate. */
    public byte[] rawBits() { return bits; }

    /**
     * Reconstruct a BloomFilter from previously serialised parameters. The
     * SSTable writer stores {@code numHashes} + {@code numBits} alongside the
     * bit array in the filter meta-block.
     */
    public static BloomFilter fromRaw(byte[] bits, int numBits, int numHashes) {
        return new BloomFilter(bits, numBits, numHashes);
    }

    // ---------------- hashing ----------------

    /** A small, fast 32-bit hash (FNV-1a). RocksDb uses MurmurHash; FNV-1a is
     *  adequate for non-cryptographic membership testing and avoids importing a
     *  Murmur implementation. */
    static int hash32(byte[] data) {
        int h = (int) 0x811c9dc5;
        for (byte b : data) {
            h ^= (b & 0xff);
            h *= 0x01000193;
        }
        return h;
    }

    // ---------------- builder ----------------

    /**
     * Constructs a BloomFilter sized for an expected number of keys. Use
     * {@code build()} after adding all keys to materialise the filter.
     */
    public static final class Builder {

        private final int bitsPerKey;
        private final java.util.List<int[]> keyHashes = new java.util.ArrayList<>();

        public Builder() { this(Constants.BLOOM_BITS_PER_KEY); }

        public Builder(int bitsPerKey) {
            if (bitsPerKey < 1) {
                throw new IllegalArgumentException("bitsPerKey must be >= 1");
            }
            this.bitsPerKey = bitsPerKey;
        }

        public Builder add(Key key) {
            Objects.requireNonNull(key, "key");
            int hash = hash32(key.bytes());
            int delta = (hash >>> 17) | (hash << 15);
            keyHashes.add(new int[] {hash, delta});
            return this;
        }

        public BloomFilter build() {
            int n = keyHashes.size();
            // numHashes = bitsPerKey * ln 2 — capped at 30 to match RocksDb.
            int k = Math.max(1, Math.min(30, (int) Math.round(bitsPerKey * 0.6931)));
            // Total bits — at least 64 to avoid degenerate behaviour at very small n.
            int numBits = Math.max(64, n * bitsPerKey);
            int numBytes = (numBits + 7) / 8;
            byte[] bits = new byte[numBytes];

            for (int[] hd : keyHashes) {
                int h = hd[0];
                int delta = hd[1];
                for (int i = 0; i < k; i++) {
                    int bitPos = Math.floorMod(h, numBits);
                    bits[bitPos >>> 3] |= (byte) (1 << (bitPos & 7));
                    h += delta;
                }
            }
            return new BloomFilter(bits, numBits, k);
        }
    }
}
