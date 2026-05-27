package com.hkg.rocksdb.integrity;

import java.util.zip.Checksum;

/**
 * Pure-Java port of the XXH64 streaming hash (Yann Collet,
 * <a href="https://github.com/Cyan4973/xxHash">Cyan4973/xxHash</a>). XXH64
 * is the file-level checksum RocksDB uses to detect whole-file corruption
 * outside any individual block — the §5 "above-block" integrity layer.
 *
 * <p>The algorithm processes input in 32-byte stripes through four
 * independent lane accumulators, then folds them into a 64-bit digest
 * with an avalanche step. Tail bytes are merged with the same constants
 * as the reference implementation, so a digest produced here is bit-
 * identical to {@code XXH64()} from the C library.
 *
 * <p>Implements {@link Checksum} so it can be used with
 * {@link java.util.zip.CheckedInputStream}; {@link #getValue()} and
 * {@link #digest()} are equivalent.
 *
 * <p>Single-shot via {@link #hash(byte[])}; streaming via repeated
 * {@link #update(byte[], int, int)} followed by {@link #digest()}.
 */
public final class XxHash64 implements Checksum {

    // Reference XXH64 prime constants (from xxhash.h).
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private static final int STRIPE_BYTES = 32;

    private final long seed;
    private long v1, v2, v3, v4;
    private long totalLen;
    private final byte[] tail = new byte[STRIPE_BYTES];
    private int tailLen;

    public XxHash64() {
        this(0L);
    }

    public XxHash64(long seed) {
        this.seed = seed;
        reset();
    }

    /** Single-shot helper — equivalent to {@code new XxHash64().update(data,0,len).digest()}. */
    public static long hash(byte[] data) {
        return hash(data, 0, data.length, 0L);
    }

    public static long hash(byte[] data, int off, int len, long seed) {
        XxHash64 h = new XxHash64(seed);
        h.update(data, off, len);
        return h.digest();
    }

    @Override
    public void reset() {
        v1 = seed + PRIME64_1 + PRIME64_2;
        v2 = seed + PRIME64_2;
        v3 = seed;
        v4 = seed - PRIME64_1;
        totalLen = 0L;
        tailLen = 0;
    }

    @Override
    public void update(int b) {
        byte by = (byte) (b & 0xff);
        update(new byte[] {by}, 0, 1);
    }

    @Override
    public void update(byte[] buf, int off, int len) {
        if (buf == null) throw new NullPointerException("buf");
        if (off < 0 || len < 0 || off + len > buf.length) {
            throw new IndexOutOfBoundsException("off=" + off + " len=" + len
                + " bufLen=" + buf.length);
        }
        if (len == 0) return;

        totalLen += len;
        int p = off;
        int end = off + len;

        // 1) If we have a partial stripe buffered, fill it first.
        if (tailLen > 0) {
            int need = STRIPE_BYTES - tailLen;
            if (len < need) {
                System.arraycopy(buf, p, tail, tailLen, len);
                tailLen += len;
                return;
            }
            System.arraycopy(buf, p, tail, tailLen, need);
            consumeStripe(tail, 0);
            p += need;
            tailLen = 0;
        }

        // 2) Consume full 32-byte stripes directly from the input.
        int stripeEnd = end - STRIPE_BYTES;
        while (p <= stripeEnd) {
            consumeStripe(buf, p);
            p += STRIPE_BYTES;
        }

        // 3) Buffer the leftover tail.
        if (p < end) {
            tailLen = end - p;
            System.arraycopy(buf, p, tail, 0, tailLen);
        }
    }

    private void consumeStripe(byte[] data, int off) {
        v1 = round(v1, readLongLe(data, off));
        v2 = round(v2, readLongLe(data, off + 8));
        v3 = round(v3, readLongLe(data, off + 16));
        v4 = round(v4, readLongLe(data, off + 24));
    }

    /** Returns the 64-bit digest. Idempotent — does not advance the stream. */
    public long digest() {
        long h64;
        if (totalLen >= STRIPE_BYTES) {
            h64 = Long.rotateLeft(v1, 1)
                + Long.rotateLeft(v2, 7)
                + Long.rotateLeft(v3, 12)
                + Long.rotateLeft(v4, 18);
            h64 = mergeRound(h64, v1);
            h64 = mergeRound(h64, v2);
            h64 = mergeRound(h64, v3);
            h64 = mergeRound(h64, v4);
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += totalLen;

        // Mix tail bytes.
        int p = 0;
        int rem = tailLen;
        while (rem >= 8) {
            long k1 = round(0L, readLongLe(tail, p));
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            p += 8;
            rem -= 8;
        }
        if (rem >= 4) {
            h64 ^= (readIntLe(tail, p) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            p += 4;
            rem -= 4;
        }
        while (rem > 0) {
            h64 ^= (tail[p] & 0xFFL) * PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
            p++;
            rem--;
        }

        // Final avalanche.
        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;
        return h64;
    }

    @Override
    public long getValue() {
        return digest();
    }

    private static long round(long acc, long input) {
        acc += input * PRIME64_2;
        acc = Long.rotateLeft(acc, 31);
        acc *= PRIME64_1;
        return acc;
    }

    private static long mergeRound(long acc, long val) {
        val = round(0L, val);
        acc ^= val;
        acc = acc * PRIME64_1 + PRIME64_4;
        return acc;
    }

    private static long readLongLe(byte[] b, int off) {
        return ((long) (b[off] & 0xff))
            | (((long) (b[off + 1] & 0xff)) << 8)
            | (((long) (b[off + 2] & 0xff)) << 16)
            | (((long) (b[off + 3] & 0xff)) << 24)
            | (((long) (b[off + 4] & 0xff)) << 32)
            | (((long) (b[off + 5] & 0xff)) << 40)
            | (((long) (b[off + 6] & 0xff)) << 48)
            | (((long) (b[off + 7] & 0xff)) << 56);
    }

    private static int readIntLe(byte[] b, int off) {
        return (b[off] & 0xff)
            | ((b[off + 1] & 0xff) << 8)
            | ((b[off + 2] & 0xff) << 16)
            | ((b[off + 3] & 0xff) << 24);
    }
}
