package com.hkg.rocksdb.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * The RocksDb Slice equivalent: a (backing array, offset, length) window
 * over a byte array. Value equality compares the windowed bytes.
 *
 * <p>The backing array MAY be shared across multiple Slices; callers must
 * not mutate it after handing it to a Slice. This mirrors the RocksDb
 * C++ design where {@code Slice} is a pointer + length and ownership of
 * the underlying bytes is the caller's responsibility.
 */
public record Slice(byte[] backing, int offset, int length) {

    public Slice {
        Objects.requireNonNull(backing, "backing");
        if (offset < 0 || length < 0 || offset + length > backing.length) {
            throw new IllegalArgumentException(
                "invalid slice: backing.length=" + backing.length
                    + " offset=" + offset + " length=" + length);
        }
    }

    /** Return the byte at the given index, bounds-checked against this slice. */
    public byte get(int i) {
        if (i < 0 || i >= length) {
            throw new IndexOutOfBoundsException("index " + i + " not in [0, " + length + ")");
        }
        return backing[offset + i];
    }

    /** Return a fresh byte[] copy of this slice's window. */
    public byte[] toBytes() {
        return Arrays.copyOfRange(backing, offset, offset + length);
    }

    /** Construct a Slice covering the full byte[] (no defensive copy). */
    public static Slice of(byte[] data) {
        Objects.requireNonNull(data, "data");
        return new Slice(data, 0, data.length);
    }

    /** Construct a Slice from a UTF-8 string. */
    public static Slice of(String s) {
        Objects.requireNonNull(s, "s");
        return Slice.of(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Slice other)) return false;
        if (this.length != other.length) return false;
        return Arrays.equals(
            this.backing, this.offset, this.offset + this.length,
            other.backing, other.offset, other.offset + other.length);
    }

    @Override
    public int hashCode() {
        int h = 1;
        for (int i = 0; i < length; i++) {
            h = 31 * h + backing[offset + i];
        }
        return h;
    }

    @Override
    public String toString() {
        return "Slice(len=" + length + ")";
    }
}
