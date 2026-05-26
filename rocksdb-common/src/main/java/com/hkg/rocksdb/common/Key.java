package com.hkg.rocksdb.common;

import java.util.Arrays;
import java.util.Objects;

/**
 * A user-supplied key. Wraps a {@link Slice}; ordering is lexicographic
 * over the windowed bytes. Equality is value-based (the windowed bytes).
 */
public final class Key implements Comparable<Key> {

    private final Slice slice;

    private Key(Slice slice) {
        this.slice = Objects.requireNonNull(slice, "slice");
    }

    public Slice slice() { return slice; }
    public int length() { return slice.length(); }
    public byte[] bytes() { return slice.toBytes(); }

    public static Key of(byte[] data) {
        return new Key(Slice.of(data));
    }

    public static Key of(String s) {
        return new Key(Slice.of(s));
    }

    public static Key of(Slice slice) {
        return new Key(slice);
    }

    /** Lexicographic byte comparison over the slice window. */
    @Override
    public int compareTo(Key other) {
        Objects.requireNonNull(other, "other");
        int n = Math.min(this.slice.length(), other.slice.length());
        for (int i = 0; i < n; i++) {
            int a = this.slice.get(i) & 0xff;
            int b = other.slice.get(i) & 0xff;
            if (a != b) return a - b;
        }
        return this.slice.length() - other.slice.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key other)) return false;
        return this.slice.equals(other.slice);
    }

    @Override
    public int hashCode() {
        return slice.hashCode();
    }

    @Override
    public String toString() {
        return "Key(" + Arrays.toString(slice.toBytes()) + ")";
    }
}
