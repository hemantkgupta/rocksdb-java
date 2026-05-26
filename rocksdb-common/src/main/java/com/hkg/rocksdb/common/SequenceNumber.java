package com.hkg.rocksdb.common;

/**
 * A 56-bit sequence number — RocksDb's per-instance monotonic write counter.
 *
 * <p>The 56-bit bound (max = 2^56 - 1) matches RocksDb's on-disk encoding,
 * which packs the sequence + 8-bit {@link ValueType} tag into a single 64-bit
 * word in the internal-key suffix.
 */
public record SequenceNumber(long value) implements Comparable<SequenceNumber> {

    public static final long MAX = (1L << 56) - 1L;
    public static final SequenceNumber ZERO = new SequenceNumber(0L);

    public SequenceNumber {
        if (value < 0L || value > MAX) {
            throw new IllegalArgumentException(
                "sequence number must be in [0, 2^56 - 1]; got " + value);
        }
    }

    /** Return prev + 1, throwing if it would exceed the 56-bit max. */
    public static SequenceNumber next(SequenceNumber prev) {
        return new SequenceNumber(prev.value + 1L);
    }

    @Override
    public int compareTo(SequenceNumber other) {
        return Long.compare(this.value, other.value);
    }
}
