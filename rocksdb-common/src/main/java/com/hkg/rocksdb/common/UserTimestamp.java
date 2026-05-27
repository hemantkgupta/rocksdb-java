package com.hkg.rocksdb.common;

/**
 * CP 27 — application-supplied 64-bit timestamp carried alongside the
 * engine-assigned {@link SequenceNumber}. Per ADR 0007 + FAST 2021 §6,
 * user-defined timestamps let the application order writes by an external
 * clock (commit timestamp, snapshot epoch, hybrid logical clock) while the
 * engine continues to use its monotonic sequence for internal recovery
 * ordering.
 *
 * <p>The engine treats user timestamps as opaque 8-byte big-endian values;
 * the application defines the meaning (Unix epoch, HLC, CockroachDB-style
 * 80-bit truncated to 64, etc.). The visibility predicate at read time is
 * {@code entry.userTs ≤ asOfTs}, where {@code asOfTs} is supplied by the
 * caller.
 *
 * <p>This minimal port stores user timestamps inline at the tail of the
 * stored key (8 bytes BE). A per-engine in-memory index tracks the set of
 * timestamps written per userKey so {@link com.hkg.rocksdb.engine.RocksDb#getAsOf
 * getAsOf} can find the floor without a scan API. The index is rebuilt at
 * open time by iterating every SSTable's entries — O(unique-keys ×
 * unique-timestamps) memory; acceptable for the pedagogical scope but a
 * production engine would persist a per-CF index sidecar.
 */
public record UserTimestamp(long value) implements Comparable<UserTimestamp> {

    /** Fixed-width on-disk encoding of a user timestamp. */
    public static final int ENCODED_LENGTH = 8;

    @Override
    public int compareTo(UserTimestamp other) {
        return Long.compare(this.value, other.value);
    }
}
