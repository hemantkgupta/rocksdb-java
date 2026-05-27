package com.hkg.rocksdb.common;

import java.util.Objects;

/**
 * A range tombstone: every user key in {@code [startKey, endKey)} is logically
 * deleted as of {@link #sequence sequence}. The end key is EXCLUSIVE, matching
 * RocksDB and the SQL convention.
 *
 * <p>Range tombstones are CP 17's faithful-to-RocksDB representation of
 * {@code DeleteRange(start, end)}. One record covers an arbitrary number of
 * user keys; the alternative — expanding into N point tombstones at write
 * time — is exactly what DeleteRange was designed to avoid (see ADR 0009 and
 * §6.6 of the implementation plan).
 *
 * <p>Semantics under a snapshot read at {@code asOf}:
 * <ul>
 *   <li>A range tombstone with {@code seq ≤ asOf} that
 *       {@link #covers(Key) covers} a user key suppresses every Put for that
 *       key with {@code seq ≤ asOf AND seq < tombstone.seq}.</li>
 *   <li>A Put with {@code seq > tombstone.seq} wins (the writer beats the
 *       earlier deleter), preserving the FAST 2021 §6.6 failure-mode
 *       contract: {@code DeleteRange + later Put → Put visible}.</li>
 * </ul>
 */
public record RangeTombstone(Key startKey, Key endKey, SequenceNumber sequence) {

    public RangeTombstone {
        Objects.requireNonNull(startKey, "startKey");
        Objects.requireNonNull(endKey, "endKey");
        Objects.requireNonNull(sequence, "sequence");
        if (startKey.compareTo(endKey) >= 0) {
            throw new IllegalArgumentException(
                "startKey must be strictly less than endKey (got start=" + startKey
                    + ", end=" + endKey + ")");
        }
    }

    /** True iff {@code userKey} falls in {@code [startKey, endKey)}. */
    public boolean covers(Key userKey) {
        Objects.requireNonNull(userKey, "userKey");
        return startKey.compareTo(userKey) <= 0 && userKey.compareTo(endKey) < 0;
    }
}
