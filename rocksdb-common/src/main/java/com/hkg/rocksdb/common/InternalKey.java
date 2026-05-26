package com.hkg.rocksdb.common;

import java.util.Objects;

/**
 * The RocksDb internal key: {@code (userKey, sequence, type)}. Ordering
 * mirrors RocksDb: equivalent to comparing the packed 8-byte trailer
 * {@code ((seq << 8) | type)} in DESCENDING order — that is,
 * {@code (userKey ASC, sequence DESC, type DESC)}. The DESC tie-break on
 * type is load-bearing for the read path's probe construction: a snapshot
 * lookup at sequence {@code S} probes {@code (userKey, S, VALUE)}, and
 * with DESC tag ordering a same-sequence tombstone sorts AFTER the probe,
 * so {@code ceilingEntry} correctly returns the tombstone if one exists.
 */
public record InternalKey(Key userKey, SequenceNumber sequence, ValueType type)
    implements Comparable<InternalKey> {

    public InternalKey {
        Objects.requireNonNull(userKey, "userKey");
        Objects.requireNonNull(sequence, "sequence");
        Objects.requireNonNull(type, "type");
    }

    @Override
    public int compareTo(InternalKey other) {
        int c = this.userKey.compareTo(other.userKey);
        if (c != 0) return c;
        int s = Long.compare(other.sequence.value(), this.sequence.value());
        if (s != 0) return s;
        return Byte.compare(other.type.tag(), this.type.tag());
    }
}
