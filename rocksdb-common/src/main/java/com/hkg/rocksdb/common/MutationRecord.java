package com.hkg.rocksdb.common;

/**
 * A WAL-level mutation: either a {@link Put} or a {@link Delete}.
 *
 * <p>Sealed so {@code switch} expressions over MutationRecord are exhaustively
 * checked by the compiler.
 */
public sealed interface MutationRecord permits MutationRecord.Put, MutationRecord.Delete {

    /** The user key affected by this mutation. */
    Key key();

    /** The sequence number assigned at WAL append time. */
    SequenceNumber sequence();

    record Put(Key key, Slice value, SequenceNumber sequence) implements MutationRecord {}

    record Delete(Key key, SequenceNumber sequence) implements MutationRecord {}
}
