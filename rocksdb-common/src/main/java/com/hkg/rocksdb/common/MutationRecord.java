package com.hkg.rocksdb.common;

/**
 * A WAL-level mutation: a {@link Put}, a {@link Delete}, or a {@link DeleteRange}.
 *
 * <p>Sealed so {@code switch} expressions over MutationRecord are exhaustively
 * checked by the compiler.
 *
 * <p>Note: {@link DeleteRange} (CP 17) doesn't have a single {@code key()} —
 * it spans {@code [startKey, endKey)} — so the {@link #key()} accessor on the
 * shared interface returns {@code startKey} for DeleteRange. Callers that
 * need to handle range semantics specifically should pattern-match on the
 * concrete type.
 */
public sealed interface MutationRecord
    permits MutationRecord.Put, MutationRecord.Delete, MutationRecord.DeleteRange {

    /** The first user key affected by this mutation (or the startKey for DeleteRange). */
    Key key();

    /** The sequence number assigned at WAL append time. */
    SequenceNumber sequence();

    record Put(Key key, Slice value, SequenceNumber sequence) implements MutationRecord {}

    record Delete(Key key, SequenceNumber sequence) implements MutationRecord {}

    /**
     * Range deletion: every user key in {@code [startKey, endKey)} is logically
     * deleted as of {@code sequence}. The end key is exclusive. {@link #key()}
     * returns {@link #startKey} for compatibility with the shared interface.
     */
    record DeleteRange(Key startKey, Key endKey, SequenceNumber sequence) implements MutationRecord {
        @Override public Key key() { return startKey; }
    }
}
