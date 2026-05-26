package com.hkg.rocksdb.wal;

/**
 * WAL record-fragment type tag. Same byte values as RocksDb.
 */
public enum RecordType {
    /** Zero padding at end of a block; not a real record. */
    ZERO_PADDING(0),
    /** Self-contained record fragment (fits entirely in one fragment). */
    FULL(1),
    /** First fragment of a record that spans multiple fragments. */
    FIRST(2),
    /** Middle fragment of a multi-fragment record. */
    MIDDLE(3),
    /** Last fragment of a multi-fragment record. */
    LAST(4);

    private final byte tag;

    RecordType(int tag) { this.tag = (byte) tag; }

    public byte tag() { return tag; }

    public static RecordType fromTag(byte tag) {
        for (RecordType t : values()) {
            if (t.tag == tag) return t;
        }
        throw new WalCorruptionException("unknown record type tag: " + tag);
    }
}
