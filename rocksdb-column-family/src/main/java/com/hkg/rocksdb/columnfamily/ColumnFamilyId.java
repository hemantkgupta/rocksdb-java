package com.hkg.rocksdb.columnfamily;

/**
 * Typed wrapper around the 32-bit integer that identifies a column family
 * across the WAL, MANIFEST, and SSTable filenames. The default CF — the only
 * one a LevelDB-era DB has — is always {@link #DEFAULT}, value 0; new CFs are
 * minted with monotonically increasing positive values by the
 * {@link ColumnFamilyRegistry}.
 *
 * <p>Why a record around an int: the rest of the engine deals in
 * {@code FileNumber}, {@code SequenceNumber}, {@code Lsn} — typed integer
 * wrappers that catch "passed the wrong int" bugs at compile time. CFs
 * follow the same convention.
 */
public record ColumnFamilyId(int value) implements Comparable<ColumnFamilyId> {

    public ColumnFamilyId {
        if (value < 0) {
            throw new IllegalArgumentException("ColumnFamilyId must be non-negative, got " + value);
        }
    }

    /** The implicit "default" column family, present in every DB. */
    public static final ColumnFamilyId DEFAULT = new ColumnFamilyId(0);

    @Override
    public int compareTo(ColumnFamilyId other) {
        return Integer.compare(this.value, other.value);
    }
}
