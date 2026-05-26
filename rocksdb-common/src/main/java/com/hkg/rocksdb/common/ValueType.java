package com.hkg.rocksdb.common;

/**
 * The internal-key value-type tag. RocksDb uses a one-byte tag to distinguish
 * a live value (a Put) from a deletion tombstone.
 *
 * <p>Two permitted forms:
 * <ul>
 *   <li>{@link Value} — tag {@code 0x01}; the entry carries a live value.</li>
 *   <li>{@link Deletion} — tag {@code 0x00}; the entry is a tombstone.</li>
 * </ul>
 */
public sealed interface ValueType permits ValueType.Value, ValueType.Deletion {

    byte tag();

    Value VALUE = new Value();
    Deletion DELETION = new Deletion();

    /** Decode a tag byte back into a {@link ValueType}. */
    static ValueType fromTag(byte tag) {
        return switch (tag) {
            case 0x01 -> VALUE;
            case 0x00 -> DELETION;
            default -> throw new IllegalArgumentException("unknown ValueType tag: " + tag);
        };
    }

    record Value() implements ValueType {
        @Override public byte tag() { return 0x01; }
    }

    record Deletion() implements ValueType {
        @Override public byte tag() { return 0x00; }
    }
}
