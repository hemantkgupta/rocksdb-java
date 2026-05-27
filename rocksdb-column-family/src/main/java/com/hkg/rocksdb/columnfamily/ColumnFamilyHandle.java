package com.hkg.rocksdb.columnfamily;

import java.util.Objects;

/**
 * The application-facing reference to a column family. Created by
 * {@link ColumnFamilyRegistry#create} or returned from {@link
 * ColumnFamilyRegistry#get}. Carries identity ({@link #id} + {@link #name})
 * and the {@link ColumnFamilyOptions} the CF was opened with.
 *
 * <p>Handles are stable across the life of one engine open; after a
 * {@code dropColumnFamily} the handle becomes a dangling reference and
 * subsequent operations on it fail.
 */
public record ColumnFamilyHandle(ColumnFamilyId id, String name, ColumnFamilyOptions options) {

    /** Reserved name for the implicit default column family. */
    public static final String DEFAULT_NAME = "default";

    public ColumnFamilyHandle {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(options, "options");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("ColumnFamily name must be non-empty");
        }
    }

    /** Construct the implicit default CF with leveled options. */
    public static ColumnFamilyHandle defaultHandle() {
        return new ColumnFamilyHandle(ColumnFamilyId.DEFAULT, DEFAULT_NAME, ColumnFamilyOptions.defaults());
    }
}
