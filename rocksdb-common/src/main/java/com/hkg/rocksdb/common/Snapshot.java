package com.hkg.rocksdb.common;

/**
 * A per-instance snapshot point. Reads against a snapshot see only mutations
 * with sequence ≤ {@link #sequence()}. Snapshots are reference-counted by the
 * engine; obtaining one prevents the engine from collecting newer writes'
 * older overwritten values.
 */
public record Snapshot(SequenceNumber sequence) {}
