package com.hkg.rocksdb.common;

import java.util.Iterator;
import java.util.Optional;

/**
 * The public engine interface. Implemented by the assembled {@code RocksDb}
 * class in the {@code rocksdb-engine} module; declared here so that all
 * modules can refer to it without depending on the engine implementation.
 */
public interface KvEngine extends AutoCloseable {

    /** Insert or overwrite a key/value pair. */
    void put(Key key, Slice value);

    /** Look up a key in the latest committed state. */
    Optional<Slice> get(Key key);

    /** Look up a key as of the given {@link Snapshot}. */
    Optional<Slice> get(Key key, Snapshot snapshot);

    /** Write a tombstone for the given key. Idempotent. */
    void delete(Key key);

    /** Acquire a snapshot at the current sequence number. */
    Snapshot snapshot();

    /** Release a previously acquired snapshot. */
    void releaseSnapshot(Snapshot snapshot);

    /**
     * Iterate mutations whose user key lies in {@code [from, to)}. The iterator
     * yields the newest version of each user key as observed by the read path.
     */
    Iterator<MutationRecord> scan(Key from, Key to);

    /** Force the active MemTable to flush to an L0 SSTable. */
    void flush();

    /** Graceful shutdown: flush MemTable, close WAL, persist MANIFEST. */
    @Override
    void close();
}
