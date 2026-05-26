package com.hkg.rocksdb.common;

/**
 * Three-way result of a per-source lookup in the engine's read path:
 * <ul>
 *   <li>{@link Found} — the key has a live value in this source.</li>
 *   <li>{@link Tombstoned} — the key has a deletion tombstone in this source.
 *       Older sources must NOT be consulted; the engine returns absent.</li>
 *   <li>{@link Absent} — the key isn't in this source at all; keep looking.</li>
 * </ul>
 *
 * <p>Sealed so engine read-path code can {@code switch} on the three cases
 * exhaustively.
 */
public sealed interface KeyLookup permits KeyLookup.Found, KeyLookup.Tombstoned, KeyLookup.Absent {

    record Found(Slice value) implements KeyLookup {}

    record Tombstoned() implements KeyLookup {}

    record Absent() implements KeyLookup {}

    KeyLookup ABSENT = new Absent();
    KeyLookup TOMBSTONED = new Tombstoned();
}
