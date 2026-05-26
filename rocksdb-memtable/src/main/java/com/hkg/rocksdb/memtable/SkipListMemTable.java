package com.hkg.rocksdb.memtable;

import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.KeyLookup;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.ValueType;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory write buffer. A {@link ConcurrentSkipListMap} keyed by
 * {@link InternalKey} (userKey ASC, sequence DESC) so that {@link #get}
 * naturally returns the newest version of a user key by binary-searching
 * for the smallest InternalKey with that user key.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Writes append into the active map. {@link #put} and {@link #delete}
 *       are thread-safe.</li>
 *   <li>When {@link #approximateBytes} exceeds the engine's flush threshold,
 *       the engine calls {@link #freeze()}.</li>
 *   <li>A frozen MemTable still answers {@link #get} but rejects new writes
 *       with {@link IllegalStateException}.</li>
 *   <li>The engine iterates the frozen MemTable to produce an L0 SSTable.</li>
 * </ol>
 */
public final class SkipListMemTable {

    private final ConcurrentSkipListMap<InternalKey, byte[]> entries = new ConcurrentSkipListMap<>();
    private final AtomicLong approximateBytes = new AtomicLong(0L);
    private final AtomicBoolean frozen = new AtomicBoolean(false);

    /** Insert a Put record. Rejected if frozen. */
    public void put(Key key, Slice value, SequenceNumber sequence) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(sequence, "sequence");
        ensureWritable();
        byte[] copy = value.toBytes();
        InternalKey ik = new InternalKey(key, sequence, ValueType.VALUE);
        byte[] prior = entries.put(ik, copy);
        if (prior == null) {
            approximateBytes.addAndGet(estimateBytes(key, copy.length));
        } else {
            // Same internal key (same user key + sequence + type) — overwrite is rare,
            // but accounting must not double-count.
            approximateBytes.addAndGet((long) copy.length - prior.length);
        }
    }

    /** Insert a Delete tombstone. Rejected if frozen. */
    public void delete(Key key, SequenceNumber sequence) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(sequence, "sequence");
        ensureWritable();
        InternalKey ik = new InternalKey(key, sequence, ValueType.DELETION);
        byte[] prior = entries.put(ik, new byte[0]);
        if (prior == null) {
            approximateBytes.addAndGet(estimateBytes(key, 0));
        }
    }

    /**
     * Look up a user key. Returns the value of the newest visible Put, or
     * {@link Optional#empty()} if the newest visible record is a Delete or
     * the key is absent.
     *
     * <p>If {@code snapshot} is non-null, mutations with sequence > snapshot
     * are skipped; the read returns the newest record with sequence ≤ snapshot.
     */
    public Optional<MemTableLookup> get(Key key, SequenceNumber snapshotOrNull) {
        Objects.requireNonNull(key, "key");
        // Construct a probe at the maximum sequence to position the floor cursor
        // at the newest entry for this user key.
        SequenceNumber probeSeq = snapshotOrNull != null
            ? snapshotOrNull
            : new SequenceNumber(SequenceNumber.MAX);
        InternalKey probe = new InternalKey(key, probeSeq, ValueType.VALUE);

        // ceilingEntry returns the smallest InternalKey >= probe; since
        // ordering is (userKey ASC, sequence DESC), the ceiling is either:
        //   (a) an entry with the same userKey and sequence ≤ probeSeq (we want this), or
        //   (b) an entry with a strictly greater userKey (key not present at <= snapshot).
        Map.Entry<InternalKey, byte[]> entry = entries.ceilingEntry(probe);
        if (entry == null) {
            return Optional.empty();
        }
        InternalKey hit = entry.getKey();
        if (!hit.userKey().equals(key)) {
            return Optional.empty();
        }
        // Found the newest visible record for this user key.
        if (hit.type() instanceof ValueType.Deletion) {
            return Optional.of(new MemTableLookup(true, null));
        }
        return Optional.of(new MemTableLookup(false, Slice.of(entry.getValue())));
    }

    /** Convenience overload — snapshot-less lookup. */
    public Optional<MemTableLookup> get(Key key) {
        return get(key, null);
    }

    /**
     * Three-way lookup used by the engine's read path: distinguishes
     * Found / Tombstoned / Absent. The MemTable returns Tombstoned if the
     * newest visible record is a deletion — the engine MUST stop probing
     * older sources.
     */
    public KeyLookup lookup(Key key, SequenceNumber snapshotOrNull) {
        Optional<MemTableLookup> hit = get(key, snapshotOrNull);
        if (hit.isEmpty()) return KeyLookup.ABSENT;
        if (hit.get().isDeletion()) return KeyLookup.TOMBSTONED;
        return new KeyLookup.Found(hit.get().value());
    }

    /** Approximate memtable memory footprint in bytes. */
    public long approximateBytes() {
        return approximateBytes.get();
    }

    /** Number of internal-key entries (counts every version of every key). */
    public int size() {
        return entries.size();
    }

    /** Has this MemTable been frozen? */
    public boolean isFrozen() {
        return frozen.get();
    }

    /**
     * Freeze the MemTable. Subsequent {@link #put} / {@link #delete} throw.
     * Reads continue to work. Idempotent.
     */
    public void freeze() {
        frozen.set(true);
    }

    /**
     * Iterate every internal entry in scan order (userKey ASC, sequence DESC).
     * The iterator is meant for an SSTable writer; readers should use
     * {@link #get(Key, SequenceNumber)} instead.
     */
    public Iterator<MemTableEntry> iterator() {
        Iterator<Map.Entry<InternalKey, byte[]>> raw = entries.entrySet().iterator();
        return new Iterator<>() {
            @Override public boolean hasNext() { return raw.hasNext(); }
            @Override public MemTableEntry next() {
                Map.Entry<InternalKey, byte[]> e = raw.next();
                return new MemTableEntry(e.getKey(), e.getValue());
            }
        };
    }

    private void ensureWritable() {
        if (frozen.get()) {
            throw new IllegalStateException("MemTable is frozen; no further writes accepted");
        }
    }

    private static long estimateBytes(Key key, int valueLen) {
        // Per-entry footprint approximation: key bytes + value bytes + small overhead
        // for the InternalKey (sequence + type tag = 9 bytes) and a skip-list node.
        return (long) key.length() + valueLen + 9L + 32L;
    }

    /** Result of a MemTable lookup. */
    public record MemTableLookup(boolean isDeletion, Slice value) {}

    /** A single entry as produced by {@link #iterator()}. */
    public record MemTableEntry(InternalKey internalKey, byte[] value) {}
}
