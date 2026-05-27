package com.hkg.rocksdb.memtable;

import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.KeyLookup;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.RangeTombstone;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    /**
     * CP 17 — range tombstones. Kept in insertion order; lookups linear-scan
     * (the typical MemTable has tens to hundreds of RTs, never the millions
     * of point entries). For RocksDB-grade hot-path performance an interval
     * tree would replace this, but for the plan's pedagogical scope, the
     * simple list is correct and clear.
     */
    private final CopyOnWriteArrayList<RangeTombstone> rangeTombstones = new CopyOnWriteArrayList<>();
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
     * CP 17 — insert a range tombstone {@code [startKey, endKey)} valid as of
     * {@code sequence}. Rejected if frozen.
     */
    public void deleteRange(Key startKey, Key endKey, SequenceNumber sequence) {
        Objects.requireNonNull(startKey, "startKey");
        Objects.requireNonNull(endKey, "endKey");
        Objects.requireNonNull(sequence, "sequence");
        ensureWritable();
        RangeTombstone rt = new RangeTombstone(startKey, endKey, sequence);
        rangeTombstones.add(rt);
        approximateBytes.addAndGet((long) startKey.length() + endKey.length() + 9L + 32L);
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
     * newest visible record is a deletion (a point Delete or a covering
     * RangeTombstone) — the engine MUST stop probing older sources.
     *
     * <p>CP 17 range-tombstone semantics: the MemTable computes
     * {@code rtSeq} = max sequence of any {@link RangeTombstone} with
     * {@code seq ≤ asOf} that covers {@code key}, then compares it with the
     * point-lookup hit's sequence. Whichever is newer wins. If both are
     * absent, returns Absent.
     */
    public KeyLookup lookup(Key key, SequenceNumber snapshotOrNull) {
        long asOfValue = snapshotOrNull == null ? SequenceNumber.MAX : snapshotOrNull.value();
        long rtSeq = maxCoveringRtSeqAtOrBefore(key, asOfValue);

        // Probe for the newest point entry (Put or Delete) with seq ≤ asOf.
        SequenceNumber probeSeq = snapshotOrNull != null
            ? snapshotOrNull
            : new SequenceNumber(SequenceNumber.MAX);
        InternalKey probe = new InternalKey(key, probeSeq, ValueType.VALUE);
        Map.Entry<InternalKey, byte[]> entry = entries.ceilingEntry(probe);

        long pointSeq = -1L;
        boolean pointIsDeletion = false;
        Slice pointValue = null;
        if (entry != null && entry.getKey().userKey().equals(key)) {
            pointSeq = entry.getKey().sequence().value();
            pointIsDeletion = entry.getKey().type() instanceof ValueType.Deletion;
            if (!pointIsDeletion) {
                pointValue = Slice.of(entry.getValue());
            }
        }

        if (pointSeq < 0L && rtSeq < 0L) return KeyLookup.ABSENT;
        if (rtSeq > pointSeq) {
            // Range tombstone is newer than any point entry — TOMBSTONED.
            return KeyLookup.TOMBSTONED;
        }
        // Point entry wins (or ties; either way the point's verdict is correct).
        if (pointIsDeletion) return KeyLookup.TOMBSTONED;
        return new KeyLookup.Found(pointValue);
    }

    /**
     * Max sequence number of any {@link RangeTombstone} held by this MemTable
     * whose {@code seq ≤ asOf} AND that covers {@code userKey}. Returns
     * {@code -1L} if no such RT exists. Used by the engine's read path to
     * decide whether a covering RT shadows a Put.
     */
    public long maxCoveringRtSeqAtOrBefore(Key userKey, long asOf) {
        Objects.requireNonNull(userKey, "userKey");
        long max = -1L;
        for (RangeTombstone rt : rangeTombstones) {
            long s = rt.sequence().value();
            if (s > asOf) continue;
            if (rt.covers(userKey) && s > max) max = s;
        }
        return max;
    }

    /** Snapshot of all range tombstones in insertion order. */
    public List<RangeTombstone> rangeTombstones() {
        return Collections.unmodifiableList(new ArrayList<>(rangeTombstones));
    }

    /** Number of range tombstones stored in this MemTable. */
    public int rangeTombstoneCount() {
        return rangeTombstones.size();
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
