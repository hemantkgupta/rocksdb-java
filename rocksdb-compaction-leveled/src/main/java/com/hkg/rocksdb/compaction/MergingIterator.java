package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.sstable.BlockBasedTableReader.SsTableEntry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * N-way merge over several sorted iterators of {@link SsTableEntry}, yielded
 * in InternalKey order (userKey ASC, seq DESC, type ASC — same as
 * {@code InternalKey.compareTo}).
 *
 * <p>Stable on equal keys: among equal {@link com.hkg.rocksdb.common.InternalKey}s
 * (which should be vanishingly rare in practice — two writes with the same
 * userKey and seq), the iterator added earlier wins, ensuring deterministic
 * compaction output.
 */
public final class MergingIterator implements Iterator<SsTableEntry> {

    private final PriorityQueue<HeapEntry> heap;

    public MergingIterator(List<? extends Iterator<SsTableEntry>> sources) {
        Comparator<HeapEntry> cmp = (a, b) -> {
            int c = a.current.internalKey().compareTo(b.current.internalKey());
            if (c != 0) return c;
            // Stable tie-break by insertion order.
            return Integer.compare(a.sourceIndex, b.sourceIndex);
        };
        this.heap = new PriorityQueue<>(Math.max(1, sources.size()), cmp);
        int idx = 0;
        for (Iterator<SsTableEntry> src : sources) {
            if (src.hasNext()) {
                heap.add(new HeapEntry(src, src.next(), idx));
            }
            idx++;
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public SsTableEntry next() {
        if (heap.isEmpty()) throw new NoSuchElementException();
        HeapEntry top = heap.poll();
        SsTableEntry result = top.current;
        if (top.source.hasNext()) {
            top.current = top.source.next();
            heap.add(top);
        }
        return result;
    }

    private static final class HeapEntry {
        final Iterator<SsTableEntry> source;
        SsTableEntry current;
        final int sourceIndex;

        HeapEntry(Iterator<SsTableEntry> source, SsTableEntry current, int idx) {
            this.source = source;
            this.current = current;
            this.sourceIndex = idx;
        }
    }
}
