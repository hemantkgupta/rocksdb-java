package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.sstable.BlockBasedTableReader.SsTableEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MergingIteratorTest {

    private static SsTableEntry e(String userKey, long seq, String value) {
        return new SsTableEntry(
            new InternalKey(Key.of(userKey), new SequenceNumber(seq), ValueType.VALUE),
            value.getBytes());
    }

    @Test
    void mergesTwoSortedSources() {
        Iterator<SsTableEntry> a = List.of(e("a", 5L, "x"), e("c", 5L, "y")).iterator();
        Iterator<SsTableEntry> b = List.of(e("b", 5L, "z"), e("d", 5L, "w")).iterator();
        MergingIterator merger = new MergingIterator(List.of(a, b));
        List<String> userKeys = new ArrayList<>();
        while (merger.hasNext()) {
            userKeys.add(new String(merger.next().internalKey().userKey().bytes()));
        }
        assertThat(userKeys).containsExactly("a", "b", "c", "d");
    }

    @Test
    void emitsNewerEntryFirstWithinUserKeyGroup() {
        // Same user key, different sequences. Newer (higher seq) should sort first.
        Iterator<SsTableEntry> a = List.of(e("k", 1L, "old")).iterator();
        Iterator<SsTableEntry> b = List.of(e("k", 10L, "new")).iterator();
        MergingIterator merger = new MergingIterator(List.of(a, b));
        SsTableEntry first = merger.next();
        SsTableEntry second = merger.next();
        assertThat(first.internalKey().sequence().value()).isEqualTo(10L);
        assertThat(second.internalKey().sequence().value()).isEqualTo(1L);
        assertThat(merger.hasNext()).isFalse();
    }

    @Test
    void emptySourcesYieldEmptyIterator() {
        MergingIterator merger = new MergingIterator(List.of());
        assertThat(merger.hasNext()).isFalse();
    }

    @Test
    void handlesUnevenSourceLengths() {
        Iterator<SsTableEntry> a = List.of(e("a", 1L, "1")).iterator();
        Iterator<SsTableEntry> b = List.of(e("b", 1L, "2"), e("c", 1L, "3"), e("d", 1L, "4")).iterator();
        MergingIterator merger = new MergingIterator(List.of(a, b));
        List<String> got = new ArrayList<>();
        while (merger.hasNext()) {
            got.add(new String(merger.next().internalKey().userKey().bytes()));
        }
        assertThat(got).containsExactly("a", "b", "c", "d");
    }
}
