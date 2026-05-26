package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.sstable.BlockBasedTableReader;
import com.hkg.rocksdb.sstable.BlockBasedTableWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class CompactorTest {

    private static FileMetadata writeTable(Path dbDir, long fileNum, List<InternalKey> keys, List<byte[]> values)
            throws IOException {
        Path path = dbDir.resolve(String.format("%06d.sst", fileNum));
        InternalKey smallest = null, largest = null;
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(path)) {
            for (int i = 0; i < keys.size(); i++) {
                w.add(keys.get(i), values.get(i));
                if (smallest == null) smallest = keys.get(i);
                largest = keys.get(i);
            }
            w.finish();
        }
        return new FileMetadata(new FileNumber(fileNum), Files.size(path), smallest, largest);
    }

    private static Compactor.FileNumberAllocator allocator(long start) {
        AtomicLong c = new AtomicLong(start);
        return () -> new FileNumber(c.getAndIncrement());
    }

    private static Compactor.ReaderOpener opener(Path dbDir) {
        return number -> BlockBasedTableReader.open(dbDir.resolve(number.tableFileName()));
    }

    @Test
    void mergesTwoOverlappingFilesAtL1(@TempDir Path dbDir) throws IOException {
        FileMetadata a = writeTable(dbDir, 1L,
            List.of(
                new InternalKey(Key.of("apple"), new SequenceNumber(10L), ValueType.VALUE),
                new InternalKey(Key.of("cherry"), new SequenceNumber(10L), ValueType.VALUE)),
            List.of("A1".getBytes(), "C1".getBytes()));
        FileMetadata b = writeTable(dbDir, 2L,
            List.of(
                new InternalKey(Key.of("banana"), new SequenceNumber(5L), ValueType.VALUE),
                new InternalKey(Key.of("date"), new SequenceNumber(5L), ValueType.VALUE)),
            List.of("B1".getBytes(), "D1".getBytes()));

        CompactionJob job = new CompactionJob(0, 1, List.of(a, b), List.of());
        List<FileMetadata> outputs = Compactor.run(job, Compactor.NO_SNAPSHOTS_HELD,
            Long.MAX_VALUE, dbDir, allocator(100L), opener(dbDir), false);

        assertThat(outputs).hasSize(1);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(
                dbDir.resolve(outputs.get(0).fileNumber().tableFileName()))) {
            assertThat(r.get(Key.of("apple"))).isPresent();
            assertThat(r.get(Key.of("banana"))).isPresent();
            assertThat(r.get(Key.of("cherry"))).isPresent();
            assertThat(r.get(Key.of("date"))).isPresent();
        }
    }

    @Test
    void dedupsOlderVersionWhenNoSnapshots(@TempDir Path dbDir) throws IOException {
        FileMetadata a = writeTable(dbDir, 1L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(10L), ValueType.VALUE)),
            List.of("new".getBytes()));
        FileMetadata b = writeTable(dbDir, 2L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(5L), ValueType.VALUE)),
            List.of("old".getBytes()));

        // Bottom-level compaction with no snapshots — older entry should be dropped.
        CompactionJob job = new CompactionJob(
            Constants.MAX_LEVEL_COUNT - 2, Constants.MAX_LEVEL_COUNT - 1,
            List.of(a, b), List.of());
        List<FileMetadata> outputs = Compactor.run(job, Compactor.NO_SNAPSHOTS_HELD,
            Long.MAX_VALUE, dbDir, allocator(100L), opener(dbDir), false);

        try (BlockBasedTableReader r = BlockBasedTableReader.open(
                dbDir.resolve(outputs.get(0).fileNumber().tableFileName()))) {
            Optional<Slice> v = r.get(Key.of("k"));
            assertThat(v).isPresent();
            assertThat(new String(v.get().toBytes())).isEqualTo("new");
            // Iterate — should be exactly one entry.
            int count = 0;
            var it = r.entries();
            while (it.hasNext()) { it.next(); count++; }
            assertThat(count).isEqualTo(1);
        }
    }

    @Test
    void keepsSnapshotVisibleVersion(@TempDir Path dbDir) throws IOException {
        FileMetadata a = writeTable(dbDir, 1L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(10L), ValueType.VALUE)),
            List.of("new".getBytes()));
        FileMetadata b = writeTable(dbDir, 2L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(5L), ValueType.VALUE)),
            List.of("old".getBytes()));

        // Snapshot at seq=6: the snapshot reader needs to see "old" (seq=5).
        CompactionJob job = new CompactionJob(0, 1, List.of(a, b), List.of());
        List<FileMetadata> outputs = Compactor.run(job, 6L, Long.MAX_VALUE,
            dbDir, allocator(100L), opener(dbDir), false);

        try (BlockBasedTableReader r = BlockBasedTableReader.open(
                dbDir.resolve(outputs.get(0).fileNumber().tableFileName()))) {
            // Latest (no snapshot) read sees "new".
            assertThat(new String(r.get(Key.of("k")).get().toBytes())).isEqualTo("new");
            // Snapshot read at seq=5 sees "old".
            assertThat(new String(r.get(Key.of("k"), new SequenceNumber(5L)).get().toBytes()))
                .isEqualTo("old");
        }
    }

    @Test
    void dropsBottomLevelTombstoneWhenNoSnapshots(@TempDir Path dbDir) throws IOException {
        FileMetadata a = writeTable(dbDir, 1L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(20L), ValueType.DELETION)),
            List.of(new byte[0]));
        FileMetadata b = writeTable(dbDir, 2L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(5L), ValueType.VALUE)),
            List.of("stale".getBytes()));

        // Bottom-level compaction; no snapshots — tombstone and its shadowed Put can both be dropped.
        CompactionJob job = new CompactionJob(
            Constants.MAX_LEVEL_COUNT - 2, Constants.MAX_LEVEL_COUNT - 1,
            List.of(a, b), List.of());
        List<FileMetadata> outputs = Compactor.run(job, Compactor.NO_SNAPSHOTS_HELD,
            Long.MAX_VALUE, dbDir, allocator(100L), opener(dbDir), false);

        assertThat(outputs).isEmpty();
    }

    @Test
    void retainsTombstoneAboveNonBottomLevel(@TempDir Path dbDir) throws IOException {
        FileMetadata a = writeTable(dbDir, 1L,
            List.of(new InternalKey(Key.of("k"), new SequenceNumber(20L), ValueType.DELETION)),
            List.of(new byte[0]));

        // Output level NOT the bottom level — tombstone must be retained.
        CompactionJob job = new CompactionJob(0, 1, List.of(a), List.of());
        List<FileMetadata> outputs = Compactor.run(job, Compactor.NO_SNAPSHOTS_HELD,
            Long.MAX_VALUE, dbDir, allocator(100L), opener(dbDir), false);

        assertThat(outputs).hasSize(1);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(
                dbDir.resolve(outputs.get(0).fileNumber().tableFileName()))) {
            int count = 0;
            var it = r.entries();
            while (it.hasNext()) { it.next(); count++; }
            assertThat(count).isEqualTo(1);
        }
    }
}
