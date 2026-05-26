package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockBasedTableTest {

    @Test
    void writeAndReadRoundTripSmall(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000001.sst");
        long seq = 100L;
        List<InternalKey> keys = new ArrayList<>();
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            for (int i = 0; i < 50; i++) {
                InternalKey ik = new InternalKey(Key.of(String.format("k%04d", i)),
                    new SequenceNumber(seq--), ValueType.VALUE);
                w.add(ik, ("v" + i).getBytes(StandardCharsets.UTF_8));
                keys.add(ik);
            }
            w.finish();
        }

        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            for (int i = 0; i < 50; i++) {
                Optional<Slice> v = r.get(Key.of(String.format("k%04d", i)));
                assertThat(v).isPresent();
                assertThat(new String(v.get().toBytes(), StandardCharsets.UTF_8)).isEqualTo("v" + i);
            }
            assertThat(r.get(Key.of("missing"))).isEmpty();
        }
    }

    @Test
    void readSpansMultipleDataBlocks(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000002.sst");
        long seq = 100_000L;
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            for (int i = 0; i < 5000; i++) {
                InternalKey ik = new InternalKey(Key.of(String.format("key-%07d", i)),
                    new SequenceNumber(seq--), ValueType.VALUE);
                w.add(ik, ("payload-" + i).getBytes(StandardCharsets.UTF_8));
            }
            w.finish();
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            // Spot-check across the file.
            int[] probes = {0, 1, 100, 1234, 2500, 4999};
            for (int i : probes) {
                Optional<Slice> v = r.get(Key.of(String.format("key-%07d", i)));
                assertThat(v).isPresent();
                assertThat(new String(v.get().toBytes(), StandardCharsets.UTF_8))
                    .isEqualTo("payload-" + i);
            }
            // Absent key.
            assertThat(r.get(Key.of("zzz-not-there"))).isEmpty();
            // The index must reference multiple blocks for this to be a real test.
            assertThat(r.indexBlock().size()).isGreaterThan(1);
        }
    }

    @Test
    void deletionTombstoneSuppressesValue(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000003.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            // Newer (seq DESC ordering means tombstone with higher seq goes FIRST).
            w.add(new InternalKey(Key.of("alpha"), new SequenceNumber(10L), ValueType.DELETION), new byte[0]);
            w.add(new InternalKey(Key.of("alpha"), new SequenceNumber(5L), ValueType.VALUE), "stale".getBytes());
            w.finish();
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            // Latest read sees the tombstone — absent.
            assertThat(r.get(Key.of("alpha"))).isEmpty();
            // Read as of seq=5: sees the value.
            Optional<Slice> v = r.get(Key.of("alpha"), new SequenceNumber(5L));
            assertThat(v).isPresent();
            assertThat(new String(v.get().toBytes(), StandardCharsets.UTF_8)).isEqualTo("stale");
        }
    }

    @Test
    void footerMagicMismatchRejected(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000004.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            w.add(new InternalKey(Key.of("a"), new SequenceNumber(1L), ValueType.VALUE), "v".getBytes());
            w.finish();
        }
        // Corrupt the magic at the file tail.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long mLen = raf.length();
            raf.seek(mLen - 1);
            int b = raf.read();
            raf.seek(mLen - 1);
            raf.write(b ^ 0x55);
        }
        assertThatThrownBy(() -> BlockBasedTableReader.open(file))
            .isInstanceOf(SsTableFormatException.class);
    }

    @Test
    void corruptedDataBlockTriggersChecksumException(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000005.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file, /* compress */ false)) {
            for (int i = 0; i < 100; i++) {
                w.add(new InternalKey(Key.of(String.format("k%03d", i)),
                    new SequenceNumber(1000L - i), ValueType.VALUE), ("v" + i).getBytes());
            }
            w.finish();
        }
        // Flip a byte at offset 50 — likely inside the first data block.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(50);
            int b = raf.read();
            raf.seek(50);
            raf.write(b ^ 0xFF);
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            // Reading the corrupted block should throw.
            assertThatThrownBy(() -> r.get(Key.of("k000")))
                .isInstanceOf(BlockChecksumMismatchException.class);
        }
    }

    @Test
    void iteratorYieldsEntriesInSortedOrder(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000006.sst");
        long seq = 1000L;
        List<String> keys = new ArrayList<>();
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            for (int i = 0; i < 300; i++) {
                String k = String.format("user-%05d", i);
                w.add(new InternalKey(Key.of(k), new SequenceNumber(seq--), ValueType.VALUE),
                    ("v" + i).getBytes());
                keys.add(k);
            }
            w.finish();
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries();
            int n = 0;
            String prevUserKey = "";
            while (it.hasNext()) {
                BlockBasedTableReader.SsTableEntry e = it.next();
                String uk = new String(e.internalKey().userKey().bytes(), StandardCharsets.UTF_8);
                assertThat(uk).isGreaterThanOrEqualTo(prevUserKey);
                prevUserKey = uk;
                n++;
            }
            assertThat(n).isEqualTo(300);
        }
    }

    @Test
    void compressedAndUncompressedYieldSameContent(@TempDir Path tmp) throws IOException {
        Path comp = tmp.resolve("comp.sst");
        Path raw = tmp.resolve("raw.sst");
        long seq = 5000L;
        for (Path p : new Path[] {comp, raw}) {
            boolean useCompression = (p == comp);
            try (BlockBasedTableWriter w = BlockBasedTableWriter.open(p, useCompression)) {
                long s = seq;
                for (int i = 0; i < 500; i++) {
                    w.add(new InternalKey(Key.of(String.format("kk-%06d", i)),
                        new SequenceNumber(s--), ValueType.VALUE),
                        ("repeated-value-payload-" + i).getBytes());
                }
                w.finish();
            }
        }
        try (BlockBasedTableReader compReader = BlockBasedTableReader.open(comp);
             BlockBasedTableReader rawReader = BlockBasedTableReader.open(raw)) {
            for (int i = 0; i < 500; i++) {
                Key k = Key.of(String.format("kk-%06d", i));
                Optional<Slice> a = compReader.get(k);
                Optional<Slice> b = rawReader.get(k);
                assertThat(a).isPresent();
                assertThat(b).isPresent();
                assertThat(a.get().toBytes()).isEqualTo(b.get().toBytes());
            }
        }
    }
}
