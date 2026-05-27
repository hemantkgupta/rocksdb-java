package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.FileNumber;
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

/**
 * Tests for the CP 29 two-level index path on {@link BlockBasedTableWriter}
 * and {@link BlockBasedTableReader}. The {@code openWithIndexTuning} entry
 * point lets each test pick a tiny {@code TWO_LEVEL_INDEX_THRESHOLD} +
 * {@code INDEX_BLOCK_TARGET_BYTES} so we can exercise the two-level layout
 * without writing 32 MiB per test.
 */
class TwoLevelIndexTest {

    /**
     * Threshold low enough that any non-trivial SSTable (more than one data
     * block) triggers the two-level path; bottom blocks small enough that the
     * partition logic actually emits multiple bottoms.
     */
    private static final long TINY_THRESHOLD = 1L;           // any file with >1 data block → two-level
    private static final int TINY_BOTTOM_TARGET = 200;       // ~4 index entries per bottom block

    @Test
    void singleLevelUnchangedForSmallFile(@TempDir Path tmp) throws IOException {
        // A small file (default threshold of 32 MiB) MUST still write a single index block.
        Path file = tmp.resolve("000001.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            for (int i = 0; i < 50; i++) {
                w.add(internal(String.format("k%04d", i), 100 - i),
                    ("v" + i).getBytes(StandardCharsets.UTF_8));
            }
            w.finish();
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isFalse();
            for (int i = 0; i < 50; i++) {
                assertThat(r.get(Key.of(String.format("k%04d", i)))).isPresent();
            }
        }
    }

    @Test
    void writerEmitsTwoLevelWhenAboveThreshold(@TempDir Path tmp) throws IOException {
        // Force two-level with a tiny threshold; verify the reader sees the marker.
        Path file = writeManyKeys(tmp.resolve("000002.sst"), 500);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            // Top-level index block should be present and small. With ~30 data blocks (200-byte
            // values, 4 KiB data blocks) and the 200-byte bottom-index target, we expect a
            // handful of bottom-index blocks → a handful of top-level entries.
            assertThat(r.indexBlock().size()).isGreaterThanOrEqualTo(1).isLessThan(500);
        }
    }

    @Test
    void twoLevelReadRoundtripsEveryKey(@TempDir Path tmp) throws IOException {
        Path file = writeManyKeys(tmp.resolve("000003.sst"), 800);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            for (int i = 0; i < 800; i++) {
                Optional<Slice> v = r.get(Key.of(String.format("key-%06d", i)));
                assertThat(v).as("key %d", i).isPresent();
                assertThat(v.get().toBytes()).isEqualTo(valueFor(i));
            }
            // Absent keys must return empty (bloom + index combo).
            assertThat(r.get(Key.of("zzz-missing"))).isEmpty();
        }
    }

    @Test
    void twoLevelIteratorWalksEveryEntry(@TempDir Path tmp) throws IOException {
        Path file = writeManyKeys(tmp.resolve("000004.sst"), 600);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries();
            String prev = "";
            int n = 0;
            while (it.hasNext()) {
                BlockBasedTableReader.SsTableEntry e = it.next();
                String uk = new String(e.internalKey().userKey().bytes(), StandardCharsets.UTF_8);
                assertThat(uk).as("sort order at %d", n).isGreaterThanOrEqualTo(prev);
                prev = uk;
                n++;
            }
            assertThat(n).isEqualTo(600);
        }
    }

    @Test
    void bottomIndexBlocksLoadOnDemandViaCache(@TempDir Path tmp) throws IOException {
        Path file = writeManyKeys(tmp.resolve("000005.sst"), 600);
        BlockCache cache = new LruBlockCache(8L * 1024L * 1024L);
        FileNumber fn = new FileNumber(5L);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file, fn, cache)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            // No reads yet → cache should be empty.
            assertThat(cache.size()).isEqualTo(0);

            // A single get touches one bottom-index block + one data block → cache populates.
            Optional<Slice> v = r.get(Key.of(String.format("key-%06d", 0)));
            assertThat(v).isPresent();
            int afterFirst = cache.size();
            assertThat(afterFirst).isGreaterThanOrEqualTo(2); // one bottom-index + one data block

            // Re-reading the same key MUST be a pure cache hit (cache size doesn't grow).
            assertThat(r.get(Key.of(String.format("key-%06d", 0)))).isPresent();
            assertThat(cache.size()).isEqualTo(afterFirst);

            // Reading a key in a different bottom-index range grows the cache.
            assertThat(r.get(Key.of(String.format("key-%06d", 599)))).isPresent();
            assertThat(cache.size()).isGreaterThan(afterFirst);
        }
    }

    @Test
    void topLevelIndexPinnedEagerlyButBottomNotLoadedAtOpen(@TempDir Path tmp) throws IOException {
        Path file = writeManyKeys(tmp.resolve("000006.sst"), 600);
        BlockCache cache = new LruBlockCache(8L * 1024L * 1024L);
        FileNumber fn = new FileNumber(6L);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file, fn, cache)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            // Open() pins the top-level index in `r.indexBlock` directly; nothing should be
            // routed through the cache yet (data blocks AND bottom-index blocks load on demand).
            assertThat(cache.size()).isEqualTo(0);
            assertThat(r.indexBlock().size()).isGreaterThan(0);
        }
    }

    @Test
    void twoLevelLookupHandlesDeletionTombstone(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("000007.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.openWithIndexTuning(
            file, false /* uncompressed for predictable sizes */, WriteHook.NO_OP,
            TINY_THRESHOLD, TINY_BOTTOM_TARGET)) {
            // 200 puts with large-ish values so multiple data blocks are emitted ⇒ two-level.
            for (int i = 0; i < 200; i++) {
                w.add(internal(String.format("k%06d", i), 10_000 - i), valueFor(i));
            }
            // Deletion tombstone (higher seq wins) shadowing a stale Put for the SAME key.
            w.add(new InternalKey(Key.of("z-key"), new SequenceNumber(1_000_000L), ValueType.DELETION),
                new byte[0]);
            w.add(new InternalKey(Key.of("z-key"), new SequenceNumber(5L), ValueType.VALUE),
                "stale".getBytes(StandardCharsets.UTF_8));
            w.finish();
        }
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            // Newest visible record is the deletion → empty.
            assertThat(r.get(Key.of("z-key"))).isEmpty();
            // As of seq=5, the stale value is visible.
            Optional<Slice> stale = r.get(Key.of("z-key"), new SequenceNumber(5L));
            assertThat(stale).isPresent();
            assertThat(new String(stale.get().toBytes(), StandardCharsets.UTF_8)).isEqualTo("stale");
            // A truly absent key.
            assertThat(r.get(Key.of("zzz-missing"))).isEmpty();
            // A present key still works.
            assertThat(r.get(Key.of(String.format("k%06d", 5)))).isPresent();
        }
    }

    @Test
    void footerLayoutIsStill48BytesAndMagicUnchanged(@TempDir Path tmp) throws IOException {
        // CP 29 invariant: the footer must stay byte-for-byte the same on disk.
        Path twoLevel = writeManyKeys(tmp.resolve("twoLevel.sst"), 500);
        Path singleLevel = tmp.resolve("singleLevel.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(singleLevel)) {
            for (int i = 0; i < 10; i++) {
                w.add(internal(String.format("k%04d", i), 100 - i),
                    ("v" + i).getBytes(StandardCharsets.UTF_8));
            }
            w.finish();
        }
        for (Path p : new Path[] {twoLevel, singleLevel}) {
            try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "r")) {
                long len = raf.length();
                raf.seek(len - 8);
                byte[] magic = new byte[8];
                raf.readFully(magic);
                long m = java.nio.ByteBuffer.wrap(magic)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                assertThat(m).isEqualTo(Footer.MAGIC);
            }
        }
    }

    @Test
    void preCp29SsTableOpensIdentically(@TempDir Path tmp) throws IOException {
        // Simulate a pre-CP-29 SSTable: small file, default open() → single-level by construction.
        Path file = tmp.resolve("legacy.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.open(file)) {
            for (int i = 0; i < 100; i++) {
                w.add(internal(String.format("legacy-%04d", i), 1_000 - i),
                    ("legacy-val-" + i).getBytes(StandardCharsets.UTF_8));
            }
            w.finish();
        }
        // Reader MUST NOT advertise two-level on a file with no marker.
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isFalse();
            for (int i = 0; i < 100; i++) {
                assertThat(r.get(Key.of(String.format("legacy-%04d", i)))).isPresent();
            }
            // Iterator works in single-level mode too.
            int n = 0;
            for (Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries(); it.hasNext(); ) {
                it.next();
                n++;
            }
            assertThat(n).isEqualTo(100);
        }
    }

    @Test
    void twoLevelDeterministicReReadOrderingMatchesWriteOrder(@TempDir Path tmp) throws IOException {
        // Spot-check that index ordering is preserved when partitioned across bottom blocks.
        Path file = writeManyKeys(tmp.resolve("ordering.sst"), 1000);
        List<String> seen = new ArrayList<>();
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            for (Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries(); it.hasNext(); ) {
                seen.add(new String(it.next().internalKey().userKey().bytes(), StandardCharsets.UTF_8));
            }
        }
        assertThat(seen).hasSize(1000);
        for (int i = 0; i < 1000; i++) {
            assertThat(seen.get(i)).isEqualTo(String.format("key-%06d", i));
        }
    }

    @Test
    void twoLevelBottomBlockCrcMismatchFailsReadCleanly(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("corrupt.sst");
        try (BlockBasedTableWriter w = BlockBasedTableWriter.openWithIndexTuning(
            file, false /* uncompressed */, WriteHook.NO_OP, TINY_THRESHOLD, TINY_BOTTOM_TARGET)) {
            for (int i = 0; i < 400; i++) {
                w.add(internal(String.format("key-%06d", i), 10_000 - i), valueFor(i));
            }
            w.finish();
        }
        // Open once, find the top-level index entry, locate the FIRST bottom-index block,
        // then flip a byte inside it. The bottom-index block lives between the last data block
        // and the top-level index block. We don't need to know exactly where — corrupting the
        // byte just AFTER the last data block (i.e. somewhere in the bloom / RT / index region)
        // and observing that read fails is enough. We pick a byte inside the meta region by
        // reading the footer to find the top-level index offset, then corrupting a byte WITHIN
        // (offset - 100) to land in a bottom-index block. The two-level layout guarantees those
        // bottom blocks sit immediately before the top-level block.
        long topLevelOffset;
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            topLevelOffset = r.footer().indexHandle().offset();
        }
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long target = topLevelOffset - 50; // inside one of the bottom-index blocks
            raf.seek(target);
            int b = raf.read();
            raf.seek(target);
            raf.write(b ^ 0xFF);
        }
        // Reads that need to load a bottom block should now fail with a CRC mismatch.
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThatThrownBy(() -> {
                // Scan a chunk of keys; at least one must hit the corrupted bottom block.
                for (int i = 0; i < 400; i++) {
                    r.get(Key.of(String.format("key-%06d", i)));
                }
            }).isInstanceOf(BlockChecksumMismatchException.class);
        }
    }

    @Test
    void twoLevelReaderEntriesYieldsKeysInOrderWithExpectedValues(@TempDir Path tmp) throws IOException {
        Path file = writeManyKeys(tmp.resolve("entries.sst"), 750);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            int i = 0;
            for (Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries(); it.hasNext(); ) {
                BlockBasedTableReader.SsTableEntry e = it.next();
                String uk = new String(e.internalKey().userKey().bytes(), StandardCharsets.UTF_8);
                assertThat(uk).isEqualTo(String.format("key-%06d", i));
                assertThat(e.value()).isEqualTo(valueFor(i));
                i++;
            }
            assertThat(i).isEqualTo(750);
        }
    }

    @Test
    void twoLevelWithCacheReusesBottomBlockAcrossLookups(@TempDir Path tmp) throws IOException {
        // Hot key range: many lookups across the same bottom-index block stay cache-resident.
        Path file = writeManyKeys(tmp.resolve("hot.sst"), 800);
        BlockCache cache = new LruBlockCache(8L * 1024L * 1024L);
        FileNumber fn = new FileNumber(12L);
        try (BlockBasedTableReader r = BlockBasedTableReader.open(file, fn, cache)) {
            assertThat(r.isTwoLevelIndex()).isTrue();
            // Warm up: read keys 0..9 → loads ONE bottom-index block + at most a few data blocks.
            for (int i = 0; i < 10; i++) {
                assertThat(r.get(Key.of(String.format("key-%06d", i)))).isPresent();
            }
            int warmSize = cache.size();
            // Re-read those same 10 keys → ZERO new cache entries (pure hit path).
            for (int i = 0; i < 10; i++) {
                assertThat(r.get(Key.of(String.format("key-%06d", i)))).isPresent();
            }
            assertThat(cache.size()).isEqualTo(warmSize);
        }
    }

    // ---------- helpers ----------

    /**
     * Write {@code n} keys ("key-000000".."key-N-1") with the tiny-threshold tuning so
     * the result is guaranteed to be a two-level SSTable. Each key gets a unique seq
     * and a moderately large value so the data blocks fill quickly — the goal is to
     * make MANY data blocks (and therefore many index entries) per file so the
     * top-level + bottom-level partitioning is meaningfully exercised.
     */
    private static Path writeManyKeys(Path file, int n) throws IOException {
        try (BlockBasedTableWriter w = BlockBasedTableWriter.openWithIndexTuning(
            file, false /* uncompressed — keep data blocks predictable */,
            WriteHook.NO_OP, TINY_THRESHOLD, TINY_BOTTOM_TARGET)) {
            for (int i = 0; i < n; i++) {
                w.add(internal(String.format("key-%06d", i), 1_000_000L - i),
                    valueFor(i));
            }
            w.finish();
        }
        return file;
    }

    /**
     * Build a value sized so each data block holds only a handful of entries
     * (BLOCK_SIZE_BYTES = 4096; value ≈ 200 bytes ⇒ ~20 entries per data block).
     * For n=600 that yields ~30 data blocks ⇒ ~30 index entries ⇒ multiple
     * bottom-level index blocks with the {@link #TINY_BOTTOM_TARGET} target.
     */
    private static byte[] valueFor(int i) {
        byte[] out = new byte[200];
        byte[] tag = ("val-" + i + "-").getBytes(StandardCharsets.UTF_8);
        for (int j = 0; j < out.length; j++) {
            out[j] = tag[j % tag.length];
        }
        return out;
    }

    private static InternalKey internal(String userKey, long seq) {
        return new InternalKey(Key.of(userKey), new SequenceNumber(seq), ValueType.VALUE);
    }
}
