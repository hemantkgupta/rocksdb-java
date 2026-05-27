package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.bloom.BloomFilter;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.InternalKeyCodec;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Streams (InternalKey, value) pairs into an SSTable on disk. The on-disk
 * layout matches §4.1 of the impl plan:
 *
 * <pre>
 *   data block 0
 *   data block 1
 *   ...
 *   data block N-1
 *   bloom filter meta-block
 *   meta-index block  (one entry: "filter.leveldb.BuiltinBloomFilter2" -> bloomHandle)
 *   index block       (one entry per data block: lastKey -> blockHandle)
 *   footer (48 bytes)
 * </pre>
 *
 * <p>Each block is followed by a 1-byte compression-type tag and a 4-byte
 * CRC32 trailer. CRC is computed over (block payload || compression-type).
 *
 * <p>Callers must call {@link #add(InternalKey, byte[])} in ascending
 * {@link InternalKey} order; {@link #finish()} flushes the trailing data
 * block and writes the bloom / meta-index / index / footer.
 */
public final class BlockBasedTableWriter implements Closeable {

    /**
     * Name of the bloom filter entry inside the meta-index block. The
     * {@code "filter.leveldb..."} prefix is preserved verbatim from the
     * LevelDB SSTable format — RocksDB inherits this string so that
     * SSTables written by leveldb-java continue to open here. See
     * §1.3 of the RocksDB plan ("faithful departures") and the
     * forward-compatibility invariant in §4.1.
     */
    public static final String BLOOM_META_KEY = "filter.leveldb.BuiltinBloomFilter2";

    /**
     * CP 17 — name of the range-tombstone entry inside the meta-index block.
     * Older SSTables without DeleteRange writes have no such entry; readers
     * fall back to an empty RT list (see {@link BlockBasedTableReader#rangeTombstones}).
     */
    public static final String RANGE_TOMBSTONE_META_KEY = "rocksdb.range_tombstones";

    /**
     * CP 29 — presence of this meta-index entry signals that the SSTable's
     * footer {@code indexHandle} points to a TOP-level index block whose
     * entries are {@code (largestKey -> bottomLevelIndexBlockHandle)}.
     * Absence (pre-CP-29 SSTables) means the footer {@code indexHandle}
     * points to a single index block whose entries are
     * {@code (largestKey -> dataBlockHandle)} — the original LevelDB layout.
     * See ADR-0014.
     */
    public static final String TWO_LEVEL_INDEX_META_KEY = "rocksdb.index.two_level";

    private final FileChannel channel;
    private final boolean compressBlocks;
    private final WriteHook writeHook;
    /**
     * CP 29 — file-size threshold at which the writer emits a two-level index.
     * Defaults to {@link Constants#TWO_LEVEL_INDEX_THRESHOLD}; tests override
     * with a tiny value so they don't have to write 32 MiB of data to exercise
     * the two-level path.
     */
    private final long twoLevelIndexThreshold;
    /**
     * CP 29 — target size of a single bottom-level index block when two-level
     * is in effect. Defaults to {@link Constants#INDEX_BLOCK_TARGET_BYTES}.
     */
    private final int indexBlockTargetBytes;
    /** CP 29 — accumulated (largestKey, encodedHandle) pairs, one per data block. */
    private final java.util.List<byte[]> indexKeys = new java.util.ArrayList<>();
    private final java.util.List<byte[]> indexHandles = new java.util.ArrayList<>();
    private final BloomFilter.Builder bloomBuilder = new BloomFilter.Builder();
    private final CRC32 crc = new CRC32();

    private BlockBuilder dataBlock = new BlockBuilder();
    private byte[] pendingIndexKey = null;     // largest key of the data block in flight
    private byte[] pendingIndexHandle = null;  // encoded BlockHandle of the just-flushed block
    private long fileOffset = 0L;
    private long totalEntries = 0;
    private boolean finished = false;
    /** CP 17 — range tombstones accumulated before {@link #finish()}. */
    private final java.util.List<com.hkg.rocksdb.common.RangeTombstone> rangeTombstones =
        new java.util.ArrayList<>();

    private BlockBasedTableWriter(FileChannel channel, boolean compressBlocks, WriteHook writeHook,
                                   long twoLevelIndexThreshold, int indexBlockTargetBytes) {
        this.channel = channel;
        this.compressBlocks = compressBlocks;
        this.writeHook = writeHook == null ? WriteHook.NO_OP : writeHook;
        this.twoLevelIndexThreshold = twoLevelIndexThreshold;
        this.indexBlockTargetBytes = indexBlockTargetBytes;
    }

    /** Open a new SSTable file for writing. Truncates any existing file. */
    public static BlockBasedTableWriter open(Path path) throws IOException {
        return open(path, true, WriteHook.NO_OP);
    }

    public static BlockBasedTableWriter open(Path path, boolean compressBlocks) throws IOException {
        return open(path, compressBlocks, WriteHook.NO_OP);
    }

    /**
     * Open with a {@link WriteHook} invoked once per on-disk block immediately
     * before that block's bytes are written. The rate-limited compaction path
     * passes a hook that calls {@code RateLimiter.request(bytes, LOW)} so
     * compaction writers are paced at block granularity.
     */
    public static BlockBasedTableWriter open(Path path, boolean compressBlocks, WriteHook hook)
        throws IOException {
        return openWithIndexTuning(path, compressBlocks, hook,
            Constants.TWO_LEVEL_INDEX_THRESHOLD, Constants.INDEX_BLOCK_TARGET_BYTES);
    }

    /**
     * CP 29 — test-only opener that lets the caller override the two-level
     * threshold and bottom-index block target size. Production callers use the
     * normal {@link #open(Path, boolean, WriteHook)} overload, which hard-codes
     * the {@link Constants} defaults.
     */
    public static BlockBasedTableWriter openWithIndexTuning(Path path, boolean compressBlocks,
                                                             WriteHook hook,
                                                             long twoLevelIndexThreshold,
                                                             int indexBlockTargetBytes)
        throws IOException {
        FileChannel ch = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        return new BlockBasedTableWriter(ch, compressBlocks, hook,
            twoLevelIndexThreshold, indexBlockTargetBytes);
    }

    public void add(InternalKey key, byte[] value) throws IOException {
        if (finished) {
            throw new IllegalStateException("writer already finished");
        }
        // If the previous data block was just flushed, record its index entry now —
        // we use the previous block's last key as the index entry's key.
        flushPendingIndexEntry();

        byte[] keyBytes = InternalKeyCodec.encode(key);
        dataBlock.add(keyBytes, value);
        bloomBuilder.add(key.userKey());
        totalEntries++;

        pendingIndexKey = keyBytes;

        if (dataBlock.estimatedSize() >= Constants.BLOCK_SIZE_BYTES) {
            flushDataBlock();
        }
    }

    /**
     * CP 17 — register a range tombstone that will be persisted as part of
     * the SSTable's range-tombstone meta-block at {@link #finish()}. Multiple
     * RTs may be added; order is preserved on disk.
     */
    public void addRangeTombstone(com.hkg.rocksdb.common.RangeTombstone rt) {
        java.util.Objects.requireNonNull(rt, "rt");
        if (finished) {
            throw new IllegalStateException("writer already finished");
        }
        rangeTombstones.add(rt);
    }

    public Footer finish() throws IOException {
        if (finished) {
            throw new IllegalStateException("writer already finished");
        }

        if (!dataBlock.isEmpty()) {
            flushDataBlock();
        }
        flushPendingIndexEntry();

        // Bloom filter meta-block — uncompressed; bits already pack densely.
        BloomFilter bloom = bloomBuilder.build();
        BlockHandle bloomHandle = writeBlock(BloomBlock.encode(bloom), false);

        // Optional range-tombstone meta-block (CP 17). Empty list → no block emitted;
        // older readers and SSTables without DeleteRange writes look the same on disk.
        BlockHandle rtHandle = null;
        if (!rangeTombstones.isEmpty()) {
            rtHandle = writeBlock(RangeTombstoneBlock.encode(rangeTombstones), false);
        }

        // CP 29 — decide single-level vs. two-level index based on estimated final file size.
        // Estimate = bytes already written (data + bloom + RT) + projected single-level index size.
        long estimatedFinalSize = fileOffset + estimatedSingleLevelIndexBytes();
        boolean twoLevel = estimatedFinalSize > twoLevelIndexThreshold
            && indexKeys.size() > 1;

        BlockHandle indexHandle;
        if (twoLevel) {
            indexHandle = writeTwoLevelIndex();
        } else {
            BlockBuilder indexBlock = new BlockBuilder();
            for (int i = 0; i < indexKeys.size(); i++) {
                indexBlock.add(indexKeys.get(i), indexHandles.get(i));
            }
            indexHandle = writeBlock(indexBlock.finish(), false);
        }

        // Meta-index block: bloom always present; range-tombstones + two-level marker conditional.
        // Meta-index entries must be sorted by key — sort the marker keys deterministically.
        java.util.List<byte[]> metaKeys = new java.util.ArrayList<>();
        java.util.List<byte[]> metaValues = new java.util.ArrayList<>();
        metaKeys.add(BLOOM_META_KEY.getBytes(StandardCharsets.UTF_8));
        metaValues.add(encodeHandle(bloomHandle));
        if (rtHandle != null) {
            metaKeys.add(RANGE_TOMBSTONE_META_KEY.getBytes(StandardCharsets.UTF_8));
            metaValues.add(encodeHandle(rtHandle));
        }
        if (twoLevel) {
            // The two-level marker's value is the top-level index handle (same as footer.indexHandle).
            // Encoding it explicitly keeps the meta-index self-describing.
            metaKeys.add(TWO_LEVEL_INDEX_META_KEY.getBytes(StandardCharsets.UTF_8));
            metaValues.add(encodeHandle(indexHandle));
        }
        // Sort the parallel arrays by key (UTF-8 lexicographic), since BlockBuilder requires sorted input.
        Integer[] order = new Integer[metaKeys.size()];
        for (int i = 0; i < order.length; i++) order[i] = i;
        java.util.Arrays.sort(order, (a, b) ->
            java.util.Arrays.compareUnsigned(metaKeys.get(a), metaKeys.get(b)));
        BlockBuilder metaIndex = new BlockBuilder();
        for (int i : order) {
            metaIndex.add(metaKeys.get(i), metaValues.get(i));
        }
        BlockHandle metaIndexHandle = writeBlock(metaIndex.finish(), false);

        Footer footer = new Footer(metaIndexHandle, indexHandle);
        writeFully(ByteBuffer.wrap(footer.encode()));
        channel.force(true);
        finished = true;
        return footer;
    }

    /** Approximate bytes a single-level index block would occupy on disk. */
    private long estimatedSingleLevelIndexBytes() {
        long bytes = 0L;
        for (int i = 0; i < indexKeys.size(); i++) {
            // 3 varints (header) + key + value bytes; varints are bounded above by 5 bytes apiece.
            bytes += 15L + indexKeys.get(i).length + indexHandles.get(i).length;
        }
        // Restart array + trailer + compType + crc — rough overhead.
        bytes += 64L;
        return bytes;
    }

    /**
     * CP 29 — partition the accumulated index entries into bottom-level index
     * blocks (each ≤ INDEX_BLOCK_TARGET_BYTES), write each to the file, then
     * build and write the top-level index block whose entries are
     * {@code (largestKey-of-bottom -> bottomBlockHandle)}. Returns the handle
     * of the top-level block (which becomes {@code footer.indexHandle}).
     */
    private BlockHandle writeTwoLevelIndex() throws IOException {
        BlockBuilder topLevel = new BlockBuilder();

        BlockBuilder bottom = new BlockBuilder();
        byte[] lastKeyInBottom = null;
        for (int i = 0; i < indexKeys.size(); i++) {
            byte[] key = indexKeys.get(i);
            byte[] handle = indexHandles.get(i);
            // If adding this entry would exceed the target and the bottom block is non-empty,
            // flush the current bottom block first.
            int projected = bottom.estimatedSize() + 15 + key.length + handle.length;
            if (!bottom.isEmpty() && projected > indexBlockTargetBytes) {
                BlockHandle bh = writeBlock(bottom.finish(), false);
                topLevel.add(lastKeyInBottom, encodeHandle(bh));
                bottom = new BlockBuilder();
                lastKeyInBottom = null;
            }
            bottom.add(key, handle);
            lastKeyInBottom = key;
        }
        if (!bottom.isEmpty()) {
            BlockHandle bh = writeBlock(bottom.finish(), false);
            topLevel.add(lastKeyInBottom, encodeHandle(bh));
        }

        return writeBlock(topLevel.finish(), false);
    }

    public long entryCount() { return totalEntries; }
    public long fileSize() { return fileOffset; }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    // ---------- internals ----------

    private void flushDataBlock() throws IOException {
        if (dataBlock.isEmpty()) return;
        byte[] payload = dataBlock.finish();
        BlockHandle handle = writeBlock(payload, compressBlocks);
        pendingIndexHandle = encodeHandle(handle);
        dataBlock = new BlockBuilder();
    }

    private void flushPendingIndexEntry() {
        if (pendingIndexKey != null && pendingIndexHandle != null) {
            indexKeys.add(pendingIndexKey);
            indexHandles.add(pendingIndexHandle);
            pendingIndexKey = null;
            pendingIndexHandle = null;
        }
    }

    /** Write a block: payload || compType:1 || crc32:4 LE. Returns handle covering the payload only. */
    BlockHandle writeBlock(byte[] payload, boolean compress) throws IOException {
        byte compType = Compression.TYPE_NONE;
        byte[] body = payload;
        if (compress) {
            byte[] compressed = Compression.compressDeflate(payload);
            if (compressed.length < payload.length) {
                body = compressed;
                compType = Compression.TYPE_DEFLATE;
            }
        }
        // Pace the write at block granularity. NO_OP is the default; the rate-limited path
        // wires the engine's TokenBucketRateLimiter behind this hook so a compaction worker
        // blocks until enough budget is available before any block bytes hit the channel.
        writeHook.beforeBlockWrite(body.length + 1 + 4);
        long offset = fileOffset;
        writeFully(ByteBuffer.wrap(body));
        writeFully(ByteBuffer.wrap(new byte[] {compType}));
        crc.reset();
        crc.update(body, 0, body.length);
        crc.update(compType);
        ByteBuffer trailer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        trailer.putInt((int) crc.getValue());
        trailer.flip();
        writeFully(trailer);
        return new BlockHandle(offset, body.length);
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.write(buf);
            if (n < 0) throw new IOException("channel EOF on write");
            fileOffset += n;
        }
    }

    private static byte[] encodeHandle(BlockHandle handle) {
        ByteBuffer buf = ByteBuffer.allocate(BlockHandle.MAX_ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        handle.writeTo(buf);
        byte[] out = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, out, 0, out.length);
        return out;
    }
}
