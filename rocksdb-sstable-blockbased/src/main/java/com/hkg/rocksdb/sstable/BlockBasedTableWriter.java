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

    private final FileChannel channel;
    private final boolean compressBlocks;
    private final WriteHook writeHook;
    private final BlockBuilder indexBlock = new BlockBuilder();
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

    private BlockBasedTableWriter(FileChannel channel, boolean compressBlocks, WriteHook writeHook) {
        this.channel = channel;
        this.compressBlocks = compressBlocks;
        this.writeHook = writeHook == null ? WriteHook.NO_OP : writeHook;
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
        FileChannel ch = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
        return new BlockBasedTableWriter(ch, compressBlocks, hook);
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

        // Meta-index block: bloom always present; range-tombstones entry only when non-empty.
        BlockBuilder metaIndex = new BlockBuilder();
        metaIndex.add(BLOOM_META_KEY.getBytes(StandardCharsets.UTF_8), encodeHandle(bloomHandle));
        if (rtHandle != null) {
            metaIndex.add(RANGE_TOMBSTONE_META_KEY.getBytes(StandardCharsets.UTF_8),
                encodeHandle(rtHandle));
        }
        BlockHandle metaIndexHandle = writeBlock(metaIndex.finish(), false);

        // Index block — one entry per data block, with the data block's last key.
        BlockHandle indexHandle = writeBlock(indexBlock.finish(), false);

        Footer footer = new Footer(metaIndexHandle, indexHandle);
        writeFully(ByteBuffer.wrap(footer.encode()));
        channel.force(true);
        finished = true;
        return footer;
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
            indexBlock.add(pendingIndexKey, pendingIndexHandle);
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
