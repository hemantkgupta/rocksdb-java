package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.CacheKey;
import com.hkg.rocksdb.bloom.BloomFilter;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.InternalKeyCodec;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.KeyLookup;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.ValueType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Reads SSTable files written by {@link BlockBasedTableWriter}. Opens by
 * reading the footer (last 48 bytes), then the index and bloom-filter blocks;
 * data blocks are loaded on demand. CRC32 is verified for every block read;
 * a mismatch throws {@link BlockChecksumMismatchException}.
 */
public final class BlockBasedTableReader implements Closeable {

    private final FileChannel channel;
    private final long fileSize;
    private final Footer footer;
    private final Block indexBlock;
    private final BloomFilter bloomFilter;
    private final FileNumber fileNumber;
    private final BlockCache cache;

    private BlockBasedTableReader(FileChannel channel, long fileSize, Footer footer,
                                   Block indexBlock, BloomFilter bloomFilter,
                                   FileNumber fileNumber, BlockCache cache) {
        this.channel = channel;
        this.fileSize = fileSize;
        this.footer = footer;
        this.indexBlock = indexBlock;
        this.bloomFilter = bloomFilter;
        this.fileNumber = fileNumber;
        this.cache = cache;
    }

    /** Open and parse an SSTable file with no block cache (every data-block read hits disk). */
    public static BlockBasedTableReader open(Path path) throws IOException {
        return open(path, null, null);
    }

    /**
     * Open and parse an SSTable file. When {@code cache} is non-null, data-block
     * reads are routed through the cache keyed by {@code (fileNumber, blockOffset)}.
     * The index and bloom-filter blocks are loaded eagerly at open time and are
     * not cached separately — they live in the reader instance.
     */
    public static BlockBasedTableReader open(Path path, FileNumber fileNumber, BlockCache cache)
        throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        long size = ch.size();
        if (size < Footer.ENCODED_LENGTH) {
            ch.close();
            throw new SsTableFormatException("file too short to hold footer: " + size);
        }
        // Read footer.
        ByteBuffer footerBuf = ByteBuffer.allocate(Footer.ENCODED_LENGTH);
        readAt(ch, size - Footer.ENCODED_LENGTH, footerBuf);
        Footer footer = Footer.decode(footerBuf.array());

        // Bootstrap reader without the cache — the index/bloom blocks are loaded directly,
        // skipping the cache (they would just churn it on open and they're already in-memory).
        BlockBasedTableReader bootstrap = new BlockBasedTableReader(
            ch, size, footer, null, null, fileNumber, null);

        byte[] indexBytes = bootstrap.readBlockDirect(footer.indexHandle());
        Block indexBlock = new Block(indexBytes);

        byte[] metaIndexBytes = bootstrap.readBlockDirect(footer.metaIndexHandle());
        Block metaIndex = new Block(metaIndexBytes);
        BloomFilter bloom = loadBloomFilter(bootstrap, metaIndex);

        return new BlockBasedTableReader(ch, size, footer, indexBlock, bloom, fileNumber, cache);
    }

    /**
     * Look up a user key as of the given {@link SequenceNumber}. Returns
     * {@link Optional#empty()} if the key is absent (or the most-recent
     * visible record at-or-before {@code asOf} is a tombstone).
     */
    public Optional<Slice> get(Key userKey, SequenceNumber asOf) throws IOException {
        if (!bloomFilter.mightContain(userKey)) {
            return Optional.empty();
        }
        InternalKey probe = new InternalKey(userKey, asOf, ValueType.VALUE);
        byte[] probeBytes = InternalKeyCodec.encode(probe);

        int idx = indexBlock.seekIndex(probeBytes, InternalKeyCodec::compareInternalBytes);
        if (idx < 0) {
            return Optional.empty();
        }
        Block.Entry indexEntry = indexBlock.get(idx);
        BlockHandle dataHandle = BlockHandle.readFrom(
            ByteBuffer.wrap(indexEntry.value()).order(ByteOrder.LITTLE_ENDIAN));
        byte[] dataBytes = readBlock(dataHandle);
        Block dataBlock = new Block(dataBytes);

        int hitIdx = dataBlock.seekIndex(probeBytes, InternalKeyCodec::compareInternalBytes);
        if (hitIdx < 0) {
            return Optional.empty();
        }
        Block.Entry hit = dataBlock.get(hitIdx);
        byte[] hitUserKey = InternalKeyCodec.userKeyOf(hit.key());
        if (!java.util.Arrays.equals(hitUserKey, userKey.bytes())) {
            return Optional.empty();
        }
        byte tag = InternalKeyCodec.tagOf(hit.key());
        if (tag == ValueType.DELETION.tag()) {
            return Optional.empty();
        }
        return Optional.of(Slice.of(hit.value()));
    }

    /** Lookup at the maximum sequence (no snapshot). */
    public Optional<Slice> get(Key userKey) throws IOException {
        return get(userKey, new SequenceNumber(SequenceNumber.MAX));
    }

    /**
     * Three-way lookup used by the engine's read path: distinguishes
     * {@code Found(value)}, {@code Tombstoned} (the engine must NOT probe
     * older files), and {@code Absent} (the engine keeps looking).
     */
    public KeyLookup lookup(Key userKey, SequenceNumber asOf) throws IOException {
        if (!bloomFilter.mightContain(userKey)) {
            return KeyLookup.ABSENT;
        }
        InternalKey probe = new InternalKey(userKey, asOf, ValueType.VALUE);
        byte[] probeBytes = InternalKeyCodec.encode(probe);

        int idx = indexBlock.seekIndex(probeBytes, InternalKeyCodec::compareInternalBytes);
        if (idx < 0) return KeyLookup.ABSENT;
        Block.Entry indexEntry = indexBlock.get(idx);
        BlockHandle dataHandle = BlockHandle.readFrom(
            ByteBuffer.wrap(indexEntry.value()).order(ByteOrder.LITTLE_ENDIAN));
        byte[] dataBytes = readBlock(dataHandle);
        Block dataBlock = new Block(dataBytes);

        int hitIdx = dataBlock.seekIndex(probeBytes, InternalKeyCodec::compareInternalBytes);
        if (hitIdx < 0) return KeyLookup.ABSENT;
        Block.Entry hit = dataBlock.get(hitIdx);
        byte[] hitUserKey = InternalKeyCodec.userKeyOf(hit.key());
        if (!java.util.Arrays.equals(hitUserKey, userKey.bytes())) {
            return KeyLookup.ABSENT;
        }
        byte tag = InternalKeyCodec.tagOf(hit.key());
        if (tag == ValueType.DELETION.tag()) {
            return KeyLookup.TOMBSTONED;
        }
        return new KeyLookup.Found(Slice.of(hit.value()));
    }

    /**
     * Walk every internal entry in this SSTable in sort order. Used by the
     * compactor's merging iterator.
     */
    public Iterator<SsTableEntry> entries() throws IOException {
        return new Iterator<>() {
            int dataIdx = 0;
            Block currentDataBlock = null;
            int withinBlock = 0;
            SsTableEntry next = pull();

            private SsTableEntry pull() {
                try {
                    while (true) {
                        if (currentDataBlock != null && withinBlock < currentDataBlock.size()) {
                            Block.Entry e = currentDataBlock.get(withinBlock++);
                            InternalKey ik = InternalKeyCodec.decode(e.key());
                            return new SsTableEntry(ik, e.value());
                        }
                        if (dataIdx >= indexBlock.size()) {
                            return null;
                        }
                        Block.Entry indexEntry = indexBlock.get(dataIdx++);
                        BlockHandle h = BlockHandle.readFrom(
                            ByteBuffer.wrap(indexEntry.value()).order(ByteOrder.LITTLE_ENDIAN));
                        currentDataBlock = new Block(readBlock(h));
                        withinBlock = 0;
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override public boolean hasNext() { return next != null; }
            @Override public SsTableEntry next() {
                if (next == null) throw new NoSuchElementException();
                SsTableEntry r = next;
                next = pull();
                return r;
            }
        };
    }

    public Footer footer() { return footer; }
    public BloomFilter bloomFilter() { return bloomFilter; }
    public Block indexBlock() { return indexBlock; }
    public long fileSize() { return fileSize; }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    // ---------- internals ----------

    /**
     * Read a block's payload bytes (decompressed if needed). If this reader is
     * wired to a {@link BlockCache} and a {@link FileNumber} (i.e. opened via
     * {@link #open(Path, FileNumber, BlockCache)}), the read is routed through
     * the cache so a repeated read of the same block is served from memory.
     * A CRC32 mismatch on the on-disk bytes throws
     * {@link BlockChecksumMismatchException}.
     */
    byte[] readBlock(BlockHandle handle) throws IOException {
        if (cache != null && fileNumber != null) {
            CacheKey ck = new CacheKey(fileNumber, handle.offset());
            return cache.lookupOrLoad(ck, () -> readBlockDirect(handle));
        }
        return readBlockDirect(handle);
    }

    /** Bypass the cache: read straight from disk and verify CRC. */
    byte[] readBlockDirect(BlockHandle handle) throws IOException {
        long payloadStart = handle.offset();
        long payloadLen = handle.length();
        long totalLen = payloadLen + 1L + 4L; // payload + compType + crc
        if (payloadStart < 0 || totalLen > fileSize - payloadStart) {
            throw new SsTableFormatException("block handle out of range");
        }
        ByteBuffer raw = ByteBuffer.allocate((int) totalLen);
        readAt(channel, payloadStart, raw);
        byte[] body = new byte[(int) payloadLen];
        System.arraycopy(raw.array(), 0, body, 0, body.length);
        byte compType = raw.array()[(int) payloadLen];
        int crcStored = ByteBuffer.wrap(raw.array(), (int) payloadLen + 1, 4)
            .order(ByteOrder.LITTLE_ENDIAN).getInt();

        CRC32 crc = new CRC32();
        crc.update(body, 0, body.length);
        crc.update(compType);
        int crcActual = (int) crc.getValue();
        if (crcActual != crcStored) {
            throw new BlockChecksumMismatchException(
                "block CRC mismatch at offset " + payloadStart
                    + ": stored=" + crcStored + " actual=" + crcActual);
        }
        return Compression.decode(compType, body);
    }

    private static BloomFilter loadBloomFilter(BlockBasedTableReader reader, Block metaIndex)
        throws IOException {
        for (Block.Entry e : metaIndex.entries()) {
            String name = new String(e.key(), StandardCharsets.UTF_8);
            if (BlockBasedTableWriter.BLOOM_META_KEY.equals(name)) {
                BlockHandle bh = BlockHandle.readFrom(
                    ByteBuffer.wrap(e.value()).order(ByteOrder.LITTLE_ENDIAN));
                byte[] bloomBytes = reader.readBlock(bh);
                return BloomBlock.decode(bloomBytes);
            }
        }
        throw new SsTableFormatException("no bloom filter entry in meta-index block");
    }

    private static void readAt(FileChannel ch, long position, ByteBuffer dst) throws IOException {
        // Positional read keeps the channel's current position untouched, so concurrent
        // readers sharing this channel don't race on the cursor.
        long pos = position;
        while (dst.hasRemaining()) {
            int n = ch.read(dst, pos);
            if (n < 0) throw new SsTableFormatException("unexpected EOF at " + pos);
            pos += n;
        }
        dst.flip();
    }

    /** A decoded entry as yielded by {@link #entries()}. */
    public record SsTableEntry(InternalKey internalKey, byte[] value) {}
}
