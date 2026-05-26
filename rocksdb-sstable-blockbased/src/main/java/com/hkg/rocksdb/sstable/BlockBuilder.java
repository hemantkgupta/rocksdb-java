package com.hkg.rocksdb.sstable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates sorted (key, value) byte pairs into a single SSTable block,
 * using prefix compression with restart points every {@link #RESTART_INTERVAL}
 * entries. The same layout is used for data blocks, the index block, and
 * the meta-index block (only the value bytes differ in meaning).
 *
 * <p>Layout:
 * <pre>
 *   for each entry:
 *     varint(sharedBytes)
 *     varint(unsharedBytes)
 *     varint(valueLength)
 *     unsharedKeyBytes
 *     valueBytes
 *   restart array  (4 B little-endian per restart)
 *   restart count  (4 B little-endian)
 * </pre>
 *
 * <p>The block bytes returned by {@link #finish()} do NOT include the
 * compression-type tag or CRC32 trailer; the writer (see
 * {@link BlockBasedTableWriter#writeBlock(byte[], boolean)}) appends those.
 *
 * <p>Caller's contract: keys must be added in ascending order. The builder
 * does not verify this — passing unsorted keys produces an invalid block.
 */
public final class BlockBuilder {

    /**
     * Entries between restart points. Matches RocksDb's default. Trades a
     * little CPU on lookup (linear scan within a restart group) for tighter
     * prefix compression.
     */
    public static final int RESTART_INTERVAL = 16;

    private final ByteArrayOutputStream entries = new ByteArrayOutputStream();
    private final List<Integer> restartOffsets = new ArrayList<>();
    private byte[] lastKey = new byte[0];
    private int counter = 0; // entries since last restart point
    private boolean finished = false;

    public BlockBuilder() {
        restartOffsets.add(0); // entry 0 is always a restart point
    }

    public boolean isEmpty() {
        return entries.size() == 0;
    }

    /** Approximate current block size including the restart trailer. */
    public int estimatedSize() {
        return entries.size() + 4 * restartOffsets.size() + 4;
    }

    public int entryCount() {
        return totalEntries;
    }

    private int totalEntries = 0;

    public void add(byte[] key, byte[] value) {
        if (finished) {
            throw new IllegalStateException("block already finished");
        }
        int shared;
        if (counter < RESTART_INTERVAL) {
            shared = sharedPrefix(lastKey, key);
        } else {
            // Start a new restart group.
            restartOffsets.add(entries.size());
            counter = 0;
            shared = 0;
        }
        int unsharedLen = key.length - shared;

        // Per-entry header: three varints. Max varint = 5 bytes for int; allocate 16 for headroom.
        ByteBuffer hdr = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        VarInt.writeVarInt(hdr, shared);
        VarInt.writeVarInt(hdr, unsharedLen);
        VarInt.writeVarInt(hdr, value.length);
        hdr.flip();
        try {
            entries.write(hdr.array(), 0, hdr.limit());
            if (unsharedLen > 0) {
                entries.write(key, shared, unsharedLen);
            }
            if (value.length > 0) {
                entries.write(value);
            }
        } catch (IOException e) {
            throw new RuntimeException("ByteArrayOutputStream.write should not throw", e);
        }

        lastKey = key;
        counter++;
        totalEntries++;
    }

    /** Finish the block: append the restart array + count. Returns the raw bytes. */
    public byte[] finish() {
        if (finished) {
            throw new IllegalStateException("block already finished");
        }
        ByteBuffer trailer = ByteBuffer.allocate(4 * restartOffsets.size() + 4)
            .order(ByteOrder.LITTLE_ENDIAN);
        for (int off : restartOffsets) {
            trailer.putInt(off);
        }
        trailer.putInt(restartOffsets.size());
        try {
            entries.write(trailer.array());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        finished = true;
        return entries.toByteArray();
    }

    static int sharedPrefix(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) i++;
        return i;
    }
}
