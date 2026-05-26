package com.hkg.rocksdb.sstable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A parsed SSTable block. Decodes the entries eagerly on construction.
 *
 * <p>Eager decoding trades a small amount of memory (one decoded {@link Entry}
 * list per block) for simpler binary search and iteration. Per-block size is
 * bounded at ~4 KiB, so a dozen decoded blocks are still well under a MiB —
 * acceptable for the pedagogical port.
 */
public final class Block {

    private final List<Entry> entries;

    public Block(byte[] data) {
        if (data.length < 4) {
            throw new SsTableFormatException("block too short: " + data.length);
        }
        ByteBuffer trailer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int numRestarts = trailer.getInt(data.length - 4);
        if (numRestarts < 0 || numRestarts > (data.length - 4) / 4) {
            throw new SsTableFormatException("bad restart count: " + numRestarts);
        }
        int restartsArrayLen = 4 * numRestarts + 4;
        int payloadEnd = data.length - restartsArrayLen;
        if (payloadEnd < 0) {
            throw new SsTableFormatException("restart array overflows block");
        }

        this.entries = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(data, 0, payloadEnd).order(ByteOrder.LITTLE_ENDIAN);
        byte[] lastKey = new byte[0];
        while (buf.position() < payloadEnd) {
            int shared = VarInt.readVarInt(buf);
            int unshared = VarInt.readVarInt(buf);
            int valueLen = VarInt.readVarInt(buf);
            if (shared < 0 || unshared < 0 || valueLen < 0) {
                throw new SsTableFormatException("negative length in block entry");
            }
            if (shared > lastKey.length) {
                throw new SsTableFormatException("shared prefix exceeds previous key length");
            }
            byte[] key = new byte[shared + unshared];
            System.arraycopy(lastKey, 0, key, 0, shared);
            if (unshared > 0) {
                if (buf.remaining() < unshared) {
                    throw new SsTableFormatException("block truncated at key");
                }
                buf.get(key, shared, unshared);
            }
            byte[] value = new byte[valueLen];
            if (valueLen > 0) {
                if (buf.remaining() < valueLen) {
                    throw new SsTableFormatException("block truncated at value");
                }
                buf.get(value);
            }
            entries.add(new Entry(key, value));
            lastKey = key;
        }
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * Binary-search for the first entry whose key is &gt;= {@code target}.
     * Returns {@code -1} if no such entry exists.
     */
    public int seekIndex(byte[] target, Comparator<byte[]> cmp) {
        int lo = 0;
        int hi = entries.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cmp.compare(entries.get(mid).key(), target) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo == entries.size() ? -1 : lo;
    }

    public Entry get(int index) {
        return entries.get(index);
    }

    public int size() {
        return entries.size();
    }

    public record Entry(byte[] key, byte[] value) {}
}
