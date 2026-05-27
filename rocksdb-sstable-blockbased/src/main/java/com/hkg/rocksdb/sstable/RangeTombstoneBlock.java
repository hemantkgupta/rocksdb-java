package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.RangeTombstone;
import com.hkg.rocksdb.common.SequenceNumber;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * CP 17 — serialisation of an SSTable's {@link RangeTombstone range tombstones}
 * as a dedicated meta-block. Layout (little-endian):
 *
 * <pre>
 *   count:4
 *   ; repeated count times:
 *   startLen:4
 *   start[startLen]
 *   endLen:4
 *   end[endLen]
 *   seq:8
 * </pre>
 *
 * <p>The block is referenced from the SSTable's meta-index under the key
 * {@link BlockBasedTableWriter#RANGE_TOMBSTONE_META_KEY}. Older SSTables
 * written before CP 17 simply have no such entry; readers fall back to an
 * empty RT list, preserving on-disk forward compatibility with
 * leveldb-java DBs.
 */
public final class RangeTombstoneBlock {

    private RangeTombstoneBlock() {}

    public static byte[] encode(List<RangeTombstone> tombstones) {
        int size = 4;
        for (RangeTombstone rt : tombstones) {
            size += 4 + rt.startKey().length()
                + 4 + rt.endKey().length()
                + 8;
        }
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(tombstones.size());
        for (RangeTombstone rt : tombstones) {
            byte[] sk = rt.startKey().bytes();
            byte[] ek = rt.endKey().bytes();
            buf.putInt(sk.length);
            buf.put(sk);
            buf.putInt(ek.length);
            buf.put(ek);
            buf.putLong(rt.sequence().value());
        }
        return buf.array();
    }

    public static List<RangeTombstone> decode(byte[] data) {
        if (data.length < 4) {
            throw new SsTableFormatException("range-tombstone block too short: " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        if (count < 0) {
            throw new SsTableFormatException("range-tombstone count negative: " + count);
        }
        List<RangeTombstone> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (buf.remaining() < 4) {
                throw new SsTableFormatException("RT block truncated at startLen for entry " + i);
            }
            int sLen = buf.getInt();
            if (sLen < 0 || sLen > buf.remaining()) {
                throw new SsTableFormatException("RT startLen invalid: " + sLen);
            }
            byte[] sBytes = new byte[sLen];
            buf.get(sBytes);
            if (buf.remaining() < 4) {
                throw new SsTableFormatException("RT block truncated at endLen for entry " + i);
            }
            int eLen = buf.getInt();
            if (eLen < 0 || eLen > buf.remaining()) {
                throw new SsTableFormatException("RT endLen invalid: " + eLen);
            }
            byte[] eBytes = new byte[eLen];
            buf.get(eBytes);
            if (buf.remaining() < 8) {
                throw new SsTableFormatException("RT block truncated at seq for entry " + i);
            }
            long seq = buf.getLong();
            out.add(new RangeTombstone(Key.of(sBytes), Key.of(eBytes), new SequenceNumber(seq)));
        }
        return out;
    }
}
