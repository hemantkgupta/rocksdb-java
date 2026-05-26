package com.hkg.rocksdb.wal;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Encodes / decodes a {@link MutationRecord} as the WAL payload bytes.
 *
 * <p>Hand-rolled binary framing (no Protobuf / JSON / Java serialization).
 * Layout (little-endian):
 * <pre>
 *   type:1   ; 1 = Put, 0 = Delete
 *   seq:8    ; sequence number
 *   keyLen:4
 *   key:keyLen
 *   [if Put] valLen:4 ; val:valLen
 * </pre>
 */
public final class MutationCodec {

    private MutationCodec() {}

    public static byte[] encode(MutationRecord mutation) {
        Objects.requireNonNull(mutation, "mutation");
        if (mutation instanceof MutationRecord.Put put) {
            int keyLen = put.key().length();
            int valLen = put.value().length();
            ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 4 + keyLen + 4 + valLen)
                .order(ByteOrder.LITTLE_ENDIAN);
            buf.put((byte) 1);
            buf.putLong(put.sequence().value());
            buf.putInt(keyLen);
            buf.put(put.key().slice().backing(), put.key().slice().offset(), keyLen);
            buf.putInt(valLen);
            buf.put(put.value().backing(), put.value().offset(), valLen);
            return buf.array();
        } else if (mutation instanceof MutationRecord.Delete del) {
            int keyLen = del.key().length();
            ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 4 + keyLen)
                .order(ByteOrder.LITTLE_ENDIAN);
            buf.put((byte) 0);
            buf.putLong(del.sequence().value());
            buf.putInt(keyLen);
            buf.put(del.key().slice().backing(), del.key().slice().offset(), keyLen);
            return buf.array();
        }
        throw new IllegalStateException("unreachable: sealed MutationRecord");
    }

    public static MutationRecord decode(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        if (buf.remaining() < 1 + 8 + 4) {
            throw new WalCorruptionException("payload too short to decode mutation header");
        }
        byte type = buf.get();
        long seq = buf.getLong();
        int keyLen = buf.getInt();
        if (keyLen < 0 || keyLen > buf.remaining()) {
            throw new WalCorruptionException("invalid keyLen=" + keyLen);
        }
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        Key key = Key.of(keyBytes);
        SequenceNumber sequence = new SequenceNumber(seq);

        if (type == 1) {
            if (buf.remaining() < 4) {
                throw new WalCorruptionException("Put payload missing value length");
            }
            int valLen = buf.getInt();
            if (valLen < 0 || valLen > buf.remaining()) {
                throw new WalCorruptionException("invalid valLen=" + valLen);
            }
            byte[] valBytes = new byte[valLen];
            buf.get(valBytes);
            return new MutationRecord.Put(key, Slice.of(valBytes), sequence);
        } else if (type == 0) {
            return new MutationRecord.Delete(key, sequence);
        }
        throw new WalCorruptionException("unknown mutation type byte: " + type);
    }
}
