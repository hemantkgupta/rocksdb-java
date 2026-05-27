package com.hkg.rocksdb.wal;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.SequenceNumber;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteRangeCodecTest {

    @Test
    void encodeDecodeRoundtrip() {
        MutationRecord.DeleteRange dr = new MutationRecord.DeleteRange(
            Key.of("alpha"), Key.of("omega"), new SequenceNumber(42L));
        byte[] encoded = MutationCodec.encode(dr);
        MutationRecord decoded = MutationCodec.decode(encoded);
        assertThat(decoded).isInstanceOf(MutationRecord.DeleteRange.class);
        MutationRecord.DeleteRange got = (MutationRecord.DeleteRange) decoded;
        assertThat(got.startKey()).isEqualTo(Key.of("alpha"));
        assertThat(got.endKey()).isEqualTo(Key.of("omega"));
        assertThat(got.sequence().value()).isEqualTo(42L);
    }

    @Test
    void putAndDeleteStillRoundtripUnchanged() {
        // CP 17 must not have broken backward compat for opType 0x01 / 0x00.
        MutationRecord.Put put = new MutationRecord.Put(
            Key.of("k"), com.hkg.rocksdb.common.Slice.of("v"), new SequenceNumber(1L));
        assertThat(MutationCodec.decode(MutationCodec.encode(put))).isEqualTo(put);

        MutationRecord.Delete del = new MutationRecord.Delete(
            Key.of("k"), new SequenceNumber(2L));
        assertThat(MutationCodec.decode(MutationCodec.encode(del))).isEqualTo(del);
    }

    @Test
    void unknownOpTypeRejected() {
        byte[] bogus = new byte[] {(byte) 0x09, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        try {
            MutationCodec.decode(bogus);
            org.junit.jupiter.api.Assertions.fail("expected WalCorruptionException");
        } catch (WalCorruptionException expected) {
            // ok
        }
    }
}
