package com.hkg.rocksdb.wal;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.MutationRecord;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MutationCodecTest {

    @Test
    void put_roundTrip() {
        MutationRecord.Put original = new MutationRecord.Put(
            Key.of("hello"),
            Slice.of("world"),
            new SequenceNumber(42L));
        byte[] encoded = MutationCodec.encode(original);
        MutationRecord decoded = MutationCodec.decode(encoded);
        assertThat(decoded).isInstanceOf(MutationRecord.Put.class);
        MutationRecord.Put p = (MutationRecord.Put) decoded;
        assertThat(p.key()).isEqualTo(Key.of("hello"));
        assertThat(p.value()).isEqualTo(Slice.of("world"));
        assertThat(p.sequence().value()).isEqualTo(42L);
    }

    @Test
    void delete_roundTrip() {
        MutationRecord.Delete original = new MutationRecord.Delete(
            Key.of("removed"),
            new SequenceNumber(7L));
        byte[] encoded = MutationCodec.encode(original);
        MutationRecord decoded = MutationCodec.decode(encoded);
        assertThat(decoded).isInstanceOf(MutationRecord.Delete.class);
        MutationRecord.Delete d = (MutationRecord.Delete) decoded;
        assertThat(d.key()).isEqualTo(Key.of("removed"));
        assertThat(d.sequence().value()).isEqualTo(7L);
    }

    @Test
    void emptyValuePut_roundTrip() {
        MutationRecord.Put original = new MutationRecord.Put(
            Key.of("k"), Slice.of(new byte[0]), new SequenceNumber(1L));
        byte[] encoded = MutationCodec.encode(original);
        MutationRecord.Put decoded = (MutationRecord.Put) MutationCodec.decode(encoded);
        assertThat(decoded.value().length()).isZero();
    }

    @Test
    void truncatedPayload_throwsCorruption() {
        MutationRecord.Put original = new MutationRecord.Put(
            Key.of("hello"), Slice.of("world"), new SequenceNumber(1L));
        byte[] encoded = MutationCodec.encode(original);
        byte[] truncated = new byte[encoded.length - 1];
        System.arraycopy(encoded, 0, truncated, 0, truncated.length);
        assertThatThrownBy(() -> MutationCodec.decode(truncated))
            .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void unknownTypeByte_throwsCorruption() {
        // Construct an invalid payload by hand.
        byte[] bad = new byte[] {
            (byte) 42,                                    // unknown type
            0, 0, 0, 0, 0, 0, 0, 0,                       // seq = 0
            1, 0, 0, 0,                                    // keyLen = 1
            (byte) 'k'                                    // key
        };
        assertThatThrownBy(() -> MutationCodec.decode(bad))
            .isInstanceOf(WalCorruptionException.class);
    }

    @Test
    void writerReader_endToEndViaCodec(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp)
            throws java.io.IOException {
        java.nio.file.Path log = tmp.resolve("wal.log");
        MutationRecord m1 = new MutationRecord.Put(Key.of("a"), Slice.of("1"), new SequenceNumber(1L));
        MutationRecord m2 = new MutationRecord.Delete(Key.of("a"), new SequenceNumber(2L));
        MutationRecord m3 = new MutationRecord.Put(Key.of("b"), Slice.of("2"), new SequenceNumber(3L));

        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append(MutationCodec.encode(m1));
            writer.append(MutationCodec.encode(m2));
            writer.append(MutationCodec.encode(m3));
        }
        try (LogReader reader = LogReader.open(log)) {
            MutationRecord r1 = MutationCodec.decode(reader.readRecord().orElseThrow());
            MutationRecord r2 = MutationCodec.decode(reader.readRecord().orElseThrow());
            MutationRecord r3 = MutationCodec.decode(reader.readRecord().orElseThrow());
            assertThat(reader.readRecord()).isEmpty();
            assertThat(r1).isInstanceOf(MutationRecord.Put.class);
            assertThat(r2).isInstanceOf(MutationRecord.Delete.class);
            assertThat(r3).isInstanceOf(MutationRecord.Put.class);
            assertThat(r1.sequence().value()).isEqualTo(1L);
            assertThat(r2.sequence().value()).isEqualTo(2L);
            assertThat(r3.sequence().value()).isEqualTo(3L);
        }
    }
}
