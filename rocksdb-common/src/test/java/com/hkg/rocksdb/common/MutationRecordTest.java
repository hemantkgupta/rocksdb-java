package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MutationRecordTest {

    @Test
    void putExposesKey() {
        MutationRecord r = new MutationRecord.Put(
            Key.of("k"), Slice.of("v"), new SequenceNumber(1L));
        assertThat(r.key()).isEqualTo(Key.of("k"));
    }

    @Test
    void deleteExposesKey() {
        MutationRecord r = new MutationRecord.Delete(Key.of("k"), new SequenceNumber(2L));
        assertThat(r.key()).isEqualTo(Key.of("k"));
    }

    @Test
    void instanceofPatternsOverSealedHierarchy() {
        MutationRecord put = new MutationRecord.Put(
            Key.of("k"), Slice.of("v"), new SequenceNumber(1L));
        MutationRecord del = new MutationRecord.Delete(Key.of("k"), new SequenceNumber(2L));

        assertThat(shape(put)).isEqualTo("put-1");
        assertThat(shape(del)).isEqualTo("del");
    }

    private static String shape(MutationRecord r) {
        // Pattern-matching in switch is a Java 21 feature; using instanceof
        // patterns (Java 17) over the sealed hierarchy.
        if (r instanceof MutationRecord.Put put) {
            return "put-" + put.value().length();
        } else if (r instanceof MutationRecord.Delete) {
            return "del";
        }
        throw new IllegalStateException("unreachable: sealed interface exhaustively handled");
    }
}
