package com.hkg.rocksdb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TornWriteTest {

    @Test
    void truncationMidFragment_readerStopsCleanlyAtLastFullRecord(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("first".getBytes());
            writer.append("second".getBytes());
            writer.append("third".getBytes());
        }
        long fullSize = java.nio.file.Files.size(log);

        // Simulate a process crash that wrote the headers of the next record but
        // didn't get to flush the payload. Truncate by 1 byte to chop the tail
        // of the last record.
        try (FileChannel ch = FileChannel.open(log, StandardOpenOption.WRITE)) {
            ch.truncate(fullSize - 1);
        }

        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            // We should see "first" and "second" at minimum (the records that fully committed
            // before "third"). "third" was truncated so it's lost — but the reader must NOT throw.
            assertThat(seen).contains("first", "second");
            assertThat(seen).doesNotContain("third");
        }
    }

    @Test
    void truncationWithinHeader_readerStopsClean(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("only-record".getBytes());
        }
        long fullSize = java.nio.file.Files.size(log);

        // Truncate to fewer bytes than a header — the reader sees an incomplete header at EOF.
        try (FileChannel ch = FileChannel.open(log, StandardOpenOption.WRITE)) {
            ch.truncate(WalConstants.HEADER_SIZE - 2);
        }
        assertThat(java.nio.file.Files.size(log)).isLessThan(fullSize);

        try (LogReader reader = LogReader.open(log)) {
            // No record fully committed before the truncation — readRecord returns empty.
            assertThat(reader.readRecord()).isEmpty();
        }
    }

    @Test
    void multiFragmentRecordTornAtTail_droppedSilently(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("wal.log");
        byte[] big = new byte[WalConstants.BLOCK_SIZE * 2 + 500];
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i & 0xff);

        try (LogWriter writer = LogWriter.open(log, true)) {
            writer.append("alpha".getBytes());
            writer.append(big);   // spans 3 blocks
            writer.append("beta".getBytes()); // would commit after big
        }
        long fullSize = java.nio.file.Files.size(log);

        // Truncate inside the last fragment of `big` (kill `big` mid-write, lose `beta` too).
        try (FileChannel ch = FileChannel.open(log, StandardOpenOption.WRITE)) {
            ch.truncate(WalConstants.BLOCK_SIZE * 2 + 100); // mid third block
        }
        assertThat(java.nio.file.Files.size(log)).isLessThan(fullSize);

        try (LogReader reader = LogReader.open(log)) {
            byte[] r1 = reader.readRecord().orElseThrow();
            assertThat(new String(r1)).isEqualTo("alpha");
            // The big record was torn; the LAST fragment never landed → reader returns empty.
            assertThat(reader.readRecord()).isEmpty();
        }
    }
}
