package com.hkg.rocksdb.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Reads the RocksDb-formatted WAL written by {@link LogWriter}.
 *
 * <p>Behaviour at end of file:
 * <ul>
 *   <li>If the last record is intact (CRC matches, type sequence valid), it is returned.</li>
 *   <li>If the file ends mid-fragment (header truncated, payload truncated, or CRC mismatch
 *       on the trailing fragment), the reader stops cleanly — no exception. The caller has
 *       received every record that committed before the crash. This is how RocksDb tolerates
 *       a process kill mid-WAL-write.</li>
 *   <li>If a CRC mismatch occurs in the middle of the file (a fragment with valid fragments
 *       after it), this is data corruption — the reader throws {@link WalCorruptionException}.</li>
 * </ul>
 *
 * <p>The reader uses a streaming iterator that reads one block of {@link WalConstants#BLOCK_SIZE}
 * bytes at a time.
 */
public final class LogReader implements Closeable {

    private final FileChannel channel;
    private final long fileSize;
    private final ByteBuffer blockBuf;
    private long blockStartPos;       // file offset of the current block
    private int blockBytesRead;       // bytes consumed from the current block
    private boolean eofReached;
    private final CRC32 crc = new CRC32();

    private LogReader(FileChannel channel, long fileSize) {
        this.channel = channel;
        this.fileSize = fileSize;
        this.blockBuf = ByteBuffer.allocate(WalConstants.BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        this.blockBuf.limit(0); // start empty so the first read fills it
        this.blockStartPos = 0L;
        this.blockBytesRead = 0;
        this.eofReached = false;
    }

    public static LogReader open(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        return new LogReader(ch, ch.size());
    }

    /**
     * Read the next complete logical record (reassembling fragments).
     * Returns empty when the end of the file has been reached cleanly,
     * or when a torn write is detected at the trailing fragment.
     */
    public Optional<byte[]> readRecord() throws IOException {
        java.io.ByteArrayOutputStream payload = new java.io.ByteArrayOutputStream();
        boolean inMultiFragment = false;
        while (true) {
            Optional<Fragment> next = readFragment();
            if (next.isEmpty()) {
                // EOF — either no records left, or torn write at the tail.
                if (inMultiFragment) {
                    // A FIRST/MIDDLE was followed by EOF — torn record. Drop it (RocksDb behaviour).
                    return Optional.empty();
                }
                return Optional.empty();
            }
            Fragment f = next.get();
            switch (f.type) {
                case FULL -> {
                    if (inMultiFragment) {
                        throw new WalCorruptionException(
                            "saw FULL fragment while in the middle of FIRST/MIDDLE chain");
                    }
                    return Optional.of(f.payload);
                }
                case FIRST -> {
                    if (inMultiFragment) {
                        throw new WalCorruptionException(
                            "saw FIRST fragment while already in FIRST/MIDDLE chain");
                    }
                    payload.write(f.payload, 0, f.payload.length);
                    inMultiFragment = true;
                }
                case MIDDLE -> {
                    if (!inMultiFragment) {
                        throw new WalCorruptionException("saw MIDDLE fragment without a FIRST");
                    }
                    payload.write(f.payload, 0, f.payload.length);
                }
                case LAST -> {
                    if (!inMultiFragment) {
                        throw new WalCorruptionException("saw LAST fragment without a FIRST");
                    }
                    payload.write(f.payload, 0, f.payload.length);
                    return Optional.of(payload.toByteArray());
                }
                case ZERO_PADDING -> {
                    // Should not be returned by readFragment as a real fragment, but defensive.
                }
            }
        }
    }

    /**
     * Convenience: return an iterator of every record, stopping cleanly at EOF.
     */
    public Iterator<byte[]> records() {
        return new Iterator<>() {
            byte[] next = pull();

            private byte[] pull() {
                try {
                    return readRecord().orElse(null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override public boolean hasNext() { return next != null; }
            @Override public byte[] next() {
                if (next == null) throw new NoSuchElementException();
                byte[] r = next;
                next = pull();
                return r;
            }
        };
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    // ----- internal -----

    private Optional<Fragment> readFragment() throws IOException {
        while (true) {
            int blockRemaining = blockBuf.limit() - blockBytesRead;
            if (blockRemaining < WalConstants.HEADER_SIZE) {
                // Move to next block.
                if (!fillNextBlock()) {
                    return Optional.empty(); // EOF
                }
                continue;
            }

            // Peek at the header without consuming yet.
            int headerStart = blockBytesRead;
            int crcStored = blockBuf.getInt(headerStart);
            int length = blockBuf.getShort(headerStart + 4) & 0xffff;
            byte typeTag = blockBuf.get(headerStart + 6);

            if (typeTag == RecordType.ZERO_PADDING.tag()) {
                // Padding to end of block — skip to next block.
                blockBytesRead = blockBuf.limit();
                continue;
            }

            int needed = WalConstants.HEADER_SIZE + length;
            if (blockBytesRead + needed > blockBuf.limit()) {
                // Truncated fragment at end of file (the block didn't contain the full payload).
                // Treat as torn write.
                return Optional.empty();
            }

            // Consume the header.
            blockBytesRead += WalConstants.HEADER_SIZE;
            // Extract payload.
            byte[] payload = new byte[length];
            if (length > 0) {
                blockBuf.position(blockBytesRead);
                blockBuf.get(payload, 0, length);
                blockBytesRead += length;
            }

            RecordType type;
            try {
                type = RecordType.fromTag(typeTag);
            } catch (WalCorruptionException ex) {
                // Unknown type at tail-of-file — treat as torn write rather than corruption.
                if (atTailOfFile()) {
                    return Optional.empty();
                }
                throw ex;
            }

            // Verify CRC.
            crc.reset();
            crc.update(typeTag);
            if (length > 0) {
                crc.update(payload, 0, length);
            }
            int actual = (int) crc.getValue();
            if (actual != crcStored) {
                if (atTailOfFile()) {
                    return Optional.empty(); // Torn write — last fragment didn't fully commit.
                }
                throw new WalCorruptionException("CRC mismatch at block offset "
                    + (blockStartPos + headerStart) + ": stored=" + crcStored + " actual=" + actual);
            }

            return Optional.of(new Fragment(type, payload));
        }
    }

    private boolean fillNextBlock() throws IOException {
        if (eofReached) return false;
        long nextBlockStart = (blockBytesRead == 0 && blockBuf.limit() == 0)
            ? 0L
            : blockStartPos + blockBuf.limit();
        if (nextBlockStart >= fileSize) {
            eofReached = true;
            return false;
        }
        channel.position(nextBlockStart);
        blockBuf.clear();
        int toRead = (int) Math.min(WalConstants.BLOCK_SIZE, fileSize - nextBlockStart);
        blockBuf.limit(toRead);
        int total = 0;
        while (total < toRead) {
            int n = channel.read(blockBuf);
            if (n < 0) break;
            total += n;
        }
        // Adjust limit to what we actually read.
        blockBuf.limit(total);
        blockBuf.position(0);
        blockStartPos = nextBlockStart;
        blockBytesRead = 0;
        if (total == 0) {
            eofReached = true;
            return false;
        }
        return true;
    }

    private boolean atTailOfFile() {
        // "Are we at the very end of the file after consuming the current fragment?"
        // If yes, a CRC failure or unknown-type byte is likely a torn write rather than
        // mid-file corruption. If there are more bytes after the current position, those
        // bytes are real data; treating a CRC failure as torn-write would hide real damage.
        return blockStartPos + blockBytesRead >= fileSize;
    }

    private record Fragment(RecordType type, byte[] payload) {}
}
