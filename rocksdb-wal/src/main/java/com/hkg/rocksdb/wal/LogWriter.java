package com.hkg.rocksdb.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Append-only writer for the RocksDb WAL format. Each {@link #append(byte[])}
 * writes one logical record, fragmented across blocks if necessary, and
 * (by default) calls {@code force(true)} for an fsync before returning.
 *
 * <p>Not thread-safe; the engine serialises all WAL writes through a single
 * writer instance.
 */
public final class LogWriter implements Closeable {

    private final FileChannel channel;
    private final boolean syncOnAppend;
    private int blockOffset;      // bytes written into the current block
    private long bytesWritten;    // running total of bytes written via this writer
    private final CRC32 crc = new CRC32();

    private LogWriter(FileChannel channel, boolean syncOnAppend, long initialPosition) {
        this.channel = channel;
        this.syncOnAppend = syncOnAppend;
        this.blockOffset = (int) (initialPosition % WalConstants.BLOCK_SIZE);
        this.bytesWritten = initialPosition;
    }

    /**
     * Open a fresh WAL file for writing. Creates if missing; truncates if exists.
     * @param syncOnAppend if true, fsync the file after every {@link #append}.
     */
    public static LogWriter open(Path file, boolean syncOnAppend) throws IOException {
        OpenOption[] opts = new OpenOption[] {
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        };
        FileChannel ch = FileChannel.open(file, opts);
        return new LogWriter(ch, syncOnAppend, 0L);
    }

    /** Open an existing WAL file in append mode, positioned at end-of-file. */
    public static LogWriter openForAppend(Path file, boolean syncOnAppend) throws IOException {
        FileChannel ch = FileChannel.open(file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND);
        long pos = ch.size();
        return new LogWriter(ch, syncOnAppend, pos);
    }

    /**
     * Append one logical record. Fragments it across blocks if needed.
     * The record may be zero-length (an empty payload still writes one FULL fragment).
     */
    public void append(byte[] payload) throws IOException {
        Objects.requireNonNull(payload, "payload");
        int offset = 0;
        int remaining = payload.length;
        boolean isFirstFragment = true;

        do {
            int blockLeft = WalConstants.BLOCK_SIZE - blockOffset;
            if (blockLeft < WalConstants.HEADER_SIZE) {
                // Not enough room for even a header — zero-pad the rest of the block.
                if (blockLeft > 0) {
                    ByteBuffer pad = ByteBuffer.allocate(blockLeft);
                    pad.flip();
                    pad.limit(blockLeft);
                    pad.position(0);
                    // Write blockLeft zero bytes.
                    byte[] zeros = new byte[blockLeft];
                    writeFully(ByteBuffer.wrap(zeros));
                }
                blockOffset = 0;
                blockLeft = WalConstants.BLOCK_SIZE;
            }

            int availForPayload = blockLeft - WalConstants.HEADER_SIZE;
            int fragmentLen = Math.min(remaining, availForPayload);
            boolean isLastFragment = (fragmentLen == remaining);
            RecordType type;
            if (isFirstFragment && isLastFragment) {
                type = RecordType.FULL;
            } else if (isFirstFragment) {
                type = RecordType.FIRST;
            } else if (isLastFragment) {
                type = RecordType.LAST;
            } else {
                type = RecordType.MIDDLE;
            }

            writeFragment(type, payload, offset, fragmentLen);
            isFirstFragment = false;
            offset += fragmentLen;
            remaining -= fragmentLen;
        } while (remaining > 0);

        if (syncOnAppend) {
            channel.force(true);
        }
    }

    /** Force a fsync now (in case syncOnAppend was disabled). */
    public void sync() throws IOException {
        channel.force(true);
    }

    /** Total bytes written through this writer (excluding zero padding bytes? no — including). */
    public long bytesWritten() {
        return bytesWritten;
    }

    @Override
    public void close() throws IOException {
        try {
            channel.force(true);
        } finally {
            channel.close();
        }
    }

    private void writeFragment(RecordType type, byte[] payload, int offset, int length) throws IOException {
        // Header layout: crc(4 LE) | length(2 LE) | type(1)
        ByteBuffer header = ByteBuffer.allocate(WalConstants.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        // Compute CRC over type + payload (matches RocksDb).
        crc.reset();
        crc.update(type.tag());
        if (length > 0) {
            crc.update(payload, offset, length);
        }
        int crcVal = (int) crc.getValue();
        header.putInt(crcVal);
        header.putShort((short) length);
        header.put(type.tag());
        header.flip();
        writeFully(header);
        if (length > 0) {
            writeFully(ByteBuffer.wrap(payload, offset, length));
        }
        blockOffset += WalConstants.HEADER_SIZE + length;
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int written = channel.write(buf);
            if (written < 0) {
                throw new IOException("channel reported EOF on write");
            }
            bytesWritten += written;
        }
    }
}
