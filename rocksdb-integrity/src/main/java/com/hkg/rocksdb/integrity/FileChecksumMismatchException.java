package com.hkg.rocksdb.integrity;

import java.io.IOException;

/**
 * Raised when a recomputed {@link FileChecksum} does not match the value
 * recorded in {@code FileMetadata}. Distinct from
 * {@code BlockChecksumMismatchException} — this is the §5 above-block
 * integrity layer surfacing a whole-file corruption (footer bit-flip,
 * truncation, file mis-attribution) that block-CRC32 cannot detect.
 */
public class FileChecksumMismatchException extends IOException {

    private static final long serialVersionUID = 1L;

    public FileChecksumMismatchException(String msg) {
        super(msg);
    }
}
