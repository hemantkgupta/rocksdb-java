package com.hkg.rocksdb.sstable;

/** Thrown when an SSTable file's bytes don't match the expected on-disk format. */
public class SsTableFormatException extends RuntimeException {
    public SsTableFormatException(String message) { super(message); }
    public SsTableFormatException(String message, Throwable cause) { super(message, cause); }
}
