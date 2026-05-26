package com.hkg.rocksdb.manifest;

/**
 * A single edit applied to a {@link Version}. The MANIFEST is an append-only
 * log of these edits. Implementations are records so two edits with the same
 * fields compare equal, which makes replay testing straightforward.
 */
public sealed interface VersionEdit
    permits VersionEdit.NewFile,
            VersionEdit.DeleteFile,
            VersionEdit.SetLogNumber,
            VersionEdit.SetNextFileNumber,
            VersionEdit.SetLastSequence {

    byte tag();

    /** A new SSTable has been written into {@code level}. */
    record NewFile(int level, FileMetadata metadata) implements VersionEdit {
        public NewFile {
            if (level < 0) throw new IllegalArgumentException("level must be >= 0: " + level);
        }
        @Override public byte tag() { return 0x10; }
    }

    /** An SSTable in {@code level} has been removed (consumed by compaction). */
    record DeleteFile(int level, com.hkg.rocksdb.common.FileNumber fileNumber) implements VersionEdit {
        public DeleteFile {
            if (level < 0) throw new IllegalArgumentException("level must be >= 0: " + level);
        }
        @Override public byte tag() { return 0x11; }
    }

    /** Active WAL number — recovery uses this to know which log to replay. */
    record SetLogNumber(long logNumber) implements VersionEdit {
        public SetLogNumber {
            if (logNumber < 0L) throw new IllegalArgumentException("logNumber must be >= 0: " + logNumber);
        }
        @Override public byte tag() { return 0x12; }
    }

    /** Reserve up to this file number — the next allocation will use a higher one. */
    record SetNextFileNumber(long nextFileNumber) implements VersionEdit {
        public SetNextFileNumber {
            if (nextFileNumber < 0L) throw new IllegalArgumentException("nextFileNumber must be >= 0");
        }
        @Override public byte tag() { return 0x13; }
    }

    /** Persisted high-water mark of the per-engine sequence number. */
    record SetLastSequence(long lastSequence) implements VersionEdit {
        public SetLastSequence {
            if (lastSequence < 0L) throw new IllegalArgumentException("lastSequence must be >= 0");
        }
        @Override public byte tag() { return 0x14; }
    }
}
