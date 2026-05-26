package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An immutable snapshot of the LSM's level membership at one point in time.
 *
 * <ul>
 *   <li>{@code levels[0]} — L0 files (may overlap; ordered newest-first by file number).</li>
 *   <li>{@code levels[1..MAX_LEVEL_COUNT-1]} — non-overlapping files sorted by smallest-key.</li>
 * </ul>
 *
 * <p>{@link #applyEdit(VersionEdit)} returns a new Version with the edit
 * applied; the receiver is never mutated. The MANIFEST's job is to make the
 * sequence of edits durable; the in-memory Version chain is rebuilt by
 * replaying edits at recovery.
 */
public final class Version {

    private final List<List<FileMetadata>> levels;
    private final long logNumber;
    private final long lastSequence;
    private final long nextFileNumber;

    public Version(List<List<FileMetadata>> levels, long logNumber,
                   long lastSequence, long nextFileNumber) {
        if (levels.size() != Constants.MAX_LEVEL_COUNT) {
            throw new IllegalArgumentException(
                "expected " + Constants.MAX_LEVEL_COUNT + " levels, got " + levels.size());
        }
        List<List<FileMetadata>> defensive = new ArrayList<>(Constants.MAX_LEVEL_COUNT);
        for (List<FileMetadata> level : levels) {
            defensive.add(Collections.unmodifiableList(new ArrayList<>(level)));
        }
        this.levels = Collections.unmodifiableList(defensive);
        this.logNumber = logNumber;
        this.lastSequence = lastSequence;
        this.nextFileNumber = nextFileNumber;
    }

    public static Version empty() {
        List<List<FileMetadata>> initial = new ArrayList<>(Constants.MAX_LEVEL_COUNT);
        for (int i = 0; i < Constants.MAX_LEVEL_COUNT; i++) {
            initial.add(new ArrayList<>());
        }
        return new Version(initial, 0L, 0L, 1L);
    }

    public List<List<FileMetadata>> levels() { return levels; }
    public List<FileMetadata> level(int i) { return levels.get(i); }
    public long logNumber() { return logNumber; }
    public long lastSequence() { return lastSequence; }
    public long nextFileNumber() { return nextFileNumber; }

    /** Apply a single edit, returning a new Version. The receiver is unchanged. */
    public Version applyEdit(VersionEdit edit) {
        List<List<FileMetadata>> nextLevels = new ArrayList<>(Constants.MAX_LEVEL_COUNT);
        for (List<FileMetadata> level : levels) {
            nextLevels.add(new ArrayList<>(level));
        }
        long nextLogNumber = this.logNumber;
        long nextLastSequence = this.lastSequence;
        long nextNextFileNumber = this.nextFileNumber;

        if (edit instanceof VersionEdit.NewFile nf) {
            nextLevels.get(nf.level()).add(nf.metadata());
            // Bump nextFileNumber so we never reuse a file id.
            long fn = nf.metadata().fileNumber().value();
            if (fn >= nextNextFileNumber) {
                nextNextFileNumber = fn + 1L;
            }
        } else if (edit instanceof VersionEdit.DeleteFile df) {
            List<FileMetadata> level = nextLevels.get(df.level());
            level.removeIf(fm -> fm.fileNumber().equals(df.fileNumber()));
        } else if (edit instanceof VersionEdit.SetLogNumber sln) {
            nextLogNumber = sln.logNumber();
        } else if (edit instanceof VersionEdit.SetLastSequence sls) {
            nextLastSequence = sls.lastSequence();
        } else if (edit instanceof VersionEdit.SetNextFileNumber snfn) {
            nextNextFileNumber = snfn.nextFileNumber();
        }
        return new Version(nextLevels, nextLogNumber, nextLastSequence, nextNextFileNumber);
    }

    public Version applyEdits(Iterable<VersionEdit> edits) {
        Version v = this;
        for (VersionEdit e : edits) v = v.applyEdit(e);
        return v;
    }

    /** Total bytes occupied by all SSTables at the given level. */
    public long levelSizeBytes(int level) {
        long total = 0L;
        for (FileMetadata fm : levels.get(level)) {
            total += fm.sizeBytes();
        }
        return total;
    }

    /** Every {@link FileNumber} this version references, across all levels. */
    public List<FileNumber> allFileNumbers() {
        List<FileNumber> all = new ArrayList<>();
        for (List<FileMetadata> level : levels) {
            for (FileMetadata fm : level) {
                all.add(fm.fileNumber());
            }
        }
        return all;
    }
}
