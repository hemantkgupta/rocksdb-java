package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.sstable.BlockBasedTableReader;
import com.hkg.rocksdb.sstable.BlockBasedTableReader.SsTableEntry;
import com.hkg.rocksdb.sstable.BlockBasedTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Executes a single {@link CompactionJob}: opens all input SSTables, runs an
 * N-way merge, and writes one or more output SSTables in {@code outputLevel}
 * with snapshot-aware tombstone collection.
 *
 * <p>Per-user-key rules:
 * <ol>
 *   <li>For each user-key group (entries seen in seq-descending order from
 *       the {@link MergingIterator}), keep at most one entry above the
 *       oldest live snapshot (the newest, visible to non-snapshot reads)
 *       AND at most one entry at-or-below the oldest live snapshot (the
 *       newest one a snapshot reader would observe).</li>
 *   <li>At the bottom level only: if the newest entry in a group is a
 *       tombstone with sequence &le; {@code oldestLiveSeq}, drop the
 *       tombstone AND every older entry in the group — the user key has
 *       no longer-living version.</li>
 * </ol>
 *
 * <p>This is the minimal correct behaviour for snapshot-respecting
 * compaction. RocksDb's reference does the same; per-snapshot precision
 * (keeping one entry per snapshot boundary, when multiple snapshots exist
 * at different sequences) is a future refinement and out of scope here.
 */
public final class Compactor {

    private Compactor() {}

    /**
     * Sequence value used to mean "no snapshots are held" — every existing
     * entry has seq &lt;= this, so tombstone GC fires at the bottom level
     * for everything.
     */
    public static final long NO_SNAPSHOTS_HELD = Long.MAX_VALUE;

    @FunctionalInterface
    public interface FileNumberAllocator {
        FileNumber allocate();
    }

    @FunctionalInterface
    public interface ReaderOpener {
        BlockBasedTableReader open(FileNumber number) throws IOException;
    }

    /**
     * Run a compaction. Returns the {@link FileMetadata} for each output
     * SSTable written; callers translate these into VersionEdits.
     *
     * @param job              picked compaction job (inputs + overlapping)
     * @param oldestLiveSeq    sequence of the oldest held snapshot, or
     *                         {@link #NO_SNAPSHOTS_HELD} if none
     * @param targetFileSize   roll over to a new output SSTable once an
     *                         output reaches this size
     * @param dbDir            directory in which to write new SSTables
     * @param allocator        produces the next free {@link FileNumber}
     * @param opener           opens an input SSTable for reading
     * @param compressBlocks   whether output blocks should be deflate-compressed
     */
    public static List<FileMetadata> run(CompactionJob job,
                                          long oldestLiveSeq,
                                          long targetFileSize,
                                          Path dbDir,
                                          FileNumberAllocator allocator,
                                          ReaderOpener opener,
                                          boolean compressBlocks) throws IOException {

        int outputLevel = job.outputLevel();
        boolean isBottomLevel = (outputLevel == Constants.MAX_LEVEL_COUNT - 1);

        List<BlockBasedTableReader> readers = new ArrayList<>();
        try {
            List<Iterator<SsTableEntry>> iters = new ArrayList<>();
            for (FileMetadata fm : job.allInputs()) {
                BlockBasedTableReader r = opener.open(fm.fileNumber());
                readers.add(r);
                iters.add(r.entries());
            }

            MergingIterator merger = new MergingIterator(iters);
            List<FileMetadata> outputs = new ArrayList<>();
            BlockBasedTableWriter writer = null;
            FileNumber currentNum = null;
            Path currentPath = null;
            InternalKey smallestInOutput = null;
            InternalKey largestInOutput = null;

            byte[] currentUserKey = null;
            boolean sawBelowSnapshot = false;
            boolean groupIsBottomTombstone = false;

            while (merger.hasNext()) {
                SsTableEntry e = merger.next();
                byte[] uk = e.internalKey().userKey().bytes();
                boolean newGroup = !Arrays.equals(uk, currentUserKey);

                boolean keep;
                if (newGroup) {
                    currentUserKey = uk;
                    sawBelowSnapshot = false;
                    groupIsBottomTombstone = false;
                    boolean isDel = e.internalKey().type().tag() == ValueType.DELETION.tag();
                    boolean aboveSnap = e.internalKey().sequence().value() > oldestLiveSeq;
                    if (isBottomLevel && isDel && !aboveSnap) {
                        keep = false;
                        groupIsBottomTombstone = true;
                    } else {
                        keep = true;
                        if (!aboveSnap) sawBelowSnapshot = true;
                    }
                } else if (groupIsBottomTombstone) {
                    keep = false;
                } else if (e.internalKey().sequence().value() > oldestLiveSeq) {
                    // Newer entry already kept, this is also above all snapshots — shadowed.
                    keep = false;
                } else if (!sawBelowSnapshot) {
                    keep = true;
                    sawBelowSnapshot = true;
                } else {
                    keep = false;
                }

                if (keep) {
                    if (writer == null) {
                        currentNum = allocator.allocate();
                        currentPath = dbDir.resolve(currentNum.tableFileName());
                        writer = BlockBasedTableWriter.open(currentPath, compressBlocks);
                        smallestInOutput = e.internalKey();
                    }
                    writer.add(e.internalKey(), e.value());
                    largestInOutput = e.internalKey();

                    if (writer.fileSize() >= targetFileSize) {
                        writer.finish();
                        writer.close();
                        outputs.add(new FileMetadata(currentNum, Files.size(currentPath),
                            smallestInOutput, largestInOutput));
                        writer = null;
                        smallestInOutput = null;
                        largestInOutput = null;
                    }
                }
            }

            if (writer != null) {
                writer.finish();
                writer.close();
                outputs.add(new FileMetadata(currentNum, Files.size(currentPath),
                    smallestInOutput, largestInOutput));
            }

            return outputs;
        } finally {
            for (BlockBasedTableReader r : readers) {
                try { r.close(); } catch (IOException ignored) {}
            }
        }
    }

    /** Helper using the canonical {@link Constants#SST_FILE_TARGET_SIZE_BYTES}. */
    public static List<FileMetadata> run(CompactionJob job,
                                          long oldestLiveSeq,
                                          Path dbDir,
                                          FileNumberAllocator allocator,
                                          ReaderOpener opener) throws IOException {
        return run(job, oldestLiveSeq, Constants.SST_FILE_TARGET_SIZE_BYTES,
            dbDir, allocator, opener, true);
    }
}
