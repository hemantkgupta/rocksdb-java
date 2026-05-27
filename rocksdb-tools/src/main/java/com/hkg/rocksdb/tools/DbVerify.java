package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.integrity.FileChecksum;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import com.hkg.rocksdb.manifest.VersionSet;
import com.hkg.rocksdb.sstable.BlockBasedTableReader;
import com.hkg.rocksdb.sstable.BlockChecksumMismatchException;
import com.hkg.rocksdb.sstable.SsTableFormatException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Operator tool: open the active MANIFEST and walk every referenced SSTable,
 * verifying both layers of the §5 four-layer integrity model that live on
 * disk:
 * <ol>
 *   <li><b>Block CRC32</b> — per data block. The walk uses
 *       {@link BlockBasedTableReader#entries()}, which threads every data
 *       block through {@code readBlock} — a mismatch surfaces as
 *       {@link BlockChecksumMismatchException}.</li>
 *   <li><b>File XXH64</b> (CP 19) — recomputed across every byte of the
 *       SSTable file and compared against the value recorded in
 *       {@link FileMetadata}. Catches above-block corruption that block
 *       CRC cannot see (footer bit-flip, truncation, file mis-attribution).
 *       Skipped when the MANIFEST entry has no stored checksum
 *       (legacy pre-CP-19 SSTable).</li>
 * </ol>
 *
 * <p>This is the operator's only way to confirm on-disk SSTables are still
 * readable without running the engine. A single failing layer flips the
 * per-file {@link FileResult} to FAIL with the layer-specific detail.
 */
public final class DbVerify {

    private DbVerify() {}

    /**
     * Verify every SSTable referenced by the active MANIFEST under
     * {@code dbDir}. Returns a {@link Report} that lists per-file PASS or
     * FAIL with the underlying exception message. Does not throw on a
     * CRC failure — corruption is the result, not an error of the tool.
     */
    public static Report run(Path dbDir) throws IOException {
        Objects.requireNonNull(dbDir, "dbDir");
        List<FileResult> perFile = new ArrayList<>();
        int filesScanned = 0;
        long blocksScanned = 0L;
        int failures = 0;

        try (VersionSet vs = VersionSet.open(dbDir)) {
            Version v = vs.current();
            for (int level = 0; level < v.levels().size(); level++) {
                for (FileMetadata fm : v.level(level)) {
                    filesScanned++;
                    Path tablePath = dbDir.resolve(fm.fileNumber().tableFileName());
                    FileResult r = verifyOne(tablePath, level, fm);
                    perFile.add(r);
                    blocksScanned += r.blockCount();
                    if (!r.ok()) failures++;
                }
            }
        }
        return new Report(filesScanned, blocksScanned, failures, perFile);
    }

    private static FileResult verifyOne(Path path, int level, FileMetadata fm) {
        FileNumber fn = fm.fileNumber();
        long blockCount = 0L;
        try (BlockBasedTableReader r = BlockBasedTableReader.open(path)) {
            Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries();
            while (it.hasNext()) {
                it.next();
                blockCount++;
            }
        } catch (BlockChecksumMismatchException e) {
            return FileResult.fail(level, fn, blockCount, "block CRC mismatch: " + e.getMessage());
        } catch (SsTableFormatException e) {
            return FileResult.fail(level, fn, blockCount, "format error: " + e.getMessage());
        } catch (IOException e) {
            return FileResult.fail(level, fn, blockCount, "I/O error: " + e.getMessage());
        } catch (RuntimeException e) {
            // BlockBasedTableReader.entries() wraps IOExceptions in RuntimeException.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return FileResult.fail(level, fn, blockCount, cause.getClass().getSimpleName()
                + ": " + cause.getMessage());
        }

        // ----- CP 19: file-level XXH64 pass -----
        // Recompute over every byte of the SSTable and compare against the
        // value stored in FileMetadata. Skipped when the MANIFEST entry
        // predates CP 19 (no stored checksum) — pre-19 files verify clean
        // against block-CRC alone, matching the old contract.
        FileChecksum stored = fm.fileChecksum();
        if (stored != null) {
            try {
                FileChecksum actual = FileChecksum.compute(path);
                if (actual.value() != stored.value()) {
                    return FileResult.fail(level, fn, blockCount,
                        String.format("file checksum mismatch: stored=0x%016x actual=0x%016x",
                            stored.value(), actual.value()));
                }
            } catch (IOException e) {
                return FileResult.fail(level, fn, blockCount,
                    "file checksum I/O error: " + e.getMessage());
            }
        }

        return FileResult.pass(level, fn, blockCount);
    }

    /** Outcome of verifying one SSTable. */
    public record FileResult(int level, FileNumber fileNumber, long blockCount,
                              boolean ok, String detail) {

        public static FileResult pass(int level, FileNumber fn, long blocks) {
            return new FileResult(level, fn, blocks, true, "PASS");
        }

        public static FileResult fail(int level, FileNumber fn, long blocks, String detail) {
            return new FileResult(level, fn, blocks, false, detail);
        }

        public String render() {
            return String.format("L%d %s [%d entries] %s",
                level, fileNumber.tableFileName(), blockCount, detail);
        }
    }

    /** Aggregate outcome of a verify run. */
    public record Report(int filesScanned, long blocksScanned, int failures,
                          List<FileResult> perFile) {

        public boolean ok() { return failures == 0; }

        public String render() {
            StringBuilder sb = new StringBuilder();
            for (FileResult r : perFile) {
                sb.append(r.render()).append(System.lineSeparator());
            }
            sb.append(String.format("scanned=%d entries=%d failures=%d %s%n",
                filesScanned, blocksScanned, failures, ok() ? "OK" : "CORRUPT"));
            return sb.toString();
        }
    }
}
