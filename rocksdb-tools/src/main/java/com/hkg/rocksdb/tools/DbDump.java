package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import com.hkg.rocksdb.manifest.VersionSet;
import com.hkg.rocksdb.sstable.BlockBasedTableReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

/**
 * Operator tool: stream every internal entry from every SSTable referenced
 * by the active MANIFEST in human-readable form. One line per entry:
 *
 * <pre>L&lt;level&gt; &lt;file&gt; &lt;hex-user-key&gt; &lt;hex-value&gt; seq=&lt;n&gt; type=&lt;value|deletion&gt;</pre>
 *
 * <p>Entries are emitted per file, in the order they were written. No
 * cross-file merging or tombstone collapsing — this is a raw dump, not a
 * snapshot view. Use the engine's read path for that.
 */
public final class DbDump {

    private DbDump() {}

    /**
     * Dump every SSTable entry under {@code dbDir} to {@code out}. Flushes
     * but does not close {@code out}; the caller owns it.
     */
    public static void run(Path dbDir, PrintWriter out) throws IOException {
        Objects.requireNonNull(dbDir, "dbDir");
        Objects.requireNonNull(out, "out");
        try (VersionSet vs = VersionSet.open(dbDir)) {
            Version v = vs.current();
            for (int level = 0; level < v.levels().size(); level++) {
                for (FileMetadata fm : v.level(level)) {
                    dumpOneFile(dbDir, level, fm, out);
                }
            }
        }
        out.flush();
    }

    private static void dumpOneFile(Path dbDir, int level, FileMetadata fm, PrintWriter out)
        throws IOException {
        Path tablePath = dbDir.resolve(fm.fileNumber().tableFileName());
        try (BlockBasedTableReader r = BlockBasedTableReader.open(tablePath)) {
            Iterator<BlockBasedTableReader.SsTableEntry> it = r.entries();
            while (it.hasNext()) {
                BlockBasedTableReader.SsTableEntry e = it.next();
                InternalKey ik = e.internalKey();
                String typeName = ik.type() == ValueType.VALUE ? "value" : "deletion";
                out.printf("L%d %s %s %s seq=%d type=%s%n",
                    level,
                    fm.fileNumber().tableFileName(),
                    hex(ik.userKey().bytes()),
                    hex(e.value()),
                    ik.sequence().value(),
                    typeName);
            }
        }
    }

    private static String hex(byte[] data) {
        if (data == null || data.length == 0) return "";
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
