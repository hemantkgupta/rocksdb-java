package com.hkg.rocksdb.tools;

import com.hkg.rocksdb.integrity.FileChecksum;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Manifest;
import com.hkg.rocksdb.manifest.ManifestCorruptionException;
import com.hkg.rocksdb.manifest.VersionEdit;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Operator tool: open the active MANIFEST under {@code dbDir} and emit every
 * {@link VersionEdit} in append order, one line per edit. Useful for
 * diagnosing post-recovery state mismatches, audit-trailing compactions, or
 * reverse-engineering a corrupt MANIFEST.
 *
 * <p>Each line starts with a known tag so output is grep-able and parseable
 * by simple shell pipelines:
 *
 * <pre>
 * NewFile        L&lt;level&gt; file=&lt;n&gt; size=&lt;bytes&gt; smallest=&lt;hex&gt; largest=&lt;hex&gt; checksum=0x&lt;n&gt;
 * DeleteFile     L&lt;level&gt; file=&lt;n&gt;
 * SetLogNumber   &lt;n&gt;
 * SetNextFileNumber &lt;n&gt;
 * SetLastSequence   &lt;n&gt;
 * </pre>
 *
 * <p>Batches in the MANIFEST are separated by a blank line so the operator
 * can see how edits were grouped when they were appended.
 */
public final class ManifestDump {

    private ManifestDump() {}

    /**
     * Walk every edit in the active MANIFEST under {@code dbDir} and emit a
     * single line per edit to {@code out}. Flushes but does not close
     * {@code out}; the caller owns it.
     */
    public static void run(Path dbDir, PrintWriter out) throws IOException {
        Objects.requireNonNull(dbDir, "dbDir");
        Objects.requireNonNull(out, "out");
        Path manifestFile = activeManifestFile(dbDir);
        List<List<VersionEdit>> batches = Manifest.replay(manifestFile);
        boolean first = true;
        for (List<VersionEdit> batch : batches) {
            if (!first) out.println();
            first = false;
            for (VersionEdit edit : batch) {
                out.println(render(edit));
            }
        }
        out.flush();
    }

    private static Path activeManifestFile(Path dbDir) throws IOException {
        Path current = dbDir.resolve("CURRENT");
        if (!Files.exists(current)) {
            throw new ManifestCorruptionException("CURRENT file missing in " + dbDir);
        }
        String name = Files.readString(current).trim();
        if (!name.startsWith("MANIFEST-")) {
            throw new ManifestCorruptionException("CURRENT does not name a MANIFEST: " + name);
        }
        Path file = dbDir.resolve(name);
        if (!Files.exists(file)) {
            throw new ManifestCorruptionException(
                "CURRENT points to missing file: " + name);
        }
        return file;
    }

    private static String render(VersionEdit edit) {
        if (edit instanceof VersionEdit.NewFile nf) {
            FileMetadata fm = nf.metadata();
            FileChecksum ck = fm.fileChecksum();
            return String.format(
                "NewFile L%d file=%d size=%d smallest=%s largest=%s checksum=%s",
                nf.level(),
                fm.fileNumber().value(),
                fm.sizeBytes(),
                hex(fm.smallestKey().userKey().bytes()),
                hex(fm.largestKey().userKey().bytes()),
                ck == null ? "none" : String.format("0x%016x", ck.value()));
        }
        if (edit instanceof VersionEdit.DeleteFile df) {
            return String.format("DeleteFile L%d file=%d",
                df.level(), df.fileNumber().value());
        }
        if (edit instanceof VersionEdit.SetLogNumber sln) {
            return "SetLogNumber " + sln.logNumber();
        }
        if (edit instanceof VersionEdit.SetNextFileNumber snfn) {
            return "SetNextFileNumber " + snfn.nextFileNumber();
        }
        if (edit instanceof VersionEdit.SetLastSequence sls) {
            return "SetLastSequence " + sls.lastSequence();
        }
        // Defensive: the sealed permits list above is exhaustive at compile time,
        // but if a new variant is added the dump should still render *something*
        // useful rather than silently dropping it.
        return "UnknownEdit " + edit.getClass().getSimpleName();
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
