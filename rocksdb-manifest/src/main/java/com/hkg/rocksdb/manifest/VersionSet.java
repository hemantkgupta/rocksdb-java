package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Owner of the current {@link Version} for a single engine instance.
 * Applies {@link VersionEdit}s atomically: append to the MANIFEST →
 * fsync (inside {@link Manifest#append}) → swap the in-memory Version.
 *
 * <p>When the MANIFEST grows past
 * {@link Constants#MANIFEST_ROTATION_BYTES}, the entire current Version is
 * snapshotted into a new MANIFEST file, the CURRENT pointer is atomically
 * rewritten via temp + rename, and the previous MANIFEST is deleted.
 */
public final class VersionSet implements Closeable {

    private final Path dbDir;
    private final long rotationThreshold;
    private volatile Version current;
    private Manifest manifest;
    private FileNumber activeManifestNumber;

    private VersionSet(Path dbDir, Version current, Manifest manifest,
                        FileNumber activeManifestNumber, long rotationThreshold) {
        this.dbDir = dbDir;
        this.current = current;
        this.manifest = manifest;
        this.activeManifestNumber = activeManifestNumber;
        this.rotationThreshold = rotationThreshold;
    }

    /** Initialise a fresh DB directory: write an initial MANIFEST + CURRENT. */
    public static VersionSet createFresh(Path dbDir) throws IOException {
        return createFresh(dbDir, Constants.MANIFEST_ROTATION_BYTES);
    }

    public static VersionSet createFresh(Path dbDir, long rotationThreshold) throws IOException {
        Files.createDirectories(dbDir);
        Version v = Version.empty();
        FileNumber manifestNum = new FileNumber(1L);
        Path manifestFile = dbDir.resolve(manifestNum.manifestFileName());
        Manifest m = Manifest.open(manifestFile);
        m.append(List.of(
            new VersionEdit.SetLogNumber(v.logNumber()),
            new VersionEdit.SetNextFileNumber(v.nextFileNumber()),
            new VersionEdit.SetLastSequence(v.lastSequence())));
        writeCurrent(dbDir, manifestNum);
        return new VersionSet(dbDir, v, m, manifestNum, rotationThreshold);
    }

    /** Recover a VersionSet from {@code dbDir}: read CURRENT, replay edits. */
    public static VersionSet open(Path dbDir) throws IOException {
        return open(dbDir, Constants.MANIFEST_ROTATION_BYTES);
    }

    public static VersionSet open(Path dbDir, long rotationThreshold) throws IOException {
        FileNumber active = readCurrent(dbDir);
        Path manifestFile = dbDir.resolve(active.manifestFileName());
        if (!Files.exists(manifestFile)) {
            throw new ManifestCorruptionException(
                "CURRENT points to missing file: " + active.manifestFileName());
        }
        Version v = Version.empty();
        for (List<VersionEdit> batch : Manifest.replay(manifestFile)) {
            v = v.applyEdits(batch);
        }
        Manifest m = Manifest.openForAppend(manifestFile);
        return new VersionSet(dbDir, v, m, active, rotationThreshold);
    }

    public Version current() {
        return current;
    }

    public FileNumber activeManifestNumber() {
        return activeManifestNumber;
    }

    /**
     * Apply a batch of edits: append to MANIFEST (fsync inside) then swap
     * the in-memory Version. If the MANIFEST has crossed the rotation
     * threshold, rotate before returning.
     */
    public synchronized Version apply(List<VersionEdit> edits) throws IOException {
        Version next = current.applyEdits(edits);
        manifest.append(edits);
        this.current = next;
        if (manifest.bytesWritten() >= rotationThreshold) {
            rotateManifest();
        }
        return next;
    }

    /** Force a rotation regardless of size — exposed for tests. */
    public synchronized void rotateNow() throws IOException {
        rotateManifest();
    }

    private void rotateManifest() throws IOException {
        long newNum = current.nextFileNumber();
        // Reserve the file number in Version so a future allocation never reuses it.
        Version withBumped = current.applyEdit(new VersionEdit.SetNextFileNumber(newNum + 1L));
        FileNumber n = new FileNumber(newNum);
        Path newPath = dbDir.resolve(n.manifestFileName());
        Manifest newM = Manifest.open(newPath);
        newM.append(snapshotEditsOf(withBumped));
        writeCurrent(dbDir, n);
        manifest.close();
        Path oldPath = dbDir.resolve(activeManifestNumber.manifestFileName());
        Files.deleteIfExists(oldPath);
        manifest = newM;
        activeManifestNumber = n;
        current = withBumped;
    }

    static List<VersionEdit> snapshotEditsOf(Version v) {
        List<VersionEdit> edits = new ArrayList<>();
        edits.add(new VersionEdit.SetLogNumber(v.logNumber()));
        edits.add(new VersionEdit.SetNextFileNumber(v.nextFileNumber()));
        edits.add(new VersionEdit.SetLastSequence(v.lastSequence()));
        for (int level = 0; level < v.levels().size(); level++) {
            for (FileMetadata fm : v.level(level)) {
                edits.add(new VersionEdit.NewFile(level, fm));
            }
        }
        return edits;
    }

    private static FileNumber readCurrent(Path dbDir) throws IOException {
        Path current = dbDir.resolve("CURRENT");
        if (!Files.exists(current)) {
            throw new ManifestCorruptionException("CURRENT file missing in " + dbDir);
        }
        String name = Files.readString(current).trim();
        if (!name.startsWith("MANIFEST-")) {
            throw new ManifestCorruptionException("CURRENT does not name a MANIFEST: " + name);
        }
        long n;
        try {
            n = Long.parseLong(name.substring("MANIFEST-".length()));
        } catch (NumberFormatException ex) {
            throw new ManifestCorruptionException("CURRENT has unparseable manifest number: " + name);
        }
        return new FileNumber(n);
    }

    static void writeCurrent(Path dbDir, FileNumber active) throws IOException {
        Path tmp = dbDir.resolve("CURRENT.tmp");
        Files.writeString(tmp, active.manifestFileName() + "\n",
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        Files.move(tmp, dbDir.resolve("CURRENT"),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void close() throws IOException {
        manifest.close();
    }
}
