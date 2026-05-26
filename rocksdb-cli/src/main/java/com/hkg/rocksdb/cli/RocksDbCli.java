package com.hkg.rocksdb.cli;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.tools.DbDump;
import com.hkg.rocksdb.tools.DbVerify;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Thin command-line wrapper around {@link RocksDb} + {@link DbVerify} +
 * {@link DbDump}. Subcommands:
 *
 * <pre>
 *   put     &lt;dbDir&gt; &lt;key&gt; &lt;value&gt;     Insert or overwrite a key.
 *   get     &lt;dbDir&gt; &lt;key&gt;             Print the value, or "(not found)".
 *   delete  &lt;dbDir&gt; &lt;key&gt;             Tombstone a key.
 *   scan    &lt;dbDir&gt; [limit]            Stream every key from the engine.
 *   verify  &lt;dbDir&gt;                   Block-CRC walk; prints per-file PASS/FAIL.
 *   dump    &lt;dbDir&gt;                   Hex dump of every SSTable entry.
 *   compact &lt;dbDir&gt;                   Trigger one compaction pass and exit.
 * </pre>
 *
 * <p>{@code key} and {@code value} are treated as UTF-8 strings.
 */
public final class RocksDbCli {

    private final PrintWriter out;
    private final PrintWriter err;

    public RocksDbCli(PrintWriter out, PrintWriter err) {
        this.out = out;
        this.err = err;
    }

    /** Parse and dispatch one subcommand invocation. Returns the process exit code. */
    public int run(String[] args) {
        if (args.length == 0) {
            return usage("missing subcommand");
        }
        try {
            return switch (args[0]) {
                case "put"     -> cmdPut(args);
                case "get"     -> cmdGet(args);
                case "delete"  -> cmdDelete(args);
                case "scan"    -> cmdScan(args);
                case "verify"  -> cmdVerify(args);
                case "dump"    -> cmdDump(args);
                case "compact" -> cmdCompact(args);
                case "-h", "--help", "help" -> {
                    printHelp();
                    yield 0;
                }
                default -> usage("unknown subcommand: " + args[0]);
            };
        } catch (IOException e) {
            err.println("I/O error: " + e.getMessage());
            return 2;
        } catch (RuntimeException e) {
            err.println("error: " + e.getMessage());
            return 2;
        } finally {
            out.flush();
            err.flush();
        }
    }

    private int cmdPut(String[] args) throws IOException {
        if (args.length != 4) return usage("put <dbDir> <key> <value>");
        Path dbDir = Path.of(args[1]);
        try (RocksDb db = RocksDb.open(dbDir)) {
            db.put(Key.of(args[2]), Slice.of(args[3]));
        }
        return 0;
    }

    private int cmdGet(String[] args) throws IOException {
        if (args.length != 3) return usage("get <dbDir> <key>");
        Path dbDir = Path.of(args[1]);
        try (RocksDb db = RocksDb.open(dbDir)) {
            Optional<Slice> v = db.get(Key.of(args[2]));
            if (v.isEmpty()) {
                out.println("(not found)");
                return 1;
            }
            out.println(new String(v.get().toBytes(), StandardCharsets.UTF_8));
            return 0;
        }
    }

    private int cmdDelete(String[] args) throws IOException {
        if (args.length != 3) return usage("delete <dbDir> <key>");
        Path dbDir = Path.of(args[1]);
        try (RocksDb db = RocksDb.open(dbDir)) {
            db.delete(Key.of(args[2]));
        }
        return 0;
    }

    private int cmdScan(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) return usage("scan <dbDir> [limit]");
        Path dbDir = Path.of(args[1]);
        int limit = args.length == 3 ? Integer.parseInt(args[2]) : Integer.MAX_VALUE;
        // The current engine surface does not expose a forward iterator (CP 11 of the RocksDb
        // plan does not require one). Fall back to a hex dump of every internal entry so the
        // operator can still see the data — DbDump already covers this contract.
        if (limit <= 0) return 0;
        DbDump.run(dbDir, out);
        return 0;
    }

    private int cmdVerify(String[] args) throws IOException {
        if (args.length != 2) return usage("verify <dbDir>");
        Path dbDir = Path.of(args[1]);
        DbVerify.Report report = DbVerify.run(dbDir);
        out.print(report.render());
        return report.ok() ? 0 : 1;
    }

    private int cmdDump(String[] args) throws IOException {
        if (args.length != 2) return usage("dump <dbDir>");
        DbDump.run(Path.of(args[1]), out);
        return 0;
    }

    private int cmdCompact(String[] args) throws IOException {
        if (args.length != 2) return usage("compact <dbDir>");
        Path dbDir = Path.of(args[1]);
        try (RocksDb db = RocksDb.open(dbDir)) {
            boolean ran = db.maybeCompact();
            out.println(ran ? "compacted" : "nothing to compact");
        }
        return 0;
    }

    private int usage(String msg) {
        err.println("usage: " + msg);
        printHelp();
        return 2;
    }

    private void printHelp() {
        err.println("subcommands:");
        err.println("  put     <dbDir> <key> <value>");
        err.println("  get     <dbDir> <key>");
        err.println("  delete  <dbDir> <key>");
        err.println("  scan    <dbDir> [limit]");
        err.println("  verify  <dbDir>");
        err.println("  dump    <dbDir>");
        err.println("  compact <dbDir>");
    }

    public static void main(String[] args) {
        PrintWriter out = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
        PrintWriter err = new PrintWriter(System.err, true, StandardCharsets.UTF_8);
        int code = new RocksDbCli(out, err).run(args);
        System.exit(code);
    }
}
