package com.hkg.rocksdb.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDbCliTest {

    private static final class Cli {
        final StringWriter outBuf = new StringWriter();
        final StringWriter errBuf = new StringWriter();
        final RocksDbCli cli = new RocksDbCli(new PrintWriter(outBuf), new PrintWriter(errBuf));

        int run(String... args) { return cli.run(args); }
        String out() { return outBuf.toString(); }
        String err() { return errBuf.toString(); }
    }

    @Test
    void noArgsPrintsUsageAndExitsTwo() {
        Cli cli = new Cli();
        int code = cli.run();
        assertThat(code).isEqualTo(2);
        assertThat(cli.err()).contains("subcommands:");
    }

    @Test
    void unknownSubcommandReturnsTwo() {
        Cli cli = new Cli();
        int code = cli.run("frobnicate");
        assertThat(code).isEqualTo(2);
        assertThat(cli.err()).contains("unknown subcommand");
    }

    @Test
    void putGetRoundtrip(@TempDir Path dir) {
        Cli put = new Cli();
        assertThat(put.run("put", dir.toString(), "alpha", "ALPHA")).isZero();

        Cli get = new Cli();
        assertThat(get.run("get", dir.toString(), "alpha")).isZero();
        assertThat(get.out().trim()).isEqualTo("ALPHA");
    }

    @Test
    void getMissingKeyReturnsOne(@TempDir Path dir) {
        Cli get = new Cli();
        int code = get.run("get", dir.toString(), "no-such-key");
        assertThat(code).isEqualTo(1);
        assertThat(get.out()).contains("(not found)");
    }

    @Test
    void deleteSuppressesPriorValue(@TempDir Path dir) {
        assertThat(new Cli().run("put", dir.toString(), "k", "v")).isZero();
        assertThat(new Cli().run("delete", dir.toString(), "k")).isZero();
        Cli get = new Cli();
        assertThat(get.run("get", dir.toString(), "k")).isEqualTo(1);
    }

    @Test
    void verifyReportsOkOnCleanDb(@TempDir Path dir) {
        assertThat(new Cli().run("put", dir.toString(), "k", "v")).isZero();
        Cli verify = new Cli();
        int code = verify.run("verify", dir.toString());
        assertThat(code).isZero();
        assertThat(verify.out()).contains("OK");
    }

    @Test
    void dumpEmitsHexLines(@TempDir Path dir) {
        // Two puts then trigger persistence so the SSTable exists when we run dump.
        assertThat(new Cli().run("put", dir.toString(), "alpha", "ALPHA")).isZero();
        assertThat(new Cli().run("put", dir.toString(), "beta", "BETA")).isZero();
        assertThat(new Cli().run("compact", dir.toString())).isZero();

        Cli dump = new Cli();
        assertThat(dump.run("dump", dir.toString())).isZero();
        // Hex of "alpha" = 616c706861
        assertThat(dump.out()).contains("616c706861");
    }

    @Test
    void compactReturnsZeroEvenWithNothingToDo(@TempDir Path dir) {
        Cli c = new Cli();
        int code = c.run("compact", dir.toString());
        assertThat(code).isZero();
        assertThat(c.out()).containsAnyOf("compacted", "nothing to compact");
    }

    @Test
    void helpPrintsSubcommandList() {
        Cli c = new Cli();
        assertThat(c.run("--help")).isZero();
        assertThat(c.err()).contains("put").contains("get").contains("verify");
    }

    @Test
    void manifestDumpEmitsTaggedEdits(@TempDir Path dir) {
        // Write a couple of keys + compact so we get NewFile + SetLogNumber edits.
        assertThat(new Cli().run("put", dir.toString(), "alpha", "ALPHA")).isZero();
        assertThat(new Cli().run("put", dir.toString(), "beta", "BETA")).isZero();
        assertThat(new Cli().run("compact", dir.toString())).isZero();

        Cli md = new Cli();
        assertThat(md.run("manifest-dump", dir.toString())).isZero();
        assertThat(md.out()).contains("SetLogNumber");
        assertThat(md.out()).contains("NewFile L0");
    }

    @Test
    void manifestDumpMissingDirReturnsTwo(@TempDir Path dir) {
        // No engine ever opened here → CURRENT missing → ManifestCorruptionException
        // is bubbled out as a runtime error.
        Cli md = new Cli();
        int code = md.run("manifest-dump", dir.toString());
        assertThat(code).isEqualTo(2);
        assertThat(md.err()).contains("error:");
    }

    @Test
    void scanWithEmptyInitializedDbProducesNoOutput(@TempDir Path dir) {
        // Initialise the DB so CURRENT exists, but write nothing flushable.
        assertThat(new Cli().run("compact", dir.toString())).isZero();
        Cli c = new Cli();
        int code = c.run("scan", dir.toString());
        assertThat(code).isZero();
        assertThat(c.out()).isEmpty();
    }
}
