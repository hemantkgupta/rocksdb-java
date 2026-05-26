package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.sstable.WriteHook;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fixed thread pool of compaction workers. Each {@link #submit submit} hands a
 * {@link CompactionJob} to one worker, which runs {@link Compactor#run} to
 * completion and returns the resulting output {@link FileMetadata}s.
 *
 * <p>This is the CP 10 stepping-stone toward the RocksDB plan's "compaction
 * queue + workers" (§5.9). It is intentionally minimal: the engine drives the
 * picker, calls submit, and collects futures. There is no internal scheduling
 * loop here; the engine decides when to enqueue more work.
 *
 * <p>Concurrency: any number of threads may call {@link #submit} concurrently.
 * The underlying {@link Compactor#run} only reads SSTable files (which are
 * immutable) and writes new ones with engine-allocated {@link
 * com.hkg.rocksdb.common.FileNumber}s, so two workers that consume disjoint
 * input file sets are safe to run in parallel.
 */
public final class CompactionExecutor implements AutoCloseable {

    private final ExecutorService workers;
    private final int threadCount;

    public CompactionExecutor(int threadCount) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be >= 1, got " + threadCount);
        }
        this.threadCount = threadCount;
        this.workers = Executors.newFixedThreadPool(threadCount, namedDaemonFactory());
    }

    public int threadCount() {
        return threadCount;
    }

    /**
     * Submit one compaction job. The returned future yields the output
     * {@link FileMetadata}s when the merge completes. Exceptions thrown by
     * {@link Compactor#run} surface via {@link Future#get}.
     */
    public Future<List<FileMetadata>> submit(CompactionJob job,
                                              long oldestLiveSeq,
                                              long targetFileSize,
                                              Path dbDir,
                                              Compactor.FileNumberAllocator allocator,
                                              Compactor.ReaderOpener opener,
                                              boolean compressBlocks) {
        return submit(job, oldestLiveSeq, targetFileSize, dbDir, allocator, opener,
            compressBlocks, WriteHook.NO_OP);
    }

    /** Submit with an explicit {@link WriteHook} for rate-limited compaction I/O. */
    public Future<List<FileMetadata>> submit(CompactionJob job,
                                              long oldestLiveSeq,
                                              long targetFileSize,
                                              Path dbDir,
                                              Compactor.FileNumberAllocator allocator,
                                              Compactor.ReaderOpener opener,
                                              boolean compressBlocks,
                                              WriteHook writeHook) {
        Callable<List<FileMetadata>> task = () -> Compactor.run(
            job, oldestLiveSeq, targetFileSize, dbDir, allocator, opener,
            compressBlocks, writeHook);
        return workers.submit(task);
    }

    /**
     * Run one job synchronously on a worker, blocking the caller until the
     * merge completes. Used by the engine's single-job foreground compaction
     * path so the writeLock can be dropped while the merge runs.
     */
    public List<FileMetadata> runOne(CompactionJob job,
                                      long oldestLiveSeq,
                                      long targetFileSize,
                                      Path dbDir,
                                      Compactor.FileNumberAllocator allocator,
                                      Compactor.ReaderOpener opener,
                                      boolean compressBlocks) throws IOException, InterruptedException {
        Future<List<FileMetadata>> f = submit(job, oldestLiveSeq, targetFileSize,
            dbDir, allocator, opener, compressBlocks);
        try {
            return f.get();
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            throw new IOException("compaction worker failed", cause);
        }
    }

    @Override
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory namedDaemonFactory() {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "rocksdb-compaction-worker-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
