package com.example.claims;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Approach 1c - plain Java, SINGLE-PASS parallel join writing Parquet.
 *
 * Reads 800M claims once, looks each up in two in-memory 250M-row reference
 * tables (members, providers), cleanses, and writes one Parquet file per claims
 * chunk. Never materializes an intermediate result and uses no temp disk.
 *
 * <h2>Measured result (full data, commit 2031232)</h2>
 * <pre>
 *   Machine : Apple M1 Max (8P + 2E cores), 32 GB, Java 11.0.20
 *   Command : java -Xms15g -Xmx15g -XX:+AlwaysPreTouch -XX:+UseG1GC  *               -jar claims-processor-1.0.0.jar singlepass DATA OUT 250000000 9
 *   Load    : members 35.7 s + providers 37.7 s          =  73.5 s
 *   Join    : 800,000,032 read -> 748,953,178 written    = 680.2 s (1.18M rows/s)
 *   Total   : 753.7 s (12.6 min), heap 11.4 GiB, peak RSS 16.8 GB, 0 swaps
 * </pre>
 *
 * <h2>What got it there (each step measured; see SINGLEPASS_FINDINGS.md)</h2>
 * <pre>
 *   Change                                          Effect
 *   ----------------------------------------------  -----------------------------------------
 *   Original: 2 x OnHeapJoinTable (hash, 2^29 slots) needs ~35 GiB -> cannot fit 30g heap
 *   1. DenseIdJoinTable (id = array slot)           index 16 GiB -> 3.8 GiB; tables 23 GiB
 *      but 23 GiB on a 32 GB Mac swaps                join stalled: 0 rows in 9 min
 *   2. Codecs (dictionary + pattern encoding)       providers 13.7 -> 3.4 GiB; tables 11.4 GiB
 *      but -Xmx20g lets macOS compress the heap       300K rows/s (15 GB of heap compressed)
 *   3. -Xms15g -Xmx15g -XX:+AlwaysPreTouch          1.18M rows/s, 7.7 cores busy -> 12.6 min
 *   4. logback.xml at WARN (was DEBUG by default)   sample join+write 105 s -> 19 s
 *   5. Parallel reference load (1 task per part)    500M rows loaded in 73.5 s
 *   6. ParquetOutput reuses one Avro record         no per-row record allocation
 * </pre>
 *
 * <h2>Robustness fixes (no speed change, but they made the problems visible)</h2>
 * <ul>
 *   <li>{@link #runAll} cancels the remaining tasks on the first failure, and
 *       {@code main} calls {@code shutdownNow()} in a {@code finally}. Before, a failed
 *       chunk left non-daemon pool threads alive and the JVM never exited, so a
 *       failure looked like a slow run.</li>
 *   <li>Each part is written to {@code .inprogress} and renamed only on success.
 *       Before, try-with-resources wrote a valid footer even on failure, so a
 *       half-written part looked complete.</li>
 *   <li>A 30 s progress line (rows, written, heap). This is how the swap stall was found.</li>
 *   <li>Drop-reason counters (malformed / no claim id / bad key / not found).</li>
 * </ul>
 *
 * <h2>Sizing rule</h2>
 * heap ~= per table (8 B x maxId + encoded payload) + 2-3 GiB working memory.
 * Keep -Xmx no larger than the RAM the OS will keep resident: a join does random
 * lookups, so any swapped or compressed page turns into a stall.
 *
 * Usage: singlepass &lt;dataDir&gt; &lt;outDir&gt; [maxId=250000000] [threads=cores-1]
 */
public final class SinglePassParquetJoin {

    private static final int CLAIM_COLS = 12;
    private static final int MEMBER_MIN_COLS = 11;    // uses first,last,gender,plan_type,risk_score (max col 10)
    private static final int PROVIDER_MIN_COLS = 7;   // uses name,specialty,npi,network_status (max col 6)
    private static final Codecs.MemberCodec MEMBER_CODEC = new Codecs.MemberCodec();
    private static final Codecs.ProviderCodec PROVIDER_CODEC = new Codecs.ProviderCodec();

    // join+write counters (updated in batches so the progress line is live)
    private static final AtomicLong READ = new AtomicLong(), WRITTEN = new AtomicLong();
    private static final AtomicLong MALFORMED = new AtomicLong(), NO_CLAIM_ID = new AtomicLong(),
            BAD_KEY = new AtomicLong(), NO_MEMBER = new AtomicLong(), NO_PROVIDER = new AtomicLong();
    private static final AtomicLong LOADED = new AtomicLong();
    private static volatile String phase = "starting";

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        Path outDir = Path.of(args[1]);
        int maxId = args.length > 2 ? Integer.parseInt(args[2]) : 250_000_000;
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        prepareOutDir(outDir);

        long t0 = System.nanoTime();
        ScheduledExecutorService progress = startProgress(t0);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            phase = "loading members";
            DenseIdJoinTable members = load(pool, dataDir.resolve("member_enrichment"), maxId, MEMBER_CODEC, MEMBER_MIN_COLS);
            long t1 = System.nanoTime();
            report("members", members, t1 - t0);

            phase = "loading providers";
            LOADED.set(0);
            DenseIdJoinTable providers = load(pool, dataDir.resolve("provider_reference"), maxId, PROVIDER_CODEC, PROVIDER_MIN_COLS);
            long t2 = System.nanoTime();
            report("providers", providers, t2 - t1);
            logHeap("after load");

            List<Path> claimsParts = Csv.parts(dataDir.resolve("raw_claims"));
            System.out.printf("[1pass] single-pass join: %d claims chunks across %d threads -> parquet%n",
                    claimsParts.size(), threads);
            phase = "join+write";
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < claimsParts.size(); i++) {
                Path part = claimsParts.get(i);
                Path outPart = outDir.resolve(String.format("part-%05d.parquet", i));
                tasks.add(() -> { processChunk(part, outPart, members, providers); return null; });
            }
            runAll(pool, tasks);

            long t3 = System.nanoTime();
            double joinSec = (t3 - t2) / 1e9;
            System.out.printf("[1pass] DONE read=%,d written=%,d  load=%.1f s  join+write=%.1f s  total=%.1f s%n",
                    READ.get(), WRITTEN.get(), (t2 - t0) / 1e9, joinSec, (t3 - t0) / 1e9);
            System.out.printf("[1pass] dropped: malformed=%,d no_claim_id=%,d bad_member/provider_key=%,d "
                            + "member_not_found=%,d provider_not_found=%,d%n",
                    MALFORMED.get(), NO_CLAIM_ID.get(), BAD_KEY.get(), NO_MEMBER.get(), NO_PROVIDER.get());
            System.out.printf("[1pass] throughput: %,.0f rows/s (join+write)%n", READ.get() / joinSec);
            logHeap("end");
        } finally {
            pool.shutdownNow();   // never leave non-daemon workers keeping the JVM alive
            progress.shutdownNow();
        }
    }

    private static void processChunk(Path part, Path outPart,
                                     DenseIdJoinTable members, DenseIdJoinTable providers) throws IOException {
        long read = 0, written = 0, malformed = 0, noClaim = 0, badKey = 0, noMember = 0, noProvider = 0;
        String[] mf = new String[MEMBER_CODEC.fields()];
        String[] pf = new String[PROVIDER_CODEC.fields()];
        Path tmp = outPart.resolveSibling(outPart.getFileName() + ".inprogress");
        boolean ok = false;
        try (ParquetOutput out = new ParquetOutput(tmp)) {
            for (String[] c : Csv.rowsOfFile(part)) {
                if (++read % 1_000_000 == 0) { READ.addAndGet(1_000_000); WRITTEN.addAndGet(written); written = 0; }
                if (c.length < CLAIM_COLS) { malformed++; continue; }
                String claimId = Cleanse.id(c[0]);
                if (claimId == null) { noClaim++; continue; }
                String memberId = Cleanse.id(c[1]);
                String providerId = Cleanse.id(c[2]);
                long memberKey = OnHeapJoinTable.parseId(memberId);
                long providerKey = OnHeapJoinTable.parseId(providerId);
                if (memberKey < 0 || providerKey < 0) { badKey++; continue; }
                if (!members.get(memberKey, MEMBER_CODEC, mf)) { noMember++; continue; }
                if (!providers.get(providerKey, PROVIDER_CODEC, pf)) { noProvider++; continue; }

                out.write(claimId, memberId, providerId,
                        Cleanse.isoDate(c[3]), Cleanse.code(c[4]), Cleanse.code(c[5]),
                        Cleanse.amount(c[6]), Cleanse.amount(c[7]),
                        Cleanse.status(c[8]), Cleanse.state(c[9]), Cleanse.isoDate(c[10]), c[11],
                        mf[0], mf[1], mf[2], mf[3], parseDouble(mf[4]),
                        pf[0], pf[1], pf[2], pf[3]);
                written++;
            }
            ok = true;
        } finally {
            if (ok) {
                Files.move(tmp, outPart, StandardCopyOption.ATOMIC_MOVE);
            } else {
                Files.deleteIfExists(tmp);
            }
        }
        READ.addAndGet(read % 1_000_000);
        WRITTEN.addAndGet(written);
        MALFORMED.addAndGet(malformed);
        NO_CLAIM_ID.addAndGet(noClaim);
        BAD_KEY.addAndGet(badKey);
        NO_MEMBER.addAndGet(noMember);
        NO_PROVIDER.addAndGet(noProvider);
    }

    /** Loads a reference table in parallel: one task per part file. */
    private static DenseIdJoinTable load(ExecutorService pool, Path dir, int maxId,
                                         Codecs.Codec codec, int need) throws Exception {
        DenseIdJoinTable table = new DenseIdJoinTable(maxId);
        List<Callable<Void>> tasks = new ArrayList<>();
        List<Path> parts = Csv.parts(dir);
        for (int p = 0; p < parts.size(); p++) {
            final Path part = parts.get(p);
            final int partIdx = p;
            tasks.add(() -> {
                DenseIdJoinTable.Loader loader = table.newLoader(partIdx);
                long n = 0;
                for (String[] r : Csv.rowsOfFile(part)) {
                    if (r.length < need) continue;
                    long key = OnHeapJoinTable.parseId(r[0]);
                    if (key < 0) continue;
                    loader.put(key, codec.encode(r));
                    if (++n % 1_000_000 == 0) LOADED.addAndGet(1_000_000);
                }
                LOADED.addAndGet(n % 1_000_000);
                loader.finish();
                return null;
            });
        }
        runAll(pool, tasks);   // Future.get() gives happens-before for the loaders' writes
        table.freeze();
        return table;
    }

    /** Runs all tasks; on the first failure cancels the rest and rethrows. */
    private static void runAll(ExecutorService pool, List<Callable<Void>> tasks) throws Exception {
        List<Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> t : tasks) futures.add(pool.submit(t));
        try {
            for (Future<Void> f : futures) f.get();
        } catch (ExecutionException e) {
            for (Future<Void> f : futures) f.cancel(true);
            Throwable cause = e.getCause();
            if (cause instanceof Exception) throw (Exception) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw e;
        }
    }

    private static void prepareOutDir(Path outDir) throws IOException {
        Files.createDirectories(outDir);
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(outDir, "part-*.parquet*")) {
            if (ds.iterator().hasNext()) {
                throw new IllegalStateException("output dir already has part files from a previous run: "
                        + outDir + " (delete them or pick another dir)");
            }
        }
    }

    private static ScheduledExecutorService startProgress(long t0) {
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress");
            t.setDaemon(true);
            return t;
        });
        s.scheduleAtFixedRate(() -> {
            Runtime rt = Runtime.getRuntime();
            long usedGb = (rt.totalMemory() - rt.freeMemory()) >> 30;
            long count = phase.startsWith("loading") ? LOADED.get() : READ.get();
            System.out.printf("[1pass] %6.0fs  %-17s rows=%,d  written=%,d  heap=%d/%d GiB%n",
                    (System.nanoTime() - t0) / 1e9, phase, count, WRITTEN.get(), usedGb, rt.maxMemory() >> 30);
        }, 30, 30, TimeUnit.SECONDS);
        return s;
    }

    private static void report(String name, DenseIdJoinTable t, long nanos) {
        System.out.printf("[1pass] %s loaded: %,d in %.1f s  (index %.1f GiB, payload %.1f GiB in %.1f GiB segments, overflow ids=%,d, duplicate ids=%,d)%n",
                name, t.size(), nanos / 1e9, t.indexBytes() / 1073741824.0, t.payloadBytes() / 1073741824.0,
                t.segmentBytes() / 1073741824.0, t.overflowSize(), t.duplicates());
    }

    private static void logHeap(String when) {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        System.out.printf("[1pass] heap %s: used %.1f GiB of max %.1f GiB%n", when,
                (rt.totalMemory() - rt.freeMemory()) / 1073741824.0, rt.maxMemory() / 1073741824.0);
    }

    private static Double parseDouble(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
