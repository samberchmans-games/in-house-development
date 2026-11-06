package com.example.claims;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Output format: gzip is CPU-bound (~10-20 MB/s/thread) and was the bottleneck
 * at scale; plain CSV writes at disk speed (~500+ MB/s). Controlled by the
 * --gzip flag; default is plain CSV for throughput.
 */

/**
 * Approach 1b - plain Java, PARALLEL single-pass join.
 *
 * Design (mirrors how DuckDB/Spark work internally):
 *  - Broadcast both reference tables into memory ONCE (compact LongKeyJoinTable).
 *  - Partition the fact table: each of the 32 claims chunks is processed by a
 *    worker thread that reads its chunk, cleanses, joins against both tables,
 *    and writes its own output part-*.csv.gz.
 *  - No intermediate materialization, no single-threaded gzip bottleneck.
 *
 * This is the design intended to be competitive with a distributed system on
 * a single multi-core machine.
 */
public final class ParallelJavaJoin {

    private static final String HEADER =
            "claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,"
            + "billed_amount,paid_amount,claim_status,state,member_dob,source_system,"
            + "member_first_name,member_last_name,member_gender,member_plan_type,member_risk_score,"
            + "provider_name,provider_specialty,provider_npi,provider_network_status\n";

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        Path outDir = Path.of(args[1]);
        int expected = args.length > 2 ? Integer.parseInt(args[2]) : 250_000_000;
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        // output format: "csv" (plain), "gzip", or "parquet"
        String format = args.length > 4 ? args[4].toLowerCase() : "csv";
        boolean gzip = "gzip".equals(format);
        boolean parquet = "parquet".equals(format);
        Files.createDirectories(outDir);
        long t0 = System.nanoTime();

        System.out.println("[par] loading members...");
        LongKeyJoinTable members = loadMembers(dataDir.resolve("member_enrichment"), expected);
        long t1 = System.nanoTime();
        System.out.printf("[par] members loaded: %,d in %.1f s%n", members.size(), (t1 - t0) / 1e9);

        // Pass 1: claims x members, in parallel, to an intermediate dir.
        Path pass1Dir = outDir.resolveSibling(outDir.getFileName() + ".pass1");
        Files.createDirectories(pass1Dir);
        List<Path> claimsParts = Csv.parts(dataDir.resolve("raw_claims"));
        System.out.printf("[par] pass 1: %d claims chunks x members across %d threads...%n", claimsParts.size(), threads);
        AtomicLong r1 = new AtomicLong(), w1 = new AtomicLong();
        runParallel(claimsParts, pass1Dir, members, null, r1, w1, threads, true, gzip, false);
        long t2 = System.nanoTime();
        System.out.printf("[par] pass 1 DONE read=%,d written=%,d  %.1f s%n", r1.get(), w1.get(), (t2 - t1) / 1e9);
        members = null;
        System.gc();

        // Pass 2: (claims x members) x providers, in parallel, to final output.
        System.out.println("[par] pass 2: loading providers...");
        LongKeyJoinTable providers = loadProviders(dataDir.resolve("provider_reference"), expected);
        long t3 = System.nanoTime();
        System.out.printf("[par] providers loaded: %,d in %.1f s%n", providers.size(), (t3 - t2) / 1e9);

        List<Path> pass1Parts = Csv.parts(pass1Dir);
        System.out.printf("[par] pass 2: %d intermediate chunks x providers across %d threads...%n", pass1Parts.size(), threads);
        AtomicLong r2 = new AtomicLong(), w2 = new AtomicLong();
        runParallel(pass1Parts, outDir, null, providers, r2, w2, threads, false, gzip, parquet);
        long t4 = System.nanoTime();
        System.out.printf("[par] pass 2 DONE read=%,d written=%,d  %.1f s%n", r2.get(), w2.get(), (t4 - t3) / 1e9);
        System.out.printf("[par] TOTAL %.1f s  (loadM=%.1f, pass1=%.1f, loadP=%.1f, pass2=%.1f)%n",
                (t4 - t0) / 1e9, (t1 - t0) / 1e9, (t2 - t1) / 1e9, (t3 - t2) / 1e9, (t4 - t3) / 1e9);
    }

    private static void runParallel(List<Path> parts, Path outDir,
                                    LongKeyJoinTable members, LongKeyJoinTable providers,
                                    AtomicLong rowsRead, AtomicLong rowsWritten,
                                    int threads, boolean pass1, boolean gzip, boolean parquet) throws Exception {
        String ext = parquet ? ".parquet" : (gzip ? ".csv.gz" : ".csv");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Future<?>[] futures = new Future[parts.size()];
        for (int i = 0; i < parts.size(); i++) {
            final Path part = parts.get(i);
            final Path outPart = outDir.resolve(String.format("part-%05d%s", i, ext));
            futures[i] = pool.submit(() -> {
                if (pass1) processChunkPass1(part, outPart, members, rowsRead, rowsWritten, gzip);
                else processChunkPass2(part, outPart, providers, rowsRead, rowsWritten, gzip, parquet);
            });
        }
        for (Future<?> f : futures) f.get();
        pool.shutdown();
    }

    private static final String PASS1_HEADER =
            "claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,"
            + "billed_amount,paid_amount,claim_status,state,member_dob,source_system,"
            + "member_first_name,member_last_name,member_gender,member_plan_type,member_risk_score\n";

    /** Pass 1: cleanse claims, join members, write claims+member attributes. */
    private static void processChunkPass1(Path part, Path outPart, LongKeyJoinTable members,
                                          AtomicLong rowsRead, AtomicLong rowsWritten, boolean gzip) {
        long read = 0, written = 0;
        try (BufferedWriter out = writer(outPart, gzip)) {
            out.write(PASS1_HEADER);
            for (String[] c : Csv.rowsOfFile(part)) {
                read++;
                String claimId = Cleanse.id(c[0]);
                if (claimId == null) continue;
                long memberKey = LongKeyJoinTable.parseId(Cleanse.id(c[1]));
                long providerKey = LongKeyJoinTable.parseId(Cleanse.id(c[2]));
                if (memberKey < 0 || providerKey < 0) continue;

                byte[] m = members.get(memberKey);
                if (m == null) continue;

                String claimDate = Cleanse.isoDate(c[3]);
                String serviceCode = Cleanse.code(c[4]);
                String diagCode = Cleanse.code(c[5]);
                Double billed = Cleanse.amount(c[6]);
                Double paid = Cleanse.amount(c[7]);
                String status = Cleanse.status(c[8]);
                String state = Cleanse.state(c[9]);
                String dob = Cleanse.isoDate(c[10]);

                StringBuilder sb = new StringBuilder(160);
                sb.append(claimId).append(',').append(c[1].trim().toUpperCase()).append(',')
                  .append(c[2].trim().toUpperCase()).append(',')
                  .append(nvl(claimDate)).append(',').append(nvl(serviceCode)).append(',')
                  .append(nvl(diagCode)).append(',')
                  .append(billed == null ? "" : billed.toString()).append(',')
                  .append(paid == null ? "" : paid.toString()).append(',')
                  .append(nvl(status)).append(',').append(nvl(state)).append(',')
                  .append(nvl(dob)).append(',').append(c[11]).append(',')
                  .append(new String(m, StandardCharsets.UTF_8).replace('|', ',')).append('\n');
                out.write(sb.toString());
                written++;
            }
        } catch (Exception e) {
            throw new RuntimeException("pass1 chunk failed: " + part, e);
        }
        rowsRead.addAndGet(read);
        rowsWritten.addAndGet(written);
    }

    /** Pass 2: read cleansed claims+members (memory-mapped), join providers, write final output. */
    private static void processChunkPass2(Path part, Path outPart, LongKeyJoinTable providers,
                                          AtomicLong rowsRead, AtomicLong rowsWritten, boolean gzip, boolean parquet) {
        long read = 0, written = 0;
        try {
            if (parquet) {
                try (ParquetOutput out = new ParquetOutput(outPart)) {
                    for (String[] cm : Csv.rowsMapped(part)) {
                        read++;
                        long providerKey = LongKeyJoinTable.parseId(cm[2]);
                        if (providerKey < 0) continue;
                        byte[] p = providers.get(providerKey);
                        if (p == null) continue;
                        String[] pa = new String(p, StandardCharsets.UTF_8).split("\\|", -1);
                        out.write(cm[0], cm[1], cm[2], cm[3], cm[4], cm[5],
                                parseDouble(cm[6]), parseDouble(cm[7]), cm[8], cm[9], cm[10], cm[11],
                                cm[12], cm[13], cm[14], cm[15], parseDouble(cm[16]),
                                pa[0], pa[1], pa[2], pa[3]);
                        written++;
                    }
                }
            } else {
                try (BufferedWriter out = writer(outPart, gzip)) {
                    out.write(HEADER);
                    for (String[] cm : Csv.rowsMapped(part)) {
                        read++;
                        long providerKey = LongKeyJoinTable.parseId(cm[2]);
                        if (providerKey < 0) continue;
                        byte[] p = providers.get(providerKey);
                        if (p == null) continue;

                        StringBuilder sb = new StringBuilder(200);
                        for (int i = 0; i < 17; i++) sb.append(cm[i]).append(',');
                        sb.setLength(sb.length() - 1);
                        sb.append(',').append(new String(p, StandardCharsets.UTF_8).replace('|', ',')).append('\n');
                        out.write(sb.toString());
                        written++;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("pass2 chunk failed: " + part, e);
        }
        rowsRead.addAndGet(read);
        rowsWritten.addAndGet(written);
    }

    private static BufferedWriter writer(Path f, boolean gzip) throws java.io.IOException {
        java.io.OutputStream os = Files.newOutputStream(f);
        if (gzip) os = new GZIPOutputStream(os, 1 << 16);
        return new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8), 1 << 16);
    }

    private static Double parseDouble(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** member_id -> "first_name|last_name|gender|plan_type|risk_score" */
    private static LongKeyJoinTable loadMembers(Path dir, int expected) {
        LongKeyJoinTable t = new LongKeyJoinTable(expected);
        for (String[] r : Csv.rows(dir)) {
            long key = LongKeyJoinTable.parseId(r[0]);
            if (key >= 0) {
                t.put(key, String.join("|", r[1], r[2], r[3], r[7], r[10]).getBytes(StandardCharsets.UTF_8));
            }
        }
        return t;
    }

    /** provider_id -> "provider_name|specialty|npi|network_status" */
    private static LongKeyJoinTable loadProviders(Path dir, int expected) {
        LongKeyJoinTable t = new LongKeyJoinTable(expected);
        for (String[] r : Csv.rows(dir)) {
            long key = LongKeyJoinTable.parseId(r[0]);
            if (key >= 0) {
                t.put(key, String.join("|", r[1], r[2], r[3], r[6]).getBytes(StandardCharsets.UTF_8));
            }
        }
        return t;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
