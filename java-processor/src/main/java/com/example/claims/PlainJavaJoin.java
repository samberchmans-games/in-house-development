package com.example.claims;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

/**
 * Approach 1 - plain Java, SEQUENTIAL two-pass join.
 *
 * Why two passes: two full PackedJoinTables (250M members + 250M providers)
 * need ~42 GB - more than the machine's 34 GB RAM. So we join one reference
 * at a time, keeping only ONE 250M-entry table in memory (~21 GB peak):
 *
 *   Pass 1: load members,  stream claims -> claims+members intermediate (.csv.gz)
 *   Pass 2: load providers, stream intermediate -> final merged output (.csv.gz)
 *
 * The claims feed is cleansed once in pass 1; pass 2 re-reads already-clean data.
 * No intermediate is ever held in memory - each pass streams row-by-row.
 */
public final class PlainJavaJoin {

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        Path outFile = Path.of(args[1]);
        int expected = args.length > 2 ? Integer.parseInt(args[2]) : 250_000_000;
        Path intermediate = outFile.resolveSibling(outFile.getFileName() + ".pass1.csv.gz");
        long t0 = System.nanoTime();

        // ---- Pass 1: claims x members ----
        System.out.println("[plain] pass 1: loading members...");
        PackedJoinTable members = loadMembers(dataDir.resolve("member_enrichment"), expected);
        long t1 = System.nanoTime();
        System.out.printf("[plain] members loaded: %,d in %.1f s%n", members.size(), (t1 - t0) / 1e9);

        long rows1 = 0, written1 = 0;
        try (BufferedWriter out = gzipWriter(intermediate)) {
            // cleansed claims + member attributes
            out.write("claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,"
                    + "billed_amount,paid_amount,claim_status,state,member_dob,source_system,"
                    + "member_first_name,member_last_name,member_gender,member_plan_type,member_risk_score\n");
            for (String[] c : Csv.rows(dataDir.resolve("raw_claims"))) {
                rows1++;
                String claimId = Cleanse.id(c[0]);
                String memberId = Cleanse.id(c[1]);
                String providerId = Cleanse.id(c[2]);
                if (claimId == null || memberId == null || providerId == null) continue;

                byte[] m = members.get(memberId);
                if (m == null) continue;

                String claimDate = Cleanse.isoDate(c[3]);
                String serviceCode = Cleanse.code(c[4]);
                String diagCode = Cleanse.code(c[5]);
                Double billed = Cleanse.amount(c[6]);
                Double paid = Cleanse.amount(c[7]);
                String status = Cleanse.status(c[8]);
                String state = Cleanse.state(c[9]);
                String dob = Cleanse.isoDate(c[10]);

                out.write(String.join(",",
                        claimId, memberId, providerId,
                        nvl(claimDate), nvl(serviceCode), nvl(diagCode),
                        billed == null ? "" : billed.toString(),
                        paid == null ? "" : paid.toString(),
                        nvl(status), nvl(state), nvl(dob), c[11]));
                out.write(',');
                out.write(new String(m, StandardCharsets.UTF_8).replace('|', ','));
                out.write('\n');
                written1++;
                if (rows1 % 50_000_000 == 0) {
                    System.out.printf("[plain] pass1 %,d read, %,d written, %.0f rows/s%n",
                            rows1, written1, rows1 / ((System.nanoTime() - t1) / 1e9));
                }
            }
        }
        long t2 = System.nanoTime();
        System.out.printf("[plain] pass 1 DONE read=%,d written=%,d  %.1f s%n", rows1, written1, (t2 - t1) / 1e9);
        members = null; // free the member table before loading providers

        // ---- Pass 2: (claims x members) x providers ----
        System.out.println("[plain] pass 2: loading providers...");
        System.gc();
        PackedJoinTable providers = loadProviders(dataDir.resolve("provider_reference"), expected);
        long t3 = System.nanoTime();
        System.out.printf("[plain] providers loaded: %,d in %.1f s%n", providers.size(), (t3 - t2) / 1e9);

        long rows2 = 0, written2 = 0;
        try (BufferedWriter out = gzipWriter(outFile)) {
            out.write("claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,"
                    + "billed_amount,paid_amount,claim_status,state,member_dob,source_system,"
                    + "member_first_name,member_last_name,member_gender,member_plan_type,member_risk_score,"
                    + "provider_name,provider_specialty,provider_npi,provider_network_status\n");
            for (String[] cm : Csv.rowsOfFile(intermediate)) {
                rows2++;
                byte[] p = providers.get(cm[2]); // provider_id
                if (p == null) continue;

                // columns 0..16 as-is, then provider attributes
                out.write(String.join(",", java.util.Arrays.copyOf(cm, 17)));
                out.write(',');
                out.write(new String(p, StandardCharsets.UTF_8).replace('|', ','));
                out.write('\n');
                written2++;
                if (rows2 % 50_000_000 == 0) {
                    System.out.printf("[plain] pass2 %,d read, %,d written, %.0f rows/s%n",
                            rows2, written2, rows2 / ((System.nanoTime() - t3) / 1e9));
                }
            }
        }
        long t4 = System.nanoTime();
        System.out.printf("[plain] pass 2 DONE read=%,d written=%,d  %.1f s%n", rows2, written2, (t4 - t3) / 1e9);
        System.out.printf("[plain] TOTAL %.1f s  (load-members=%.1f, pass1=%.1f, load-providers=%.1f, pass2=%.1f)%n",
                (t4 - t0) / 1e9, (t1 - t0) / 1e9, (t2 - t1) / 1e9, (t3 - t2) / 1e9, (t4 - t3) / 1e9);
    }

    private static BufferedWriter gzipWriter(Path f) throws java.io.IOException {
        return new BufferedWriter(new OutputStreamWriter(
                new GZIPOutputStream(Files.newOutputStream(f), 1 << 16),
                StandardCharsets.UTF_8), 1 << 16);
    }

    /** member_id -> "first_name|last_name|gender|plan_type|risk_score" */
    private static PackedJoinTable loadMembers(Path dir, int expected) {
        PackedJoinTable t = new PackedJoinTable(expected);
        for (String[] r : Csv.rows(dir)) {
            t.put(r[0], String.join("|", r[1], r[2], r[3], r[7], r[10]).getBytes(StandardCharsets.UTF_8));
        }
        return t;
    }

    /** provider_id -> "provider_name|specialty|npi|network_status" */
    private static PackedJoinTable loadProviders(Path dir, int expected) {
        PackedJoinTable t = new PackedJoinTable(expected);
        for (String[] r : Csv.rows(dir)) {
            t.put(r[0], String.join("|", r[1], r[2], r[3], r[6]).getBytes(StandardCharsets.UTF_8));
        }
        return t;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
