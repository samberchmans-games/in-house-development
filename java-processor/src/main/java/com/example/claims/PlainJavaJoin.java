package com.example.claims;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

/**
 * Approach 1 - plain Java.
 *
 * Three-way merge: raw_claims x member_enrichment x provider_reference.
 * Both 250M-row reference datasets are loaded into PackedJoinTables (flat
 * byte-arena hash maps, ~50 bytes/entry vs ~200 for HashMap<String,String[]>),
 * then the 800M-row claims feed is streamed, cleansed, joined and written.
 */
public final class PlainJavaJoin {

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        Path outFile = Path.of(args[1]);
        int expected = args.length > 2 ? Integer.parseInt(args[2]) : 25_000_000;
        long t0 = System.nanoTime();

        System.out.println("[plain] loading members...");
        PackedJoinTable members = loadMembers(dataDir.resolve("member_enrichment"), expected);
        long t1 = System.nanoTime();
        System.out.printf("[plain] members loaded: %,d in %.1f s%n", members.size(), (t1 - t0) / 1e9);

        System.out.println("[plain] loading providers...");
        PackedJoinTable providers = loadProviders(dataDir.resolve("provider_reference"), expected);
        long t2 = System.nanoTime();
        System.out.printf("[plain] providers loaded: %,d in %.1f s%n", providers.size(), (t2 - t1) / 1e9);

        System.out.println("[plain] streaming claims...");
        long rows = 0, matched = 0, written = 0;
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                new GZIPOutputStream(Files.newOutputStream(outFile), 1 << 16),
                StandardCharsets.UTF_8), 1 << 16)) {
            out.write("claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,"
                    + "billed_amount,paid_amount,claim_status,state,member_dob,source_system,"
                    + "member_first_name,member_last_name,member_gender,member_plan_type,member_risk_score,"
                    + "provider_name,provider_specialty,provider_npi,provider_network_status\n");
            for (String[] c : Csv.rows(dataDir.resolve("raw_claims"))) {
                rows++;
                String claimId = Cleanse.id(c[0]);
                String memberId = Cleanse.id(c[1]);
                String providerId = Cleanse.id(c[2]);
                if (claimId == null || memberId == null || providerId == null) continue;

                byte[] m = members.get(memberId);
                byte[] p = providers.get(providerId);
                if (m == null || p == null) continue;
                matched++;

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
                out.write(',');
                out.write(new String(p, StandardCharsets.UTF_8).replace('|', ','));
                out.write('\n');
                written++;
                if (rows % 25_000_000 == 0) {
                    long now = System.nanoTime();
                    System.out.printf("[plain] %,d rows read, %,d written, %.0f rows/s%n",
                            rows, written, rows / ((now - t2) / 1e9));
                }
            }
        }
        long t3 = System.nanoTime();
        System.out.printf("[plain] DONE read=%,d matched=%,d written=%,d  join+write=%.1f s  total=%.1f s%n",
                rows, matched, written, (t3 - t2) / 1e9, (t3 - t0) / 1e9);
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
