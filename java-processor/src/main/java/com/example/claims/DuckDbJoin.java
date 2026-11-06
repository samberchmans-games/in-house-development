package com.example.claims;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Approach 2 - DuckDB via JDBC.
 *
 * The same three-way cleanse + join + merge as PlainJavaJoin, expressed as
 * one SQL statement executed by DuckDB's parallel, vectorized, out-of-core
 * engine directly against the .csv.gz files.
 */
public final class DuckDbJoin {

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        Path outFile = Path.of(args[1]);
        String memoryLimit = args.length > 2 ? args[2] : "12GB";

        String claims = dataDir.resolve("raw_claims") + "/*.csv.gz";
        String members = dataDir.resolve("member_enrichment") + "/*.csv.gz";
        String providers = dataDir.resolve("provider_reference") + "/*.csv.gz";

        long t0 = System.nanoTime();
        try (Connection con = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = con.createStatement()) {
            st.execute("SET memory_limit='" + memoryLimit + "'");
            st.execute("SET preserve_insertion_order=false");
            st.execute("SET temp_directory='" + dataDir.resolve("duckdb_tmp") + "'");
            st.execute("SET max_temp_directory_size='400GiB'");

            // Phase 1: cleanse the dirty claims feed once into typed Parquet.
            java.nio.file.Files.createDirectories(dataDir.resolve("duckdb_tmp"));
            String cleanClaims = dataDir.resolve("duckdb_tmp/clean_claims.parquet").toString();
            String cleanseSql = String.join("\n",
                "COPY (",
                "  SELECT",
                "    upper(trim(claim_id))    AS claim_id,",
                "    upper(trim(member_id))   AS member_id,",
                "    upper(trim(provider_id)) AS provider_id,",
                "    try_strptime(claim_date, '%Y-%m-%d')::DATE AS claim_date,",
                "    nullif(upper(trim(service_code)), 'XXXXX') AS service_code,",
                "    nullif(upper(trim(diagnosis_code)), '9999') AS diagnosis_code,",
                "    try_cast(replace(replace(trim(billed_amount), '$', ''), ',', '') AS DOUBLE) AS billed_amount,",
                "    try_cast(replace(replace(trim(paid_amount), '$', ''), ',', '') AS DOUBLE) AS paid_amount,",
                "    CASE upper(trim(claim_status))",
                "      WHEN 'PAID' THEN 'PAID' WHEN 'PD' THEN 'PAID'",
                "      WHEN 'DENIED' THEN 'DENIED' WHEN 'D' THEN 'DENIED'",
                "      WHEN 'PENDING' THEN 'PENDING' WHEN 'P' THEN 'PENDING'",
                "    END AS claim_status,",
                "    CASE",
                "      WHEN upper(trim(state)) IN ('TEXAS','TX') THEN 'TX'",
                "      WHEN upper(trim(state)) IN ('CALIFORNIA','CA') THEN 'CA'",
                "      WHEN upper(trim(state)) IN ('FLORIDA','FL') THEN 'FL'",
                "      WHEN upper(trim(state)) IN ('NEW YORK','NY') THEN 'NY'",
                "      WHEN upper(trim(state)) IN ('ILLINOIS','IL') THEN 'IL'",
                "      WHEN upper(trim(state)) IN ('PENNSYLVANIA','PA') THEN 'PA'",
                "      WHEN upper(trim(state)) IN ('OHIO','OH') THEN 'OH'",
                "      WHEN upper(trim(state)) IN ('GEORGIA','GA') THEN 'GA'",
                "      WHEN upper(trim(state)) IN ('NORTH CAROLINA','NC') THEN 'NC'",
                "      WHEN upper(trim(state)) IN ('MICHIGAN','MI') THEN 'MI'",
                "    END AS state,",
                "    try_strptime(member_dob, '%Y-%m-%d')::DATE AS member_dob,",
                "    source_system",
                "  FROM read_csv('" + claims + "', header=true, all_varchar=true)",
                ") TO '" + cleanClaims + "' (FORMAT PARQUET, COMPRESSION 'zstd')");
            System.out.println("[duckdb] phase 1: cleansing claims -> parquet...");
            long p0 = System.nanoTime();
            st.execute(cleanseSql);
            System.out.printf("[duckdb] phase 1 done in %.1f s%n", (System.nanoTime() - p0) / 1e9);

            // Phase 2a: claims x members -> temp parquet (one 250M hash table in memory).
            String cmJoin = dataDir.resolve("duckdb_tmp/claims_members.parquet").toString();
            String joinSqlA = String.join("\n",
                "COPY (",
                "  SELECT c.*,",
                "    m.first_name AS member_first_name, m.last_name AS member_last_name,",
                "    m.gender AS member_gender, m.plan_type AS member_plan_type,",
                "    m.risk_score AS member_risk_score",
                "  FROM read_parquet('" + cleanClaims + "') c",
                "  JOIN read_csv('" + members + "', header=true) m ON c.member_id = m.member_id",
                "  WHERE c.claim_id IS NOT NULL",
                "    AND c.member_id IS NOT NULL AND c.member_id NOT IN ('UNKNOWN', 'N/A')",
                "    AND c.provider_id IS NOT NULL AND c.provider_id NOT IN ('UNKNOWN', 'N/A')",
                ") TO '" + cmJoin + "' (FORMAT PARQUET, COMPRESSION 'zstd')");
            System.out.println("[duckdb] phase 2a: claims x members...");
            long p1 = System.nanoTime();
            st.execute(joinSqlA);
            System.out.printf("[duckdb] phase 2a done in %.1f s%n", (System.nanoTime() - p1) / 1e9);

            // Phase 2b: (claims x members) x providers -> final output.
            String joinSqlB = String.join("\n",
                "COPY (",
                "  SELECT cm.*,",
                "    p.provider_name, p.specialty AS provider_specialty,",
                "    p.npi AS provider_npi, p.network_status AS provider_network_status",
                "  FROM read_parquet('" + cmJoin + "') cm",
                "  JOIN read_csv('" + providers + "', header=true) p ON cm.provider_id = p.provider_id",
                ") TO '" + outFile + "' (FORMAT CSV, HEADER)");
            System.out.println("[duckdb] phase 2b: ... x providers -> output...");
            long p2 = System.nanoTime();
            st.execute(joinSqlB);
            System.out.printf("[duckdb] phase 2b done in %.1f s%n", (System.nanoTime() - p2) / 1e9);
        }
        long t1 = System.nanoTime();
        System.out.printf("[duckdb] DONE in %.1f s -> %s%n", (t1 - t0) / 1e9, outFile);
    }
}
