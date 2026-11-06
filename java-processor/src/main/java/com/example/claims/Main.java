package com.example.claims;

/**
 * Entry point.
 *
 * Usage:
 *   java -cp claims-processor.jar com.example.claims.Main <mode> <dataDir> <outFile> [memoryLimit]
 *
 *   singlepass (recommended; see SinglePassParquetJoin for the measured 12.6 min run):
 *     java -Xms15g -Xmx15g -XX:+AlwaysPreTouch -XX:+UseG1GC -jar target/claims-processor-1.0.0.jar \
 *          singlepass <dataDir> <outDir> [maxId=250000000] [threads=cores-1]
 *     Size -Xmx to the data (~11.4 GiB live + 2-3 GiB), NOT to the machine: on a
 *     32 GB Mac a 20-30 GB heap is swapped/compressed and the join stalls.
 *
 *     mode        plain | parallel | singlepass | duckdb
 *     dataDir     folder containing raw_claims/, member_enrichment/, provider_reference/
 *                 e.g. /Users/samjoshuaberchmans/Documents/in-house-data/data
 *     outFile     output .csv.gz path
 *     memoryLimit duckdb only, default 12GB
 *
 * Examples:
 *   java -Xmx24g -jar target/claims-processor-1.0.0.jar plain /path/to/data out-plain.csv.gz
 *   java -jar target/claims-processor-1.0.0.jar duckdb /path/to/data out-duckdb.csv.gz 12GB
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: Main <plain|parallel|singlepass|duckdb> <dataDir> <outFileOrDir> [mode options]");
            System.exit(1);
        }
        String[] rest = java.util.Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "plain":
                PlainJavaJoin.main(rest);
                break;
            case "parallel":
                ParallelJavaJoin.main(rest);
                break;
            case "singlepass":
                SinglePassParquetJoin.main(rest);
                break;
            case "duckdb":
                DuckDbJoin.main(rest);
                break;
            default:
                System.err.println("unknown mode: " + args[0]);
                System.exit(1);
        }
    }
}
