# Claims Data Processing — Approaches

Benchmark task: **three-way merge** of the generated datasets —

- `raw_claims` (800M rows, dirty) ⋈ `member_enrichment` (250M) ⋈ `provider_reference` (250M)
- with cleansing (nulls, mixed date formats, currency-as-text, status/state normalization)
- output: single merged dataset

Data location (outside the repo): `/Users/samjoshuaberchmans/Documents/in-house-data/data`

## Approaches considered

| # | Approach | Description | Verdict |
|---|----------|-------------|---------|
| 1 | **Plain Java** | Stream gzip CSVs, load both 250M reference sets into `HashMap`s, stream 800M claims, cleanse + join + write | ✅ Built — baseline, zero deps |
| 2 | **DuckDB (JDBC)** | Same logic as one SQL statement on DuckDB's parallel vectorized engine, reading `.csv.gz` directly | ✅ Built — realistic production option |
| 3 | Apache Spark (local) | DataFrame join, local mode | 📄 Documented — graduate to this when data > single machine (~1 TB+) or need multi-node |
| 4 | Apache Flink | Streaming join | ❌ Skipped — designed for unbounded streams, not batch files |
| 5 | Java parallel streams | #1 with ForkJoinPool | ❌ Folded into #1 — memory-bound, no external sort |
| 6 | Off-heap maps (Chronicle Map / MapDB) | #1 but side inputs off-heap | 📄 Documented — use when 250M×2 maps exceed heap; DuckDB already does this internally |
| 7 | Polars / DataFusion | Rust engines | ❌ Out of scope — not Java |

## The two built approaches

### 1. Plain Java (`PlainJavaJoin`)

- `Csv` — streaming iterator over partitioned `.csv.gz` (gzip + buffered, 64 KB buffers)
- `Cleanse` — all dirty-data rules (id/status/state/amount/date/code normalization)
- Two `HashMap<String,String[]>` side inputs (250M entries each) → needs **-Xmx24g** on the full data
- Claims streamed once; O(1) lookups; single pass

### 2. DuckDB (`DuckDbJoin`)

- One `COPY (SELECT ... JOIN ... JOIN ...) TO ...` statement
- DuckDB parallelizes the scan, does the hash join out-of-core, and spills to disk if needed
- Memory controlled with `SET memory_limit` (default 12 GB)

## Build & run

```bash
cd java-processor
mvn -q package

DATA=/Users/samjoshuaberchmans/Documents/in-house-data/data

# plain java (full run needs big heap)
java -Xmx24g -jar target/claims-processor-1.0.0.jar plain $DATA out-plain.csv.gz

# duckdb
java -jar target/claims-processor-1.0.0.jar duckdb $DATA out-duckdb.csv.gz 12GB
```

## Benchmark results

_(filled in after runs — see below)_

| Approach | Dataset | Wall time | Throughput | Peak memory |
|----------|---------|-----------|------------|-------------|
| plain    | sample  | TBD | TBD | TBD |
| duckdb   | sample  | TBD | TBD | TBD |
| plain    | full    | TBD | TBD | TBD |
| duckdb   | full    | TBD | TBD | TBD |
