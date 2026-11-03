#!/usr/bin/env python3
"""
Synthetic dirty claims data generator (Parquet, DuckDB-powered).

Generates 3 datasets designed for join / enrichment / data-quality examples:

  1. raw_claims          - dirty claims fact data (default 1B rows)
                           nulls, dupes, mixed date formats, bad codes,
                           currency-as-text, casing/whitespace issues
  2. member_enrichment   - member reference data for JOIN + ENRICH demos
                           (default 250M rows, join key: member_id)
  3. provider_reference  - provider reference data for PURE JOIN demos
                           (default 250M rows, join key: provider_id)

Target row size ~100 bytes (raw). Output formats (--format):
  parquet-zstd    partitioned Parquet, ZSTD compression (default, smallest)
  parquet-snappy  partitioned Parquet, Snappy compression (faster, bigger)
  csv             partitioned plain CSV (uncompressed, ~150 GB at full scale)
  csv-gzip        partitioned gzip-compressed CSV

Usage:
  # small validation sample (~1M / 400K / 400K rows)
  python3 generate_claims_data.py --sample --out-dir ./data

  # full scale (1B / 250M / 250M rows) - long running
  python3 generate_claims_data.py --out-dir ./data

  # full scale as gzipped CSVs
  python3 generate_claims_data.py --format csv-gzip --out-dir ./data

  # custom sizes
  python3 generate_claims_data.py --claims-rows 2500000000 \
      --member-rows 400000000 --provider-rows 400000000 --out-dir ./data

  # measure raw bytes/row for the claims schema (writes a temp CSV)
  python3 generate_claims_data.py --csv-probe 200000 --out-dir /tmp/probe
"""

import argparse
import math
import multiprocessing as mp
import os
import shutil
import sys
import time

import duckdb

MEMBER_POOL = 250_000_000   # member_id space: MBR000000001..MBR250000000
PROVIDER_POOL = 250_000_000  # provider_id space: PRV0000001..PRV250000000

CPT_CODES = [
    "99213", "99214", "99203", "99204", "99232", "99233", "99285", "99284",
    "70450", "71045", "71046", "80053", "85025", "83036", "81001", "36415",
    "93000", "97110", "97140", "20610", "J0696", "A0425",
]
ICD10_CODES = [
    "E11.9", "I10", "J06.9", "M54.5", "R05.9", "N39.0", "K21.9", "F41.9",
    "J02.9", "R10.9", "Z00.00", "I25.10", "E78.5", "G47.33", "M17.9",
    "R51.9", "J45.909", "E03.9", "D64.9", "K30",
]
STATES = [
    "TX", "CA", "FL", "NY", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
]
STATE_NAMES = [
    "Texas", "California", "Florida", "New York", "Illinois", "Pennsylvania",
    "Ohio", "Georgia", "North Carolina", "Michigan",
]
FIRST_NAMES = [
    "JAMES", "MARY", "JOHN", "PATRICIA", "ROBERT", "JENNIFER", "MICHAEL",
    "LINDA", "DAVID", "ELIZABETH", "WILLIAM", "BARBARA", "RICHARD", "SUSAN",
    "JOSEPH", "JESSICA", "THOMAS", "SARAH", "CHARLES", "KAREN",
    "CARLOS", "MARIA", "WEI", "PRIYA", "AHMED", "FATIMA", "YUKI", "OLGA",
]
LAST_NAMES = [
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER",
    "DAVIS", "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ",
    "WILSON", "ANDERSON", "THOMAS", "TAYLOR", "MOORE", "JACKSON", "MARTIN",
    "LEE", "PATEL", "CHEN", "KIM", "NGUYEN", "KHAN", "SINGH", "MURPHY",
]
SPECIALTIES = [
    "FAMILY MEDICINE", "INTERNAL MEDICINE", "PEDIATRICS", "CARDIOLOGY",
    "ORTHOPEDICS", "RADIOLOGY", "EMERGENCY MEDICINE", "DERMATOLOGY",
    "PSYCHIATRY", "OB/GYN", "NEUROLOGY", "ONCOLOGY", "UROLOGY",
    "ENDOCRINOLOGY", "GASTROENTEROLOGY", "PULMONOLOGY", "NEPHROLOGY",
    "ANESTHESIOLOGY", "OPHTHALMOLOGY", "GENERAL SURGERY",
]
PLAN_TYPES = ["HMO", "PPO", "EPO", "POS", "HDHP"]


def _lst(values):
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def claims_select(start, end):
    """Dirty claims rows for global row ids in [start, end)."""
    return f"""
WITH src AS (
  SELECT i,
         random() AS r1, random() AS r2, random() AS r3,
         random() AS r4, random() AS r5, random() AS r6,
         random() AS r7, random() AS r8, random() AS r9,
         -- 0.5% duplicate of the previous claim id
         i - (random() < 0.005)::INTEGER AS base_i,
         DATE '2021-01-01' + (random() * 1826)::INTEGER AS svc_date,
         DATE '1940-01-01' + (random() * 25550)::INTEGER AS dob_date,
         round((20 + random() * 4980)::DECIMAL(10,2), 2) AS amt
  FROM generate_series({start}, {end}) t(i)
)
SELECT
  CASE
    WHEN r1 < 0.005 THEN NULL
    WHEN r1 < 0.015 THEN lower('CLM' || lpad(base_i::VARCHAR, 11, '0'))
    WHEN r1 < 0.025 THEN ' CLM' || lpad(base_i::VARCHAR, 11, '0') || ' '
    ELSE 'CLM' || lpad(base_i::VARCHAR, 11, '0')
  END AS claim_id,
  CASE
    WHEN r2 < 0.020 THEN NULL
    WHEN r2 < 0.030 THEN 'UNKNOWN'
    WHEN r2 < 0.040 THEN lower('MBR' || lpad((1 + (r9 * {MEMBER_POOL})::BIGINT)::VARCHAR, 9, '0'))
    ELSE 'MBR' || lpad((1 + (r9 * {MEMBER_POOL})::BIGINT)::VARCHAR, 9, '0')
  END AS member_id,
  CASE
    WHEN r3 < 0.020 THEN NULL
    WHEN r3 < 0.030 THEN 'N/A'
    ELSE 'PRV' || lpad((1 + (r8 * {PROVIDER_POOL})::BIGINT)::VARCHAR, 7, '0')
  END AS provider_id,
  CASE
    WHEN r4 < 0.70 THEN strftime(svc_date, '%Y-%m-%d')
    WHEN r4 < 0.85 THEN strftime(svc_date, '%m/%d/%Y')
    WHEN r4 < 0.95 THEN strftime(svc_date, '%d-%b-%Y')
    WHEN r4 < 0.98 THEN '2024-13-45'
    ELSE NULL
  END AS claim_date,
  CASE
    WHEN r5 < 0.86 THEN {_lst(CPT_CODES)}[(1 + r6 * {len(CPT_CODES)})::INTEGER]
    WHEN r5 < 0.90 THEN lower({_lst(CPT_CODES)}[(1 + r6 * {len(CPT_CODES)})::INTEGER])
    WHEN r5 < 0.94 THEN ' ' || {_lst(CPT_CODES)}[(1 + r6 * {len(CPT_CODES)})::INTEGER] || ' '
    WHEN r5 < 0.97 THEN 'XXXXX'
    ELSE NULL
  END AS service_code,
  CASE
    WHEN r6 < 0.80 THEN {_lst(ICD10_CODES)}[(1 + r7 * {len(ICD10_CODES)})::INTEGER]
    WHEN r6 < 0.87 THEN replace({_lst(ICD10_CODES)}[(1 + r7 * {len(ICD10_CODES)})::INTEGER], '.', '')
    WHEN r6 < 0.92 THEN lower({_lst(ICD10_CODES)}[(1 + r7 * {len(ICD10_CODES)})::INTEGER])
    WHEN r6 < 0.95 THEN '9999'
    ELSE NULL
  END AS diagnosis_code,
  CASE
    WHEN r7 < 0.85 THEN amt::VARCHAR
    WHEN r7 < 0.90 THEN '$' || format('{{:,.2f}}', amt)
    WHEN r7 < 0.93 THEN (-amt)::VARCHAR
    WHEN r7 < 0.96 THEN ''
    ELSE NULL
  END AS billed_amount,
  CASE
    WHEN r8 < 0.05 THEN NULL
    WHEN r8 < 0.08 THEN round(amt * 1.4, 2)          -- paid > billed (invalid)
    ELSE round(amt * (0.4 + r9 * 0.6), 2)
  END AS paid_amount,
  CASE
    WHEN r9 < 0.40 THEN 'PAID'
    WHEN r9 < 0.50 THEN 'paid'
    WHEN r9 < 0.55 THEN 'Paid '
    WHEN r9 < 0.60 THEN 'PD'
    WHEN r9 < 0.75 THEN 'DENIED'
    WHEN r9 < 0.80 THEN 'D'
    WHEN r9 < 0.90 THEN 'PENDING'
    WHEN r9 < 0.93 THEN 'P'
    ELSE NULL
  END AS claim_status,
  CASE
    WHEN r1 < 0.80 THEN {_lst(STATES)}[(1 + r2 * {len(STATES)})::INTEGER]
    WHEN r1 < 0.88 THEN {_lst(STATE_NAMES)}[(1 + r2 * {len(STATE_NAMES)})::INTEGER]
    WHEN r1 < 0.94 THEN lower({_lst(STATES)}[(1 + r2 * {len(STATES)})::INTEGER])
    WHEN r1 < 0.97 THEN 'XX'
    ELSE NULL
  END AS state,
  CASE
    WHEN r3 < 0.70 THEN strftime(dob_date, '%Y-%m-%d')
    WHEN r3 < 0.85 THEN strftime(dob_date, '%m/%d/%Y')
    WHEN r3 < 0.92 THEN '1900-01-01'                  -- placeholder junk
    WHEN r3 < 0.95 THEN '2099-12-31'                  -- impossible future dob
    ELSE NULL
  END AS member_dob,
  {_lst(["SRC_A", "SRC_B", "SRC_C", "SRC_D", "SRC_E"])}[(1 + r4 * 5)::INTEGER] AS source_system
FROM src
"""


def members_select(start, end):
    return f"""
WITH src AS (
  SELECT i, random() AS r1, random() AS r2, random() AS r3,
         random() AS r4, random() AS r5, random() AS r6, random() AS r7
  FROM generate_series({start}, {end}) t(i)
)
SELECT
  'MBR' || lpad(i::VARCHAR, 9, '0') AS member_id,
  {_lst(FIRST_NAMES)}[(1 + r1 * {len(FIRST_NAMES)})::INTEGER] AS first_name,
  {_lst(LAST_NAMES)}[(1 + r2 * {len(LAST_NAMES)})::INTEGER] AS last_name,
  CASE WHEN r3 < 0.48 THEN 'M' WHEN r3 < 0.96 THEN 'F' ELSE 'U' END AS gender,
  strftime(DATE '1940-01-01' + (r4 * 25550)::INTEGER, '%Y-%m-%d') AS dob,
  {_lst(STATES)}[(1 + r5 * {len(STATES)})::INTEGER] AS state,
  lpad((r6 * 99999)::INTEGER::VARCHAR, 5, '0') AS zip_code,
  {_lst(PLAN_TYPES)}[(1 + r7 * {len(PLAN_TYPES)})::INTEGER] AS plan_type,
  strftime(DATE '2020-01-01' + (r1 * 1460)::INTEGER, '%Y-%m-%d') AS plan_start_date,
  strftime(DATE '2024-01-01' + (r2 * 730)::INTEGER, '%Y-%m-%d') AS plan_end_date,
  round((r3 * 5)::DECIMAL(6,2), 2) AS risk_score,
  'PRV' || lpad((1 + (r4 * {PROVIDER_POOL})::BIGINT)::VARCHAR, 7, '0') AS pcp_provider_id,
  strftime(DATE '2023-01-01' + (r5 * 1095)::INTEGER, '%Y-%m-%d') AS last_eligibility_date
FROM src
"""


def providers_select(start, end):
    return f"""
WITH src AS (
  SELECT i, random() AS r1, random() AS r2, random() AS r3,
         random() AS r4, random() AS r5, random() AS r6
  FROM generate_series({start}, {end}) t(i)
)
SELECT
  'PRV' || lpad(i::VARCHAR, 7, '0') AS provider_id,
  {_lst(LAST_NAMES)}[(1 + r1 * {len(LAST_NAMES)})::INTEGER] || ' '
    || {_lst(SPECIALTIES)}[(1 + r2 * {len(SPECIALTIES)})::INTEGER] || ' CLINIC' AS provider_name,
  {_lst(SPECIALTIES)}[(1 + r2 * {len(SPECIALTIES)})::INTEGER] AS specialty,
  lpad((1000000000 + (r3 * 999999999)::BIGINT)::VARCHAR, 10, '0') AS npi,
  {_lst(STATES)}[(1 + r4 * {len(STATES)})::INTEGER] AS state,
  lpad((r5 * 99999)::INTEGER::VARCHAR, 5, '0') AS zip_code,
  CASE WHEN r6 < 0.70 THEN 'IN_NETWORK' WHEN r6 < 0.90 THEN 'OUT_OF_NETWORK' ELSE 'PAR' END AS network_status,
  strftime(DATE '2019-01-01' + (r1 * 1460)::INTEGER, '%Y-%m-%d') AS contract_start_date,
  strftime(DATE '2024-01-01' + (r2 * 1460)::INTEGER, '%Y-%m-%d') AS contract_end_date,
  CASE WHEN r3 < 0.4 THEN 'MEDICARE_100' WHEN r3 < 0.7 THEN 'MEDICARE_120' ELSE 'COMMERCIAL_BASE' END AS fee_schedule,
  'GRP' || lpad((1 + (r4 * 2000000)::INTEGER)::VARCHAR, 7, '0') AS group_id,
  strftime(DATE '2022-01-01' + (r5 * 1460)::INTEGER, '%Y-%m-%d') AS last_credentialing_date
FROM src
"""


DATASETS = {
    "raw_claims": claims_select,
    "member_enrichment": members_select,
    "provider_reference": providers_select,
}


def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


FORMATS = {
    "parquet-zstd": ("parquet", "zstd"),
    "parquet-snappy": ("parquet", "snappy"),
    "csv": ("csv", None),
    "csv-gzip": ("csv", "gzip"),
}


def _write_chunk(task):
    """Worker: write one chunk file. Runs in a child process with its own DuckDB connection."""
    name, chunk_idx, start, end, path, fmt, compression, memory_limit = task
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET preserve_insertion_order=false")
    select_fn = DATASETS[name]
    if fmt == "parquet":
        con.execute(
            f"COPY ({select_fn(start, end)}) TO '{path}' "
            f"(FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE 1000000)"
        )
    else:
        con.execute(
            f"COPY ({select_fn(start, end)}) TO '{path}' (FORMAT CSV, HEADER)"
        )
    con.close()
    return chunk_idx, end - start


def generate(name, select_fn, total_rows, chunk_size, out_dir, out_format, workers, memory_limit):
    fmt, compression = FORMATS[out_format]
    ds_dir = os.path.join(out_dir, name)
    os.makedirs(ds_dir, exist_ok=True)
    ext = {"parquet": "parquet", "csv": "csv", None: "csv"}[fmt]
    if out_format == "csv-gzip":
        ext = "csv.gz"
    chunks = math.ceil(total_rows / chunk_size)

    tasks = []
    for c in range(chunks):
        start = c * chunk_size + 1
        end = min(start + chunk_size, total_rows + 1)
        path = os.path.join(ds_dir, f"part-{c:05d}.{ext}")
        tasks.append((name, c, start, end, path, fmt, compression, memory_limit))

    t0 = time.time()
    done = 0
    with mp.Pool(processes=workers) as pool:
        for chunk_idx, nrows in pool.imap_unordered(_write_chunk, tasks):
            done += nrows
            rate = done / max(time.time() - t0, 0.001)
            print(
                f"[{name}] chunk {chunk_idx + 1}/{chunks} finished  rows={done:,}  "
                f"rate={rate:,.0f} rows/s  eta={(total_rows - done) / max(rate, 1) / 60:,.1f} min",
                flush=True,
            )
    size = dir_size(ds_dir)
    print(
        f"[{name}] DONE rows={total_rows:,}  size={size / 1e9:.2f} GB  "
        f"({size / total_rows:.1f} bytes/row on disk)  "
        f"elapsed={(time.time() - t0) / 60:.1f} min",
        flush=True,
    )
    return size


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", default="./data")
    ap.add_argument("--claims-rows", type=int, default=1_000_000_000)
    ap.add_argument("--member-rows", type=int, default=250_000_000)
    ap.add_argument("--provider-rows", type=int, default=250_000_000)
    ap.add_argument("--chunk-size", type=int, default=25_000_000)
    ap.add_argument("--format", default="parquet-zstd", choices=list(FORMATS),
                    help="output format (default: parquet-zstd)")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2),
                    help="parallel writer processes (default: CPU count - 2)")
    ap.add_argument("--sample", action="store_true", help="1M claims / 400K members / 400K providers")
    ap.add_argument("--only", choices=list(DATASETS), help="generate a single dataset")
    ap.add_argument("--csv-probe", type=int, metavar="N", help="write N claims rows to CSV to measure raw bytes/row")
    ap.add_argument("--memory-limit", default="12GB",
                    help="DuckDB memory limit PER WORKER process")
    args = ap.parse_args()

    if args.csv_probe:
        os.makedirs(args.out_dir, exist_ok=True)
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{args.memory_limit}'")
        csv_path = os.path.join(args.out_dir, "claims_probe.csv")
        con.execute(f"COPY ({claims_select(1, args.csv_probe + 1)}) TO '{csv_path}' (FORMAT CSV, HEADER)")
        size = os.path.getsize(csv_path)
        print(f"CSV probe: {args.csv_probe:,} rows, {size:,} bytes -> {size / args.csv_probe:.1f} raw bytes/row (incl. header)")
        return

    rows = {
        "raw_claims": args.claims_rows,
        "member_enrichment": args.member_rows,
        "provider_reference": args.provider_rows,
    }
    if args.sample:
        rows = {"raw_claims": 1_000_000, "member_enrichment": 400_000, "provider_reference": 400_000}

    if not args.sample:
        for name in rows:
            if args.only and name != args.only:
                continue
            ds_dir = os.path.join(args.out_dir, name)
            if os.path.isdir(ds_dir) and os.listdir(ds_dir):
                print(f"ERROR: {ds_dir} already exists and is not empty; refusing to overwrite.", file=sys.stderr)
                sys.exit(1)

    total_size = 0
    t0 = time.time()
    for name, n in rows.items():
        if args.only and name != args.only:
            continue
        total_size += generate(name, DATASETS[name], n, args.chunk_size, args.out_dir,
                               args.format, args.workers, args.memory_limit)

    print(f"\nALL DONE  total={total_size / 1e9:.2f} GB  elapsed={(time.time() - t0) / 60:.1f} min  out={os.path.abspath(args.out_dir)}")


if __name__ == "__main__":
    main()
