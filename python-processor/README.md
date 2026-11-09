# Single-pass claims join in Python (no Spark)

One job, one machine: **800M dirty claims ⋈ 250M members ⋈ 250M providers**, cleansed,
de-duplicated and written as Parquet, on a 32 GB laptop, in minutes, with **zero temp disk**.

It's the non-Spark answer to a job that is usually handed to Spark: two joins against
250M-row tables, too big to broadcast, which Spark would run as shuffle-heavy sort-merge
joins. Measured results are in [FINDINGS.md](FINDINGS.md).

```
python -m claims run <dataDir> <outDir> [--threads 8] [--as-of YYYY-MM-DD]
```

## How it works

1. **Load both reference tables into memory, compactly** (~6 B per member row, ~12 B per
   provider row, plus a 4 B index slot per id):
   - **The id is the index.** Ids are dense (`MBR000000001…MBR250000000`), so
     `row_of_id = int32[maxId+2]`. No hash table, no hashing, no probing.
   - **Strings become 1–2 byte dictionary codes.** Every name column has under 600 distinct
     values. `risk_score` is stored as uint16 hundredths and `npi` as uint64.
   - Each part writes straight into its own slot range of the final arrays, so nothing is
     concatenated or copied after loading.
   - Each encoder checks its assumption (column width, `d.dd` scores, NPIs without leading
     zeros) and **fails loudly** instead of corrupting data.
2. **Stream the claims once**, one part per thread, in ~150K-row Arrow batches:
   gzip + CSV parse → cleanse with Arrow compute kernels → look up both tables with NumPy →
   drop duplicates → write Parquet. Python runs once per batch. Every per-row step runs in
   Arrow's C++ code or in NumPy.
3. **Never materialize an intermediate result.** There's no claims⋈members table and no
   shuffle; the only disk written is the final output.

Output: 23 columns. Each claims part is written as files of ~4M rows
(`part-00000-000.parquet`, …), Snappy-compressed.

## Cleansing rules

| Column | Dirty input | Output |
|---|---|---|
| ids | `' clm00000000002 '`, `UNKNOWN`, `N/A`, empty | trimmed upper-case, or null (row dropped if it's a join key) |
| `claim_date`, `member_dob` | `2024-01-15`, `01/15/2024`, `15-Jan-2024`, `2024-13-45`, `2023-02-29` | `date32`; impossible dates → null, checked against the real calendar |
| `member_dob` | placeholder `1900-01-01`, future `2099-12-31`, born after the claim | null |
| `billed_amount`, `paid_amount` | `"$4,703.89"`, `-50.00`, empty | float64 (negatives kept and flagged) |
| `claim_status` | `PAID`/`paid`/`Paid `/`PD`, `DENIED`/`D`, `PENDING`/`P` | `PAID` / `DENIED` / `PENDING`, else null |
| `state` | `TX`, `tx`, `Texas`, `XX`, `ZZ` | USPS code (50 states + DC), else null |
| `service_code` (CPT) | `' 99284 '`, `j0696`, `XXXXX` | upper-case; placeholder → null |
| `diagnosis_code` (ICD-10) | `E119`, `e11.9`, `J45909`, `9999` | dotted upper-case `E11.9`, `J45.909`; placeholder → null |
| `flag_negative_billed` | | `billed_amount < 0` |
| `flag_paid_exceeds_billed` | | `paid_amount > billed_amount` |

**Duplicates:** a claim id is written once, at its **first occurrence in file order**
(part order, then row order). Within a part a bitmap of seen ids handles it. After all parts
finish, ids a later part shares with an earlier one are removed from the later part's file.
Only the file containing the duplicate is rewritten.

**Duplicate reference ids** (the generator repeats each part's boundary id, see
`java-processor/SINGLEPASS_FINDINGS.md` §16): the lowest part wins.

**Dropped rows are counted by reason.** Every row read ends up in exactly one of written /
malformed / no_claim_id / bad_key / member_not_found / provider_not_found /
duplicate_claim_id, and the job checks that they add up.

## How it's verified

| Check | What it proves |
|---|---|
| `tests/test_cleanse.py` | every rule on hand-picked dirty values (leap years, impossible dates, every format), run through the job's Arrow code **and** the reference SQL |
| `tests/test_encoders.py` | the compact columns round-trip exactly and fail loudly on data that breaks their assumptions; dedup bookkeeping |
| `tests/test_end_to_end.py` | a hand-written dataset with every kind of dirt (quoted amounts, both kinds of duplicate, malformed rows, missing keys) against a hand-written expected output; failed-part cleanup; the overwrite guard |
| `python -m claims reference <data> <out> --as-of …` | the whole output on the real samples (25M and 50M claims) against `claims/reference.py`, an **independent implementation in DuckDB SQL** with its own CSV parser, date parsing, joins and dedup. Row-level `EXCEPT ALL` both ways |
| output checks on the full run | 0 duplicate claim ids, 0 placeholder/future DOBs, plausible date ranges, all counters add up |

DuckDB is used only as a **test oracle** here. It isn't the solution, and it can't run
the full join on this machine (`BLOG_NOTES` §6).

```
pip install -r requirements.txt
pytest tests                                   # 36 tests, < 1 s
python -m claims run <sample> <out> --as-of 2026-11-07
python -m claims reference <sample> <out> --as-of 2026-11-07
python -m claims verify <outA> <outB>          # compare two runs row by row
```

## Benchmark hygiene (learned the hard way)

- Run with the lid open on AC power, under `caffeinate -dims`. Python's `perf_counter`
  stops while macOS sleeps, so the job also checks wall-clock time and warns if the machine
  slept during the run.
- Watch the macOS footprint (`top` MEM / CMPRS), not just RSS. RSS includes pages that
  have been freed but not yet reclaimed.
