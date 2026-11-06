# Claims Data Processing in Python: Plan and Approaches

**Status: plan only. No Python processor code has been written yet.** This document is
written before coding, as `java-processor/APPROACHES.md` was. Measured results go in
`python-processor/FINDINGS.md` once the runs are done.

## 1. Goal

Run the same benchmark as the Java work, in Python:

- `raw_claims` (800,000,032 rows, dirty) ⋈ `member_enrichment` (250,000,010) ⋈ `provider_reference` (250,000,010)
- the same cleansing rules (`java-processor/.../Cleanse.java`)
- output: one merged dataset, the same 21-column schema as `ParquetOutput.java`, written as Parquet

The question for the blog: **how close can Python get to the Java single-pass result
(12.6 min, 11.4 GiB tables, 0 temp disk) on the same 32 GB laptop, and which Python tool
gets there?**

### Fixed inputs (kept the same so results can be compared with Java)

| | Value |
|---|---|
| Data | `/Users/samjoshuaberchmans/Documents/in-house-data/data` (the same files Java read, **not regenerated**) |
| Format | partitioned `.csv.gz`, 25,000,001 rows per part (32 / 10 / 10 parts) |
| Machine | M1 Max (8P + 2E cores), 32 GB RAM |
| Java baseline to match | **748,953,178** rows written; 3,999,935 dropped for claim id; 47,046,919 dropped for bad key; 0 not found |
| Sample baseline to match | part-00000 of each table → **234,688** rows |

### Rules carried over from Java for parity

- **Cleansing:** port `Cleanse` exactly, gaps included (ISO-only dates, placeholder DOBs
  kept, no dedup; see BLOG_NOTES §20). Fixing those would change the row-level output and
  make the comparison with Java meaningless. Any fix comes after parity, as a separate step.
- **Duplicate reference ids** (generator off-by-one, SINGLEPASS_FINDINGS §16): the **lowest
  part index wins**, the same as the Java loader.
- **Inner join** on both tables; count every dropped row by reason, as Java does.

## 2. Approaches considered

| # | Approach | Idea | Verdict |
|---|----------|------|---------|
| P1 | **Pure Python** | `csv` + `gzip` + two `dict`s, stream claims | ✅ Build. **Sample only.** It is the Python version of Bug #2 (object overhead). The full run is estimated, not attempted. |
| P2 | **DuckDB (Python API)** | The same SQL as `DuckDbJoin.java` through `duckdb` 1.x (Java used JDBC 1.1.3) | ✅ Build. Sample, then one **time-boxed** full attempt with a temp-disk cap. A newer engine may do better than §6; either outcome is a data point. |
| P3 | **Polars (lazy, streaming engine)** | `scan_csv` → cleanse expressions → two joins → `sink_parquet` | ✅ Build. Sample, then one time-boxed full attempt. This is the tool most Python readers will reach for. |
| P4 | **Arrow + NumPy single pass** | Port of the Java winner: reference tables as **dictionary-encoded Arrow columns** behind a dense id → row index; claims streamed in Arrow batches, joined with vectorized `take`, written with `ParquetWriter` | ✅ Build. **Expected winner.** The full run is the headline number. |
| P5 | pandas | `read_csv` + `merge` | ❌ Skip. Not built for this: eager, mostly single-threaded, object-dtype strings. Mention briefly in the blog. |
| P6 | PySpark (local) | DataFrame join | 📄 Document only. The same reasoning as Java §19 Option B: two sort-merge joins, shuffle spill. |
| P7 | Numba / Cython | Compile the cleansing loop | 📄 Document only, unless profiling shows P4 is CPU-bound in Python-level code rather than inside Arrow. |
| P8 | Free-threaded CPython 3.13t | Threads without the GIL | ❌ Skip for now. Library wheels are still uneven, and P4 does its heavy work in Arrow C++, which already releases the GIL. |

## 3. How P4 works (the planned design)

The design follows SINGLEPASS_FINDINGS §15: don't hash when the id is already an index,
and shrink the data rather than the memory limit.

### 3a. Reference tables: dense index plus dictionary-encoded columns

- Read each table's parts with `pyarrow.csv`, keeping only the columns the output needs
  (members: first, last, gender, plan_type, risk_score; providers: name, specialty, npi,
  network_status).
- **Dense index:** parse `MBR000000123` → 123 and build `row_of_id = np.full(maxId+1, -1, int32)`.
  At 4 B per slot that is ~1 GiB per table, half of Java's `long[]`. Assigning the parts in
  reverse order makes the **lowest part win** on duplicate ids.
- **Dictionary encoding:** every string column comes from a small fixed list in the
  generator (first and last names, specialties; `provider_name` = `LAST + SPECIALTY + " CLINIC"`).
  Arrow `dictionary_encode()` turns each one into small integer codes, which is what Java's
  `Codecs` did by hand. `risk_score` → `float32` (or int16 hundredths); `npi` → `uint64`.

Estimated resident size (to be measured):

| | Index | Columns | Total |
|---|---|---|---|
| members | ~0.93 GiB | ~2–4 GiB | ~3–5 GiB |
| providers | ~0.93 GiB | ~3–4 GiB | ~4–5 GiB |
| **both** | | | **~7–10 GiB** (Java: 11.4 GiB) |

If a column's dictionary turns out large (e.g. the ~4% non-pattern provider names), it stays
a plain string column. That is a measured decision, not a guess.

### 3b. Claims: streaming, vectorized cleansing, join with `take`

For each claims part, as one task:

1. `pyarrow.csv.open_csv` streams record batches out of the gzip file (with all columns read as strings, the same as Java).
2. Cleansing runs as `pyarrow.compute` kernels (trim, upper, `replace_substring`, `cast`,
   `is_in`, regex match for the ISO date shape), with no per-row Python code.
3. The ids are parsed to integers, then `row_of_id[key]` gives row positions. A filter keeps
   only rows with both keys valid and found, with each drop counted by reason.
4. `members.take(rows)` and `providers.take(rows)` copy the attributes across. `take` on
   dictionary columns copies only the small codes.
5. A per-part `ParquetWriter` (Snappy, like Java) writes to `.inprogress` and renames the
   file only on success.

### 3c. Parallelism: threads first, processes as the fallback

- Arrow releases the GIL in CSV parsing, gzip, compute kernels, `take` and Parquet
  writing, so a **thread pool** (one part per task, as in Java) should keep 8–9 cores
  busy and share the reference tables for free.
- If profiling shows the GIL is the bottleneck: **`multiprocessing` with `fork`**, with the
  tables loaded before the fork. Arrow and NumPy buffers have no per-row refcounts, so
  copy-on-write keeps the pages shared.
- The memory rule from Java §15b still applies: the working set must stay resident
  (**no swap, no macOS compression**), so peak RSS is a hard target of **≤ ~17 GB**.

## 4. Steps (in order)

Each step ends with measured numbers written into FINDINGS.md before the next one starts.

| # | Step | Done when |
|---|------|-----------|
| 0 | Branch `python-code-develop` (fast-forwarded to `develop`); `.venv` + pinned `requirements.txt` (pyarrow, numpy, duckdb, polars, psutil) | `pip install -r` works; versions recorded |
| 1 | **Sample dataset**: a directory of symlinks to part-00000 of each table | Exists; no data copied |
| 2 | **`cleanse.py`**: port `Cleanse` in two forms, scalar (for P1) and Arrow-compute (for P4), plus a small test table of dirty inputs → expected outputs from the Java rules | Both forms pass the same cases |
| 3 | **Common harness**: CLI `python -m claims <approach> <data> <out> [opts]`; phase timers; 30 s progress line (rows, written, RSS); drop-reason counters; peak RSS via `/usr/bin/time -l` | Prints the same summary block as Java |
| 4 | **P1 pure Python** on the sample | Time, RSS, bytes/entry measured; 234,688 rows |
| 5 | **P2 DuckDB** on the sample | 234,688 rows; time |
| 6 | **P3 Polars** on the sample | 234,688 rows; time |
| 7 | **P4 Arrow + NumPy** on the sample, then a **2-part sample** to exercise threads and the duplicate boundary ids | Rows match; reference table sizes measured |
| 8 | **Parity check**: compare each sample output with Java's using DuckDB `EXCEPT ALL` in both directions | 0 differing rows (or every difference explained) |
| 9 | **P4 full run** | 748,953,178 rows, drop counters equal to Java's, time + memory profile |
| 10 | **P2 / P3 full attempts**, time-boxed (~45 min each, temp-disk cap ~150 GB, killed above that) | A finished time, or "did not finish after X min / Y GB" |
| 11 | **Write-up**: FINDINGS.md + a Python section appended to BLOG_NOTES (outside git) | Comparison table Java vs P1–P4 |

## 5. What gets measured (the same columns as the Java tables)

Wall time by phase (load members / load providers / join + write), rows/s, peak RSS,
memory compressed by macOS (`top` CMPRS), swap, CPU (user+sys ÷ wall = cores busy), temp
disk, output size, rows written and every drop counter.

## 6. Predictions (written down now so the blog can check them)

| Approach | Sample (25M) | Full (800M) | Why |
|---|---|---|---|
| P1 pure Python | 3–6 min | **won't fit**: ~150–250 B/entry × 500M ≈ 75–125 GB | dicts of tuples of `str` |
| P2 DuckDB | ~20–30 s | likely the same spill story as Java §6 | the 3-way join materializes claims⋈members |
| P3 Polars | ~15–30 s | uncertain; the hash join build side is 250M string keys | streaming helps the claims side, not the build side |
| P4 Arrow + NumPy | ~20–40 s | **~12–20 min**, ~10 GiB tables | the same algorithm as Java; Arrow C++ does the per-row work |

## 7. Risks and open questions

- **Cleansing as kernels:** `amount` (strip `$` and `,`, then parse, with invalid → null) and
  `isoDate` (shape plus range check) need care to match Java exactly. `cast` with
  `safe=False` or a regex pre-filter is the plan, and the step 2 tests are the guard.
- **Gzip is single-threaded per file.** Same limit as Java, and why one task per part is the unit of parallelism.
- **Tail imbalance**: 32 parts on 9 workers (Java §19). Use 8 workers (4 even waves) from the start.
- **`take` with dictionary columns** must stay encoded right through to the Parquet writer.
  If it decodes, memory and output size go up. Check this on the sample.
- **Environment:** `/opt/anaconda3` Python 3.11 has duckdb/pyarrow/numpy but not polars;
  `/usr/local/bin/python3` (3.13) has none of them. A project `.venv` with pinned
  versions avoids depending on either.

## 8. Layout (planned)

```
python-processor/
  APPROACHES.md          this file
  FINDINGS.md            measured results (after the runs)
  requirements.txt
  claims/
    __main__.py          CLI + harness (timers, progress, counters)
    cleanse.py           scalar + Arrow-compute cleansing
    p1_pure_python.py
    p2_duckdb.py
    p3_polars.py
    p4_arrow_singlepass.py
  tests/test_cleanse.py
```
