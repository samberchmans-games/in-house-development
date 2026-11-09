# Single-pass claims join in Python: measured results

Design and rules: [README.md](README.md). Machine: Apple M1 Max (8P + 2E cores), 32 GB,
macOS. Python 3.13.5, pyarrow 26.0.1, numpy 2.5.4. Input: the generated dataset, 42 GB of
`.csv.gz` (800,000,032 claims, 250,000,010 members, 250,000,010 providers).

---

## 1. Result

```
caffeinate -dims python -m claims run <data> <out> --threads 8 --as-of 2026-11-07
```

| Phase | Time | Notes |
|---|---|---|
| Load 250M members | 26.5 s | 10 parts in parallel |
| Load 250M providers | 25.3 s | |
| Join + cleanse + dedup + write 800M claims | 325.5 s | **2.46M rows/s**, 32 parts on 8 threads |
| Cross-part dedup | 19.3 s | 27 ids, 27 files rewritten in parallel |
| **Total** | **396.6 s (6.6 min)** | wall clock 397.9 s: no sleep |

| | |
|---|---|
| Rows read → written | 800,000,032 → **745,464,302** |
| Dropped: null/UNKNOWN claim id | 3,999,935 |
| Dropped: null/UNKNOWN/unparseable member or provider key | 47,046,919 |
| Dropped: valid key, no matching member / provider | 0 / 0 |
| Dropped: duplicate claim id | 3,488,876 |
| Malformed rows | 0 |
| Reference tables in memory | **6.05 GiB** (index 1.86 + columns 4.19) |
| Peak macOS footprint | **14 GB** (≤ 2.6 GB of it compressed); peak RSS 15.7 GiB |
| CPU | 2,858 s user + 116 s sys ≈ 7.5 cores busy |
| Output | 192 Parquet files (~4M rows each), 38 GB, Snappy |
| **Temp disk** | **0** |

Conditions: battery power, swap already 6.5 GB full from other apps. Run-to-run variation
on this laptop is ~±20% (§5), so treat this as **~5–7 min**.

---

## 2. The output is correct: how we know

**1. Independent reference implementation.** `claims/reference.py` implements the same
rules in DuckDB SQL, with its own CSV parser, date parsing (`try_strptime`), joins and dedup
(`QUALIFY row_number()`). Row-level `EXCEPT ALL` in both directions over all 23 columns:

| Dataset | Claims | Job output | Reference | Differences |
|---|---|---|---|---|
| sample 1 (part 0 of each table) | 25,000,001 | 234,679 | 234,679 | **0** |
| sample 2 (parts 0–1) | 50,000,002 | 1,871,033 | 1,871,033 | **0** |
| tiny hand-made dataset (tests) | 12 | 5 | 5 | **0** |

The reference can't run the full join on this machine (it materializes the 3-way join;
see BLOG_NOTES §6), so the full run is checked by (2) and (3).

**2. Data-quality checks on the full output** (745,464,302 rows):

| Check | Result | What the generator put in |
|---|---|---|
| duplicate claim ids | **0** | 0.5% duplicated + part-boundary repeats |
| `member_dob` = 1900-01-01 / after as-of / after claim date | **0 / 0 / 0** | 7% placeholder, 3% `2099-12-31` |
| undotted ICD-10 codes | **0** | 7% `E119`-style |
| `source_system` not `SRC_A..E` | **0** | |
| `claim_date` range | 2021-01-01 … 2026-01-01 | `2021-01-01 + 0..1826 days` ✅ |
| `member_dob` range | 1940-01-01 … 2009-12-14 | `1940-01-01 + 0..25550 days` ✅ |
| `claim_date` null | 5.0% | 3% `2024-13-45` + 2% null ✅ |
| `member_dob` null | 15.5% | 7% placeholder + 3% future + 5% null ✅ |
| `billed_amount` null | 7.0% | 3% empty + 4% null ✅ |
| `flag_negative_billed` | 3.0% | 3% negated amounts ✅ |
| `flag_paid_exceeds_billed` | 5.5% | 3% `amt × 1.4` + paid > negative billed ✅ |

**3. Counters add up:** 745,464,302 + 3,999,935 + 47,046,919 + 3,488,876 = 800,000,032.
The duplicates removed equal the 3,488,876 extra rows that had been in the earlier,
non-deduplicating output (`count(*) - count(DISTINCT claim_id)`).

**4. 36 unit and end-to-end tests** (`pytest tests`, < 1 s), see README.

---

## 3. What changed on the way, and what each change bought

| # | Change | Measured effect |
|---|---|---|
| 1 | Reference tables as dictionary codes behind a dense id index | both tables **6.05 GiB** (Java's hand-written codecs: 11.4 GiB) |
| 2 | Stream reference parts and encode batch by batch, not whole parts | 2-part sample peak RSS 8.2 → 5.7 GiB, load 24.0 → 17.6 s |
| 3 | Write each part straight into its slot range of the final arrays (no per-part buffers, no concatenation) | sample 1 peak RSS 4.3 → **2.1 GiB**; full load footprint ~21 GB → **~6 GB** |
| 4 | Real dates (3 formats, calendar-checked), DOB rules, ICD-10 dots, USPS states, flags, exact dedup | `claim_date` null 30.0% → **5.0%**; duplicate claim ids 3,488,876 → **0** |
| 5 | Output in ~4M-row files; cross-part dedup rewrites only the affected files, in parallel | dedup step 523 s → **19.3 s**; total 865 → **397 s** |
| 6 | One thread per CSV reader (no Arrow read-ahead threads inside each part) | join footprint 14 → 13 GB, compressed 4.7 → 1.9 GB, not slower (8-part test: 90.9 → 87.8 s) |

---

## 4. Bugs and traps found (good blog material)

### 4a. Arrow's `strptime` silently "fixes" impossible dates
```
pc.strptime(['2023-02-29', '2024-02-31'], '%Y-%m-%d')  ->  2023-03-01, 2024-03-02
```
No error and no null: invalid day-of-month values roll over into the next month. In this
generated data the only bad date is `2024-13-45`, which `strptime` does reject (month 13),
so it would have got away with it here. A real feed with `02-30` or `04-31` would be
silently shifted. The job parses with regexes and validates against the real calendar
(leap years included) in NumPy; the tests cover these cases. DuckDB's `try_strptime` rejects
them, which is what the reference uses.

### 4b. The "rare" cross-part duplicate happened 27 times out of 31 boundaries
The generator's off-by-one (`generate_series` is inclusive; Java findings §16) doesn't
only duplicate reference ids. **Every claims part also starts with the previous part's last
claim id.** The first dedup version rewrote whole 1.2 GB part files for this, one by one:
523 s of an 865 s run. With 4M-row files, the same fix rewrites 27 small files in 19 s.

> Lesson: design the "rare" path for the data you actually have. Here, a generator bug
> made it happen at almost every boundary.

### 4c. Memory nobody was using (allocator fragmentation)
Loading 2.3 GiB of members left RSS at **12.6 GiB**, and Arrow's pool held 0 bytes of it.
The cause was ~900 small per-batch NumPy arrays per part, kept alive until a final
`np.concatenate`, which fragmented the macOS allocator. Writing each batch straight into
its final position fixed it (change 3).

### 4d. A SQL scoping trap in the reference implementation
With the same alias `k` for both deduplicated reference subqueries, the provider subquery's
`WHERE k IS NOT NULL` / `PARTITION BY k` bound to the **member** table's `m.k`. DuckDB lets a
`FROM` subquery see earlier `FROM` items (an implicit LATERAL join), so it kept one provider
per member and silently lost rows. The hand-written tiny dataset caught it (4 rows instead
of 5). Fixed with distinct aliases.

> Lesson: a reference implementation needs tests too. Hand-written expected output is the
> only check that doesn't trust any engine.

### 4e. The laptop slept through a benchmark
One run reported 491.7 s while the process took 45 minutes: the Mac slept twice (lid
closed; the second sleep logged as "Dark Wake Thermal Emergency"). `time.perf_counter()`
doesn't advance during macOS sleep. The job now compares wall-clock time with
`perf_counter` and warns.

### 4f. Java's CSV reader mis-reads 4% of claims (found while comparing outputs)
About 4% of claim lines quote the amount (`"$4,066.19"`). Java's `Csv` splits on every comma,
so those lines get 13 fields and every column from `billed_amount` on shifts by one
(`source_system` ends up holding a date of birth). Row counts were unaffected, so it went
unnoticed. In the Java output, **30,081,528 rows** were affected. Arrow's CSV parser
(and Spark's, and DuckDB's) handle quotes correctly.

---

## 5. Run-to-run variation

The same job (before the cleansing changes, which add work) on the same input:
389.8 s, 322.6 s, 308.0 s, plus one invalid run (machine slept). All produced
**identical output** (per-part row-hash checksum, 32/32 parts). On a laptop that is also
running other apps, expect ±20%.

---

## 6. Open items

- Re-time on AC power with other apps closed (this run: battery, 6.5 GB of swap in use).
- Output is 38 GB with Snappy; ZSTD would shrink it at some CPU cost (not measured).
- The data still has the generator's off-by-one. Regenerating would remove the 27
  cross-part duplicates and the 9 duplicate reference ids.
