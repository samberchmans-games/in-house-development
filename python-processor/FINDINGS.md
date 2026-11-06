# Python claims processor: measured results

Companion to `APPROACHES.md` (the plan, written before coding) and
`java-processor/SINGLEPASS_FINDINGS.md` (the Java run this is compared with).

Machine: Apple M1 Max (8P + 2E cores), 32 GB. Python 3.13.5, pyarrow 26.0.1,
numpy 2.5.4, duckdb 2.0.0, polars 2.1.0. Same input files as the Java run (not regenerated).

---

## 1. Headline: Python joins the full 800M claims in 6.5 minutes

```
python -m claims arrow <data> <out> --threads 8
```

| | Java `singlepass` | Python P4 (Arrow + NumPy) |
|---|---|---|
| Load 250M members | 35.7 s | **23.8 s** |
| Load 250M providers | 37.7 s | **24.6 s** |
| Join + write 800M claims | 680.2 s | **341.4 s** |
| **Total (job timer)** | **753.7 s (12.6 min)** | **389.8 s (6.5 min)** |
| Wall clock incl. process start/exit | — | 417.1 s |
| Throughput (join+write) | 1.18M rows/s | **2.34M rows/s** |
| Reference tables in memory | 11.4 GiB | **6.05 GiB** |
| Peak RSS | 16.8 GB | **12.1 GiB** (13.0 GB) |
| CPU (user + sys) | 5,657 + 161 s | **2,365 + 380 s** |
| Cores busy | ~7.7 | ~7.0 |
| Output | 32 Parquet files, 37 GB | 32 Parquet files, 38 GB (Snappy) |
| Temp disk | 0 | 0 |

**Counters: identical to Java, every one.**

| | Java | Python P4 |
|---|---|---|
| Rows read | 800,000,032 | 800,000,032 |
| Rows written | 748,953,178 | **748,953,178** |
| Dropped: null/UNKNOWN claim_id | 3,999,935 | 3,999,935 |
| Dropped: bad member/provider key | 47,046,919 | 47,046,919 |
| Dropped: member / provider not found | 0 / 0 | 0 / 0 |
| Duplicate reference ids (lowest part wins) | 9 / 9 | 9 / 9 |

The 27 s between the job timer and the wall clock is process start-up plus interpreter
shutdown; it was not investigated further.

### Why Python is twice as fast as Java here

Both jobs kept about the same number of cores busy (~7). Python simply needed **half as
much total CPU work** to do the same job:

| | Java | Python P4 |
|---|---|---|
| CPU time summed over all cores (user + sys) | 5,818 s (≈ 97 core-minutes) | 2,745 s (≈ 46 core-minutes) |
| ÷ cores busy on average | ÷ 7.7 | ÷ 7.0 |
| = elapsed (wall-clock) time | ≈ 755 s (12.6 min) | ≈ 390 s (6.5 min) |

So the difference is per-row cost, not parallelism:

- **Python never touches a row.** Every per-row operation (gzip, CSV parsing, trim, upper,
  regex, cast, `take`, Parquet encoding) runs inside Arrow's C++ kernels on ~170K-row
  batches. Python code runs once per batch, about 4,700 times per claims part.
- Java's hot loop allocates per row: `BufferedReader.readLine()` → `String.split(",")` →
  a `String[]` and 12 `String`s → cleansing creates more `String`s → an Avro record →
  parquet-mr. That is a lot of short-lived objects and GC work, even with the record reused.
- Arrow's CSV parser is a vectorized, multi-threaded parser; `split` is not.

> Lesson: in Python, performance comes from **keeping the rows out of Python**. P1 (pure
> Python) runs at 434K rows/s on one core; P4 runs at 2.34M rows/s on the same machine
> using the same algorithm, because Arrow does the work below the interpreter.

---

## 2. Memory: smaller than Java's hand-written codecs

The same two ideas as Java §15, using Arrow and NumPy instead of hand-written byte arenas:

| Structure | Java | Python P4 |
|---|---|---|
| Index (id → row) | `long[250M+1]` = 1.9 GiB per table | `int32[250M+2]` = **0.93 GiB** per table |
| Member payload | 4.1 GiB (~17 B/row, codec) | **1.40 GiB (6.0 B/row)** |
| Provider payload | 3.4 GiB (~15 B/row, codec) | **2.79 GiB (12.0 B/row)** |
| **Both tables** | **11.4 GiB** | **6.05 GiB** |

How the columns are stored:

| Column(s) | Distinct values | Stored as |
|---|---|---|
| first_name, last_name | 28, 28 | int8 dictionary code |
| gender, plan_type | 3, 5 | int8 dictionary code |
| risk_score (`d.dd`) | — | uint16 hundredths (0xFFFF = null) |
| provider_name | 560 | int16 dictionary code (whole name, no pattern parsing needed) |
| specialty, network_status | 20, 3 | int8 dictionary code |
| npi (10 digits, never leading 0) | unique | uint64 |

Java's codec stored the provider name as its `<LAST>` prefix and rebuilt
`<LAST> <SPECIALTY> CLINIC` on lookup. With only 560 distinct full names, a plain
dictionary over the whole name is simpler and smaller. Each encoder **verifies** its
assumption on load (d.dd scores round-trip exactly; NPIs round-trip through uint64;
under 32,768 distinct values) and fails loudly instead of corrupting data.

The memory profile over the run (from the job's own 30 s progress line):

| t | Phase | RSS |
|---|---|---|
| 48 s | load done | 10.6 GiB |
| 120 s | join+write, 154M rows | 9.7 GiB |
| 240 s | join+write, 420M rows | 8.1 GiB |
| 360 s | join+write, 716M rows | 8.6 GiB |
| end | | 10.8 GiB (peak 12.1 GiB) |

RSS after load (10.6 GiB) is well above the 6.05 GiB of tables: freed loader buffers are
kept by the allocators and handed back gradually. The macOS memory-compression monitor
for this run sampled the wrong PID (the `/usr/bin/time` wrapper), so compression was not
measured. The steady 7 cores busy and 2.34M rows/s rule out the swap/compression stall of
Java §15b.

### Bug found on the way: the loader's transient memory

The first version read each 25M-row reference part whole (`read_csv`) before encoding it.
That's ~1.5 GB of Arrow strings per part, times 8 threads. On the 2-part sample:

| Loader | Peak RSS | Load time |
|---|---|---|
| `read_csv` whole part, then encode | 8.2 GiB | 24.0 s |
| `open_csv` stream, encode each 16 MB batch | **5.7 GiB** | **17.6 s** |

### Re-runs: is it repeatable?

The full job was run three times on the same input:

| Run | Code | Job time | Join+write | Peak RSS | Counters = Java | Output vs run 1 |
|---|---|---|---|---|---|---|
| 1 | as committed | 389.8 s | 341.4 s (2.34M rows/s) | 12.1 GiB | ✅ | — |
| 2 | same | **322.6 s (5.4 min)** | 274.4 s (2.92M rows/s) | 16.6 GiB | ✅ | **identical** (per-part row-hash checksum, 32/32 parts) |
| 3 | + loader buffer fix | ~~491.7 s~~ invalid | — | 13.1 GiB | ✅ | **identical** (32/32 parts) |

- **Deterministic:** all three runs wrote the same 748,953,178 rows, part for part, and no
  `.inprogress` files were left behind.
- **Run-to-run timing varies ~20%** (5.4–6.5 min) on a laptop that is also running other
  apps. The swap file was already 6.7–7.9 GB full before each run started.
- **Run 3's timing is invalid: the Mac went to sleep twice during it** (13:04–13:19 and
  13:24–13:39 in `pmset -g log`; the second was a *"Dark Wake Thermal Emergency"*).
  Python's `perf_counter` doesn't advance during sleep, so the job reported 491.7 s while the
  process took 45 minutes of wall-clock time. The harness now also measures wall-clock time
  and prints a warning when they differ. Benchmark runs need the lid open on AC power, with
  `caffeinate -dims` keeping the machine awake.

### Memory: what macOS actually saw

Run 2 sampled the macOS process footprint (`top` MEM / CMPRS) correctly for the first time:
**~20 GB footprint with 10–11 GB of it compressed**, from the end of the load onward,
although the tables are only 6.05 GiB. Throughput didn't suffer (run 2 was the fastest),
so the compressed pages were memory nobody touched again, not the tables. It is still more
than the design intended, so it was traced on the members load alone (2.33 GiB of real data):

| Step (members only) | RSS | Note |
|---|---|---|
| original loader | **12.6 GiB** | Arrow pool holds 0 bytes; swapping Arrow's allocator to the system allocator: still 10.7 GiB |
| + one preallocated buffer per part (`_Buf`) | **8.8 GiB** | compressed 1.3 MB instead of GBs |
| timeline: after part tasks | 2.0 GiB | as designed |
| timeline: + dense index + final concatenation | 6.1 GiB | short-lived NumPy temporaries macOS keeps cached after free |

Cause: the loader kept ~1,500 small per-batch NumPy arrays per part alive until a final
`np.concatenate`. Interleaved with each batch's short-lived temporaries, that fragments the
macOS allocator's medium-size region, so freed memory can't be handed back. Writing each
batch straight into one preallocated array per part fixes most of it.

Effect: sample peak RSS **4.3 → 2.6 GiB**. On the full run (run 3), RSS after load went 10.8 →
9.3 GiB, but footprint during the join was still 15–17 GB with 5–7 GB compressed. Run 3
happened during sleep/dark-wake cycles under system memory pressure, so this needs a clean
re-run before drawing conclusions. The join phase's own working memory (8 threads, each
buffering up to 1M output rows plus a Parquet row group) hasn't been measured separately yet.

---

## 3. Bug #6: Java's CSV split mis-reads 4% of claims (found by the parity check)

About 4% of raw claim lines have a quoted amount:

```
CLM00000000030,MBR107872988,PRV217792746,2024-11-12,99204,E03.9,"$4,066.19",2679.19,paid,Illinois,04/17/1973,SRC_D
```

Java's `Csv` splits on every comma (`line.split(",", -1)`) without handling quotes, so this
line becomes 13 fields and everything from `billed_amount` on shifts one place:

| Column | Java reads | Correct |
|---|---|---|
| billed_amount | `"$4` → null | 4066.19 |
| paid_amount | `066.19"` → null | 2679.19 |
| claim_status | `2679.19` → null | PAID |
| state | `paid` → null | IL |
| member_dob | `Illinois` → null | null (not ISO) |
| source_system | `04/17/1973` | SRC_D |

Row counts are unaffected because the ids come first, which is why nobody noticed.
The Java side accepts rows with ≥ 12 fields, so it never flagged them as malformed.

Every Python approach uses a real CSV parser. `python -m claims verify` compares outputs
row by row and puts every differing row into one of two classes:

| Dataset | Rows | Differing | Quoted-amount rows | Unexplained |
|---|---|---|---|---|
| sample 1 (1 part) | 234,688 | 9,386 (4.0%) | 9,386 | **0** |
| sample 2 (2 parts) | 1,871,401 | 75,112 (4.0%) | 75,112 | **0** |
| full (32 parts) | 748,953,178 | 30,081,528 (4.0%) | 30,081,528 | **0** |

The full check compared each Java part file with the matching Python part file
(`python -m claims verify <java-out> <py-out> <data>`, 18 min). All 32 parts: 0 unexplained.

So the Python output is the correct one for those rows, and the Java full output has
**30,081,528 rows** with shifted amounts, status, state, DOB and source system. This adds to
BLOG_NOTES §20: the Java numbers for `billed_amount = null` etc. are inflated by these rows.

> Lesson: "the row counts match" is not verification. A column-level diff found what three
> engines agreeing on 748,953,178 rows did not.

---

## 4. All four approaches on the samples

Sample 1 = part-00000 of each table (25M claims × 25M members × 25M providers).

| Approach | Total | Load | Join+write | Peak RSS | Cores busy | Output vs Java |
|---|---|---|---|---|---|---|
| Java `singlepass` | 55.5 s | 33.4 s | 22.1 s | 6.1 GiB (6g heap) | ~1.0 | baseline |
| P1 pure Python (interned) | 178.2 s | 120.6 s | 57.6 s | 10.9 GiB | 1.0 | ✅ |
| P2 DuckDB | 24.4 s | — | — | 7.9 GiB | 4.2 | ✅ |
| P3 Polars | **17.4 s** | — | — | 10.3 GiB | 5.6 | ✅ |
| P4 Arrow + NumPy | 37.7 s | 22.1 s | 15.6 s | **4.3 GiB** | 1.5 | ✅, all counters equal |

✅ = identical to Java except the quoted-amount rows (§3), 0 unexplained.
P1, P2, P3 and P4 outputs are **identical to each other** (verify: IDENTICAL).

Sample 1 has a single claims file, so P4 and Java (one part per thread) run the claims
on one thread. Polars and DuckDB split a single file across threads, which is most of
their lead here. It doesn't carry over to the full data (32 parts).

### P1: what Python objects cost

Measured on 2M rows per table, then on the whole sample:

| | Members | Providers |
|---|---|---|
| naive (`str` per field) | 388 B/entry | 414 B/entry |
| `sys.intern` repeated values | 196 B/entry | 298 B/entry |
| whole sample, interned (25M each) | 220 B/entry | 221 B/entry |
| P4 for comparison (index + columns) | **10 B/entry** | **16 B/entry** |

At 500M reference rows, interned P1 needs ~100 GiB: it can't run on a 32 GB machine.
It is the Python version of Java Bug #2 (`HashMap<String,String[]>` at ~200 B/entry).

---

## 5. DuckDB and Polars on the full data

_Pending: time-boxed attempts (~45 min, 150 GB temp-disk cap)._

---

## 6. Predictions vs measured (from APPROACHES.md §6)

| Approach | Predicted sample | Measured | Predicted full | Measured full |
|---|---|---|---|---|
| P1 | 3–6 min | **3.0 min** | won't fit (75–125 GB) | won't fit (~100 GiB at 220 B/entry) |
| P2 DuckDB | 20–30 s | **24.4 s** | likely spills like Java §6 | _pending_ |
| P3 Polars | 15–30 s | **17.4 s** | uncertain | _pending_ |
| P4 | 20–40 s | **37.7 s** | 12–20 min, ~10 GiB tables | **6.5 min, 6.05 GiB** (beat the prediction) |
