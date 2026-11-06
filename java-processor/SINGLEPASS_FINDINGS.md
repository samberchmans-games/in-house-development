# Single-pass Java join: from "never finishes" to 12.6 minutes

Follow-up to BLOG_NOTES.md. These sections pick up where §6–§7 left off: the plain-Java
single-pass join that reads 800M claims once, looks each one up in two 250M-row reference
tables held in memory, and writes Parquet.

Machine: MacBook Pro, Apple Silicon, 10 cores, 32 GB RAM. Java 11, G1 GC.

---

## 14. Final result (full data)

```
java -Xms15g -Xmx15g -XX:+AlwaysPreTouch -XX:+UseG1GC -jar target/claims-processor-1.0.0.jar \
  singlepass <data> <out-parquet> 250000000 9
```

| Phase | Time | Notes |
|---|---|---|
| Load 250M members | 35.7 s | parallel, one task per part file |
| Load 250M providers | 37.7 s | |
| Join + write 800M claims | 680.2 s | **1.18M rows/s**, 9 threads |
| **Total** | **753.7 s (12.6 min)** | |

| | |
|---|---|
| Rows read | 800,000,032 |
| Rows written | **748,953,178** |
| Dropped: null/UNKNOWN claim_id | 3,999,935 |
| Dropped: null/UNKNOWN/unparseable member or provider key | 47,046,919 |
| Dropped: valid key but no matching member / provider | **0 / 0** |
| Heap with both tables loaded | **11.4 GiB** |
| Peak RSS | 16.8 GB |
| CPU | 5,657 s user over 755 s wall ≈ 7.5 cores busy |
| Output | 32 Parquet files (Snappy), **37 GB** |
| Temp disk | **0** |

All 52M dropped rows are from the dirt baked into the generator (§2). Every claim with a
clean key found its member and its provider.

---

## 15. Bug #4: the job that was "running" but did nothing

The first full run sat for 9 minutes in the join phase with **0 rows processed**. Four
separate problems were stacked on top of each other.

### 15a. Two hash tables that could never fit

`OnHeapJoinTable` sized for 250M entries rounds its slot count up to a power of two, 2²⁹,
and keeps **two** `long[]` arrays per table (keys + payload refs):

| | Index | Payload ("a\|b\|c" text) | Total |
|---|---|---|---|
| members | 8 GiB | 250M × 23.4 B = 5.5 GiB | 13.5 GiB |
| providers | 8 GiB | 250M × 58.8 B = 13.7 GiB | 21.7 GiB |
| **both** | | | **≈ 35 GiB** |

35 GiB doesn't fit a 30 GiB heap, never mind a 32 GB laptop. The code comment claimed
"~31 GB" and nobody had checked it against the real data.

**Fix: the id *is* the slot.** Ids are dense (`MBR000000001` … `MBR250000000`), so a hash
table is unnecessary: one `long[maxId+1]` per table, indexed directly. No keys array, no
hashing, no probing. Index: 16 GiB → **3.8 GiB**. (The rare id above `maxId` goes to a
small overflow map instead of failing.)

> Lesson: before writing a hash table, ask whether your keys are already an array index.

### 15b. A 30 GB heap on a 32 GB Mac is a swap file

With the index fixed, the tables used 23 GiB and the join *still* stalled:

- RSS was only 6.8 GB; 17 of 18 GB of swap was in use
- GC was **not** the problem (7.9 s total)
- worker threads used 81 s of CPU in 540 s of wall time

A join does a **random** lookup into ~19 GiB of payload for every claim. Once macOS has
pushed that memory to swap or the memory compressor, nearly every lookup is a page fault.

Even at `-Xmx20g` it ran at only 300K rows/s: `top` showed the JVM at **20 GB footprint,
15 GB of it compressed**. The rest of the laptop wasn't idle either: ~3 GB wired, ~5 GB of
file cache from streaming 27 GB of gzip, and ~8 GB across browser/IDE/apps. That left
roughly 14–16 GB of real RAM for Java, and G1 happily grows into whatever `-Xmx` allows.

> Lesson: the limit that matters is not `-Xmx`, it's **how much of your random-access
> working set the OS will keep resident**. On a 32 GB laptop that's well under 20 GB.

### 15c. Shrink the data, not the heap: dictionary + pattern encoding

The provider payload was the big one (13.7 GiB), and it was mostly redundant. Profiling
the real data:

| Column | Observation | Encoding |
|---|---|---|
| specialty | 20 distinct values | 1-byte dictionary code |
| network_status | 3 distinct values | 1-byte dictionary code |
| npi | always exactly 10 digits | 40-bit integer (5 B instead of 10) |
| provider_name | 95.8% are `<LAST> <SPECIALTY> CLINIC` | store only `<LAST>`, rebuild on lookup |
| gender / plan_type | 3 / 5 values | 1-byte dictionary codes |
| risk_score | `d.dd` | 2-byte integer hundredths |

Rows that don't match the expected shape (empty name, odd npi, full dictionary) are stored
verbatim behind a 1-byte tag, so the encoding is lossless for **every** input, not just the
common case.

| | Before | After |
|---|---|---|
| provider payload | 13.7 GiB | **3.4 GiB** (~15 B/row) |
| member payload | 5.4 GiB | **4.1 GiB** |
| heap with both tables | 23.0 GiB | **11.4 GiB** |

This is the same idea Parquet and DuckDB use internally (dictionary encoding, bit-packing),
done by hand for one join.

Then size the heap to the data, not the machine: `-Xms15g -Xmx15g -XX:+AlwaysPreTouch`.
CPU went from 214% to **705%**, compressed JVM memory from 15 GB to **179 MB**, and
throughput from 300K to **1.18M rows/s**.

| Run | Heap | Live data | Join rate | Outcome |
|---|---|---|---|---|
| original | 30g | ~35 GiB needed | — | can't fit |
| dense index | 26g | 23.0 GiB | ~0 rows/s | stalled on swap, killed |
| + compact encoding | 20g | 11.4 GiB | 300K rows/s | heap compressed by macOS, killed |
| + right-sized heap | **15g** | 11.4 GiB | **1.18M rows/s** | **done in 12.6 min** |

### 15d. Debug logging nobody asked for

The fat jar bundles Logback (pulled in transitively by Hadoop) with no `logback.xml`.
Logback's default is **DEBUG to stdout**, and parquet-mr logs several lines for every page
and column chunk it writes.

On a 25M-claim sample, join+write went from **105 s → 19 s** (5.5×) just by adding a
`logback.xml` with root level `WARN`. Most of that is the logging; the rest is reusing one
Avro record instead of allocating one per row.

> Lesson: check what your dependencies log by default before benchmarking anything.

### 15e. Smaller robustness bugs found along the way

- **The JVM never exits on failure.** If any chunk threw, `main` rethrew from `f.get()`
  without shutting down the thread pool. Its non-daemon threads kept the JVM alive
  forever, so the failure looked exactly like a slow job. Fixed with `shutdownNow()` in a
  `finally` block.
- **Failed chunks left valid-looking Parquet.** try-with-resources closed the writer on the
  exception path, which writes a proper footer, so a half-written part looked complete.
  Parts are now written as `.inprogress` and renamed only on success.
- **No progress output.** A 12-minute job that prints nothing can't be told apart from a
  hung one. The job now prints rows / written / heap every 30 s; that's how 15a–15b were
  found.
- **Silent payload truncation.** The payload length was stored as `len & 0xFF`, so a
  payload over 255 bytes would silently corrupt the table. It now throws instead.
- **Drop reasons are counted.** Inner joins drop rows quietly; the job now reports why
  (the table in §14).

---

## 16. Bug #5: the generator's off-by-one (a cousin of Bug #1)

`generate_series(start, end)` in DuckDB is **inclusive on both ends**, but the generator
treated it as `[start, end)` (its own docstring says so). Every part therefore has one
extra row, and **adjacent parts share their boundary id with different attributes**:

```
member_enrichment/part-00000:  MBR025000001,SUSAN,...
member_enrichment/part-00001:  MBR025000001,AHMED,...
```

That explains the odd row counts in §2: 250,000,010 = 10 × 25,000,001, and 800,000,032 =
32 × 25,000,001. It produces 9 duplicated member ids and 9 duplicated provider ids, plus one
id (250,000,001) beyond the intended range.

Why it matters:
- **A SQL join fans out on these** (2 output rows per claim referencing a duplicated id),
  the same failure mode as Bug #1, just 25 million times smaller.
- **A parallel loader becomes non-deterministic**: "last write wins" depends on thread
  timing. The Java loader now resolves duplicates deterministically, with the lowest part
  index winning (the same result as a sequential load), and reports `duplicate ids=9`.

Fix: `range(start, end)`, which is end-exclusive (applied to the generator; data not yet
regenerated).

> Lesson (again): when row counts end in a suspicious `…010` or `…032`, look at the
> boundaries.

---

## 17. Verification

- On 1-part and 2-part samples, the new code's output was compared with the original
  implementation using DuckDB `EXCEPT ALL` in both directions: **0 differing rows**,
  including null names and specialties and the duplicated boundary ids.
- Full run: 748,953,178 rows in DuckDB = the job's own count; 0 claims with a valid key
  failed to find their member or provider.

---

## 18. Memory profile of the final run

Machine: Apple **M1 Max** (8 performance + 2 efficiency cores), 32 GB.
JVM: `-Xms15g -Xmx15g -XX:+AlwaysPreTouch -XX:+UseG1GC`, 9 worker threads.

**What lives in the heap (measured by the job):**

| Structure | Size | Notes |
|---|---|---|
| members index (`long[250M+1]`) | 1.9 GiB | 8 B per id, direct-indexed |
| providers index (`long[250M+1]`) | 1.9 GiB | |
| members payload segments | 4.2 GiB | 4.1 GiB used, ~17 B/row |
| providers payload segments | 3.4 GiB | ~15 B/row |
| **Reference tables total** | **11.4 GiB** | identical after load and at end: no growth |
| Join working memory (9 Parquet writers, row buffers, young gen) | ~1–2 GiB | heap stayed at 12–13 / 15 GiB throughout |

**Heap over time:**

| t | Phase | Heap used |
|---|---|---|
| 30 s | loading members | 6 GiB |
| 60 s | loading providers | 11 GiB |
| 73 s | load done (after GC) | 11.4 GiB |
| 90–750 s | join+write | 12–13 GiB, flat |

**Process / OS view:**

| Metric | Value |
|---|---|
| Peak RSS | 16.8 GB (15 GB pre-touched heap + metaspace, threads, direct buffers) |
| Peak memory footprint | 16.9 GB |
| Swaps | **0** |
| Hard page faults | **1,050** |
| JVM memory compressed by macOS | ~179 MB (vs 15 GB in the stalled 20g run) |
| GC | G1, ~8 s total GC time in the earlier runs; not a factor |
| CPU | 5,657 s user + 161 s sys over 755 s ≈ 7.7 cores busy |
| Temp disk | 0 |

**Rule of thumb:** heap ≈ (8 B × max id + encoded payload) per table + ~2–3 GiB working
memory. For this data that's 11.4 + ~3 ≈ 15 GiB, and the machine needs ~17 GB of RAM it
can keep resident for the JVM.

---

## 19. Same config on EMR: how long would it take? (estimate, not measured)

There are two different questions here: running **this jar** on EMR hardware, or rewriting
the job in **Spark** on EMR.

### Matching the hardware

The job needs ~17 GB resident and scales with **physical** cores.

| | Cores | RAM | Fit |
|---|---|---|---|
| Laptop (M1 Max) | 8 P + 2 E | 32 GB | baseline |
| **m7g.2xlarge** (Graviton3) | 8 vCPU = **8 physical cores** | 32 GiB | closest match |
| m6i/r6i.2xlarge (Intel) | 8 vCPU = **4 cores × 2 hyperthreads** | 32 / 64 GiB | ~half the real cores |
| m7g.4xlarge | 16 physical cores | 64 GiB | the "scale up" option |

> Watch the vCPU trap: on x86, 8 vCPU is 4 physical cores with hyperthreading. On
> Graviton, 1 vCPU is 1 core.

Per-core speed: an M1 Firestorm core is roughly **1.5× a Graviton3 core** on single-threaded
integer work like gzip inflate and string parsing (rough, from public single-core benchmarks).
Re-measure before quoting this.

### Option A: run this jar as an EMR step (single node)

Estimated from the measured CPU time (5,657 core-seconds at M1 speed) divided by the cores
available and their relative speed:

| Instance | Join+write | + load | + copy 27 GB in from S3 | + cluster start | **Wall clock** |
|---|---|---|---|---|---|
| Laptop (measured) | 11.3 min | 1.2 min | — | — | **12.6 min** |
| m7g.2xlarge | ~17–20 min | ~2 min | ~2 min | 5–10 min | **~26–34 min** (~21–24 min of job time) |
| m7g.4xlarge | ~9–11 min | ~1.5 min | ~1.5 min | 5–10 min | **~17–24 min** (~12–14 min of job time) |
| m6i.2xlarge | ~25–30 min | ~3 min | ~2 min | 5–10 min | **~35–45 min** |

Notes:
- The jar reads local files, so stage the input onto local NVMe or EBS first
  (`aws s3 cp --recursive`, or s3-dist-cp). Writing 37 GB of output back to S3 adds a few
  minutes unless the output is written to S3 directly.
- **EBS throughput:** the job averages ~40 MB/s read + ~55 MB/s write. gp3's default
  125 MB/s baseline is tight once the S3 copy overlaps it; provision 250+ MB/s, or use an
  instance-store variant (e.g. m7gd).
- **YARN memory:** on a 32 GiB node, EMR leaves roughly 24 GiB to YARN containers. A 15 GiB
  heap fits, but run it as a plain `command-runner.jar` step on a single-node cluster
  rather than inside a Spark executor.
- With 16 threads, 32 chunks split into 2 even waves, so the laptop's tail imbalance
  (32 chunks on 9 threads) disappears.

### Option B: rewrite in Spark on EMR (same 8 cores / 32 GB)

The single-pass trick doesn't carry over. Two 250M-row tables are far over Spark's 8 GB
broadcast limit, so Spark would use two **sort-merge joins**, which means shuffling all
800M claims twice plus both reference tables, and spilling to disk. That is the same
materialization problem DuckDB hit in §6, made worse by shuffle overhead.

Rough estimate on 8 cores / 32 GB: **45–90 min**, with 100+ GB of shuffle spill on local
disk. Spark only beats the single-pass jar when you add more nodes than this, e.g. a
5–10 node cluster could bring it to ~10–15 min. That trades cost for elasticity (§12).

### Cost

Use §12's formula with the new times: **cost ≈ hourly rate (EC2 + EMR uplift) × wall-clock
hours**, including cluster startup. At ~0.4–0.6 hours per run on a 2xlarge-class node, a run
costs well under $1. As §12 already concluded, fixed overhead (startup, staging data, EKS
fees) dominates the compute cost. Re-check the per-hour prices for your region before
publishing.

---

## Corrections to BLOG_NOTES.md

| Section | Notes say | Measured |
|---|---|---|
| §2 | member/provider "250M ✅ unique" | 250,000,001 distinct ids, 9 duplicated per table (§16) |
| §10 | plain Java "needs ~20 GB heap" | 11.4 GiB live, run with a 15 GB heap |
| §12 | laptop join "~25–30 min" | **12.6 min total** (680 s join+write); redo the AWS cost math with ~13 min |
| §13 | Parquet output "~15–20 GB" | **37 GB** (Snappy). ZSTD would shrink it if size matters |
| §13 | join "~8–12 min" with Parquet output | 11.3 min join+write, with CSV.gz input |
| §8 | plain Java 68.5 s on the 25M sample | 51.7 s on the sample after these fixes; DuckDB's 27 s is still faster at that size |

## Still on the table

- **Tail imbalance**: 32 chunks across 9 threads means the last wave runs 5 chunks on 9
  cores. Splitting chunks or using 8 threads (4 even waves) would trim the end.
- **Gzip input** is now the main per-thread cost; Parquet or zstd input would help (§13).
- **Re-run DuckDB with the generator fix** so both engines join the same, duplicate-free
  data.
