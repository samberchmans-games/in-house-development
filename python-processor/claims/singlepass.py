"""Single-pass claims join: 800M claims x 250M members x 250M providers on one machine.

Both reference tables are held in memory as compact NumPy columns behind a dense
id -> row index; the claims are streamed once, cleansed with Arrow compute kernels,
joined with vectorized lookups, de-duplicated and written straight to Parquet.
No intermediate result is ever materialized and no temp disk is used.

Reference table layout (~6 + 12 bytes per member / provider row):
  index           int32[maxId+2]   row of each id, -1 = absent (last slot: sentinel)
  string columns  int8/int16 codes into one shared dictionary (width declared per column)
  risk_score      uint16 hundredths, 0xFFFF = null
  npi             uint64, 0 = null

Loading: every part writes straight into its own fixed slot range of the final arrays
(part i owns rows [i*CAP, i*CAP + n_i)). Nothing is concatenated afterwards; the unused
tail of each range is never touched, so it never becomes resident memory.

De-duplication: a claim id is written once, at its first occurrence in file order
(part order, then row order). Within a part a bitmap of seen ids does it; after all
parts finish, ids that also appear in an earlier part are removed from the later
part's file (rare: the generator only duplicates adjacent rows).

Parallelism: a thread pool, one task per part file. Arrow releases the GIL while it
decompresses, parses, runs kernels and writes Parquet.
"""

import datetime
import os
import threading
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from . import cleanse as c
from .common import (CLAIM_COLS, CLAIMS, GIB, MEMBERS, OUTPUT_FIELDS, PROVIDERS, Run, parts,
                     prepare_out_dir, rss_gib)

BLOCK_SIZE = 16 << 20          # decompressed CSV bytes per Arrow batch (~150K rows)
ROW_GROUP_ROWS = 1_000_000     # output rows buffered per Parquet row group
FILE_ROWS = 4_000_000          # output rows per Parquet file (~200 MB): small enough to rewrite
CSV_THREADS = False            # one thread per part is enough; Arrow's own CSV threads add
                               # read-ahead memory (join footprint 14 -> 13 GB) and were not faster
PART_CAPACITY = 1 << 25        # max rows per reference part (33.5M; parts here are 25M)
NULL_CODE = -1
RISK_NULL = 0xFFFF
CLAIM_KEY_RE = r"^CLM(?P<n>[0-9]{11})$"   # canonical claim ids -> bitmap; others -> a set
SEEN_BLOCK_BITS = 20                      # bitmap blocks of 1M claim ids


def _convert(columns):
    return pcsv.ConvertOptions(
        column_types={col: pa.string() for col in columns}, include_columns=columns,
        strings_can_be_null=True, quoted_strings_can_be_null=True, null_values=[""])


def _np(a, fill):
    return pc.fill_null(a, fill).to_numpy(zero_copy_only=False)


# ---------------------------------------------------------------- column encoders
# Each encoder states its assumption about the data and fails loudly if it is wrong.

class DictColumn:
    """Low-cardinality strings as integer codes into one shared dictionary."""

    def __init__(self, src, dtype=np.int8):
        self.src, self.dtype = src, np.dtype(dtype)
        self.limit = np.iinfo(self.dtype).max
        self.values, self._lookup, self._lock = [], {}, threading.Lock()

    def _code(self, v):
        code = self._lookup.get(v)
        if code is None:
            code = len(self.values)
            if code > self.limit:
                raise ValueError(f"{self.src}: more than {self.limit + 1} distinct values; "
                                 f"widen its dtype (now {self.dtype})")
            self._lookup[v] = code
            self.values.append(v)
        return code

    def encode(self, arr):
        d = arr.dictionary_encode()
        local = d.dictionary.to_pylist()
        with self._lock:   # codes are global: map this batch's dictionary onto them
            remap = np.array([self._code(v) for v in local] + [NULL_CODE], dtype=self.dtype)
        return remap[_np(d.indices, len(local))]

    def finish(self, codes):
        self.codes = codes
        self.dictionary = pa.array(self.values, pa.string())
        self.type = pa.dictionary(pa.from_numpy_dtype(self.dtype), pa.string())

    def take(self, rows):
        k = self.codes[rows]
        return pa.DictionaryArray.from_arrays(pa.array(k, mask=k < 0), self.dictionary)


class RiskColumn:
    """d.dd scores as uint16 hundredths; h / 100.0 is exactly the parsed double."""

    dtype, type = np.dtype(np.uint16), pa.float64()

    def __init__(self, src):
        self.src = src

    def encode(self, arr):
        f = _np(pc.cast(arr, pa.float64()), np.nan)
        nan = np.isnan(f)
        h = np.rint(np.where(nan, 0, f) * 100)
        if not np.all(nan | ((h / 100.0 == f) & (h >= 0) & (h < RISK_NULL))):
            raise ValueError(f"{self.src}: values that are not d.dd hundredths in [0, 655.34]")
        return np.where(nan, RISK_NULL, h).astype(self.dtype)

    def finish(self, codes):
        self.codes = codes

    def take(self, rows):
        h = self.codes[rows]
        return pa.array(h / 100.0, mask=h == RISK_NULL)


class NpiColumn:
    """NPIs as uint64 (they never start with 0, so the text round-trips)."""

    dtype, type = np.dtype(np.uint64), pa.string()

    def __init__(self, src):
        self.src = src

    def encode(self, arr):
        ok = pc.match_substring_regex(arr, r"^[1-9][0-9]{0,18}$")
        if pc.any(pc.and_(pc.is_valid(arr), pc.invert(ok))).as_py():
            raise ValueError(f"{self.src}: values that do not round-trip through uint64")
        return _np(pc.cast(arr, pa.uint64()), 0)

    def finish(self, codes):
        self.codes = codes

    def take(self, rows):
        v = self.codes[rows]
        return pc.cast(pa.array(v, mask=v == 0), pa.string())


# ---------------------------------------------------------------- reference table

class RefTable:
    def __init__(self, name, key_col, columns, capacity=PART_CAPACITY):
        self.name, self.key_col, self.columns, self.cap = name, key_col, columns, capacity

    def load(self, run, paths, threads):
        if len(paths) * self.cap >= 2**31:
            raise ValueError("too many parts for int32 row numbers; widen the index")
        total = len(paths) * self.cap
        # Final arrays, allocated once. Part i fills rows [i*cap, i*cap + n_i); the rest of
        # its range is never written, so those pages never become resident.
        keys = np.empty(total, np.int32)
        arrays = {n: np.empty(total, e.dtype) for n, e in self.columns.items()}
        srcs = [self.key_col] + [e.src for e in self.columns.values()]
        sizes, max_ids = [0] * len(paths), [-1] * len(paths)

        def task(i):
            reader = pcsv.open_csv(paths[i], read_options=pcsv.ReadOptions(block_size=BLOCK_SIZE),
                                   convert_options=_convert(srcs))
            base, n = i * self.cap, 0
            for b in reader:
                m = b.num_rows
                if n + m > self.cap:
                    raise ValueError(f"{paths[i]}: more than {self.cap:,} rows; raise PART_CAPACITY")
                k = _np(c.parse_id(b.column(self.key_col)), -1)
                if k.max(initial=-1) >= 2**31 - 2:
                    raise ValueError(f"{paths[i]}: ids beyond int32; widen the index")
                keys[base + n: base + n + m] = k
                max_ids[i] = max(max_ids[i], int(k.max(initial=-1)))
                for name, e in self.columns.items():
                    arrays[name][base + n: base + n + m] = e.encode(b.column(e.src))
                n += m
                run.stats.add(loaded=m)
            sizes[i] = n

        run_all(threads, [lambda i=i: task(i) for i in range(len(paths))])

        # Dense index. Parts are applied last-to-first so the LOWEST part wins on a
        # duplicate id (the generator repeats each part's boundary id; findings section 16).
        self.max_id = max(max_ids)
        sentinel = self.max_id + 1                       # invalid keys land here, then reset
        self.index = np.full(self.max_id + 2, -1, dtype=np.int32)
        self.duplicates = 0
        for i in reversed(range(len(paths))):
            k = keys[i * self.cap: i * self.cap + sizes[i]]
            k[k < 0] = sentinel
            prev = self.index[k]
            self.duplicates += int(np.count_nonzero(prev >= 0)) - int(np.count_nonzero(prev[k == sentinel] >= 0))
            self.index[k] = np.arange(i * self.cap, i * self.cap + sizes[i], dtype=np.int32)
        self.index[sentinel] = -1
        del keys, prev, k
        self.rows = sum(sizes)
        for name, e in self.columns.items():
            e.finish(arrays[name])
        pa.default_memory_pool().release_unused()
        return self

    def lookup(self, keys):
        rows = np.full(len(keys), -1, dtype=np.int32)
        ok = (keys >= 0) & (keys <= self.max_id)
        rows[ok] = self.index[keys[ok]]
        return rows

    def report(self, secs):
        col_bytes = sum(e.dtype.itemsize for e in self.columns.values()) * self.rows
        dicts = ", ".join(f"{n}={len(e.values)}" for n, e in self.columns.items() if hasattr(e, "values"))
        return (f"{self.name} loaded: {self.rows:,} in {secs:.1f} s  (index {self.index.nbytes / GIB:.2f} GiB, "
                f"columns {col_bytes / GIB:.2f} GiB = {col_bytes / self.rows:.1f} B/row, "
                f"duplicate ids={self.duplicates:,}, dictionary sizes: {dicts})")


def members_table(capacity=PART_CAPACITY):
    return RefTable("members", "member_id", {
        "member_first_name": DictColumn("first_name"), "member_last_name": DictColumn("last_name"),
        "member_gender": DictColumn("gender"), "member_plan_type": DictColumn("plan_type"),
        "member_risk_score": RiskColumn("risk_score")}, capacity)


def providers_table(capacity=PART_CAPACITY):
    return RefTable("providers", "provider_id", {
        "provider_name": DictColumn("provider_name", np.int16),
        "provider_specialty": DictColumn("specialty"),
        "provider_npi": NpiColumn("npi"), "provider_network_status": DictColumn("network_status")},
        capacity)


# ---------------------------------------------------------------- de-duplication

def claim_keys(cid):
    """Numeric part of canonical claim ids (CLM + 11 digits); -1 for anything else."""
    return _np(pc.cast(pc.struct_field(pc.extract_regex(cid, CLAIM_KEY_RE), [0]), pa.int64()), -1)


class SeenClaims:
    """Claim ids one part has written. Canonical ids (CLM + 11 digits) go in a sparse
    bitmap of 1M-id blocks; anything else in a set (none in this data)."""

    MASK = (1 << SEEN_BLOCK_BITS) - 1

    def __init__(self):
        self.blocks, self.other = {}, set()

    def keep_first(self, cid):
        """Marks and returns rows that are the first occurrence of their claim id."""
        return self.keep_first_keys(cid)[0]

    def keep_first_keys(self, cid):
        """(keep mask, numeric claim keys with -1 for non-canonical ids)."""
        n = len(cid)
        keep = np.zeros(n, bool)
        if n == 0:
            return keep, np.empty(0, np.int64)
        num = claim_keys(cid)
        canon = num >= 0
        idx = np.flatnonzero(canon)
        if len(idx):
            _, first = np.unique(num[idx], return_index=True)     # first in this batch
            idx = np.sort(idx[first])
            blk = num[idx] >> SEEN_BLOCK_BITS
            for b in np.unique(blk):
                at = idx[blk == b]
                bits = self.blocks.get(b)
                if bits is None:
                    bits = self.blocks[b] = np.zeros(1 << SEEN_BLOCK_BITS, bool)
                off = num[at] & self.MASK
                new = ~bits[off]
                keep[at[new]] = True
                bits[off[new]] = True
        for i in np.flatnonzero(~canon):                        # rare slow path
            s = cid[int(i)].as_py()
            if s not in self.other:
                self.other.add(s)
                keep[i] = True
        return keep, num

    def packed(self):
        return {b: np.packbits(v) for b, v in self.blocks.items()}, self.other


def cross_part_duplicates(seen_by_part):
    """For each part, the claim ids it shares with an EARLIER part (to remove from it)."""
    global_blocks, global_other, out = {}, set(), {}
    for i, (blocks, other) in enumerate(seen_by_part):
        ids, strs = [], other & global_other
        for b, bits in blocks.items():
            g = global_blocks.get(b)
            if g is None:
                global_blocks[b] = bits.copy()
                continue
            both = g & bits
            if both.any():
                ids.append((b << SEEN_BLOCK_BITS) + np.flatnonzero(np.unpackbits(both)))
            g |= bits
        global_other |= other
        if ids or strs:
            out[i] = (np.concatenate(ids) if ids else np.empty(0, np.int64), strs)
    return out


def remove_claims(path, ids, strs, schema):
    """Rewrites one output file without the given claim ids. Returns rows removed."""
    tmp = path.with_name(path.name + ".inprogress")
    removed = 0
    other = pa.array(sorted(strs), pa.string())
    with pq.ParquetWriter(tmp, schema, compression="snappy") as w:
        for b in pq.ParquetFile(path).iter_batches(batch_size=ROW_GROUP_ROWS):
            cid = b.column("claim_id")
            drop = np.isin(claim_keys(cid), ids) | _np(pc.is_in(cid, value_set=other), False)
            removed += int(drop.sum())
            w.write_table(pa.Table.from_batches([b.filter(pa.array(~drop))]).cast(schema))
    os.replace(tmp, path)
    return removed


# ---------------------------------------------------------------- claims

def output_schema(members, providers):
    types = {n: e.type for t in (members, providers) for n, e in t.columns.items()}
    return pa.schema([(n, types.get(n, typ)) for n, typ in OUTPUT_FIELDS])


def join_batch(b, members, providers, seen, schema, stats, as_of):
    """Returns (output batch or None, its numeric claim keys)."""
    cid, mid, pid = c.ident(b.column(0)), c.ident(b.column(1)), c.ident(b.column(2))
    has_claim = _np(pc.is_valid(cid), False)
    mk, pk = _np(c.parse_id(mid), -1), _np(c.parse_id(pid), -1)
    good_key = has_claim & (mk >= 0) & (pk >= 0)
    mrow, prow = members.lookup(mk), providers.lookup(pk)
    has_member = good_key & (mrow >= 0)
    joined = np.flatnonzero(has_member & (prow >= 0))
    first, keys = seen.keep_first_keys(pc.take(cid, pa.array(joined)))
    sel = joined[first]
    n, written = len(b), len(sel)
    stats.add(read=n, written=written,
              no_claim_id=n - np.count_nonzero(has_claim),
              bad_key=np.count_nonzero(has_claim) - np.count_nonzero(good_key),
              member_not_found=np.count_nonzero(good_key) - np.count_nonzero(has_member),
              provider_not_found=np.count_nonzero(has_member) - len(joined),
              duplicate_claim_id=len(joined) - written)
    if not written:
        return None, keys[first]
    idx = pa.array(sel)
    col = lambda i: pc.take(b.column(i), idx)
    claim_date = c.date(col(3))
    billed, paid = c.amount(col(6)), c.amount(col(7))
    mr, pr = mrow[sel], prow[sel]
    arrays = [
        pc.take(cid, idx), pc.take(mid, idx), pc.take(pid, idx),
        claim_date, c.cpt(col(4)), c.icd10(col(5)), billed, paid,
        c.status(col(8)), c.state(col(9)), c.dob(col(10), claim_date, as_of), col(11),
        *(e.take(mr) for e in members.columns.values()),
        *(e.take(pr) for e in providers.columns.values()),
        c.flag_negative(billed), c.flag_paid_exceeds(paid, billed),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema), keys[first]


class PartWriter:
    """Writes one claims part as files of ~FILE_ROWS rows (part-00000-000.parquet, ...).

    Each file records the range of claim keys in it, so the cross-part dedup can find
    the one file that holds a duplicate. Files are written as .inprogress and only
    renamed when the whole part succeeded."""

    def __init__(self, out_dir, stem, schema):
        self.out_dir, self.stem, self.schema = out_dir, stem, schema
        self.files, self._w, self._rows = [], None, 0     # files: [path, min_key, max_key, has_other]

    def write(self, table, keys):
        if self._w is None:
            path = self.out_dir / f"{self.stem}-{len(self.files):03d}.parquet"
            self.files.append([path, np.iinfo(np.int64).max, -1, False])
            self._w = pq.ParquetWriter(self._tmp(path), self.schema, compression="snappy")
        self._w.write_table(table)
        f = self.files[-1]
        canon = keys[keys >= 0]
        if len(canon):
            f[1], f[2] = min(f[1], int(canon.min())), max(f[2], int(canon.max()))
        f[3] = f[3] or bool((keys < 0).any())
        self._rows += table.num_rows
        if self._rows >= FILE_ROWS:
            self._close()

    def _tmp(self, path):
        return path.with_name(path.name + ".inprogress")

    def _close(self):
        if self._w is not None:
            self._w.close()
            self._w, self._rows = None, 0

    def commit(self):
        self._close()
        for f in self.files:
            os.replace(self._tmp(f[0]), f[0])

    def abort(self):
        try:
            self._close()
        finally:
            for f in self.files:
                self._tmp(f[0]).unlink(missing_ok=True)


def process_part(run, path, out_dir, members, providers, schema, as_of, abort):
    malformed = []

    def on_invalid(row):
        malformed.append(1)
        return "skip"

    reader = pcsv.open_csv(
        path, read_options=pcsv.ReadOptions(block_size=BLOCK_SIZE, use_threads=CSV_THREADS),
        parse_options=pcsv.ParseOptions(invalid_row_handler=on_invalid),
        convert_options=_convert(CLAIM_COLS))
    if reader.schema.names != CLAIM_COLS:
        raise ValueError(f"{path}: unexpected header {reader.schema.names}")
    seen = SeenClaims()
    out = PartWriter(out_dir, path.name.replace(".csv.gz", ""), schema)
    ok = False
    try:
        pending, keys, rows = [], [], 0
        for batch in reader:
            if abort.is_set():
                raise RuntimeError("aborted: another part failed")
            b, k = join_batch(batch, members, providers, seen, schema, run.stats, as_of)
            if b is not None:
                pending.append(b)
                keys.append(k)
                rows += b.num_rows
            if rows >= ROW_GROUP_ROWS:
                out.write(pa.Table.from_batches(pending), np.concatenate(keys))
                pending, keys, rows = [], [], 0
        if pending:
            out.write(pa.Table.from_batches(pending), np.concatenate(keys))
        out.commit()
        ok = True
    finally:
        run.stats.add(read=len(malformed), malformed=len(malformed))
        if not ok:
            out.abort()
    return seen.packed(), out.files


def run_all(threads, tasks, abort=None):
    """Runs tasks on a pool; on the first failure stops the rest and re-raises."""
    with ThreadPoolExecutor(threads) as ex:
        futures = [ex.submit(t) for t in tasks]
        done, _ = wait(futures, return_when=FIRST_EXCEPTION)
        for f in done:
            if f.exception():
                if abort:
                    abort.set()
                for g in futures:
                    g.cancel()
                raise f.exception()
        return [f.result() for f in futures]


def fix_cross_part_duplicates(results, schema, threads):
    """Removes claim ids that a part shares with an earlier part from the later part's
    files. Returns (duplicate ids found, rows removed, files rewritten)."""
    dups = cross_part_duplicates([seen for seen, _ in results])
    jobs = []
    for i, (ids, strs) in dups.items():
        for path, lo, hi, has_other in results[i][1]:
            hit = bool(len(ids)) and bool(np.any((ids >= lo) & (ids <= hi)))
            if hit or (strs and has_other):
                jobs.append(lambda path=path, ids=ids, strs=strs: remove_claims(path, ids, strs, schema))
    removed = sum(run_all(threads, jobs)) if jobs else 0
    return sum(len(i) + len(s) for i, s in dups.values()), removed, len(jobs)


def main(data, out, threads=8, as_of=None, capacity=PART_CAPACITY, quiet=False):
    as_of = as_of or datetime.date.today().isoformat()
    run = Run("claims", quiet=quiet)
    out = prepare_out_dir(out)
    with run.time_phase("loading members"):
        members = members_table(capacity).load(run, parts(data, MEMBERS), threads)
    run.log(members.report(run.times["loading members"]))
    with run.time_phase("loading providers"):
        providers = providers_table(capacity).load(run, parts(data, PROVIDERS), threads)
    run.log(providers.report(run.times["loading providers"]))
    run.log(f"rss after load: {rss_gib():.1f} GiB")

    claim_parts = parts(data, CLAIMS)
    schema = output_schema(members, providers)
    run.log(f"single-pass join: {len(claim_parts)} claims parts across {threads} threads -> parquet")
    abort = threading.Event()
    with run.time_phase("join+write"):
        results = run_all(threads, [
            (lambda p=p: process_part(run, p, out, members, providers, schema, as_of, abort))
            for p in claim_parts], abort)
    with run.time_phase("cross-part dedup"):
        n_ids, removed, rewritten = fix_cross_part_duplicates(results, schema, threads)
        run.stats.add(written=-removed, duplicate_claim_id=removed)
    n_files = sum(len(files) for _, files in results)
    run.finish({"as-of date for DOB checks": as_of,
                "output files": n_files,
                "cross-part dedup": f"{n_ids} duplicate ids, {removed} rows removed, {rewritten} files "
                                    f"rewritten in {run.times['cross-part dedup']:.1f} s",
                "rss at end": f"{rss_gib():.1f} GiB"})
    return run.stats
