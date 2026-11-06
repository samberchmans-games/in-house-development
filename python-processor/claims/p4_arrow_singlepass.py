"""P4: Arrow + NumPy single-pass join, a port of java-processor SinglePassParquetJoin.

Both reference tables are held in memory as compact NumPy columns behind a dense
id -> row index; the 800M claims are streamed once, cleansed with Arrow compute
kernels, joined with vectorized lookups and written straight to Parquet.

Memory layout (the Python version of DenseIdJoinTable + Codecs):
  index          int32[maxId+1]   row of each id, -1 = absent   (4 B per id slot)
  string columns int8/int16 codes into one shared dictionary    (1-2 B per row)
  risk_score     uint16 hundredths, 0xFFFF = null               (2 B per row)
  npi            uint64, 0 = null                               (8 B per row)

Parallelism: a thread pool, one task per part file (as in Java). Arrow releases the
GIL while it decompresses, parses, runs kernels and writes Parquet.
"""

import os
import threading
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from . import cleanse as c
from .common import (CLAIMS, GIB, MEMBER_COLS, MEMBERS, OUTPUT_FIELDS, PROVIDER_COLS,
                     PROVIDERS, Run, parts, prepare_out_dir, rss_gib)

BLOCK_SIZE = 16 << 20          # decompressed CSV bytes per Arrow batch (~170K claims)
ROW_GROUP_ROWS = 1_000_000     # buffer this many output rows per Parquet row group
NULL_CODE = -1
RISK_NULL = 0xFFFF


def _convert(columns):
    return pcsv.ConvertOptions(
        column_types={col: pa.string() for col in columns}, include_columns=columns,
        strings_can_be_null=True, quoted_strings_can_be_null=True, null_values=[""])


def _chunks(arr):
    return arr.chunks if isinstance(arr, pa.ChunkedArray) else [arr]


# ---------------------------------------------------------------- column encoders

class DictColumn:
    """Low-cardinality strings as small integer codes into one shared dictionary."""

    def __init__(self, src):
        self.src, self.values, self._lookup, self._lock = src, [], {}, threading.Lock()

    def _code(self, v):
        code = self._lookup.get(v)
        if code is None:
            code = len(self.values)
            if code > 32767:
                raise ValueError(f"{self.src}: over 32,767 distinct values, not a dictionary column")
            self._lookup[v] = code
            self.values.append(v)
        return code

    def encode(self, arr):
        out = []
        for ch in _chunks(arr):
            d = ch.dictionary_encode()
            local = d.dictionary.to_pylist()
            with self._lock:   # codes are global: map this chunk's dictionary onto them
                remap = np.array([self._code(v) for v in local] + [NULL_CODE], dtype=np.int16)
            out.append(remap[d.indices.fill_null(len(local)).to_numpy()])
        return np.concatenate(out)

    def finish(self, codes):
        self.dictionary = pa.array(self.values, pa.string())
        self.codes = codes.astype(np.int8) if len(self.values) <= 127 else codes
        self.type = pa.dictionary(pa.from_numpy_dtype(self.codes.dtype), pa.string())

    def take(self, rows):
        k = self.codes[rows]
        return pa.DictionaryArray.from_arrays(pa.array(k, mask=k < 0), self.dictionary)


class RiskColumn:
    """d.dd scores as uint16 hundredths. h/100.0 is the same double Java writes."""

    type = pa.float64()

    def __init__(self, src):
        self.src = src

    def encode(self, arr):
        f = pc.cast(arr, pa.float64()).to_numpy(zero_copy_only=False)
        nan = np.isnan(f)
        h = np.rint(np.where(nan, 0, f) * 100)
        if not np.all(nan | ((h / 100.0 == f) & (h >= 0) & (h < RISK_NULL))):
            raise ValueError(f"{self.src}: values that are not d.dd hundredths")
        return np.where(nan, RISK_NULL, h).astype(np.uint16)

    def finish(self, codes):
        self.codes = codes

    def take(self, rows):
        h = self.codes[rows]
        return pa.array(h / 100.0, mask=h == RISK_NULL)


class NpiColumn:
    """10-digit NPIs as uint64 (they never start with 0, so the text round-trips)."""

    type = pa.string()

    def __init__(self, src):
        self.src = src

    def encode(self, arr):
        ok = pc.match_substring_regex(arr, r"^[1-9][0-9]{0,18}$")
        if pc.any(pc.and_(pc.is_valid(arr), pc.invert(ok))).as_py():
            raise ValueError(f"{self.src}: values that do not round-trip through uint64")
        return pc.cast(arr, pa.uint64()).fill_null(0).to_numpy()

    def finish(self, codes):
        self.codes = codes

    def take(self, rows):
        v = self.codes[rows]
        return pc.cast(pa.array(v, mask=v == 0), pa.string())


# ---------------------------------------------------------------- reference table

class _Buf:
    """Append-only NumPy buffer: one big allocation per part instead of a list of
    ~900 per-batch arrays (~150 batches x 6). Keeping those small arrays alive until a final
    np.concatenate fragmented the macOS allocator: RSS stayed at 12.6 GiB after
    loading 2.3 GiB of members, and macOS later compressed ~10 GB of it."""

    def __init__(self, dtype, capacity=1 << 25):     # 33.5M rows: a 25M-row part never grows
        self.a, self.n = np.empty(capacity, dtype), 0  # untouched pages are never resident

    def append(self, x):
        need = self.n + len(x)
        if need > len(self.a):
            grown = np.empty(max(need, len(self.a) * 3 // 2), self.a.dtype)
            grown[:self.n] = self.a[:self.n]
            self.a = grown
        self.a[self.n:need] = x
        self.n = need

    def array(self):
        return self.a[:self.n]



class RefTable:
    def __init__(self, name, key_col, columns):
        self.name, self.key_col, self.columns = name, key_col, columns   # {out_name: encoder}

    def load(self, run, paths, threads):
        srcs = [self.key_col] + [e.src for e in self.columns.values()]
        results = [None] * len(paths)

        def task(i):
            # Stream the part and encode batch by batch: a whole 25M-row part as Arrow
            # strings is ~1.5 GB, times 8 threads; the encoded batches are a few MB.
            reader = pcsv.open_csv(paths[i], read_options=pcsv.ReadOptions(block_size=BLOCK_SIZE),
                                   convert_options=_convert(srcs))
            keys, cols = _Buf(np.int64), {n: None for n in self.columns}
            for b in reader:
                keys.append(c.a_parse_id(b.column(self.key_col)).fill_null(-1).to_numpy())
                for n, e in self.columns.items():
                    codes = e.encode(b.column(e.src))
                    cols[n] = cols[n] or _Buf(codes.dtype)
                    cols[n].append(codes)
                run.stats.add(loaded=b.num_rows)
            results[i] = (keys.array(), {n: v.array() for n, v in cols.items()})

        _run_all(threads, [lambda i=i: task(i) for i in range(len(paths))])
        pa.default_memory_pool().release_unused()

        # Dense index. Parts are applied last-to-first so the LOWEST part wins on a
        # duplicate id, the same as the Java loader (generator off-by-one, findings section 16).
        sizes = [len(k) for k, _ in results]
        offsets = np.concatenate([[0], np.cumsum(sizes)])
        if offsets[-1] >= 2**31:
            raise ValueError("more than 2^31 rows: widen the index to int64")
        self.max_id = int(max(k.max() for k, _ in results))
        self.index = np.full(self.max_id + 1, -1, dtype=np.int32)
        self.duplicates = 0
        for i in reversed(range(len(results))):
            keys = results[i][0]
            ok = keys >= 0
            self.duplicates += int(np.count_nonzero(self.index[keys[ok]] >= 0))
            self.index[keys[ok]] = (offsets[i] + np.flatnonzero(ok)).astype(np.int32)
        self.rows = int(offsets[-1])
        for n, e in self.columns.items():
            e.finish(np.concatenate([cols[n] for _, cols in results]))
            for _, cols in results:
                cols[n] = None
        return self

    def lookup(self, keys):
        rows = np.full(len(keys), -1, dtype=np.int32)
        ok = (keys >= 0) & (keys <= self.max_id)
        rows[ok] = self.index[keys[ok]]
        return rows

    def report(self, secs):
        col_bytes = sum(e.codes.nbytes for e in self.columns.values())
        dicts = ", ".join(f"{n}={len(e.values)}" for n, e in self.columns.items() if hasattr(e, "values"))
        return (f"{self.name} loaded: {self.rows:,} in {secs:.1f} s  (index {self.index.nbytes / GIB:.2f} GiB, "
                f"columns {col_bytes / GIB:.2f} GiB = {col_bytes / self.rows:.1f} B/row, "
                f"duplicate ids={self.duplicates:,}, dictionary sizes: {dicts})")


def members_table():
    return RefTable("members", "member_id", {
        "member_first_name": DictColumn("first_name"), "member_last_name": DictColumn("last_name"),
        "member_gender": DictColumn("gender"), "member_plan_type": DictColumn("plan_type"),
        "member_risk_score": RiskColumn("risk_score")})


def providers_table():
    return RefTable("providers", "provider_id", {
        "provider_name": DictColumn("provider_name"), "provider_specialty": DictColumn("specialty"),
        "provider_npi": NpiColumn("npi"), "provider_network_status": DictColumn("network_status")})


assert [e.src for e in members_table().columns.values()] == MEMBER_COLS[1:]
assert [e.src for e in providers_table().columns.values()] == PROVIDER_COLS[1:]


# ---------------------------------------------------------------- claims

CLAIM_NAMES = ["claim_id", "member_id", "provider_id", "claim_date", "service_code",
               "diagnosis_code", "billed_amount", "paid_amount", "claim_status", "state",
               "member_dob", "source_system"]


def output_schema(members, providers):
    types = {n: e.type for t in (members, providers) for n, e in t.columns.items()}
    return pa.schema([(n, types.get(n, typ)) for n, typ in OUTPUT_FIELDS])


def join_batch(b, members, providers, schema, stats):
    cid, mid, pid = c.a_id(b.column(0)), c.a_id(b.column(1)), c.a_id(b.column(2))
    has_claim = pc.is_valid(cid).to_numpy(zero_copy_only=False)
    mk = c.a_parse_id(mid).fill_null(-1).to_numpy()
    pk = c.a_parse_id(pid).fill_null(-1).to_numpy()
    good_key = has_claim & (mk >= 0) & (pk >= 0)
    mrow, prow = members.lookup(mk), providers.lookup(pk)
    has_member = good_key & (mrow >= 0)
    keep = has_member & (prow >= 0)
    n, written = len(b), int(np.count_nonzero(keep))
    stats.add(read=n, written=written,
              no_claim_id=n - np.count_nonzero(has_claim),
              bad_key=np.count_nonzero(has_claim) - np.count_nonzero(good_key),
              member_not_found=np.count_nonzero(good_key) - np.count_nonzero(has_member),
              provider_not_found=np.count_nonzero(has_member) - written)
    if not written:
        return None
    sel = np.flatnonzero(keep)
    idx = pa.array(sel)
    col = lambda i: pc.take(b.column(i), idx)
    mr, pr = mrow[sel], prow[sel]
    arrays = [
        pc.take(cid, idx), pc.take(mid, idx), pc.take(pid, idx),
        c.a_iso_date(col(3)), c.a_code(col(4)), c.a_code(col(5)),
        c.a_amount(col(6)), c.a_amount(col(7)),
        c.a_status(col(8)), c.a_state(col(9)), c.a_iso_date(col(10)), col(11),
        *(e.take(mr) for e in members.columns.values()),
        *(e.take(pr) for e in providers.columns.values()),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def process_part(run, path, out_path, members, providers, schema, abort):
    malformed = []

    def on_invalid(row):
        malformed.append(1)
        return "skip"

    reader = pcsv.open_csv(
        path, read_options=pcsv.ReadOptions(block_size=BLOCK_SIZE),
        parse_options=pcsv.ParseOptions(invalid_row_handler=on_invalid),
        convert_options=_convert(CLAIM_NAMES))
    if reader.schema.names != CLAIM_NAMES:
        raise ValueError(f"{path}: unexpected header {reader.schema.names}")
    tmp = out_path.with_name(out_path.name + ".inprogress")
    ok = False
    try:
        with pq.ParquetWriter(tmp, schema, compression="snappy") as w:
            pending, rows = [], 0
            for batch in reader:
                if abort.is_set():
                    raise RuntimeError("aborted: another part failed")
                out = join_batch(batch, members, providers, schema, run.stats)
                if out is not None:
                    pending.append(out)
                    rows += out.num_rows
                if rows >= ROW_GROUP_ROWS:
                    w.write_table(pa.Table.from_batches(pending))
                    pending, rows = [], 0
            if pending:
                w.write_table(pa.Table.from_batches(pending))
        ok = True
    finally:
        run.stats.add(malformed=len(malformed))
        if ok:
            os.replace(tmp, out_path)
        else:
            tmp.unlink(missing_ok=True)


def _run_all(threads, tasks, abort=None):
    """Runs tasks on a pool; on the first failure cancels the rest and re-raises."""
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


def main(data, out, threads=8):
    run = Run("py:arrow")
    out = prepare_out_dir(out)
    with run.time_phase("loading members"):
        members = members_table().load(run, parts(data, MEMBERS), threads)
    run.log(members.report(run.times["loading members"]))
    with run.time_phase("loading providers"):
        providers = providers_table().load(run, parts(data, PROVIDERS), threads)
    run.log(providers.report(run.times["loading providers"]))
    run.log(f"rss after load: {rss_gib():.1f} GiB")

    claim_parts = parts(data, CLAIMS)
    schema = output_schema(members, providers)
    run.log(f"single-pass join: {len(claim_parts)} claims parts across {threads} threads -> parquet")
    abort = threading.Event()
    with run.time_phase("join+write"):
        _run_all(threads, [
            (lambda p=p: process_part(run, p, out / p.name.replace(".csv.gz", ".parquet"),
                                      members, providers, schema, abort))
            for p in claim_parts], abort)
    run.finish({"rss at end": f"{rss_gib():.1f} GiB"})
