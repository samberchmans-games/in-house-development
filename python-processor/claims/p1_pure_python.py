"""P1: pure Python. csv + gzip + two dicts, single thread.

The Python twin of Java's first PlainJavaJoin (HashMap<String,String[]>), and of
Bug #2: at hundreds of millions of entries, object overhead decides everything.

  * --intern off: every field is its own str object (the naive version)
  * --intern on : repeated values (names, codes) share one str object via sys.intern,
                  the first fix most Python developers reach for

pyarrow is used only to write the Parquet output, so the result can be compared
with the other approaches.
"""

import csv
import gzip
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from . import cleanse as c
from .common import (CLAIM_COLS, CLAIMS, GIB, MEMBERS, OUTPUT_COLUMNS, OUTPUT_SCHEMA,
                     PROVIDERS, Run, parts, prepare_out_dir, rss_gib)

FLUSH_ROWS = 200_000
MEMBER_IDX = (1, 2, 3, 7, 10)     # first, last, gender, plan_type, risk_score
PROVIDER_IDX = (1, 2, 3, 6)       # name, specialty, npi, network_status


def _rows(path):
    with gzip.open(path, "rt", newline="") as f:
        r = csv.reader(f)
        next(r)
        yield from r


def load(run, paths, idx, intern, limit):
    table, dups, need = {}, 0, max(idx) + 1
    norm = (lambda v: sys.intern(v) if v else None) if intern else (lambda v: v or None)
    for p in paths:                       # in part order, so the lowest part wins
        for n, r in enumerate(_rows(p), 1):
            if limit and n > limit:
                break
            if len(r) < need:
                continue
            k = c.parse_id(r[0])
            if k < 0:
                continue
            if k in table:
                dups += 1
                continue
            table[k] = tuple(norm(r[i]) for i in idx)
            if n % 1_000_000 == 0:
                run.stats.add(loaded=1_000_000)
    return table, dups


def _risk(v):
    try:
        return float(v) if v else None
    except ValueError:
        return None


def main(data, out, intern=True, limit=0):
    run = Run("py:pure")
    out = prepare_out_dir(out)
    base = rss_gib()
    with run.time_phase("loading members"):
        members, mdup = load(run, parts(data, MEMBERS), MEMBER_IDX, intern, limit)
    m_gib = rss_gib() - base
    run.log(f"members loaded: {len(members):,} in {run.times['loading members']:.1f} s  "
            f"(+{m_gib:.2f} GiB RSS = {m_gib * GIB / max(len(members), 1):.0f} B/entry, duplicate ids={mdup})")
    with run.time_phase("loading providers"):
        providers, pdup = load(run, parts(data, PROVIDERS), PROVIDER_IDX, intern, limit)
    p_gib = rss_gib() - base - m_gib
    run.log(f"providers loaded: {len(providers):,} in {run.times['loading providers']:.1f} s  "
            f"(+{p_gib:.2f} GiB RSS = {p_gib * GIB / max(len(providers), 1):.0f} B/entry, duplicate ids={pdup})")

    s = run.stats
    with run.time_phase("join+write"):
        for p in parts(data, CLAIMS):
            cols = [[] for _ in OUTPUT_COLUMNS]
            target = out / p.name.replace(".csv.gz", ".parquet")
            tmp = target.with_name(target.name + ".inprogress")
            read = written = malformed = no_claim = bad = no_m = no_p = 0
            with pq.ParquetWriter(tmp, OUTPUT_SCHEMA, compression="snappy") as w:
                for n, r in enumerate(_rows(p), 1):
                    if limit and n > limit:
                        break
                    read += 1
                    if read % 1_000_000 == 0:
                        s.add(read=1_000_000)
                    if len(r) < CLAIM_COLS:
                        malformed += 1
                        continue
                    claim_id = c.id_(r[0])
                    if claim_id is None:
                        no_claim += 1
                        continue
                    member_id, provider_id = c.id_(r[1]), c.id_(r[2])
                    mk, pk = c.parse_id(member_id), c.parse_id(provider_id)
                    if mk < 0 or pk < 0:
                        bad += 1
                        continue
                    m = members.get(mk)
                    if m is None:
                        no_m += 1
                        continue
                    pv = providers.get(pk)
                    if pv is None:
                        no_p += 1
                        continue
                    row = (claim_id, member_id, provider_id, c.iso_date(r[3]), c.code(r[4]),
                           c.code(r[5]), c.amount(r[6]), c.amount(r[7]), c.status(r[8]),
                           c.state(r[9]), c.iso_date(r[10]), r[11] or None,
                           m[0], m[1], m[2], m[3], _risk(m[4]), pv[0], pv[1], pv[2], pv[3])
                    for col, v in zip(cols, row):
                        col.append(v)
                    written += 1
                    if written % FLUSH_ROWS == 0:
                        w.write_table(pa.Table.from_arrays(cols, schema=OUTPUT_SCHEMA))
                        cols = [[] for _ in OUTPUT_COLUMNS]
                if cols[0]:
                    w.write_table(pa.Table.from_arrays(cols, schema=OUTPUT_SCHEMA))
            tmp.rename(target)
            s.add(read=read % 1_000_000, written=written, malformed=malformed, no_claim_id=no_claim,
                  bad_key=bad, member_not_found=no_m, provider_not_found=no_p)
    run.finish({"intern": intern, "limit per part": limit or "none"})
