"""Row-level comparison of two output directories (or an output dir and a SQL relation).

EXCEPT ALL in both directions over all output columns: every row, every value,
duplicates counted. When both dirs hold same-named part files, the comparison runs
part by part, so a 750M-row check never needs one giant EXCEPT ALL.
"""

import time
from pathlib import Path

import duckdb

from .common import OUTPUT_COLUMNS

COLS = ", ".join(OUTPUT_COLUMNS)


def compare(con, a_sql, b_sql, show=5):
    """a_sql / b_sql: relations (SELECT ... or read_parquet(...)). Returns (rows_a, rows_b, only_a, only_b)."""
    con.execute(f"CREATE OR REPLACE TEMP VIEW a AS SELECT {COLS} FROM {a_sql}")
    con.execute(f"CREATE OR REPLACE TEMP VIEW b AS SELECT {COLS} FROM {b_sql}")
    na, nb = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("a", "b"))
    con.execute("CREATE OR REPLACE TEMP TABLE only_a AS SELECT * FROM a EXCEPT ALL SELECT * FROM b")
    con.execute("CREATE OR REPLACE TEMP TABLE only_b AS SELECT * FROM b EXCEPT ALL SELECT * FROM a")
    oa, ob = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("only_a", "only_b"))
    for t, n in (("only_a", oa), ("only_b", ob)):
        if n and show:
            print(f"[verify] sample of {t}:")
            for r in con.execute(f"SELECT * FROM {t} ORDER BY claim_id LIMIT {show}").fetchall():
                print("   ", r)
    return na, nb, oa, ob


def report(na, nb, oa, ob, t0):
    print(f"[verify] rows: A={na:,}  B={nb:,}   only in A: {oa:,}   only in B: {ob:,}")
    print(f"[verify] {'IDENTICAL' if not (oa or ob) else 'DIFFERENT'} ({time.perf_counter() - t0:.1f} s)")
    return not (oa or ob)


def main(a, b):
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    names_a = sorted(p.name for p in Path(a).glob("part-*.parquet"))
    names_b = sorted(p.name for p in Path(b).glob("part-*.parquet"))
    if names_a == names_b and len(names_a) > 1:
        totals = [0] * 4
        for n in names_a:
            r = compare(con, f"read_parquet('{a}/{n}')", f"read_parquet('{b}/{n}')")
            totals = [x + y for x, y in zip(totals, r)]
            print(f"[verify] {n}: rows A={r[0]:,} B={r[1]:,}  differing A={r[2]:,} B={r[3]:,}  "
                  f"({time.perf_counter() - t0:.0f} s)", flush=True)
        return report(*totals, t0)
    return report(*compare(con, f"read_parquet('{a}/*.parquet')", f"read_parquet('{b}/*.parquet')"), t0)
