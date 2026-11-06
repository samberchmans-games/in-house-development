"""Row-level parity check between two output directories (e.g. Java vs Python).

EXCEPT ALL in both directions over all 21 columns. When the raw data dir is given,
differing rows are split into:

  * quoted-amount rows: the raw line has a quoted amount such as "$4,703.89". Java
    splits lines on every comma, so its columns from billed_amount on are shifted for
    these rows; a real CSV parser reads them correctly. Expected to differ.
  * unexplained: anything else. Must be 0.

When both dirs hold one output file per claims part with the same names (Java
singlepass and P4 do), the comparison runs part by part, each against its own raw
part, so the full 750M-row check never needs one giant EXCEPT ALL.
"""

import time
from pathlib import Path

import duckdb

from .common import CLAIMS, OUTPUT_COLUMNS, parts

COLS = ", ".join(OUTPUT_COLUMNS)


def compare(con, a_glob, b_glob, raw_files=None):
    """Returns (rows_a, rows_b, only_a, only_b, unexplained_a, unexplained_b)."""
    con.execute(f"CREATE OR REPLACE VIEW a AS SELECT {COLS} FROM read_parquet('{a_glob}')")
    con.execute(f"CREATE OR REPLACE VIEW b AS SELECT {COLS} FROM read_parquet('{b_glob}')")
    na, nb = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("a", "b"))
    con.execute("CREATE OR REPLACE TEMP TABLE only_a AS SELECT * FROM a EXCEPT ALL SELECT * FROM b")
    con.execute("CREATE OR REPLACE TEMP TABLE only_b AS SELECT * FROM b EXCEPT ALL SELECT * FROM a")
    oa, ob = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("only_a", "only_b"))
    ua, ub = oa, ob
    if raw_files and (oa or ob):
        files = "[" + ", ".join(f"'{p}'" for p in raw_files) + "]"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE quoted AS SELECT DISTINCT upper(trim(claim_id)) AS claim_id
                        FROM read_csv({files}, all_varchar=true, header=true)
                        WHERE contains(billed_amount, ',') OR contains(paid_amount, ',')""")
        ua, ub = (con.execute(f"SELECT count(*) FROM {t} WHERE claim_id NOT IN (SELECT claim_id FROM quoted)")
                  .fetchone()[0] for t in ("only_a", "only_b"))
        for t, u in (("only_a", ua), ("only_b", ub)):
            if u:
                print(f"[verify] unexplained rows in {t}:",
                      con.execute(f"SELECT * FROM {t} WHERE claim_id NOT IN (SELECT claim_id FROM quoted) LIMIT 5").fetchall())
    return na, nb, oa, ob, ua, ub


def main(a, b, raw=None):
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    names_a = sorted(p.name for p in Path(a).glob("part-*.parquet"))
    names_b = sorted(p.name for p in Path(b).glob("part-*.parquet"))
    raw_parts = {p.name.replace(".csv.gz", ".parquet"): p for p in parts(raw, CLAIMS)} if raw else {}
    if names_a == names_b and len(names_a) > 1 and (not raw or set(names_a) <= set(raw_parts)):
        totals = [0] * 6
        for n in names_a:
            r = compare(con, f"{a}/{n}", f"{b}/{n}", [raw_parts[n]] if raw else None)
            totals = [x + y for x, y in zip(totals, r)]
            print(f"[verify] {n}: rows A={r[0]:,} B={r[1]:,}  differing A={r[2]:,} B={r[3]:,}  "
                  f"unexplained A={r[4]:,} B={r[5]:,}  ({time.perf_counter() - t0:.0f} s)", flush=True)
        na, nb, oa, ob, ua, ub = totals
    else:
        na, nb, oa, ob, ua, ub = compare(con, f"{a}/*.parquet", f"{b}/*.parquet",
                                         parts(raw, CLAIMS) if raw else None)
    print(f"[verify] rows: A={na:,}  B={nb:,}")
    print(f"[verify] only in A: {oa:,}   only in B: {ob:,}")
    if raw:
        print(f"[verify] quoted-amount rows: A={oa - ua:,} B={ob - ub:,}   unexplained: A={ua:,} B={ub:,}")
    print(f"[verify] {'IDENTICAL' if not (oa or ob) else 'DIFFERENT'} ({time.perf_counter() - t0:.1f} s)")
