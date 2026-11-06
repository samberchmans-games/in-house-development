"""Row-level parity check between two output directories (e.g. Java vs Python).

EXCEPT ALL in both directions over all 21 columns. When the raw data dir is given,
differing rows are split into:

  * quoted-amount rows: the raw line has a quoted amount such as "$4,703.89". Java
    splits lines on every comma, so its columns from billed_amount on are shifted for
    these rows; a real CSV parser reads them correctly. Expected to differ.
  * unexplained: anything else. Must be 0.
"""

import time

import duckdb

from .common import CLAIMS, OUTPUT_COLUMNS, parts

COLS = ", ".join(OUTPUT_COLUMNS)


def main(a, b, raw=None):
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    for name, d in (("a", a), ("b", b)):
        con.execute(f"CREATE VIEW {name} AS SELECT {COLS} FROM read_parquet('{d}/*.parquet')")
    na, nb = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("a", "b"))
    con.execute("CREATE TEMP TABLE only_a AS SELECT * FROM a EXCEPT ALL SELECT * FROM b")
    con.execute("CREATE TEMP TABLE only_b AS SELECT * FROM b EXCEPT ALL SELECT * FROM a")
    oa, ob = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("only_a", "only_b"))
    print(f"[verify] rows: A={na:,}  B={nb:,}")
    print(f"[verify] only in A: {oa:,}   only in B: {ob:,}")
    if raw and (oa or ob):
        files = "[" + ", ".join(f"'{p}'" for p in parts(raw, CLAIMS)) + "]"
        con.execute(f"""CREATE TEMP TABLE quoted AS SELECT DISTINCT upper(trim(claim_id)) AS claim_id
                        FROM read_csv({files}, all_varchar=true, header=true)
                        WHERE contains(billed_amount, ',') OR contains(paid_amount, ',')""")
        for t in ("only_a", "only_b"):
            q, u = con.execute(f"""SELECT count(*) FILTER (WHERE claim_id IN (SELECT claim_id FROM quoted)),
                                          count(*) FILTER (WHERE claim_id NOT IN (SELECT claim_id FROM quoted))
                                   FROM {t}""").fetchone()
            print(f"[verify] {t}: quoted-amount rows={q:,}  unexplained={u:,}")
            if u:
                print(con.execute(f"SELECT * FROM {t} WHERE claim_id NOT IN (SELECT claim_id FROM quoted) LIMIT 5").fetchall())
    print(f"[verify] {'IDENTICAL' if not (oa or ob) else 'DIFFERENT'} ({time.perf_counter() - t0:.1f} s)")
