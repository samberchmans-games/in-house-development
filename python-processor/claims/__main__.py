"""CLI: python -m claims <approach> <dataDir> <outDir> [options]

  pure    P1 pure Python (csv + dicts), single thread
  duckdb  P2 DuckDB, one SQL statement
  polars  P3 Polars lazy query on the streaming engine
  arrow   P4 Arrow + NumPy single pass (the port of Java singlepass)
  verify  compare two output dirs: python -m claims verify <outA> <outB> [rawClaimsDir]
"""

import argparse
import os


def main():
    ap = argparse.ArgumentParser(prog="python -m claims", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("approach", choices=["pure", "duckdb", "polars", "arrow", "verify"])
    ap.add_argument("data")
    ap.add_argument("out")
    ap.add_argument("raw", nargs="?", help="verify only: data dir, to explain differences")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--no-intern", action="store_true", help="pure: keep every field as its own str")
    ap.add_argument("--limit", type=int, default=0, help="pure: read at most N rows per part")
    ap.add_argument("--memory-limit", default="14GB", help="duckdb")
    ap.add_argument("--max-temp", default="150GB", help="duckdb: temp-disk cap")
    a = ap.parse_args()

    if a.approach == "pure":
        from .p1_pure_python import main as run
        run(a.data, a.out, intern=not a.no_intern, limit=a.limit)
    elif a.approach == "duckdb":
        from .p2_duckdb import main as run
        run(a.data, a.out, threads=a.threads, memory_limit=a.memory_limit, max_temp=a.max_temp)
    elif a.approach == "polars":
        os.environ["POLARS_MAX_THREADS"] = str(a.threads)   # must be set before polars is imported
        from .p3_polars import main as run
        run(a.data, a.out)
    elif a.approach == "arrow":
        from .p4_arrow_singlepass import main as run
        run(a.data, a.out, threads=a.threads)
    else:
        from .verify import main as run
        run(a.data, a.out, a.raw)


if __name__ == "__main__":
    main()
