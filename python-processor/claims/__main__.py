"""CLI.

  python -m claims run <dataDir> <outDir> [--threads 8] [--as-of YYYY-MM-DD]
      the single-pass join (see singlepass.py)
  python -m claims reference <dataDir> <jobOutDir> --as-of YYYY-MM-DD
      check a job output against the independent DuckDB SQL implementation (samples only)
  python -m claims verify <outA> <outB>
      row-level comparison of two outputs (e.g. two runs)
"""

import argparse
import sys


def main():
    ap = argparse.ArgumentParser(prog="python -m claims", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["run", "reference", "verify"])
    ap.add_argument("a", help="run/reference: data dir; verify: first output dir")
    ap.add_argument("b", help="run: output dir; reference: job output dir; verify: second output dir")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--as-of", help="date for the 'DOB in the future' rule (default: today)")
    a = ap.parse_args()

    if a.command == "run":
        from .singlepass import main as run
        run(a.a, a.b, threads=a.threads, as_of=a.as_of)
    elif a.command == "reference":
        if not a.as_of:
            ap.error("reference needs --as-of (use the same date the job logged)")
        from .reference import main as ref
        sys.exit(0 if ref(a.a, a.b, a.as_of) else 1)
    else:
        from .verify import main as verify
        sys.exit(0 if verify(a.a, a.b) else 1)


if __name__ == "__main__":
    main()
