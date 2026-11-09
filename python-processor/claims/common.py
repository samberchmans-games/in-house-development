"""Shared harness: input layout, output schema, counters, progress line, summary."""

import os
import resource
import sys
import threading
import time
from pathlib import Path

import psutil
import pyarrow as pa

CLAIMS, MEMBERS, PROVIDERS = "raw_claims", "member_enrichment", "provider_reference"
CLAIM_COLS = ["claim_id", "member_id", "provider_id", "claim_date", "service_code",
              "diagnosis_code", "billed_amount", "paid_amount", "claim_status", "state",
              "member_dob", "source_system"]

# 23 output columns: the claim, member and provider attributes, plus two data-quality flags.
OUTPUT_FIELDS = [
    ("claim_id", pa.string()), ("member_id", pa.string()), ("provider_id", pa.string()),
    ("claim_date", pa.date32()), ("service_code", pa.string()), ("diagnosis_code", pa.string()),
    ("billed_amount", pa.float64()), ("paid_amount", pa.float64()),
    ("claim_status", pa.string()), ("state", pa.string()), ("member_dob", pa.date32()),
    ("source_system", pa.string()),
    ("member_first_name", pa.string()), ("member_last_name", pa.string()),
    ("member_gender", pa.string()), ("member_plan_type", pa.string()),
    ("member_risk_score", pa.float64()),
    ("provider_name", pa.string()), ("provider_specialty", pa.string()),
    ("provider_npi", pa.string()), ("provider_network_status", pa.string()),
    ("flag_negative_billed", pa.bool_()), ("flag_paid_exceeds_billed", pa.bool_()),
]
OUTPUT_COLUMNS = [n for n, _ in OUTPUT_FIELDS]

GIB = 1 << 30


def parts(data_dir, table):
    d = Path(data_dir) / table
    ps = sorted(p for p in d.iterdir() if p.name.startswith("part-") and p.name.endswith(".csv.gz"))
    if not ps:
        raise SystemExit(f"no part-*.csv.gz files in {d}")
    return ps


def prepare_out_dir(out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if any(out.glob("part-*")):
        raise SystemExit(f"output dir already has part files from a previous run: {out} "
                         "(delete them or pick another dir)")
    return out


def rss_gib():
    return psutil.Process().memory_info().rss / GIB


def peak_rss_gib():
    # macOS reports ru_maxrss in bytes (Linux: KiB).
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / GIB if sys.platform == "darwin" else r * 1024 / GIB


class Stats:
    """Thread-safe counters. Every claim row read ends up in exactly one of
    written / malformed / no_claim_id / bad_key / member_not_found / provider_not_found /
    duplicate_claim_id."""

    FIELDS = ("read", "written", "malformed", "no_claim_id", "bad_key", "member_not_found",
              "provider_not_found", "duplicate_claim_id", "loaded")

    def __init__(self):
        self._lock = threading.Lock()
        for f in self.FIELDS:
            setattr(self, f, 0)

    def add(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, getattr(self, k) + int(v))

    def dropped(self):
        return {f: getattr(self, f) for f in self.FIELDS[2:-1]}


class Run:
    """Phase timers, a 30 s progress line and the final summary block."""

    def __init__(self, tag, every=30.0, quiet=False):
        self.tag, self.stats, self.phase, self.quiet = tag, Stats(), "starting", quiet
        self.t0, self.wall0 = time.perf_counter(), time.time()
        self.times = {}
        self._stop = threading.Event()
        self._every = every
        threading.Thread(target=self._progress, daemon=True).start()

    def log(self, msg):
        if not self.quiet:
            print(f"[{self.tag}] {msg}", flush=True)

    def _progress(self):
        while not self._stop.wait(self._every):
            s = self.stats
            n = s.loaded if self.phase.startswith("loading") else s.read
            self.log(f"{time.perf_counter() - self.t0:6.0f}s  {self.phase:<17} rows={n:,}  "
                     f"written={s.written:,}  rss={rss_gib():.1f} GiB")

    def time_phase(self, name):
        run = self

        class _T:
            def __enter__(self):
                run.phase = name
                self.t = time.perf_counter()

            def __exit__(self, *exc):
                run.times[name] = time.perf_counter() - self.t

        return _T()

    def finish(self, extra=None):
        self._stop.set()
        s, total = self.stats, time.perf_counter() - self.t0
        join = self.times.get("join+write", 0.0)
        load = sum(v for k, v in self.times.items() if k.startswith("loading"))
        self.log(f"DONE read={s.read:,} written={s.written:,}  load={load:.1f} s  "
                 f"join+write={join:.1f} s  total={total:.1f} s")
        self.log("dropped: " + " ".join(f"{k}={v:,}" for k, v in s.dropped().items()))
        accounted = s.written + sum(s.dropped().values())
        if accounted != s.read:
            self.log(f"WARNING: {s.read - accounted:,} rows read are not accounted for")
        if join and s.read:
            self.log(f"throughput: {s.read / join:,.0f} rows/s (join+write)")
        cpu = os.times()
        self.log(f"cpu: user={cpu.user:.0f} s sys={cpu.system:.0f} s  "
                 f"(~{(cpu.user + cpu.system) / total:.1f} cores busy)  peak rss={peak_rss_gib():.1f} GiB")
        # perf_counter stops while macOS sleeps; the wall clock does not. A gap means the
        # machine slept (or was suspended) mid-run, so the timings are not a clean benchmark.
        wall = time.time() - self.wall0
        if wall > total * 1.05 + 5:
            self.log(f"WARNING: wall clock {wall:.0f} s vs job time {total:.0f} s: the machine slept "
                     f"for ~{wall - total:.0f} s during the run; do not use these timings")
        for k, v in (extra or {}).items():
            self.log(f"{k}: {v}")
