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
CLAIM_COLS = 12

# Same 21 columns, names and types as java-processor ParquetOutput.SCHEMA.
OUTPUT_FIELDS = [
    ("claim_id", pa.string()), ("member_id", pa.string()), ("provider_id", pa.string()),
    ("claim_date", pa.string()), ("service_code", pa.string()), ("diagnosis_code", pa.string()),
    ("billed_amount", pa.float64()), ("paid_amount", pa.float64()),
    ("claim_status", pa.string()), ("state", pa.string()), ("member_dob", pa.string()),
    ("source_system", pa.string()),
    ("member_first_name", pa.string()), ("member_last_name", pa.string()),
    ("member_gender", pa.string()), ("member_plan_type", pa.string()),
    ("member_risk_score", pa.float64()),
    ("provider_name", pa.string()), ("provider_specialty", pa.string()),
    ("provider_npi", pa.string()), ("provider_network_status", pa.string()),
]
OUTPUT_SCHEMA = pa.schema(OUTPUT_FIELDS)
OUTPUT_COLUMNS = [n for n, _ in OUTPUT_FIELDS]

# Reference columns the output needs (same as the Java codecs).
MEMBER_COLS = ["member_id", "first_name", "last_name", "gender", "plan_type", "risk_score"]
PROVIDER_COLS = ["provider_id", "provider_name", "specialty", "npi", "network_status"]

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
    """Thread-safe counters with the same drop reasons as the Java job."""

    FIELDS = ("read", "written", "malformed", "no_claim_id", "bad_key", "member_not_found",
              "provider_not_found", "loaded")

    def __init__(self):
        self._lock = threading.Lock()
        for f in self.FIELDS:
            setattr(self, f, 0)

    def add(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, getattr(self, k) + int(v))


class Run:
    """Phase timers, a 30 s progress line and the final summary block."""

    def __init__(self, tag, every=30.0):
        self.tag, self.stats, self.phase = tag, Stats(), "starting"
        self.t0 = time.perf_counter()
        self.times = {}
        self._stop = threading.Event()
        self._every = every
        threading.Thread(target=self._progress, daemon=True).start()

    def log(self, msg):
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
        self.log(f"dropped: malformed={s.malformed:,} no_claim_id={s.no_claim_id:,} "
                 f"bad_member/provider_key={s.bad_key:,} member_not_found={s.member_not_found:,} "
                 f"provider_not_found={s.provider_not_found:,}")
        if join and s.read:
            self.log(f"throughput: {s.read / join:,.0f} rows/s (join+write)")
        cpu = os.times()
        self.log(f"cpu: user={cpu.user:.0f} s sys={cpu.system:.0f} s  "
                 f"(~{(cpu.user + cpu.system) / total:.1f} cores busy)  peak rss={peak_rss_gib():.1f} GiB")
        for k, v in (extra or {}).items():
            self.log(f"{k}: {v}")
