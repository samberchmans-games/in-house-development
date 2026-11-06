"""P3: Polars lazy query, run on the streaming engine and sunk straight to Parquet.

The cleansing rules are Polars expressions (tested against the same cases as cleanse.py).
Duplicate reference ids: parts are concatenated in order and unique(keep="first",
maintain_order=True) keeps the lowest part's row.
"""

import polars as pl

from . import cleanse as c
from .common import (CLAIMS, MEMBER_COLS, MEMBERS, OUTPUT_COLUMNS, PROVIDER_COLS, PROVIDERS,
                     Run, parts, prepare_out_dir)

_NULL = pl.lit(None, dtype=pl.String)


def _tu(x):
    return x.str.strip_chars().str.to_uppercase()


def _null_if_in(t, values):
    return pl.when(t.is_in(list(values))).then(_NULL).otherwise(t)


def _id(x):
    return _null_if_in(_tu(x), c.ID_NULLS)


def _status(x):
    return _tu(x).replace_strict(c.STATUS_MAP, default=None, return_dtype=pl.String)


def _state(x):
    t = _tu(x)
    v = pl.when(t.str.len_chars() > 2).then(
        t.replace_strict(c.STATE_NAME_TO_CODE, default=None, return_dtype=pl.String)).otherwise(t)
    return pl.when(v.str.contains(r"^[A-Z]{2}$") & (v != "XX")).then(v).otherwise(_NULL)


def _amount(x):
    v = x.str.strip_chars().str.replace_all("$", "", literal=True) \
        .str.replace_all(",", "", literal=True).str.strip_chars()
    return pl.when(v.str.contains(c._AMOUNT_RE)).then(v).otherwise(_NULL).cast(pl.Float64, strict=False)


def _iso_date(x):
    t = x.str.strip_chars()
    part = lambda a, n: t.str.slice(a, n).cast(pl.Int32, strict=False)
    ok = (t.str.contains(c._ISO_RE) & part(0, 4).is_between(1900, 2100)
          & part(5, 2).is_between(1, 12) & part(8, 2).is_between(1, 31))
    return pl.when(ok).then(t).otherwise(_NULL)


def _code(x):
    return _null_if_in(_tu(x), c.CODE_NULLS)


def _parse_id(x):
    return x.str.extract(c._ID_RE, 1).cast(pl.Int64, strict=False)


CLEANSE = {"id": _id, "status": _status, "state": _state, "amount": _amount,
           "iso_date": _iso_date, "code": _code, "parse_id": _parse_id}


def _empty_null(name):
    col = pl.col(name)
    return pl.when(col == "").then(_NULL).otherwise(col).alias(name)


def _scan(paths, columns=None):
    frames = [pl.scan_csv(p, infer_schema=False) for p in paths]
    lf = pl.concat(frames, how="vertical")
    return lf.select(columns) if columns else lf


def _ref(paths, cols):
    key = cols[0]
    return (_scan(paths, cols)
            .with_columns(_parse_id(pl.col(key)).alias("k"), *(_empty_null(n) for n in cols[1:]))
            .drop(key)
            .filter(pl.col("k").is_not_null())
            .unique(subset="k", keep="first", maintain_order=True))


def build(data):
    members = _ref(parts(data, MEMBERS), MEMBER_COLS).select(
        "k", pl.col("first_name").alias("member_first_name"), pl.col("last_name").alias("member_last_name"),
        pl.col("gender").alias("member_gender"), pl.col("plan_type").alias("member_plan_type"),
        pl.col("risk_score").cast(pl.Float64, strict=False).alias("member_risk_score"))
    providers = _ref(parts(data, PROVIDERS), PROVIDER_COLS).select(
        "k", "provider_name", pl.col("specialty").alias("provider_specialty"),
        pl.col("npi").alias("provider_npi"), pl.col("network_status").alias("provider_network_status"))
    claims = (_scan(parts(data, CLAIMS))
              .with_columns(_id(pl.col("claim_id")).alias("claim_id"),
                            _id(pl.col("member_id")).alias("member_id"),
                            _id(pl.col("provider_id")).alias("provider_id"))
              .filter(pl.col("claim_id").is_not_null())
              .with_columns(_parse_id(pl.col("member_id")).alias("mk"),
                            _parse_id(pl.col("provider_id")).alias("pk")))
    return (claims
            .join(members, left_on="mk", right_on="k", how="inner")
            .join(providers, left_on="pk", right_on="k", how="inner")
            .with_columns(_iso_date(pl.col("claim_date")).alias("claim_date"),
                          _code(pl.col("service_code")).alias("service_code"),
                          _code(pl.col("diagnosis_code")).alias("diagnosis_code"),
                          _amount(pl.col("billed_amount")).alias("billed_amount"),
                          _amount(pl.col("paid_amount")).alias("paid_amount"),
                          _status(pl.col("claim_status")).alias("claim_status"),
                          _state(pl.col("state")).alias("state"),
                          _iso_date(pl.col("member_dob")).alias("member_dob"),
                          _empty_null("source_system"))
            .select(OUTPUT_COLUMNS))


def main(data, out):
    """Thread count comes from POLARS_MAX_THREADS, which __main__ sets before importing polars."""
    run = Run("py:polars")
    out = prepare_out_dir(out)
    with run.time_phase("join+write"):
        build(data).sink_parquet(out / "part-00000.parquet", compression="snappy", engine="streaming")
        written = pl.scan_parquet(out / "part-00000.parquet").select(pl.len()).collect().item()
    run.stats.add(written=written)
    run.finish({"polars": pl.__version__, "threads": pl.thread_pool_size(),
                "note": "one lazy query: load, join and write are not separable; drop counters not computed"})
