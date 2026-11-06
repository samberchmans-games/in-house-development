"""P2: DuckDB through its Python API. One SQL statement, like DuckDbJoin.java.

The cleansing rules are SQL macros (tested against the same cases as cleanse.py).
Duplicate reference ids are resolved with the lowest part winning, via QUALIFY.
"""

import duckdb

from . import cleanse as c
from .common import CLAIMS, MEMBERS, PROVIDERS, Run, parts, prepare_out_dir

_states = ", ".join(f"'{k}': '{v}'" for k, v in c.STATE_NAME_TO_CODE.items())
_statuses = ", ".join(f"'{k}': '{v}'" for k, v in c.STATUS_MAP.items())

MACROS = [
    "CREATE OR REPLACE MACRO c_tu(x) AS upper(trim(x))",
    "CREATE OR REPLACE MACRO c_id(x) AS CASE WHEN c_tu(x) IN ('', 'UNKNOWN', 'N/A') THEN NULL ELSE c_tu(x) END",
    f"CREATE OR REPLACE MACRO c_status(x) AS MAP {{{_statuses}}}[c_tu(x)]",
    f"""CREATE OR REPLACE MACRO c_state(x) AS (
          WITH v AS (SELECT CASE WHEN length(c_tu(x)) > 2 THEN MAP {{{_states}}}[c_tu(x)] ELSE c_tu(x) END AS s)
          SELECT CASE WHEN regexp_full_match(s, '[A-Z]{{2}}') AND s <> 'XX' THEN s END FROM v)""",
    f"""CREATE OR REPLACE MACRO c_amount(x) AS CASE
          WHEN regexp_matches(trim(replace(replace(trim(x), '$', ''), ',', '')), '{c._AMOUNT_RE}')
          THEN CAST(trim(replace(replace(trim(x), '$', ''), ',', '')) AS DOUBLE) END""",
    f"""CREATE OR REPLACE MACRO c_iso_date(x) AS CASE
          WHEN regexp_matches(trim(x), '{c._ISO_RE}')
           AND CAST(substr(trim(x), 1, 4) AS INT) BETWEEN 1900 AND 2100
           AND CAST(substr(trim(x), 6, 2) AS INT) BETWEEN 1 AND 12
           AND CAST(substr(trim(x), 9, 2) AS INT) BETWEEN 1 AND 31
          THEN trim(x) END""",
    "CREATE OR REPLACE MACRO c_code(x) AS CASE WHEN c_tu(x) IN ('', 'XXXXX', '9999') THEN NULL ELSE c_tu(x) END",
    f"CREATE OR REPLACE MACRO c_parse_id(x) AS TRY_CAST(NULLIF(regexp_extract(x, '{c._ID_RE}', 1), '') AS BIGINT)",
]


def _csv(paths):
    files = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
    return f"read_csv({files}, all_varchar=true, header=true, filename=true)"


def join_sql(data, out):
    return f"""
COPY (
  WITH m AS (
    SELECT c_parse_id(member_id) AS k, NULLIF(first_name, '') AS first_name, NULLIF(last_name, '') AS last_name,
           NULLIF(gender, '') AS gender, NULLIF(plan_type, '') AS plan_type, TRY_CAST(risk_score AS DOUBLE) AS risk_score
    FROM {_csv(parts(data, MEMBERS))}
    WHERE k IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY k ORDER BY filename) = 1
  ), p AS (
    SELECT c_parse_id(provider_id) AS k, NULLIF(provider_name, '') AS provider_name, NULLIF(specialty, '') AS specialty,
           NULLIF(npi, '') AS npi, NULLIF(network_status, '') AS network_status
    FROM {_csv(parts(data, PROVIDERS))}
    WHERE k IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY k ORDER BY filename) = 1
  ), cl AS (
    SELECT c_id(claim_id) AS claim_id, c_id(member_id) AS member_id, c_id(provider_id) AS provider_id, *
           EXCLUDE (claim_id, member_id, provider_id)
    FROM {_csv(parts(data, CLAIMS))}
  )
  SELECT cl.claim_id, cl.member_id, cl.provider_id,
         c_iso_date(claim_date) AS claim_date, c_code(service_code) AS service_code,
         c_code(diagnosis_code) AS diagnosis_code,
         c_amount(billed_amount) AS billed_amount, c_amount(paid_amount) AS paid_amount,
         c_status(claim_status) AS claim_status, c_state(state) AS state,
         c_iso_date(member_dob) AS member_dob, NULLIF(source_system, '') AS source_system,
         m.first_name AS member_first_name, m.last_name AS member_last_name,
         m.gender AS member_gender, m.plan_type AS member_plan_type, m.risk_score AS member_risk_score,
         p.provider_name, p.specialty AS provider_specialty, p.npi AS provider_npi,
         p.network_status AS provider_network_status
  FROM cl
  JOIN m ON m.k = c_parse_id(cl.member_id)
  JOIN p ON p.k = c_parse_id(cl.provider_id)
  WHERE cl.claim_id IS NOT NULL
) TO '{out}' (FORMAT parquet, COMPRESSION snappy, PER_THREAD_OUTPUT true, FILENAME_PATTERN 'part-{{i}}')
"""


def main(data, out, threads=8, memory_limit="14GB", temp_dir=None, max_temp="150GB"):
    run = Run("py:duckdb")
    out = prepare_out_dir(out)
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{temp_dir or out / '.duckdb_tmp'}'")
    con.execute(f"SET max_temp_directory_size = '{max_temp}'")
    for m in MACROS:
        con.execute(m)
    with run.time_phase("join+write"):
        con.execute(join_sql(data, out))
        written = con.execute(f"SELECT count(*) FROM read_parquet('{out}/part-*.parquet')").fetchone()[0]
    run.stats.add(written=written)
    run.finish({"duckdb": duckdb.__version__, "memory_limit": memory_limit,
                "note": "one SQL statement: load, join and write are not separable; drop counters not computed"})
