"""Independent reference implementation of the job's rules, in DuckDB SQL (a test oracle).

Not a competing solution: it exists only to check the single-pass job's output. It
shares no code with the job (its own CSV parser, date parsing, dedup, joins), so
agreement between the two is real evidence, not one implementation agreeing with itself.

It materializes the full 3-way join, so it is meant for the samples, not the full
800M rows (that is exactly what failed for DuckDB in the Java work, BLOG_NOTES section 6).

  python -m claims reference <dataDir> <jobOutDir> --as-of YYYY-MM-DD
"""

import time

import duckdb

from . import cleanse as c
from .common import CLAIM_COLS, CLAIMS, MEMBERS, PROVIDERS, parts
from .verify import compare, report

_names = ", ".join(f"'{k}': '{v}'" for k, v in c.STATE_NAME_TO_CODE.items())
_codes = ", ".join(f"'{s}'" for s in c.STATE_CODES)
_status = ", ".join(f"'{k}': '{v}'" for k, v in c.STATUS_MAP.items())

MACROS = [
    "CREATE OR REPLACE MACRO r_tu(x) AS upper(trim(x))",
    "CREATE OR REPLACE MACRO r_id(x) AS CASE WHEN r_tu(x) IN ('', 'UNKNOWN', 'N/A') THEN NULL ELSE r_tu(x) END",
    "CREATE OR REPLACE MACRO r_num(x) AS TRY_CAST(NULLIF(regexp_extract(x, '^[^0-9]*([0-9]{1,18})$', 1), '') AS BIGINT)",
    f"CREATE OR REPLACE MACRO r_status(x) AS MAP {{{_status}}}[r_tu(x)]",
    f"""CREATE OR REPLACE MACRO r_state(x) AS (
          SELECT CASE WHEN s IN ({_codes}) THEN s END
          FROM (SELECT CASE WHEN length(r_tu(x)) > 2 THEN MAP {{{_names}}}[r_tu(x)] ELSE r_tu(x) END AS s))""",
    "CREATE OR REPLACE MACRO r_cpt(x) AS CASE WHEN r_tu(x) IN ('', 'XXXXX', '9999') THEN NULL ELSE r_tu(x) END",
    """CREATE OR REPLACE MACRO r_icd(x) AS CASE
          WHEN r_cpt(x) NOT LIKE '%.%' AND length(r_cpt(x)) > 3
          THEN substr(r_cpt(x), 1, 3) || '.' || substr(r_cpt(x), 4) ELSE r_cpt(x) END""",
    """CREATE OR REPLACE MACRO r_amount(x) AS (
          SELECT CASE WHEN regexp_full_match(v, '[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)') THEN CAST(v AS DOUBLE) END
          FROM (SELECT trim(replace(replace(trim(x), '$', ''), ',', '')) AS v))""",
    """CREATE OR REPLACE MACRO r_date(x) AS (
          SELECT CASE WHEN year(d) BETWEEN 1900 AND 2100 THEN d END
          FROM (SELECT CAST(coalesce(try_strptime(trim(x), '%Y-%m-%d'), try_strptime(trim(x), '%m/%d/%Y'),
                                     try_strptime(trim(x), '%d-%b-%Y')) AS DATE) AS d))""",
    """CREATE OR REPLACE MACRO r_dob(x, claim_date, as_of) AS (
          SELECT CASE WHEN d = DATE '1900-01-01' OR d > CAST(as_of AS DATE) OR d > claim_date THEN NULL ELSE d END
          FROM (SELECT r_date(x) AS d))""",
]


HEADERS = {
    CLAIMS: CLAIM_COLS,
    MEMBERS: ["member_id", "first_name", "last_name", "gender", "dob", "state", "zip_code", "plan_type",
              "plan_start_date", "plan_end_date", "risk_score", "pcp_provider_id", "last_eligibility_date"],
    PROVIDERS: ["provider_id", "provider_name", "specialty", "npi", "state", "zip_code", "network_status",
                "contract_start_date", "contract_end_date", "fee_schedule", "group_id",
                "last_credentialing_date"],
}


def _csv(path, table):
    """Explicit layout (no sniffing); rows with the wrong number of fields are skipped,
    as the job does."""
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in HEADERS[table])
    return (f"read_csv('{path}', header=true, delim=',', quote='\"', escape='\"', "
            f"columns={{{cols}}}, ignore_errors=true)")


def build(con, data, as_of):
    for m in MACROS:
        con.execute(m)
    # Claims in file order: scan each part single-threaded so row_number() is the row's position.
    con.execute("SET threads = 1")
    con.execute("SET preserve_insertion_order = true")
    con.execute("CREATE OR REPLACE TEMP TABLE cl (part INT, rn BIGINT, claim_id VARCHAR, member_id VARCHAR, "
                "provider_id VARCHAR, claim_date VARCHAR, service_code VARCHAR, diagnosis_code VARCHAR, "
                "billed_amount VARCHAR, paid_amount VARCHAR, claim_status VARCHAR, state VARCHAR, "
                "member_dob VARCHAR, source_system VARCHAR)")
    for i, p in enumerate(parts(data, CLAIMS)):
        con.execute(f"INSERT INTO cl SELECT {i}, row_number() OVER (), * FROM {_csv(p, CLAIMS)}")
    con.execute("RESET threads")

    def ref(table, key, cols, k):
        sel = ", ".join(cols)
        files = " UNION ALL ".join(f"SELECT {i} AS part, * FROM {_csv(p, table)}" for i, p in enumerate(parts(data, table)))
        return f"""(SELECT r_num({key}) AS {k}, {sel} FROM ({files}) WHERE {k} IS NOT NULL
                    QUALIFY row_number() OVER (PARTITION BY {k} ORDER BY part) = 1)"""

    # Materialized, with a distinct key alias per table. A trap found while testing: written
    # inline with the same alias `k` in both subqueries, the provider subquery's `k` bound to
    # the member table's m.k (DuckDB lets a FROM subquery see earlier FROM items, i.e. an
    # implicit LATERAL join), so it kept one provider per member and silently lost rows.
    con.execute("CREATE OR REPLACE TEMP TABLE m_ref AS " + ref(MEMBERS, "member_id", [
        "first_name", "last_name", "gender", "plan_type", "TRY_CAST(risk_score AS DOUBLE) AS risk_score"], "mk")[1:-1])
    con.execute("CREATE OR REPLACE TEMP TABLE p_ref AS " + ref(PROVIDERS, "provider_id", [
        "provider_name", "specialty", "npi", "network_status"], "pk")[1:-1])
    m, p = "m_ref", "p_ref"
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE expected AS
WITH j AS (
  SELECT cl.part, cl.rn, r_id(cl.claim_id) AS cid, r_id(cl.member_id) AS mid, r_id(cl.provider_id) AS pid,
         cl.*, m.first_name, m.last_name, m.gender, m.plan_type, m.risk_score,
         p.provider_name, p.specialty, p.npi, p.network_status
  FROM cl
  JOIN {m} m ON m.mk = r_num(r_id(cl.member_id))
  JOIN {p} p ON p.pk = r_num(r_id(cl.provider_id))
  WHERE r_id(cl.claim_id) IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY cid ORDER BY cl.part, cl.rn) = 1
), f AS (
  SELECT cid AS claim_id, mid AS member_id, pid AS provider_id,
         r_date(claim_date) AS claim_date, r_cpt(service_code) AS service_code,
         r_icd(diagnosis_code) AS diagnosis_code,
         r_amount(billed_amount) AS billed_amount, r_amount(paid_amount) AS paid_amount,
         r_status(claim_status) AS claim_status, r_state(state) AS state,
         r_dob(member_dob, r_date(claim_date), '{as_of}') AS member_dob, NULLIF(source_system, '') AS source_system,
         NULLIF(first_name, '') AS member_first_name, NULLIF(last_name, '') AS member_last_name,
         NULLIF(gender, '') AS member_gender, NULLIF(plan_type, '') AS member_plan_type,
         risk_score AS member_risk_score, NULLIF(provider_name, '') AS provider_name,
         NULLIF(specialty, '') AS provider_specialty, NULLIF(npi, '') AS provider_npi,
         NULLIF(network_status, '') AS provider_network_status
  FROM j
)
SELECT *, coalesce(billed_amount < 0, false) AS flag_negative_billed,
          coalesce(paid_amount > billed_amount, false) AS flag_paid_exceeds_billed
FROM f""")


def main(data, job_out, as_of):
    t0 = time.perf_counter()
    con = duckdb.connect()
    build(con, data, as_of)
    print(f"[reference] expected rows: {con.execute('SELECT count(*) FROM expected').fetchone()[0]:,} "
          f"({time.perf_counter() - t0:.1f} s)")
    return report(*compare(con, "expected", f"read_parquet('{job_out}/*.parquet')"), t0)
