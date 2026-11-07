"""Dirty inputs -> expected outputs. Each case runs through the job's Arrow functions AND
the independent reference SQL (reference.py), which must both match."""

import datetime as dt

import duckdb
import pyarrow as pa
import pytest

from claims import cleanse as c
from claims.reference import MACROS

D = dt.date

CASES = {
    "ident": [
        ("CLM00000000001", "CLM00000000001"), (" clm00000000002 ", "CLM00000000002"),
        ("UNKNOWN", None), (" n/a", None), ("", None), (None, None),
    ],
    "parse_id": [
        ("MBR000000123", 123), ("PRV250000001", 250000001), ("123", 123),
        ("MBR12A3", None), ("MBR", None), ("UNKNOWN", None), (None, None),
    ],
    "status": [
        ("PAID", "PAID"), ("paid", "PAID"), ("Paid ", "PAID"), ("PD", "PAID"),
        ("D", "DENIED"), ("DENIED", "DENIED"), ("P", "PENDING"), ("PENDING", "PENDING"),
        ("X", None), ("", None), (None, None),
    ],
    "state": [
        ("TX", "TX"), ("tx", "TX"), (" ny ", "NY"), ("Texas", "TX"), ("North Carolina", "NC"),
        ("Nevada", "NV"), ("district of columbia", "DC"), ("XX", None), ("ZZ", None),
        ("Atlantis", None), ("X", None), ("T1", None), ("", None), (None, None),
    ],
    "cpt": [
        ("99213", "99213"), (" 99284 ", "99284"), ("j0696", "J0696"),
        ("XXXXX", None), ("9999", None), ("", None), (None, None),
    ],
    "icd10": [
        ("E11.9", "E11.9"), ("E119", "E11.9"), ("e11.9", "E11.9"), ("J45909", "J45.909"),
        ("i2510", "I25.10"), ("Z0000", "Z00.00"), ("K30", "K30"), ("I10", "I10"),
        ("9999", None), ("", None), (None, None),
    ],
    "amount": [
        ("3560.44", 3560.44), ("$4,703.89", 4703.89), ("$92.05", 92.05), ("-1160.62", -1160.62),
        ("1742.5", 1742.5), (" 12 ", 12.0), ("+5", 5.0), ("5.", 5.0), (".5", 0.5),
        ("abc", None), ("1e3", None), ("$", None), ("", None), (None, None),
    ],
    "date": [
        ("2024-01-15", D(2024, 1, 15)), (" 2024-01-15 ", D(2024, 1, 15)),
        ("01/15/2024", D(2024, 1, 15)), ("15-Jan-2024", D(2024, 1, 15)), ("15-SEP-2024", D(2024, 9, 15)),
        ("2024-02-29", D(2024, 2, 29)), ("02/29/2024", D(2024, 2, 29)), ("2000-02-29", D(2000, 2, 29)),
        ("2023-02-29", None), ("1900-02-29", None), ("2024-02-30", None), ("2024-04-31", None),
        ("31-Apr-2024", None), ("2024-13-45", None), ("13/01/2024", None), ("15-Foo-2024", None),
        ("1899-12-31", None), ("2101-01-01", None), ("1900-01-01", D(1900, 1, 1)),
        ("2024/01/15", None), ("", None), (None, None),
    ],
}
ARROW = {"ident": c.ident, "parse_id": c.parse_id, "status": c.status, "state": c.state,
         "cpt": c.cpt, "icd10": c.icd10, "amount": c.amount, "date": c.date}
SQL = {"ident": "r_id", "parse_id": "r_num", "status": "r_status", "state": "r_state",
       "cpt": "r_cpt", "icd10": "r_icd", "amount": "r_amount", "date": "r_date"}

# (dob, claim_date, expected) with as_of = 2026-11-07
DOB_CASES = [
    ("1980-05-17", D(2024, 1, 1), D(1980, 5, 17)),
    ("05/17/1980", D(2024, 1, 1), D(1980, 5, 17)),
    ("1980-05-17", None, D(1980, 5, 17)),
    ("1900-01-01", D(2024, 1, 1), None),           # placeholder
    ("2099-12-31", None, None),                    # after as_of
    ("2026-11-08", None, None),                    # one day after as_of
    ("2026-11-07", None, D(2026, 11, 7)),
    ("2024-03-02", D(2024, 3, 1), None),           # born after the claim
    ("2024-03-01", D(2024, 3, 1), D(2024, 3, 1)),
    ("1980-02-30", D(2024, 1, 1), None),
    (None, D(2024, 1, 1), None),
]
AS_OF = "2026-11-07"


@pytest.fixture(scope="module")
def con():
    con = duckdb.connect()
    for m in MACROS:
        con.execute(m)
    return con


def _sql(con, fn, values, typ="VARCHAR"):
    rows = con.execute(f"SELECT {fn}(x) FROM (SELECT unnest(?::{typ}[]) AS x, "
                       f"generate_subscripts(?::{typ}[], 1) AS i) ORDER BY i", [values, values]).fetchall()
    return [r[0] for r in rows]


@pytest.mark.parametrize("fn", list(CASES))
def test_arrow(fn):
    raws, wants = zip(*CASES[fn])
    assert ARROW[fn](pa.array(raws, pa.string())).to_pylist() == list(wants)


@pytest.mark.parametrize("fn", list(CASES))
def test_reference_sql(con, fn):
    raws, wants = zip(*CASES[fn])
    assert _sql(con, SQL[fn], list(raws)) == list(wants)


def test_dob_arrow():
    raws, claims, wants = zip(*DOB_CASES)
    got = c.dob(pa.array(raws, pa.string()), pa.array(claims, pa.date32()), AS_OF)
    assert got.to_pylist() == list(wants)


def test_dob_reference_sql(con):
    got = [con.execute(f"SELECT r_dob(?::VARCHAR, ?::DATE, '{AS_OF}')", [r, cd]).fetchone()[0]
           for r, cd, _ in DOB_CASES]
    assert got == [w for *_, w in DOB_CASES]


def test_flags():
    billed = pa.array([-5.0, 10.0, 10.0, None, 10.0])
    paid = pa.array([1.0, 12.0, 8.0, 5.0, None])
    assert c.flag_negative(billed).to_pylist() == [True, False, False, False, False]
    assert c.flag_paid_exceeds(paid, billed).to_pylist() == [True, True, False, False, False]


def test_days_from_civil_matches_python():
    import numpy as np
    days = [D(1900, 1, 1), D(1970, 1, 1), D(2000, 2, 29), D(2024, 12, 31), D(2100, 12, 31)]
    y, m, d = (np.array([getattr(x, f) for x in days]) for f in ("year", "month", "day"))
    assert c.days_from_civil(y, m, d).tolist() == [(x - D(1970, 1, 1)).days for x in days]
