"""One table of dirty inputs -> expected outputs (from the Java Cleanse rules).

The scalar, Arrow, DuckDB and Polars forms must all produce the expected value.
"""

import duckdb
import polars as pl
import pyarrow as pa
import pytest

from claims import cleanse as c
from claims.p2_duckdb import MACROS
from claims.p3_polars import CLEANSE as PL

CASES = {
    "id": [
        ("CLM00000000001", "CLM00000000001"), (" clm00000000002 ", "CLM00000000002"),
        ("UNKNOWN", None), (" n/a", None), ("", None), (None, None),
    ],
    "status": [
        ("PAID", "PAID"), ("paid", "PAID"), ("Paid ", "PAID"), ("PD", "PAID"),
        ("D", "DENIED"), ("DENIED", "DENIED"), ("P", "PENDING"), ("PENDING", "PENDING"),
        ("X", None), ("", None), (None, None),
    ],
    "state": [
        ("TX", "TX"), ("tx", "TX"), (" ny ", "NY"), ("Texas", "TX"), ("North Carolina", "NC"),
        ("Nevada", None), ("XX", None), ("X", None), ("T1", None), ("", None), (None, None),
    ],
    "amount": [
        ("3560.44", 3560.44), ("$4,703.89", 4703.89), ("$92.05", 92.05), ("-1160.62", -1160.62),
        ("1742.5", 1742.5), (" 12 ", 12.0), ("+5", 5.0), ("5.", 5.0), (".5", 0.5), ("1e3", 1000.0),
        ("abc", None), ("$", None), ("", None), ("4046.84\"", None), (None, None),
    ],
    "iso_date": [
        ("2024-01-15", "2024-01-15"), (" 2024-01-15 ", "2024-01-15"), ("1900-01-01", "1900-01-01"),
        ("2099-12-31", "2099-12-31"), ("2024-02-31", "2024-02-31"),   # Java does no month-length check
        ("2024-13-45", None), ("1899-12-31", None), ("2101-01-01", None),
        ("01/15/2024", None), ("15-Jan-2024", None), ("2024-1-15", None), ("", None), (None, None),
    ],
    "code": [
        ("99213", "99213"), (" 99284 ", "99284"), ("j0696", "J0696"), ("E11.9", "E11.9"),
        ("XXXXX", None), ("9999", None), ("", None), (None, None),
    ],
    "parse_id": [
        ("MBR000000123", 123), ("PRV250000001", 250000001), ("123", 123),
        ("MBR12A3", None), ("MBR", None), ("UNKNOWN", None), (None, None),
    ],
}
SCALAR = {"id": c.id_, "status": c.status, "state": c.state, "amount": c.amount,
          "iso_date": c.iso_date, "code": c.code,
          "parse_id": lambda v: (lambda k: None if k < 0 else k)(c.parse_id(v))}
ARROW = {"id": c.a_id, "status": c.a_status, "state": c.a_state, "amount": c.a_amount,
         "iso_date": c.a_iso_date, "code": c.a_code, "parse_id": c.a_parse_id}
SQL = {"id": "c_id", "status": "c_status", "state": "c_state", "amount": "c_amount",
       "iso_date": "c_iso_date", "code": "c_code", "parse_id": "c_parse_id"}

PARAMS = [(fn, raw, want) for fn, cases in CASES.items() for raw, want in cases]


@pytest.mark.parametrize("fn,raw,want", PARAMS)
def test_scalar(fn, raw, want):
    assert SCALAR[fn](raw) == want


@pytest.mark.parametrize("fn", list(CASES))
def test_arrow(fn):
    raws, wants = zip(*CASES[fn])
    assert ARROW[fn](pa.array(raws, pa.string())).to_pylist() == list(wants)


@pytest.fixture(scope="module")
def con():
    con = duckdb.connect()
    for m in MACROS:
        con.execute(m)
    return con


@pytest.mark.parametrize("fn", list(CASES))
def test_duckdb(con, fn):
    raws, wants = zip(*CASES[fn])
    got = [r[0] for r in con.execute(
        f"SELECT {SQL[fn]}(x) FROM (SELECT unnest(?::VARCHAR[]) AS x, generate_subscripts(?::VARCHAR[], 1) AS i) ORDER BY i",
        [list(raws), list(raws)]).fetchall()]
    assert got == list(wants)


@pytest.mark.parametrize("fn", list(CASES))
def test_polars(fn):
    raws, wants = zip(*CASES[fn])
    got = pl.DataFrame({"x": list(raws)}, schema={"x": pl.String}).select(PL[fn](pl.col("x")))
    assert got.to_series().to_list() == list(wants)
