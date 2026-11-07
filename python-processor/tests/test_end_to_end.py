"""End to end on a tiny hand-made dataset with every kind of dirt, plus the failure path.

The expected output is written out by hand below, row by row, so this test does not
depend on any other implementation. The reference SQL is then checked against it too.
"""

import datetime as dt
import gzip

import duckdb
import pyarrow.parquet as pq
import pytest

from claims import reference, singlepass
from claims.common import OUTPUT_COLUMNS
from claims.verify import compare

AS_OF = "2026-11-07"
D = dt.date

MEMBER_HDR = ("member_id,first_name,last_name,gender,dob,state,zip_code,plan_type,plan_start_date,"
              "plan_end_date,risk_score,pcp_provider_id,last_eligibility_date")
PROVIDER_HDR = ("provider_id,provider_name,specialty,npi,state,zip_code,network_status,"
                "contract_start_date,contract_end_date,fee_schedule,group_id,last_credentialing_date")
CLAIM_HDR = ("claim_id,member_id,provider_id,claim_date,service_code,diagnosis_code,billed_amount,"
             "paid_amount,claim_status,state,member_dob,source_system")
M_TAIL = ",x,TX,11111,{plan},2020-01-01,2025-01-01,{risk},PRV000000001,2024-01-01"
P_TAIL = ",TX,11111,{net},2020-01-01,2025-01-01,MEDICARE_120,GRP0000001,2024-01-01"

DATA = {
    "member_enrichment": [
        [f"MBR000000001,ANNA,SMITH,F" + M_TAIL.format(plan="HMO", risk="1.25"),
         f"MBR000000002,BOB,JONES,M" + M_TAIL.format(plan="PPO", risk="2.50")],
        [f"MBR000000002,ZED,ZULU,M" + M_TAIL.format(plan="EPO", risk="9.99"),   # duplicate id: part 0 wins
         f"MBR000000003,CARL,LEE,M" + M_TAIL.format(plan="HMO", risk="")],
    ],
    "provider_reference": [
        ["PRV000000001,SMITH CARDIOLOGY CLINIC,CARDIOLOGY,1234567890" + P_TAIL.format(net="IN_NETWORK"),
         "PRV000000002,,DERMATOLOGY,1987654321" + P_TAIL.format(net="OUT_OF_NETWORK")],
    ],
    "raw_claims": [
        [
            'CLM00000000001,MBR000000001,PRV000000001,2024-01-15,99213,E119,"$1,234.50",1000.00,paid,Texas,01/02/1980,SRC_A',
            "CLM00000000001,MBR000000002,PRV000000002,15-Jan-2024,99213,K30,10,5,PAID,TX,1980-01-01,SRC_A",  # dup (adjacent)
            " clm00000000002 ,mbr000000002,PRV000000002,02/29/2024,j0696,i10,-50.00,10.00,D,ny,1900-01-01,SRC_B",
            "CLM00000000003,MBR000000003,PRV000000001,2023-02-29,XXXXX,9999,,,P,XX,2099-12-31,SRC_C",
            "CLM00000000004,UNKNOWN,PRV000000001,2024-01-01,99213,K30,1,1,PAID,TX,1980-01-01,SRC_A",        # bad key
            "CLM00000000005,MBR000000009,PRV000000001,2024-01-01,99213,K30,1,1,PAID,TX,1980-01-01,SRC_A",   # no member
            "CLM00000000006,MBR000000001,PRV000000009,2024-01-01,99213,K30,1,1,PAID,TX,1980-01-01,SRC_A",   # no provider
            ",MBR000000001,PRV000000001,2024-01-01,99213,K30,1,1,PAID,TX,1980-01-01,SRC_A",                # no claim id
            "CLM00000000007,MBR000000001",                                                                   # malformed
            "CLM00000000008,MBR000000001,PRV000000001,2024-03-01,99214,J45909,100.00,150.00,PD,ZZ,2024-03-02,SRC_D",
        ],
        [
            "CLM00000000008,MBR000000002,PRV000000001,2024-03-05,99213,K30,1,1,PAID,TX,1980-01-01,SRC_A",   # dup across parts
            "CLM00000000009,MBR000000002,PRV000000001,2024-13-45,99213,k30,10,5,PENDING,California,1975-06-30,SRC_E",
        ],
    ],
}
HEADERS = {"member_enrichment": MEMBER_HDR, "provider_reference": PROVIDER_HDR, "raw_claims": CLAIM_HDR}

_ANNA = ("ANNA", "SMITH", "F", "HMO", 1.25)
_BOB = ("BOB", "JONES", "M", "PPO", 2.5)
_CARL = ("CARL", "LEE", "M", "HMO", None)
_P1 = ("SMITH CARDIOLOGY CLINIC", "CARDIOLOGY", "1234567890", "IN_NETWORK")
_P2 = (None, "DERMATOLOGY", "1987654321", "OUT_OF_NETWORK")
EXPECTED = [
    ("CLM00000000001", "MBR000000001", "PRV000000001", D(2024, 1, 15), "99213", "E11.9", 1234.5, 1000.0,
     "PAID", "TX", D(1980, 1, 2), "SRC_A", *_ANNA, *_P1, False, False),
    ("CLM00000000002", "MBR000000002", "PRV000000002", D(2024, 2, 29), "J0696", "I10", -50.0, 10.0,
     "DENIED", "NY", None, "SRC_B", *_BOB, *_P2, True, True),
    ("CLM00000000003", "MBR000000003", "PRV000000001", None, None, None, None, None,
     "PENDING", None, None, "SRC_C", *_CARL, *_P1, False, False),
    ("CLM00000000008", "MBR000000001", "PRV000000001", D(2024, 3, 1), "99214", "J45.909", 100.0, 150.0,
     "PAID", None, None, "SRC_D", *_ANNA, *_P1, False, True),
    ("CLM00000000009", "MBR000000002", "PRV000000001", None, "99213", "K30", 10.0, 5.0,
     "PENDING", "CA", D(1975, 6, 30), "SRC_E", *_BOB, *_P1, False, False),
]


def write_dataset(root, data=DATA, corrupt=None):
    for table, part_rows in data.items():
        d = root / table
        d.mkdir(parents=True)
        for i, rows in enumerate(part_rows):
            path = d / f"part-{i:05d}.csv.gz"
            if (table, i) == corrupt:
                path.write_bytes(b"\x1f\x8bnot really gzip" * 100)
                continue
            with gzip.open(path, "wt", newline="") as f:
                f.write("\n".join([HEADERS[table], *rows]) + "\n")
    return root


@pytest.fixture
def tiny(tmp_path):
    return write_dataset(tmp_path / "data")


def _rows(out):
    t = pq.read_table(out)
    return sorted(tuple(r[c] for c in OUTPUT_COLUMNS) for r in t.to_pylist())


def test_output_matches_hand_written_expectation(tiny, tmp_path):
    stats = singlepass.main(tiny, tmp_path / "out", threads=2, as_of=AS_OF, capacity=16, quiet=True)
    assert _rows(tmp_path / "out") == EXPECTED
    assert (stats.read, stats.written) == (12, 5)
    assert stats.dropped() == {"malformed": 1, "no_claim_id": 1, "bad_key": 1, "member_not_found": 1,
                               "provider_not_found": 1, "duplicate_claim_id": 2}
    assert not list((tmp_path / "out").glob("*.inprogress"))


def test_output_schema(tiny, tmp_path):
    singlepass.main(tiny, tmp_path / "out", threads=1, as_of=AS_OF, capacity=16, quiet=True)
    schema = pq.read_schema(tmp_path / "out" / "part-00000-000.parquet")
    assert schema.names == OUTPUT_COLUMNS
    assert str(schema.field("claim_date").type) == "date32[day]"
    assert str(schema.field("flag_negative_billed").type) == "bool"
    # the part rewritten by the cross-part dedupe keeps the same schema
    assert pq.read_schema(tmp_path / "out" / "part-00001-000.parquet") == schema


def test_reference_sql_agrees_on_tiny_data(tiny, tmp_path):
    singlepass.main(tiny, tmp_path / "out", threads=2, as_of=AS_OF, capacity=16, quiet=True)
    con = duckdb.connect()
    reference.build(con, tiny, AS_OF)
    na, nb, oa, ob = compare(con, "expected", f"read_parquet('{tmp_path / 'out'}/*.parquet')", show=0)
    assert (na, nb, oa, ob) == (5, 5, 0, 0)


def test_part_too_large_fails_loudly(tiny, tmp_path):
    with pytest.raises(ValueError, match="raise PART_CAPACITY"):
        singlepass.main(tiny, tmp_path / "out", threads=1, as_of=AS_OF, capacity=1, quiet=True)


def test_failed_part_leaves_no_partial_files(tmp_path):
    data = write_dataset(tmp_path / "data", corrupt=("raw_claims", 1))
    with pytest.raises(Exception):
        singlepass.main(data, tmp_path / "out", threads=2, as_of=AS_OF, capacity=16, quiet=True)
    assert not list((tmp_path / "out").glob("*.inprogress"))
    assert not list((tmp_path / "out").glob("part-00001-*"))


def test_refuses_to_overwrite_previous_output(tiny, tmp_path):
    singlepass.main(tiny, tmp_path / "out", threads=1, as_of=AS_OF, capacity=16, quiet=True)
    with pytest.raises(SystemExit, match="already has part files"):
        singlepass.main(tiny, tmp_path / "out", threads=1, as_of=AS_OF, capacity=16, quiet=True)


def test_many_small_files_and_targeted_cross_part_rewrite(tiny, tmp_path, monkeypatch, capsys):
    # tiny batches and one row per file: rotation, per-file key ranges, and the cross-part
    # dedup must rewrite only the file holding the duplicate (CLM00000000008 in part 1)
    monkeypatch.setattr(singlepass, "BLOCK_SIZE", 256)
    monkeypatch.setattr(singlepass, "ROW_GROUP_ROWS", 1)
    monkeypatch.setattr(singlepass, "FILE_ROWS", 1)
    stats = singlepass.main(tiny, tmp_path / "out", threads=2, as_of=AS_OF, capacity=16)
    assert _rows(tmp_path / "out") == EXPECTED
    assert stats.duplicate_claim_id == 2
    files = sorted(p.name for p in (tmp_path / "out").glob("*.parquet"))
    assert len(files) >= 5 and files[0] == "part-00000-000.parquet"
    assert "1 duplicate ids, 1 rows removed, 1 files rewritten" in capsys.readouterr().out
