"""Reference-table encoders: round-trips, and loud failures when the data breaks their assumptions."""

import numpy as np
import pyarrow as pa
import pytest

from claims.singlepass import (DictColumn, NpiColumn, RiskColumn, SeenClaims,
                               cross_part_duplicates)


def test_dict_column_round_trip_shares_codes_across_batches():
    col = DictColumn("x")
    a = col.encode(pa.array(["B", "A", None, "B"]))
    b = col.encode(pa.array(["C", "A"]))
    col.finish(np.concatenate([a, b]))
    assert col.take(np.arange(6)).to_pylist() == ["B", "A", None, "B", "C", "A"]
    assert col.codes.dtype == np.int8


def test_dict_column_fails_when_too_many_values():
    col = DictColumn("x")                                  # int8: at most 128 values
    with pytest.raises(ValueError, match="more than 128 distinct"):
        col.encode(pa.array([f"v{i}" for i in range(200)]))
    wide = DictColumn("x", np.int16)
    wide.encode(pa.array([f"v{i}" for i in range(200)]))  # fine


def test_risk_round_trips_exactly():
    col = RiskColumn("risk")
    vals = ["0.00", "3.29", "9.99", None, "1.10"]
    col.finish(col.encode(pa.array(vals)))
    assert col.take(np.arange(5)).to_pylist() == [0.0, 3.29, 9.99, None, 1.1]


@pytest.mark.parametrize("bad", ["3.291", "-1.00", "700.00"])
def test_risk_fails_on_unexpected_shape(bad):
    with pytest.raises(ValueError, match="hundredths"):
        RiskColumn("risk").encode(pa.array(["1.00", bad]))


def test_npi_round_trips_and_rejects_leading_zero():
    col = NpiColumn("npi")
    col.finish(col.encode(pa.array(["1548493043", None])))
    assert col.take(np.arange(2)).to_pylist() == ["1548493043", None]
    with pytest.raises(ValueError, match="round-trip"):
        NpiColumn("npi").encode(pa.array(["0123456789"]))


def test_seen_claims_keeps_first_occurrence_across_batches():
    s = SeenClaims()
    k1 = s.keep_first(pa.array(["CLM00000000001", "CLM00000000001", "CLM00000000002", "ODD-1", "ODD-1"]))
    k2 = s.keep_first(pa.array(["CLM00000000002", "CLM00000000003", "ODD-1", "ODD-2"]))
    assert k1.tolist() == [True, False, True, True, False]
    assert k2.tolist() == [False, True, False, True]


def test_cross_part_duplicates_are_assigned_to_the_later_part():
    parts = []
    for ids in (["CLM00000000001", "CLM00000000002", "X"],
                ["CLM00000000002", "CLM00000003000", "X"],
                ["CLM00000003000", "CLM00002000000"]):
        s = SeenClaims()
        s.keep_first(pa.array(ids))
        parts.append(s.packed())
    dups = cross_part_duplicates(parts)
    assert sorted(dups) == [1, 2]
    assert dups[1][0].tolist() == [2] and dups[1][1] == {"X"}
    assert dups[2][0].tolist() == [3000] and dups[2][1] == set()
