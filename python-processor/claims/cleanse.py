"""Port of java-processor Cleanse.java, in two forms that must agree.

* scalar functions (``id_``, ``status``, ...) for the pure-Python approach (P1)
* Arrow-compute functions (``a_id``, ``a_status``, ...) that work on whole columns (P4)

The DuckDB (P2) and Polars (P3) approaches express the same rules in their own
languages; tests/test_cleanse.py runs one table of dirty inputs through all four.

Parity, not perfection: the rules deliberately keep the Java gaps (ISO-only dates,
placeholder DOBs kept, negative amounts kept), see BLOG_NOTES section 20.

Empty strings are treated as null everywhere. Java gets the same effect because every
Cleanse function maps "" to null and ParquetOutput writes "" as null.
"""

import re

import pyarrow as pa
import pyarrow.compute as pc

STATE_NAME_TO_CODE = {
    "TEXAS": "TX", "CALIFORNIA": "CA", "FLORIDA": "FL", "NEW YORK": "NY",
    "ILLINOIS": "IL", "PENNSYLVANIA": "PA", "OHIO": "OH", "GEORGIA": "GA",
    "NORTH CAROLINA": "NC", "MICHIGAN": "MI",
}
STATUS_MAP = {"PAID": "PAID", "PD": "PAID", "DENIED": "DENIED", "D": "DENIED",
              "PENDING": "PENDING", "P": "PENDING"}
ID_NULLS = ("", "UNKNOWN", "N/A")
CODE_NULLS = ("", "XXXXX", "9999")

# Java Double.parseDouble accepts more than this (hex, "NaN", a trailing 'd'), but none of
# those shapes occur in the data; this covers every amount pattern that does.
_AMOUNT_RE = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$"
_ISO_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
# OnHeapJoinTable.parseId: skip leading non-digits, then the rest must be all digits.
_ID_RE = r"^[^0-9]*([0-9]{1,18})$"

_amount_re = re.compile(_AMOUNT_RE)
_iso_re = re.compile(_ISO_RE)
_id_re = re.compile(_ID_RE)


# ---------------------------------------------------------------- scalar (P1)

def id_(raw):
    if raw is None:
        return None
    v = raw.strip().upper()
    return None if v in ID_NULLS else v


def status(raw):
    if raw is None:
        return None
    return STATUS_MAP.get(raw.strip().upper())


def state(raw):
    if raw is None:
        return None
    v = raw.strip().upper()
    if len(v) > 2:
        v = STATE_NAME_TO_CODE.get(v)
    if v is None or len(v) != 2 or not ("A" <= v[0] <= "Z" and "A" <= v[1] <= "Z") or v == "XX":
        return None
    return v


def amount(raw):
    if raw is None:
        return None
    v = raw.strip().replace("$", "").replace(",", "").strip()
    if not _amount_re.match(v):
        return None
    return float(v)


def iso_date(raw):
    if raw is None:
        return None
    v = raw.strip()
    if not _iso_re.match(v):
        return None
    y, m, d = int(v[0:4]), int(v[5:7]), int(v[8:10])
    if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
        return v
    return None


def code(raw):
    if raw is None:
        return None
    v = raw.strip().upper()
    return None if v in CODE_NULLS else v


def parse_id(v):
    """Numeric part of an id (MBR000000123 -> 123), or -1 if it has no clean digit tail."""
    if v is None:
        return -1
    m = _id_re.match(v)
    return int(m.group(1)) if m else -1


# ---------------------------------------------------------------- Arrow (P4)

_NULL_STR = pa.scalar(None, pa.string())
_STATUS_KEYS = pa.array(list(STATUS_MAP))
_STATUS_VALS = pa.array(list(STATUS_MAP.values()))
_STATE_KEYS = pa.array(list(STATE_NAME_TO_CODE))
_STATE_VALS = pa.array(list(STATE_NAME_TO_CODE.values()))
_ID_NULLS = pa.array(ID_NULLS)
_CODE_NULLS = pa.array(CODE_NULLS)


def _trim_upper(a):
    return pc.utf8_upper(pc.utf8_trim_whitespace(a))


def _null_where(cond, a):
    return pc.if_else(cond, _NULL_STR, a)


def a_id(a):
    t = _trim_upper(a)
    return _null_where(pc.is_in(t, value_set=_ID_NULLS), t)


def a_status(a):
    return pc.take(_STATUS_VALS, pc.index_in(_trim_upper(a), value_set=_STATUS_KEYS))


def a_state(a):
    t = _trim_upper(a)
    mapped = pc.take(_STATE_VALS, pc.index_in(t, value_set=_STATE_KEYS))
    v = pc.if_else(pc.greater(pc.utf8_length(t), 2), mapped, t)
    ok = pc.and_(pc.match_substring_regex(v, "^[A-Z]{2}$"), pc.not_equal(v, "XX"))
    return pc.if_else(ok, v, _NULL_STR)


def a_amount(a):
    v = pc.utf8_trim_whitespace(a)
    v = pc.replace_substring(v, "$", "")
    v = pc.replace_substring(v, ",", "")
    v = pc.utf8_trim_whitespace(v)
    v = pc.if_else(pc.match_substring_regex(v, _AMOUNT_RE), v, _NULL_STR)
    return pc.cast(v, pa.float64())


def _between(x, lo, hi):
    return pc.and_(pc.greater_equal(x, lo), pc.less_equal(x, hi))


def a_iso_date(a):
    t = pc.utf8_trim_whitespace(a)
    t = pc.if_else(pc.match_substring_regex(t, _ISO_RE), t, _NULL_STR)
    y = pc.cast(pc.utf8_slice_codeunits(t, 0, 4), pa.int32())
    m = pc.cast(pc.utf8_slice_codeunits(t, 5, 7), pa.int32())
    d = pc.cast(pc.utf8_slice_codeunits(t, 8, 10), pa.int32())
    ok = pc.and_(pc.and_(_between(y, 1900, 2100), _between(m, 1, 12)), _between(d, 1, 31))
    return pc.if_else(ok, t, _NULL_STR)


def a_code(a):
    t = _trim_upper(a)
    return _null_where(pc.is_in(t, value_set=_CODE_NULLS), t)


def a_parse_id(a):
    """Int64 id numbers; null where Java's parseId would return -1."""
    digits = pc.struct_field(pc.extract_regex(a, _ID_RE.replace("([0-9]", "(?P<d>[0-9]")), [0])
    return pc.cast(digits, pa.int64())
