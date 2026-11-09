"""Cleansing rules for the dirty claims feed, as vectorized Arrow/NumPy functions.

Every function takes a string column and returns a cleaned column; no per-row Python.

Rules (what the generator's dirt looks like -> what comes out):
  ids        ' clm00000000002 ', 'UNKNOWN', 'N/A'      -> trimmed upper-case, or null
  dates      2024-01-15 | 01/15/2024 | 15-Jan-2024     -> date32; impossible dates
             (2024-13-45, 2023-02-29) -> null. Checked against the real calendar:
             Arrow's strptime silently rolls 2023-02-29 over to 2023-03-01.
  amounts    '$4,703.89', '-50.00', ''                 -> float64 (negatives kept and flagged)
  status     PAID/paid/Paid /PD, DENIED/D, PENDING/P   -> PAID | DENIED | PENDING, else null
  state      'TX', 'tx', 'Texas', 'XX', 'ZZ'           -> USPS code (50 states + DC), else null
  CPT code   ' 99284 ', 'j0696', 'XXXXX'               -> upper-case, placeholders -> null
  ICD-10     'E119', 'e11.9', '9999'                   -> dotted upper-case 'E11.9', else null
  DOB        placeholder 1900-01-01, future dates, or after the claim date -> null
"""

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL",
    "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA",
    "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA",
    "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
}
STATE_CODES = sorted(STATE_NAME_TO_CODE.values())
STATUS_MAP = {"PAID": "PAID", "PD": "PAID", "DENIED": "DENIED", "D": "DENIED",
              "PENDING": "PENDING", "P": "PENDING"}
ID_NULLS = ("", "UNKNOWN", "N/A")
CODE_NULLS = ("", "XXXXX", "9999")
PLACEHOLDER_DOB = "1900-01-01"
MIN_YEAR, MAX_YEAR = 1900, 2100

AMOUNT_RE = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$"
ID_RE = r"^[^0-9]*(?P<n>[0-9]{1,18})$"     # numeric tail of an id: MBR000000123 -> 123
DATE_FORMATS = {   # name -> regex with named groups; month is a number or a 3-letter name
    "ymd": r"^(?P<y>[0-9]{4})-(?P<m>[0-9]{1,2})-(?P<d>[0-9]{1,2})$",
    "mdy": r"^(?P<m>[0-9]{1,2})/(?P<d>[0-9]{1,2})/(?P<y>[0-9]{4})$",
    "dmy": r"^(?P<d>[0-9]{1,2})-(?P<b>[A-Za-z]{3})-(?P<y>[0-9]{4})$",
}
_MONTH_NAMES = pa.array(["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                         "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])
_DAYS_IN_MONTH = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])

_NULL_STR = pa.scalar(None, pa.string())
_STATUS_KEYS, _STATUS_VALS = pa.array(list(STATUS_MAP)), pa.array(list(STATUS_MAP.values()))
_STATE_KEYS = pa.array(list(STATE_NAME_TO_CODE))
_STATE_VALS = pa.array(list(STATE_NAME_TO_CODE.values()))
_STATE_CODES = pa.array(STATE_CODES)
_ID_NULLS, _CODE_NULLS = pa.array(ID_NULLS), pa.array(CODE_NULLS)


def _trim_upper(a):
    return pc.utf8_upper(pc.utf8_trim_whitespace(a))


def _null_where(cond, a):
    return pc.if_else(cond, _NULL_STR, a)


def _np(a, fill):
    return pc.fill_null(a, fill).to_numpy(zero_copy_only=False)


# ---------------------------------------------------------------- ids and codes

def ident(a):
    t = _trim_upper(a)
    return _null_where(pc.is_in(t, value_set=_ID_NULLS), t)


def parse_id(a):
    """Int64 id numbers (MBR000000123 -> 123); null when there is no clean digit tail."""
    return pc.cast(pc.struct_field(pc.extract_regex(a, ID_RE), [0]), pa.int64())


def status(a):
    return pc.take(_STATUS_VALS, pc.index_in(_trim_upper(a), value_set=_STATUS_KEYS))


def state(a):
    t = _trim_upper(a)
    named = pc.take(_STATE_VALS, pc.index_in(t, value_set=_STATE_KEYS))
    v = pc.if_else(pc.greater(pc.utf8_length(t), 2), named, t)
    return pc.if_else(pc.is_in(v, value_set=_STATE_CODES), v, _NULL_STR)


def cpt(a):
    t = _trim_upper(a)
    return _null_where(pc.is_in(t, value_set=_CODE_NULLS), t)


def icd10(a):
    """Upper-case, placeholders -> null, and restore the dot: E119 -> E11.9, J45909 -> J45.909."""
    t = cpt(a)
    undotted = pc.and_(pc.invert(pc.match_substring(t, ".")), pc.greater(pc.utf8_length(t), 3))
    dotted = pc.binary_join_element_wise(pc.utf8_slice_codeunits(t, 0, 3),
                                         pc.utf8_slice_codeunits(t, 3), ".")
    return pc.if_else(undotted, dotted, t)


# ---------------------------------------------------------------- amounts

def amount(a):
    v = pc.utf8_trim_whitespace(a)
    v = pc.replace_substring(pc.replace_substring(v, "$", ""), ",", "")
    v = pc.utf8_trim_whitespace(v)
    v = pc.if_else(pc.match_substring_regex(v, AMOUNT_RE), v, _NULL_STR)
    return pc.cast(v, pa.float64())


# ---------------------------------------------------------------- dates

def days_from_civil(y, m, d):
    """Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's algorithm)."""
    y = y - (m <= 2)
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _valid_ymd(y, m, d):
    ok = (y >= MIN_YEAR) & (y <= MAX_YEAR) & (m >= 1) & (m <= 12) & (d >= 1)
    mi = np.where(ok, m, 1) - 1
    leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0))
    return ok & (d <= _DAYS_IN_MONTH[mi] + ((mi == 1) & leap))


def date(a, formats=("ymd", "mdy", "dmy")):
    """date32 column from any of the given formats; impossible dates -> null."""
    t = pc.utf8_trim_whitespace(a)
    n = len(t)
    y, m, d = (np.zeros(n, np.int64) for _ in range(3))
    found = np.zeros(n, bool)
    for f in formats:
        s = pc.extract_regex(t, DATE_FORMATS[f])
        hit = _np(pc.is_valid(s), False) & ~found
        if not hit.any():
            continue
        fields = [x.name for x in s.type]
        part = lambda name: _np(pc.cast(pc.struct_field(s, [fields.index(name)]), pa.int64()), 0)
        if "b" in fields:
            mon = pc.add(pc.index_in(pc.utf8_upper(pc.struct_field(s, [fields.index("b")])),
                                     value_set=_MONTH_NAMES), 1)
            mm = _np(pc.cast(mon, pa.int64()), 0)
        else:
            mm = part("m")
        y[hit], m[hit], d[hit] = part("y")[hit], mm[hit], part("d")[hit]
        found |= hit
    ok = found & _valid_ymd(y, m, d)
    days = np.where(ok, days_from_civil(y, m, d), 0).astype(np.int32)
    return pa.array(days, mask=~ok).view(pa.date32())


def _date_scalar_days(iso):
    y, m, d = (int(x) for x in iso.split("-"))
    return int(days_from_civil(np.int64(y), np.int64(m), np.int64(d)))


def dob(a, claim_date, as_of):
    """Member DOB: parsed like any date, then the generator's junk is removed:
    the 1900-01-01 placeholder, dates after `as_of` (e.g. 2099-12-31), and dates after the claim."""
    v = date(a)
    days = _np(pc.cast(v, pa.int32()), 0)
    claim = _np(pc.cast(claim_date, pa.int32()), np.iinfo(np.int32).max)
    bad = (days == _date_scalar_days(PLACEHOLDER_DOB)) | (days > _date_scalar_days(as_of)) | (days > claim)
    return pa.array(days, mask=_np(pc.is_null(v), True) | bad).view(pa.date32())


# ---------------------------------------------------------------- flags

def flag_negative(billed):
    return pc.fill_null(pc.less(billed, 0.0), False)


def flag_paid_exceeds(paid, billed):
    return pc.fill_null(pc.greater(paid, billed), False)
