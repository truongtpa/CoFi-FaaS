#!/usr/bin/env python3
"""
TPC-H Parameter Extractor
Extracts substitution parameters from qgen-generated SQL files.

Supports both standard qgen output and Spark SQL / DuckDB variants:
  • date '1995-03-17'           (standard)
  • CAST('1995-03-17' AS DATE)  (Spark SQL / DuckDB)
  • interval '91' day           (standard)
  • INTERVAL 91 DAYS            (Spark SQL / DuckDB)

Directory structure expected:
  <root>/q{N}/seed{1..5}.sql

Output:
  tpch_params.json  →  { "q1": { "seed1": { "delta": 91 }, ... }, ... }
"""

import re
import json
import glob
import os
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Cast helpers
# ---------------------------------------------------------------------------

def _int(m, g=1):  return int(m.group(g))


def _str(m, g=1):  return m.group(g).strip("'")


def _date(m, g=1): return m.group(g).strip("'")


# Regex fragment matching either date syntax; use with _dcast()
#   date '1995-03-17'
#   CAST('1995-03-17' AS DATE)
_D = r"(?:date\s*'(\d{4}-\d\d-\d\d)'|CAST\s*\(\s*'(\d{4}-\d\d-\d\d)'\s*AS\s*DATE\s*\))"


def _dcast(m):
    """Return whichever capture group holds the date string."""
    for i in range(1, m.lastindex + 1):
        if m.group(i):
            return m.group(i)
    return None


# ---------------------------------------------------------------------------
# Per-query extractor table
# Each entry: (param_name, regex_pattern, cast_fn)
# cast_fn receives the match object.
# ---------------------------------------------------------------------------

EXTRACTORS: dict[int, list[tuple]] = {
    1: [
        # delta: int days
        # standard: interval '91' day
        # Spark:    INTERVAL 91 DAYS
        ("delta", r"INTERVAL\s+'?(\d+)'?\s+DAYS?", _int),
    ],
    2: [
        ("size", r"p_size\s*=\s*(\d+)", _int),
        ("type", r"p_type\s+like\s*'%([^']+)'", _str),
        ("region", r"r_name\s*=\s*'([^']+)'", _str),
    ],
    3: [
        ("segment", r"c_mktsegment\s*=\s*'([^']+)'", _str),
        ("date", r"o_orderdate\s*<\s*" + _D, _dcast),
    ],
    4: [
        ("date", r"o_orderdate\s*>=\s*" + _D, _dcast),
    ],
    5: [
        ("region", r"r_name\s*=\s*'([^']+)'", _str),
        ("date", r"o_orderdate\s*>=\s*" + _D, _dcast),
    ],
    6: [
        ("date", r"l_shipdate\s*>=\s*" + _D, _dcast),
        ("discount", r"l_discount\s+between\s+([\d.]+)\s*-", float),
        ("quantity", r"l_quantity\s*<\s*([\d.]+)", float),
    ],
    # Q7 & Q8 handled via SPECIAL below
    9: [
        ("color", r"p_name\s+like\s*'%([^'%]+)%'", _str),
    ],
    10: [
        ("date", r"o_orderdate\s*>=\s*" + _D, _dcast),
    ],
    11: [
        ("nation", r"n_name\s*=\s*'([^']+)'", _str),
        ("fraction", r"\*\s*(0\.\d+)\b", float),
    ],
    # Q12 handled via SPECIAL below
    13: [
        # NOT o_comment LIKE '%express%packages%'
        ("word1", r"o_comment\s+(?:not\s+)?like\s+'%([^%]+)%", _str),
        ("word2", r"o_comment\s+(?:not\s+)?like\s+'%[^%]+%([^'%]+)%'", _str),
    ],
    14: [
        ("date", r"l_shipdate\s*>=\s*" + _D, _dcast),
    ],
    15: [
        ("date", r"l_shipdate\s*>=\s*" + _D, _dcast),
    ],
    16: [
        ("brand", r"p_brand\s*<>\s*'([^']+)'", _str),
        # NOT p_type LIKE 'LARGE PLATED%'
        ("type", r"p_type\s+(?:not\s+)?like\s+'([^'%]+)%'", _str),
        # p_size IN (12, 36, ...) — positive IN list
        ("sizes", r"p_size\s+in\s*\(([^)]+)\)",
         lambda m: [int(x.strip()) for x in m.group(1).split(",")]),
    ],
    17: [
        ("brand", r"p_brand\s*=\s*'([^']+)'", _str),
        ("container", r"p_container\s*=\s*'([^']+)'", _str),
    ],
    18: [
        ("quantity", r"having\s+sum\s*\(\s*l_quantity\s*\)\s*>\s*([\d.]+)", float),
    ],
    # Q19 handled via SPECIAL below
    20: [
        ("color", r"p_name\s+like\s*'([^'%]+)%'", _str),
        ("date", r"l_shipdate\s*>=\s*" + _D, _dcast),
        ("nation", r"n_name\s*=\s*'([^']+)'", _str),
    ],
    21: [
        ("nation", r"n_name\s*=\s*'([^']+)'", _str),
    ],
    22: [
        ("i1", r"substring\s*\([^)]+\)\s*in\s*\(([^)]+)\)",
         lambda m: [x.strip().strip("'") for x in m.group(1).split(",")]),
    ],
}


# ---------------------------------------------------------------------------
# Special-case extractors (multi-value or ordering-sensitive)
# ---------------------------------------------------------------------------

def extract_q7(sql: str) -> dict:
    nations = re.findall(r"n_name\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    return {
        "nation1": nations[0] if len(nations) > 0 else None,
        "nation2": nations[1] if len(nations) > 1 else None,
    }


def extract_q8(sql: str) -> dict:
    # nation: appears as CASE WHEN nation = 'FRANCE' (column aliased from n2.n_name)
    #         NOT as a WHERE predicate n_name = '...'
    nation = re.search(r"WHEN\s+nation\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    region = re.search(r"r_name\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    ptype = re.search(r"p_type\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    return {
        "nation": _str(nation) if nation else None,
        "region": _str(region) if region else None,
        "type": _str(ptype) if ptype else None,
    }


def extract_q12(sql: str) -> dict:
    # shipmodes: IN ('SHIP', 'REG AIR') — not = '...'
    in_m = re.search(r"l_shipmode\s+in\s*\(([^)]+)\)", sql, re.IGNORECASE)
    modes = [x.strip().strip("'") for x in in_m.group(1).split(",")] if in_m else []
    date = re.search(r"l_receiptdate\s*>=\s*" + _D, sql, re.IGNORECASE)
    return {
        "shipmode1": modes[0] if len(modes) > 0 else None,
        "shipmode2": modes[1] if len(modes) > 1 else None,
        "date": _dcast(date) if date else None,
    }


def extract_q19(sql: str) -> dict:
    # Only 6 substitution params per TPC-H spec: BRAND1-3 and QTY1-3
    # size and container are fixed in the template
    brands = re.findall(r"p_brand\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    quantities = re.findall(r"l_quantity\s*>=\s*([\d.]+)", sql, re.IGNORECASE)
    return {
        "brand1": brands[0] if len(brands) > 0 else None,
        "brand2": brands[1] if len(brands) > 1 else None,
        "brand3": brands[2] if len(brands) > 2 else None,
        "quantity1": int(float(quantities[0])) if len(quantities) > 0 else None,
        "quantity2": int(float(quantities[1])) if len(quantities) > 1 else None,
        "quantity3": int(float(quantities[2])) if len(quantities) > 2 else None,
    }


SPECIAL: dict[int, Any] = {
    7: extract_q7,
    8: extract_q8,
    12: extract_q12,
    19: extract_q19,
}


# ---------------------------------------------------------------------------
# Generic extractor
# ---------------------------------------------------------------------------

def extract_generic(qnum: int, sql: str) -> dict:
    result = {}
    for name, pattern, cast in EXTRACTORS.get(qnum, []):
        m = re.search(pattern, sql, re.IGNORECASE | re.MULTILINE)
        if m:
            try:
                result[name] = cast(m)
            except Exception:
                result[name] = m.group(1)
        else:
            result[name] = None
    return result


def extract_params(qnum: int, sql: str) -> dict:
    if qnum in SPECIAL:
        return SPECIAL[qnum](sql)
    return extract_generic(qnum, sql)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(queries_root: str = ".", output_file: str = "tpch_params.json"):
    all_params: dict[str, Any] = {}

    pattern = os.path.join(queries_root, "q*", "seed*.sql")
    files = sorted(glob.glob(pattern))

    if not files:
        pattern2 = os.path.join(queries_root, "**", "*.sql")
        files = sorted(glob.glob(pattern2, recursive=True))

    if not files:
        print(f"No SQL files found under: {queries_root}")
        return

    print(f"Found {len(files)} SQL files\n")

    for fpath in files:
        path = Path(fpath)
        parts = path.parts

        q_dir = next((p for p in parts if re.match(r'^q\d+$', p, re.I)), None)
        if q_dir:
            q_num = int(re.search(r'\d+', q_dir).group())
            seed_key = path.stem
        else:
            m = re.match(r'q?(\d+)', path.stem)
            q_num = int(m.group(1)) if m else None
            seed_key = path.stem

        if q_num is None:
            print(f"  SKIP (no query number): {fpath}")
            continue

        sql = path.read_text(errors="replace")
        params = extract_params(q_num, sql)

        q_key = f"q{q_num}"
        all_params.setdefault(q_key, {})[seed_key] = params
        print(f"  q{q_num:2d}/{seed_key}  →  {params}")

    sorted_params = {
        f"q{i}": all_params[f"q{i}"]
        for i in range(1, 23)
        if f"q{i}" in all_params
    }

    out = Path(output_file)
    out.write_text(json.dumps(sorted_params, indent=2, ensure_ascii=False))
    print(f"\nSaved → {out.resolve()}  ({len(files)} files processed)")
    return sorted_params

if __name__ == "__main__":
    folder = '/Users/truongtpa/Sites/BLOOM-FaaS/ansible-setup/tpc-h/tpch100'
    output = '/Users/truongtpa/Sites/BLOOM-FaaS/ansible-setup/tpc-h/tpch100/parms-tpch-100.json'
    main(folder, output)