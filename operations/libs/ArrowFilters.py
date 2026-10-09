"""
sql_arrow_filter.py
SQL WHERE clause → PyArrow compute Expression

Supports: AND, OR, NOT, =, !=, <, <=, >, >=,
          IS NULL, IS NOT NULL, IN, NOT IN, BETWEEN, LIKE

Auto-casts literals based on PyArrow schema:
  date32       → datetime.date
  timestamp    → datetime.datetime
  decimal128   → decimal.Decimal
  int*         → int
  float*/double→ float
  string/utf8  → str
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import sqlglot
import sqlglot.expressions as exp


def _cast_literal(value: str, arrow_type: pa.DataType) -> Any:
    if pa.types.is_date(arrow_type):
        return date.fromisoformat(value)
    if pa.types.is_timestamp(arrow_type):
        return datetime.fromisoformat(value)
    if pa.types.is_decimal(arrow_type):
        return Decimal(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    return value


def _resolve_type(col_name: str, schema: pa.Schema | None) -> pa.DataType | None:
    if schema is None:
        return None
    try:
        return schema.field(col_name).type
    except KeyError:
        return None


_SQLGLOT_TYPE_MAP = {
    exp.DataType.Type.DATE: lambda v: date.fromisoformat(v),
    exp.DataType.Type.DATETIME: lambda v: datetime.fromisoformat(v),
    exp.DataType.Type.TIMESTAMP: lambda v: datetime.fromisoformat(v),
    exp.DataType.Type.DECIMAL: lambda v: Decimal(v),
    exp.DataType.Type.FLOAT: lambda v: float(v),
    exp.DataType.Type.DOUBLE: lambda v: float(v),
    exp.DataType.Type.INT: lambda v: int(v),
    exp.DataType.Type.BIGINT: lambda v: int(v),
    exp.DataType.Type.SMALLINT: lambda v: int(v),
    exp.DataType.Type.VARCHAR: lambda v: v,
    exp.DataType.Type.TEXT: lambda v: v,
}


# ─────────────────────────────────────────────────────────────────────────────
# INTERVAL arithmetic  — DATE/TIMESTAMP ± INTERVAL n unit → Python date/datetime
# ─────────────────────────────────────────────────────────────────────────────

def _apply_interval(base: date | datetime, interval: exp.Interval, sign: int) -> date | datetime:
    """
    Add or subtract a sqlglot Interval node from a Python date/datetime.
    sign: +1 for Add, -1 for Sub.
    Supported units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEK.
    """
    n_raw = interval.this  # Literal node or raw string
    n = int(n_raw.this if isinstance(n_raw, exp.Literal) else n_raw) * sign
    unit = (interval.unit.name if interval.unit else "DAY").upper()

    if unit == "YEAR":
        try:
            return base.replace(year=base.year + n)
        except ValueError:
            # Feb 29 edge case
            return base.replace(year=base.year + n, day=28)
    if unit == "MONTH":
        month = base.month - 1 + n
        year = base.year + month // 12
        month = month % 12 + 1
        import calendar
        day = min(base.day, calendar.monthrange(year, month)[1])
        return base.replace(year=year, month=month, day=day)
    if unit == "WEEK":
        return base + timedelta(weeks=n)
    if unit == "DAY":
        return base + timedelta(days=n)
    if unit == "HOUR":
        return base + timedelta(hours=n)
    if unit == "MINUTE":
        return base + timedelta(minutes=n)
    if unit == "SECOND":
        return base + timedelta(seconds=n)

    raise ValueError(f"Unsupported INTERVAL unit: {unit}")


def _eval_arithmetic(node: exp.Expression, arrow_type: pa.DataType | None) -> Any:
    """
    Evaluate DATE/TIMESTAMP ± INTERVAL expressions to a Python scalar.
    E.g.  DATE '1994-01-01' + INTERVAL '1' YEAR  →  date(1995, 1, 1)
    """
    if isinstance(node, (exp.Add, exp.Sub)):
        left, right = node.left, node.right
        sign = +1 if isinstance(node, exp.Add) else -1

        # base ± interval  or  interval ± base
        if isinstance(right, exp.Interval):
            base = _extract_value(left, arrow_type)
            return _apply_interval(base, right, sign)
        if isinstance(left, exp.Interval):
            base = _extract_value(right, arrow_type)
            return _apply_interval(base, left, sign)

    # Not an arithmetic node — delegate to normal extraction
    return _extract_value(node, arrow_type)


# ─────────────────────────────────────────────────────────────────────────────
# Value extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_value(node: exp.Expression, arrow_type: pa.DataType | None) -> Any:
    """
    Extract a Python scalar from a sqlglot value node.
    Handles: Null, Cast, Literal, and Add/Sub with Interval.
    """
    if isinstance(node, exp.Null):
        return None

    # Cast(Literal, to=DataType) — e.g. DATE '1995-03-15'
    if isinstance(node, exp.Cast):
        inner = node.this
        raw = inner.this if isinstance(inner, exp.Literal) else str(inner)
        dtype = node.to.this
        converter = _SQLGLOT_TYPE_MAP.get(dtype)
        if converter:
            return converter(raw)
        return _cast_literal(raw, arrow_type) if arrow_type is not None else raw

    # Plain Literal
    if isinstance(node, exp.Literal):
        raw: str = node.this
        if arrow_type is not None:
            return _cast_literal(raw, arrow_type)
        if node.is_string:
            return raw
        return int(raw) if "." not in raw else float(raw)

    # Arithmetic with INTERVAL — delegate
    if isinstance(node, (exp.Add, exp.Sub)):
        return _eval_arithmetic(node, arrow_type)

    raise ValueError(f"Unsupported value node: {type(node).__name__}: {node}")


# ─────────────────────────────────────────────────────────────────────────────
# Operand classification
# ─────────────────────────────────────────────────────────────────────────────

def _col_type(node: exp.Expression, schema: pa.Schema | None) -> tuple[str, pa.DataType | None]:
    name = node.name
    return name, _resolve_type(name, schema)


def _is_value_node(node: exp.Expression) -> bool:
    """True for nodes that represent a scalar value (literal, cast, null, arithmetic)."""
    return isinstance(node, (exp.Literal, exp.Cast, exp.Null, exp.Add, exp.Sub))


def _binary_operands(
        node: exp.Expression, schema: pa.Schema | None
) -> tuple[pc.Expression, Any]:
    """
    Decompose a binary comparison into (pc.field(col), scalar_value).
    Also handles col-vs-col via pc.field on both sides.
    """
    left, right = node.left, node.right

    # col OP literal/expression
    if isinstance(left, exp.Column) and _is_value_node(right):
        col, atype = _col_type(left, schema)
        return pc.field(col), _extract_value(right, atype)

    # literal/expression OP col  (flipped)
    if isinstance(right, exp.Column) and _is_value_node(left):
        col, atype = _col_type(right, schema)
        return pc.field(col), _extract_value(left, atype)

    # col OP col
    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
        return pc.field(left.name), pc.field(right.name)

    raise ValueError(f"Unsupported binary operands: {left!r} / {right!r}")


def _convert(node: exp.Expression, schema: pa.Schema | None) -> pc.Expression:
    # Logical
    if isinstance(node, exp.And):
        return _convert(node.left, schema) & _convert(node.right, schema)

    if isinstance(node, exp.Or):
        return _convert(node.left, schema) | _convert(node.right, schema)

    if isinstance(node, exp.Not):
        inner = node.this
        if isinstance(inner, exp.Is):
            col = inner.this.name
            return pc.field(col).is_valid()
        return ~_convert(inner, schema)

    if isinstance(node, exp.Is):
        col = node.this.name
        return pc.field(col).is_null()

    # Comparisons
    if isinstance(node, exp.EQ):
        f, v = _binary_operands(node, schema)
        return f == v

    if isinstance(node, exp.NEQ):
        f, v = _binary_operands(node, schema)
        return f != v

    if isinstance(node, exp.GT):
        f, v = _binary_operands(node, schema)
        return f > v

    if isinstance(node, exp.GTE):
        f, v = _binary_operands(node, schema)
        return f >= v

    if isinstance(node, exp.LT):
        f, v = _binary_operands(node, schema)
        return f < v

    if isinstance(node, exp.LTE):
        f, v = _binary_operands(node, schema)
        return f <= v

    # BETWEEN
    if isinstance(node, exp.Between):
        col, atype = _col_type(node.this, schema)
        lo = _extract_value(node.args["low"], atype)
        hi = _extract_value(node.args["high"], atype)
        return (pc.field(col) >= lo) & (pc.field(col) <= hi)

    # IN / NOT IN
    if isinstance(node, exp.In):
        col, atype = _col_type(node.this, schema)
        values = [_extract_value(v, atype) for v in node.args["expressions"]]
        expr = pc.field(col).isin(values)
        return ~expr if node.args.get("negated") else expr

    # LIKE
    if isinstance(node, exp.Like):
        col = node.this.name
        pattern: str = node.args["expression"].this
        substring = pattern.strip("%")
        return pc.match_substring(pc.field(col), substring)

    # ILIKE
    if isinstance(node, exp.ILike):
        col = node.this.name
        pattern: str = node.args["expression"].this
        substring = pattern.strip("%")
        return pc.match_substring(pc.field(col), substring, ignore_case=True)

    raise NotImplementedError(f"Unsupported SQL node: {type(node).__name__}: {node}")


def SQL2ArrowFilter(where_clause: str, schema: pa.Schema | None = None) -> pc.Expression:
    ast = sqlglot.parse_one(f"SELECT * FROM _t WHERE {where_clause}")
    where = ast.find(exp.Where)
    if where is None:
        raise ValueError("No WHERE clause parsed from input")
    return _convert(where.this, schema)