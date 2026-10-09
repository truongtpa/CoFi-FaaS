import re
import sqlglot
from pathlib import Path

OUT_DIR = Path("/Users/truongtpa/Sites/BLOOM-FaaS/ansible-setup/tpc-h/tpch300")

def convert_q1(sql: str) -> str:
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+DAY\(\d+\)",
        r"INTERVAL \1 DAYS",
        sql, flags=re.IGNORECASE
    )
    return to_spark_sql(sql)

def beautify_sql(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, write="spark", pretty=True)[0]
    except Exception:
        return sql

def convert_q15(sql: str) -> str:
    view_cols_match = re.search(
        r'create\s+view\s+\w+\s*\(([^)]+)\)\s*as',
        sql, re.IGNORECASE
    )
    view_body_match = re.search(
        r'create\s+view\s+\w+\s*\([^)]*\)\s*as\s*(select[\s\S]+?);',
        sql, re.IGNORECASE
    )

    if not view_cols_match or not view_body_match:
        return to_spark_sql(sql)

    col_aliases = [c.strip() for c in view_cols_match.group(1).split(',')]
    view_body_raw = view_body_match.group(1).strip()
    def inject_aliases(select_sql: str, aliases: list) -> str:
        select_match = re.match(
            r'(select\s+)([\s\S]+?)(\s+from\s+)',
            select_sql, re.IGNORECASE
        )
        if not select_match:
            return select_sql

        prefix = select_match.group(1)
        select_list = select_match.group(2)
        rest = select_sql[select_match.end(2):]

        exprs = []
        depth = 0
        current = []
        for ch in select_list:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if ch == ',' and depth == 0:
                exprs.append(''.join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            exprs.append(''.join(current).strip())

        # Gán alias
        aliased = [f"{expr} AS {alias}" for expr, alias in zip(exprs, aliases)]
        return prefix + ', '.join(aliased) + rest

    view_body = inject_aliases(view_body_raw, col_aliases)

    rewritten = f"""
            SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
            FROM supplier
            JOIN (
                {view_body}
            ) AS revenue0 ON s_suppkey = supplier_no
            WHERE total_revenue = (
                SELECT MAX(total_revenue)
                FROM (
                    {view_body}
                ) AS revenue_max
            )
            ORDER BY s_suppkey
        """.strip()

    return sqlglot.transpile(rewritten, write="spark", pretty=True)[0]

def to_spark_sql(sql: str) -> str:
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'where\s+rownum\s*<=\s*-\d+\s*;?', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r';\s*where\s+rownum\s*<=\s*(\d+)\s*;?', r'\nLIMIT \1;', sql, flags=re.IGNORECASE)
    return sqlglot.transpile(sql, write="spark")[0]

converters = {
    1:  convert_q1,
    15: convert_q15,
}

for q in range(1, 23):
    for seed in range(1, 16):
        path = OUT_DIR / f"q{q}" / f"seed{seed}.sql"
        if not path.exists():
            print(f"  skip {path}")
            continue

        original = path.read_text()
        convert_fn = converters.get(q, to_spark_sql)
        converted = beautify_sql(convert_fn(original))
        path.write_text(converted)
        print(f"q{q}/seed{seed}.sql")