#!/usr/bin/env python3
"""
Measure Bloom filter false-positive rate for TPCH query join keys.

The script reads only seed1.sql for each TPCH query, skips Q1/Q6/Q22, builds
local partition Bloom filters from /mnt/ssd/tpch100-parquet/<table>.parquet/*.parquet,
merges the partition filters, then measures FPR on the opposite join table.

Outputs:
  /Users/truongtpa/Sites/BLOOM-FaaS/fpr-results/tpch100-bf-fpr.csv
  /Users/truongtpa/Sites/BLOOM-FaaS/fpr-results/tpch100-bf-fpr.json
  /Users/truongtpa/Sites/BLOOM-FaaS/fpr-results/tpch100-bf-fpr.png
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import platform
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent
OPERATIONS_ROOT = REPO_ROOT / "operations"
QUERY_ROOT = REPO_ROOT / "ansible-setup/tpc-h/tpch100"
PARQUET_ROOT = Path("/root/tpch100-parquet")
CALIBRATION_ROOT = REPO_ROOT / "benchmarks/bf-analyze"
OUTPUT_ROOT = REPO_ROOT / "fpr-results"
DEFAULT_ERROR_RATE = 0.001
SKIP_QUERIES = {1, 6, 22}
DEFAULT_TEST_QUERIES = {3, 9}

TPCH_TABLES = {
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
}

TPCH_COLUMN_PREFIXES = {
    "c_": "customer",
    "l_": "lineitem",
    "n_": "nation",
    "o_": "orders",
    "p_": "part",
    "ps_": "partsupp",
    "r_": "region",
    "s_": "supplier",
}


@dataclass(frozen=True)
class JoinEdge:
    query: int
    build_table: str
    build_col: str
    probe_table: str
    probe_col: str


def die(message: str) -> None:
    print(f"[ERROR] {message}", file=sys.stderr)
    sys.exit(1)


def ensure_deps() -> None:
    missing = []
    for module in ("duckdb", "fastbloom_rs"):
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    if missing:
        die(
            "Missing Python packages: "
            + ", ".join(missing)
            + ". Install project requirements before running this script."
        )


def extension_path() -> Path:
    arch = platform.uname().machine
    subdir = "arm" if arch == "arm64" else "x86"
    path = OPERATIONS_ROOT / "extention" / subdir / "bfextension.duckdb_extension"
    if not path.exists():
        die(f"bfextension not found: {path}")
    return path


def q(sql_ident: str) -> str:
    return '"' + sql_ident.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parquet_glob(table: str) -> str:
    return str(PARQUET_ROOT / f"{table}.parquet" / "*.parquet")


def existing_parquet_files(table: str) -> list[str]:
    files = sorted(glob.glob(parquet_glob(table)))
    if not files:
        raise FileNotFoundError(f"No parquet files for table {table}: {parquet_glob(table)}")
    return files


def connect_duckdb():
    import duckdb

    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    return conn


def try_load_bf_extension(conn) -> tuple[bool, str | None]:
    try:
        conn.execute(f"LOAD {sql_string(str(extension_path()))}")
        return True, None
    except Exception as exc:
        return False, str(exc)


def table_row_count(conn, table: str) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM read_parquet({sql_string(parquet_glob(table))})"
        ).fetchone()[0]
    )


def distinct_count(conn, table: str, column: str) -> int:
    return int(
        conn.execute(
            f"""
            SELECT COUNT(DISTINCT {q(column)})
            FROM read_parquet({sql_string(parquet_glob(table))})
            WHERE {q(column)} IS NOT NULL
            """
        ).fetchone()[0]
    )


def column_is_integral(conn, table: str, column: str) -> bool:
    rows = conn.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_string(parquet_glob(table))})").fetchall()
    for name, typ, *_ in rows:
        if name.lower() == column.lower():
            return str(typ).upper() in {
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "UTINYINT",
                "USMALLINT",
                "UINTEGER",
                "UBIGINT",
            }
    return False


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"\s+", " ", sql).strip()


def parse_table_aliases(sql: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    normalized = normalize_sql(sql)

    for match in re.finditer(r"\bFROM\s+(.+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|$)", normalized, re.I):
        from_part = match.group(1)
        chunks = re.split(r"\bJOIN\b|,", from_part, flags=re.I)
        for chunk in chunks:
            chunk = re.split(r"\bON\b", chunk, flags=re.I)[0].strip()
            words = [w.strip('"') for w in chunk.split() if w.upper() not in {"AS"}]
            if not words:
                continue
            table = words[0].lower()
            if table in TPCH_TABLES:
                alias = words[1].lower() if len(words) > 1 else table
                aliases[alias] = table
                aliases[table] = table

    return aliases


def parse_join_edges(query: int, sql: str) -> list[JoinEdge]:
    aliases = parse_table_aliases(sql)
    normalized = normalize_sql(sql)
    edges: list[JoinEdge] = []
    seen: set[tuple[str, str, str, str]] = set()

    def infer_table(alias: str | None, column: str) -> str | None:
        if alias:
            return aliases.get(alias.lower())
        col = column.lower()
        for prefix, table in sorted(TPCH_COLUMN_PREFIXES.items(), key=lambda x: len(x[0]), reverse=True):
            if col.startswith(prefix):
                return table
        return None

    operand = r"(?:(?P<{side}_alias>[A-Za-z_][\w]*)\.)?(?P<{side}_col>[A-Za-z_][\w]*)"
    pattern = re.compile(
        r"\b" + operand.format(side="left") + r"\s*=\s*" + operand.format(side="right") + r"\b",
        re.I,
    )
    for match in pattern.finditer(normalized):
        left_c = match.group("left_col").lower()
        right_c = match.group("right_col").lower()
        left_t = infer_table(match.group("left_alias"), left_c)
        right_t = infer_table(match.group("right_alias"), right_c)
        if not left_t or not right_t:
            continue
        if left_t == right_t:
            continue
        if not (left_c.lower().endswith("key") and right_c.lower().endswith("key")):
            continue

        left = (left_t, left_c.lower(), right_t, right_c.lower())
        right = (right_t, right_c.lower(), left_t, left_c.lower())
        for item in (left, right):
            if item not in seen:
                seen.add(item)
                edges.append(JoinEdge(query, *item))

    return edges


def discover_edges() -> list[JoinEdge]:
    edges: list[JoinEdge] = []
    for qdir in sorted(QUERY_ROOT.glob("q*"), key=lambda p: int(p.name[1:])):
        if not qdir.name[1:].isdigit():
            continue
        qnum = int(qdir.name[1:])
        if qnum in SKIP_QUERIES:
            continue
        seed = qdir / "seed1.sql"
        if seed.exists():
            edges.extend(parse_join_edges(qnum, seed.read_text()))
    return edges


def load_calibration() -> dict[int, list[dict]]:
    data: dict[int, list[dict]] = {}
    for path in CALIBRATION_ROOT.glob("tpch-100-q*-est-bf-elements.json"):
        m = re.search(r"q(\d+)-est", path.name)
        if not m:
            continue
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            data[int(m.group(1))] = payload
    return data


def calibration_for(edge: JoinEdge, calibrations: dict[int, list[dict]]) -> int | None:
    items = calibrations.get(edge.query, [])
    candidates = []
    for item in items:
        task_name = str(item.get("task_name", "")).lower()
        est = item.get("est_elements")
        if est is None:
            continue
        try:
            est_int = max(1, int(float(est)))
        except (TypeError, ValueError):
            continue
        score = 0
        if edge.build_table in task_name:
            score += 2
        if edge.build_col in task_name:
            score += 1
        if "combine" in task_name:
            score += 1
        if score:
            candidates.append((score, est_int))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def bf_file_stats(path: Path) -> dict:
    with path.open("rb") as f:
        k = struct.unpack(">I", f.read(4))[0]
        bit_bytes = f.read()
    total_bits = len(bit_bytes) * 8
    set_bits = sum(byte.bit_count() for byte in bit_bytes)
    fill_ratio = set_bits / total_bits if total_bits else 0.0
    est_inserted = None
    if total_bits and k and fill_ratio < 1.0:
        est_inserted = -total_bits / k * math.log(1 - fill_ratio)
    current_fpr = None
    if est_inserted is not None and total_bits:
        current_fpr = (1 - math.exp(-k * est_inserted / total_bits)) ** k
    return {
        "bf_bytes": path.stat().st_size,
        "bf_k": k,
        "bf_total_bits": total_bits,
        "bf_set_bits": set_bits,
        "bf_fill_ratio": fill_ratio,
        "bf_est_inserted": int(est_inserted) if est_inserted is not None else None,
        "bf_theoretical_fpr_from_fill": current_fpr,
    }


def merge_bloom_filters(part_paths: Iterable[Path], output_path: Path) -> None:
    from fastbloom_rs import BloomFilter

    merged = None
    for path in part_paths:
        bf = BloomFilter.from_file_with_hashes(str(path))
        if merged is None:
            merged = bf
        else:
            merged.union(bf)
    if merged is None:
        raise RuntimeError("No partition BF files were created")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.save_to_file_with_hashes(str(output_path))


def int64_bytes(value: int) -> bytes:
    return int(value).to_bytes(8, byteorder="little", signed=True)


def build_partitioned_bf_extension(conn, edge: JoinEdge, est_elements: int, work_dir: Path, label: str) -> Path:
    part_dir = work_dir / "parts"
    part_dir.mkdir(parents=True, exist_ok=True)
    part_bfs: list[Path] = []

    for idx, parquet_file in enumerate(existing_parquet_files(edge.build_table)):
        out = part_dir / f"{label}-{idx:04d}.bin"
        conn.execute(
            f"""
            SELECT bloom_build(
                {sql_string(parquet_file)},
                {sql_string(edge.build_col)},
                {sql_string(str(out))},
                {int(est_elements)},
                {float(DEFAULT_ERROR_RATE)}
            )
            """
        )
        part_bfs.append(out)

    merged = work_dir / f"{label}-merged.bin"
    merge_bloom_filters(part_bfs, merged)
    return merged


def build_one_python_bf(parquet_file: str, edge: JoinEdge, est_elements: int, out: Path) -> Path:
    from fastbloom_rs import FilterBuilder

    conn = connect_duckdb()
    try:
        bf = FilterBuilder(int(est_elements), float(DEFAULT_ERROR_RATE)).build_bloom_filter()
        cursor = conn.execute(
            f"""
            SELECT DISTINCT CAST({q(edge.build_col)} AS BIGINT) AS k
            FROM read_parquet({sql_string(parquet_file)})
            WHERE {q(edge.build_col)} IS NOT NULL
            """
        )
        while True:
            rows = cursor.fetchmany(100_000)
            if not rows:
                break
            for (value,) in rows:
                bf.add(int64_bytes(value))

        bf.save_to_file_with_hashes(str(out))
        return out
    finally:
        conn.close()


def build_partitioned_bf_python(
    conn,
    edge: JoinEdge,
    est_elements: int,
    work_dir: Path,
    label: str,
) -> Path:
    part_dir = work_dir / "parts"
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_files = existing_parquet_files(edge.build_table)
    part_bfs = [part_dir / f"{label}-{idx:04d}.bin" for idx in range(len(parquet_files))]

    for parquet_file, out in zip(parquet_files, part_bfs):
        build_one_python_bf(parquet_file, edge, est_elements, out)

    merged = work_dir / f"{label}-merged.bin"
    merge_bloom_filters(part_bfs, merged)
    return merged


def build_partitioned_bf(
    conn,
    edge: JoinEdge,
    est_elements: int,
    work_dir: Path,
    label: str,
    backend: str,
) -> Path:
    if backend == "extension":
        return build_partitioned_bf_extension(conn, edge, est_elements, work_dir, label)
    return build_partitioned_bf_python(conn, edge, est_elements, work_dir, label)


def measure_fpr_extension(conn, edge: JoinEdge, bf_path: Path) -> dict:
    conn.execute(f"SELECT bloom_load({sql_string(str(bf_path))})")
    build_path = parquet_glob(edge.build_table)
    probe_path = parquet_glob(edge.probe_table)

    row = conn.execute(
        f"""
        WITH
          build_keys AS (
            SELECT DISTINCT CAST({q(edge.build_col)} AS BIGINT) AS k
            FROM read_parquet({sql_string(build_path)})
            WHERE {q(edge.build_col)} IS NOT NULL
          ),
          probe_keys AS (
            SELECT CAST({q(edge.probe_col)} AS BIGINT) AS k
            FROM read_parquet({sql_string(probe_path)})
            WHERE {q(edge.probe_col)} IS NOT NULL
          ),
          negatives AS (
            SELECT p.k
            FROM probe_keys p
            ANTI JOIN build_keys b USING (k)
          )
        SELECT
          (SELECT COUNT(*) FROM probe_keys) AS probe_rows,
          (SELECT COUNT(*) FROM build_keys) AS build_distinct,
          (SELECT COUNT(*) FROM negatives) AS true_negatives,
          (SELECT COUNT(*) FROM negatives WHERE bloom_contains(k)) AS false_positives
        """
    ).fetchone()

    probe_rows, build_distinct, true_negatives, false_positives = map(int, row)
    fpr = false_positives / true_negatives if true_negatives else 0.0
    return {
        "probe_rows": probe_rows,
        "build_distinct": build_distinct,
        "true_negatives": true_negatives,
        "false_positives": false_positives,
        "fpr": fpr,
    }


def measure_fpr_python(conn, edge: JoinEdge, bf_path: Path) -> dict:
    from fastbloom_rs import BloomFilter

    build_distinct = distinct_count(conn, edge.build_table, edge.build_col)
    bf = BloomFilter.from_file_with_hashes(str(bf_path))
    build_path = parquet_glob(edge.build_table)
    probe_path = parquet_glob(edge.probe_table)

    probe_rows, true_negatives = map(
        int,
        conn.execute(
            f"""
            WITH
              build_keys AS (
                SELECT DISTINCT CAST({q(edge.build_col)} AS BIGINT) AS k
                FROM read_parquet({sql_string(build_path)})
                WHERE {q(edge.build_col)} IS NOT NULL
              ),
              probe_keys AS (
                SELECT CAST({q(edge.probe_col)} AS BIGINT) AS k
                FROM read_parquet({sql_string(probe_path)})
                WHERE {q(edge.probe_col)} IS NOT NULL
              ),
              negatives AS (
                SELECT p.k
                FROM probe_keys p
                ANTI JOIN build_keys b USING (k)
              )
            SELECT
              (SELECT COUNT(*) FROM probe_keys) AS probe_rows,
              (SELECT COUNT(*) FROM negatives) AS true_negatives
            """
        ).fetchone(),
    )

    false_positives = 0
    cursor = conn.execute(
        f"""
        WITH
          build_keys AS (
            SELECT DISTINCT CAST({q(edge.build_col)} AS BIGINT) AS k
            FROM read_parquet({sql_string(build_path)})
            WHERE {q(edge.build_col)} IS NOT NULL
          ),
          probe_keys AS (
            SELECT CAST({q(edge.probe_col)} AS BIGINT) AS k
            FROM read_parquet({sql_string(probe_path)})
            WHERE {q(edge.probe_col)} IS NOT NULL
          )
        SELECT p.k
        FROM probe_keys p
        ANTI JOIN build_keys b USING (k)
        """
    )
    while True:
        rows = cursor.fetchmany(100_000)
        if not rows:
            break
        false_positives += sum(1 for (value,) in rows if bf.contains(int64_bytes(value)))

    fpr = false_positives / true_negatives if true_negatives else 0.0
    return {
        "probe_rows": probe_rows,
        "build_distinct": build_distinct,
        "true_negatives": true_negatives,
        "false_positives": false_positives,
        "fpr": fpr,
    }


def measure_fpr(conn, edge: JoinEdge, bf_path: Path, backend: str) -> dict:
    if backend == "extension":
        return measure_fpr_extension(conn, edge, bf_path)
    return measure_fpr_python(conn, edge, bf_path)


def run(args) -> list[dict]:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    calibrations = load_calibration()
    edges = discover_edges()

    if args.only_query:
        keep = {int(q) for q in args.only_query.split(",")}
        edges = [edge for edge in edges if edge.query in keep]
    elif not args.all_queries:
        edges = [edge for edge in edges if edge.query in DEFAULT_TEST_QUERIES]

    if args.limit:
        edges = edges[: args.limit]

    conn = connect_duckdb()
    bf_backend = args.bf_backend
    if bf_backend in {"auto", "extension"}:
        loaded, load_error = try_load_bf_extension(conn)
        if loaded:
            bf_backend = "extension"
            print("[bf] backend=extension")
        elif args.bf_backend == "extension":
            raise RuntimeError(f"Could not load bfextension: {load_error}")
        else:
            bf_backend = "python"
            print(f"[bf] backend=python (bfextension unavailable: {load_error})")
    else:
        print("[bf] backend=python")
    print("[mode] serial, no threads")

    row_counts: dict[str, int] = {}
    distinct_counts: dict[tuple[str, str], int] = {}
    results: list[dict] = []

    try:
        for edge in edges:
            if not column_is_integral(conn, edge.build_table, edge.build_col):
                print(f"[skip] Q{edge.query} {edge.build_table}.{edge.build_col}: build column is not integral")
                continue
            if not column_is_integral(conn, edge.probe_table, edge.probe_col):
                print(f"[skip] Q{edge.query} {edge.probe_table}.{edge.probe_col}: probe column is not integral")
                continue

            row_counts.setdefault(edge.build_table, table_row_count(conn, edge.build_table))
            distinct_key = (edge.build_table, edge.build_col)
            distinct_counts.setdefault(distinct_key, distinct_count(conn, edge.build_table, edge.build_col))
            base_est = row_counts[edge.build_table]
            build_distinct = distinct_counts[distinct_key]
            raw_cal_est = calibration_for(edge, calibrations)
            cal_est = max(raw_cal_est or 0, build_distinct)
            variants = [("base_table_rows", base_est)]
            if cal_est and cal_est != base_est:
                variants.append(("calibration", cal_est))

            for sizing, est_elements in variants:
                print(
                    f"[run] Q{edge.query} {edge.build_table}.{edge.build_col}"
                    f" -> {edge.probe_table}.{edge.probe_col} | {sizing}={est_elements:,}"
                )
                t0 = time.perf_counter()
                with tempfile.TemporaryDirectory(prefix="tpch100-fpr-") as tmp:
                    work_dir = Path(tmp)
                    bf_path = build_partitioned_bf(conn, edge, est_elements, work_dir, sizing, bf_backend)
                    measured = measure_fpr(conn, edge, bf_path, bf_backend)
                    stats = bf_file_stats(bf_path)

                elapsed = time.perf_counter() - t0
                result = {
                    "query": f"Q{edge.query}",
                    "query_num": edge.query,
                    "build_table": edge.build_table,
                    "build_col": edge.build_col,
                    "probe_table": edge.probe_table,
                    "probe_col": edge.probe_col,
                    "sizing": sizing,
                    "bf_backend": bf_backend,
                    "est_elements": int(est_elements),
                    "raw_calibration_est_elements": raw_cal_est,
                    "build_distinct_for_sizing": build_distinct,
                    "error_rate_configured": DEFAULT_ERROR_RATE,
                    "elapsed_s": round(elapsed, 3),
                    **measured,
                    **stats,
                }
                results.append(result)
                write_outputs(results)
    finally:
        conn.close()

    return results


def write_outputs(results: list[dict]) -> None:
    json_path = OUTPUT_ROOT / "tpch100-bf-fpr.json"
    csv_path = OUTPUT_ROOT / "tpch100-bf-fpr.csv"
    json_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    if results:
        keys = list(results[0].keys())
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)


def load_results(path: Path) -> list[dict]:
    if not path.exists():
        die(f"Results file does not exist: {path}")
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text())
        if not isinstance(payload, list):
            die(f"Expected a JSON list in {path}")
        return payload
    if path.suffix.lower() == ".csv":
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            for key in ("fpr", "query_num", "est_elements", "true_negatives", "false_positives"):
                if key in row and row[key] not in ("", None):
                    try:
                        row[key] = float(row[key]) if "." in str(row[key]) else int(row[key])
                    except ValueError:
                        pass
        return rows
    die(f"Unsupported results format: {path}. Use .json or .csv")
    return []


def grouped_fpr(results: list[dict]) -> dict[str, dict[str, float]]:
    buckets: dict[str, dict[str, dict[str, float]]] = {}
    for row in results:
        query = str(row["query"])
        sizing = str(row["sizing"])
        bucket = buckets.setdefault(query, {}).setdefault(sizing, {"false_positives": 0.0, "true_negatives": 0.0})
        bucket["false_positives"] += float(row.get("false_positives") or 0)
        bucket["true_negatives"] += float(row.get("true_negatives") or 0)

    grouped: dict[str, dict[str, float]] = {}
    for query, by_sizing in sorted(buckets.items(), key=lambda item: int(str(item[0]).lstrip("Qq"))):
        grouped[query] = {}
        for sizing, counts in by_sizing.items():
            true_negatives = counts["true_negatives"]
            grouped[query][sizing] = (
                counts["false_positives"] / true_negatives * 100
                if true_negatives
                else 0.0
            )
    return grouped


def escape_svg(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_svg_chart(results: list[dict], path: Path) -> None:
    grouped = grouped_fpr(results)
    labels = list(grouped)
    if not labels:
        return

    base_vals = [grouped[label].get("base_table_rows", 0.0) for label in labels]
    cal_vals = [grouped[label].get("calibration", 0.0) for label in labels]
    max_val = max(base_vals + cal_vals + [0.001])

    left = 70
    right = 40
    top = 72
    row_h = 28
    bar_h = 9
    chart_w = 760
    width = left + chart_w + right
    height = top + len(labels) * row_h + 70

    def x_for(value: float) -> float:
        return left + (value / max_val) * chart_w

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="24" y="34" font-family="Arial, sans-serif" font-size="20" font-weight="700">TPCH100 Bloom Filter FPR by Query</text>',
        '<text x="24" y="56" font-family="Arial, sans-serif" font-size="13" fill="#555">aggregated false positives / true negatives</text>',
        f'<line x1="{left}" y1="{top - 12}" x2="{left + chart_w}" y2="{top - 12}" stroke="#ddd"/>',
    ]

    for tick in range(6):
        value = max_val * tick / 5
        x = x_for(value)
        lines.append(f'<line x1="{x:.1f}" y1="{top - 16}" x2="{x:.1f}" y2="{height - 48}" stroke="#eee"/>')
        lines.append(
            f'<text x="{x:.1f}" y="{height - 24}" text-anchor="middle" '
            f'font-family="Arial, sans-serif" font-size="11" fill="#555">{value:.3g}%</text>'
        )

    for idx, label in enumerate(labels):
        y = top + idx * row_h
        base = base_vals[idx]
        cal = cal_vals[idx]
        lines.append(
            f'<text x="24" y="{y + 11}" font-family="Arial, sans-serif" font-size="11" fill="#222">'
            f'{escape_svg(label)}</text>'
        )
        lines.append(
            f'<rect x="{left}" y="{y + 2}" width="{max(1, x_for(base) - left):.1f}" '
            f'height="{bar_h}" fill="#4C78A8"/>'
        )
        lines.append(
            f'<rect x="{left}" y="{y + 14}" width="{max(1, x_for(cal) - left):.1f}" '
            f'height="{bar_h}" fill="#F58518"/>'
        )
    legend_y = height - 52
    lines.extend(
        [
            f'<rect x="24" y="{legend_y}" width="12" height="12" fill="#4C78A8"/>',
            f'<text x="42" y="{legend_y + 11}" font-family="Arial, sans-serif" font-size="12">base table rows</text>',
            f'<rect x="170" y="{legend_y}" width="12" height="12" fill="#F58518"/>',
            f'<text x="188" y="{legend_y + 11}" font-family="Arial, sans-serif" font-size="12">calibration</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(lines))


def plot_results(results: list[dict]) -> None:
    if not results:
        return
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        svg_path = OUTPUT_ROOT / "tpch100-bf-fpr.svg"
        write_svg_chart(results, svg_path)
        print(f"[warn] matplotlib not installed; wrote SVG chart instead: {svg_path}")
        return

    grouped = grouped_fpr(results)

    labels = list(grouped)
    base_vals = [grouped[label].get("base_table_rows", 0.0) for label in labels]
    cal_vals = [grouped[label].get("calibration", 0.0) for label in labels]
    x = list(range(len(labels)))
    width = 0.42

    height = max(6, min(24, len(labels) * 0.34))
    fig, ax = plt.subplots(figsize=(16, height))
    ax.barh([i + width / 2 for i in x], base_vals, width, label="base table rows")
    ax.barh([i - width / 2 for i in x], cal_vals, width, label="calibration")
    ax.set_yticks(x)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("False positive rate (%)")
    ax.set_title("TPCH100 Bloom Filter FPR by Query")
    ax.legend()
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUTPUT_ROOT / "tpch100-bf-fpr.png", dpi=180)
    plt.close(fig)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-query", help="Comma-separated query numbers, e.g. 3,8,14")
    parser.add_argument(
        "--all-queries",
        action="store_true",
        help="Run all discovered TPCH queries. By default only Q3,Q9 are run for correctness testing.",
    )
    parser.add_argument("--limit", type=int, help="Limit number of discovered join directions, useful for smoke tests")
    parser.add_argument("--no-plot", action="store_true", help="Skip PNG chart generation")
    parser.add_argument("--plot-only", action="store_true", help="Only draw chart from an existing result file; do not run FPR")
    parser.add_argument(
        "--results-file",
        default=str(OUTPUT_ROOT / "tpch100-bf-fpr.json"),
        help="Existing .json or .csv result file used by --plot-only",
    )
    parser.add_argument(
        "--bf-backend",
        choices=["auto", "extension", "python"],
        default="auto",
        help="Bloom filter backend. auto tries bfextension first, then falls back to fastbloom-rs Python.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.plot_only:
        results = load_results(Path(args.results_file))
        plot_results(results)
        print(f"[done] rows={len(results)}")
        print(f"[done] source={Path(args.results_file)}")
        print(f"[done] chart_png={OUTPUT_ROOT / 'tpch100-bf-fpr.png'}")
        print(f"[done] chart_svg={OUTPUT_ROOT / 'tpch100-bf-fpr.svg'}")
        return

    ensure_deps()
    if not PARQUET_ROOT.exists():
        die(f"Parquet root does not exist: {PARQUET_ROOT}")
    results = run(args)
    if not args.no_plot:
        plot_results(results)
    print(f"[done] rows={len(results)}")
    print(f"[done] json={OUTPUT_ROOT / 'tpch100-bf-fpr.json'}")
    print(f"[done] csv={OUTPUT_ROOT / 'tpch100-bf-fpr.csv'}")
    if not args.no_plot:
        print(f"[done] chart={OUTPUT_ROOT / 'tpch100-bf-fpr.png'}")


if __name__ == "__main__":
    main()
