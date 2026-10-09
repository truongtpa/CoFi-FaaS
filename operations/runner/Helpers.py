import json
import math
import os
import uuid
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

import sqlglot
import sqlglot.expressions as sqlglot_exp


def _compute_adaptive_skew_threshold(build_rgs, probe_rgs, max_task_size_mb, user_override=None, max_parallel=20):
    if user_override is not None and user_override > 0:
        return user_override

    if not build_rgs or len(build_rgs) < 2:
        return max(1, len(build_rgs))

    build_sizes = [float(r.get("size_mb") or 0) for r in build_rgs if float(r.get("size_mb") or 0) > 0]

    if not build_sizes:
        return 20

    avg_build = sum(build_sizes) / len(build_sizes)
    sorted_sizes = sorted(build_sizes)
    p95_idx = int(len(sorted_sizes) * 0.95)
    p95_build = sorted_sizes[p95_idx] if p95_idx < len(sorted_sizes) else sorted_sizes[-1]
    effective_size = max(avg_build, p95_build * 0.7)

    build_budget_mb = max_task_size_mb * 0.5
    threshold_by_budget = int(build_budget_mb / max(effective_size, 0.1))
    threshold_parallelism = max(5, len(build_rgs) // max(max_parallel // 4, 2))

    threshold = min(threshold_by_budget, threshold_parallelism)
    threshold = max(5, threshold)

    return threshold


def _compute_total_size(files: List[Dict]) -> float:
    return sum(float(f.get("size_mb") or 0) for f in files)


def _is_sort_tight(rgs: List[Dict], threshold_ratio: float = 0.2) -> bool:
    if not rgs or len(rgs) < 2:
        return len(rgs) == 1

    try:
        global_min = min(r["min"] for r in rgs)
        global_max = max(r["max"] for r in rgs)
    except (TypeError, ValueError):
        return False

    try:
        global_span = global_max - global_min
        if global_span <= 0:
            return False
        total_span = sum((r["max"] - r["min"]) for r in rgs)
        avg_span = total_span / len(rgs)
        return avg_span <= threshold_ratio * global_span
    except TypeError:
        overlap_pairs = 0
        total_pairs = len(rgs) * (len(rgs) - 1) / 2
        if total_pairs == 0:
            return True
        for i in range(len(rgs)):
            for j in range(i + 1, len(rgs)):
                if _ranges_overlap(rgs[i]["min"], rgs[i]["max"], rgs[j]["min"], rgs[j]["max"]):
                    overlap_pairs += 1
        return (overlap_pairs / total_pairs) < 0.3


def _choose_join_strategy(
        build_files: List[Dict],
        probe_files: List[Dict],
        build_key: str,
        probe_key: str,
        broadcast_threshold_mb: int,
        force_strategy: str = None,
) -> tuple[str, str]:
    if force_strategy in ("broadcast", "range"):
        return force_strategy, "forced by config"
    if force_strategy == "hash":
        raise ValueError(
            "hash strategy requires explicit hash_partition + hash_join steps. "
            "adaptive_join does not support inline hash."
        )

    build_size = _compute_total_size(build_files)
    probe_size = _compute_total_size(probe_files)

    if build_size <= broadcast_threshold_mb:
        return "broadcast", f"build={build_size:.1f}MB ≤ {broadcast_threshold_mb}MB"

    build_rgs = _extract_rg_ranges(build_files, build_key)
    probe_rgs = _extract_rg_ranges(probe_files, probe_key)

    if build_rgs and probe_rgs:
        build_tight = _is_sort_tight(build_rgs)
        probe_tight = _is_sort_tight(probe_rgs)
        if build_tight and probe_tight:
            return "range", f"both sides sorted (build {len(build_rgs)} RGs, probe {len(probe_rgs)} RGs)"

    return "range", (
        f"build={build_size:.0f}MB probe={probe_size:.0f}MB, "
        f"stats not tight (may have skew - consider explicit hash_partition + hash_join)"
    )


def _extract_rg_ranges(files: List[Dict], key: str) -> List[Dict]:
    out = []
    for f in files:
        for rg in f.get("row_groups") or []:
            stats = (rg.get("stats") or {}).get(key)
            if not stats:
                continue
            mn, mx = stats.get("min"), stats.get("max")
            if mn is None or mx is None:
                continue
            out.append({
                "full_path": f["full_path"],
                "rg_id": rg["id"],
                "min": mn,
                "max": mx,
                "size_mb": float(rg.get("total_byte_size_mb") or 0),
                "num_rows": int(rg.get("num_rows") or 0),
            })
    return out


def _ranges_overlap(a_min, a_max, b_min, b_max) -> bool:
    try:
        return not (a_max < b_min or b_max < a_min)
    except TypeError:
        return True


def _pair_by_range(build_rgs: List[Dict], probe_rgs: List[Dict]) -> List[Dict]:
    if not build_rgs or not probe_rgs:
        return []

    builds_sorted = sorted(build_rgs, key=lambda x: x["min"])
    build_max_values = [b["max"] for b in builds_sorted]
    build_min_values = [b["min"] for b in builds_sorted]

    pairs = []
    for probe in probe_rgs:
        p_min, p_max = probe["min"], probe["max"]
        matching = []
        for i, build in enumerate(builds_sorted):
            try:
                if build_min_values[i] > p_max:
                    break
                if build_max_values[i] < p_min:
                    continue
            except TypeError:
                matching.append(build)
                continue
            matching.append(build)
        if matching:
            pairs.append({"probe": probe, "builds": matching})
    return pairs


def _conflict_groups(build_rgs: List[Dict], probe_rgs: List[Dict]) -> List[Dict]:
    """
    Interval-merge build and probe RGs by key-range overlap.
    Each returned group contains the minimal set of RGs that must be joined together.
    Groups with no build or no probe side are dropped (no join output possible).
    """
    if not build_rgs or not probe_rgs:
        return []

    tagged = [{'side': 'build', **rg} for rg in build_rgs] + \
             [{'side': 'probe', **rg} for rg in probe_rgs]

    try:
        tagged.sort(key=lambda x: x['min'])
    except TypeError:
        return [{'builds': list(build_rgs), 'probes': list(probe_rgs)}]

    groups: List[Dict] = []
    cur_builds: List[Dict] = []
    cur_probes: List[Dict] = []
    cur_max = None

    for rg in tagged:
        rg_min, rg_max = rg['min'], rg['max']
        try:
            start_new = cur_max is None or rg_min > cur_max
        except TypeError:
            start_new = False

        if start_new:
            if cur_builds and cur_probes:
                groups.append({'builds': cur_builds, 'probes': cur_probes})
            elif cur_builds or cur_probes:
                # One side has no match in this range — no join output, discard
                pass
            cur_builds, cur_probes, cur_max = [], [], rg_max
        else:
            try:
                cur_max = max(cur_max, rg_max)
            except TypeError:
                pass

        side_list = cur_builds if rg['side'] == 'build' else cur_probes
        side_list.append({k: v for k, v in rg.items() if k != 'side'})

    if cur_builds and cur_probes:
        groups.append({'builds': cur_builds, 'probes': cur_probes})

    return groups


def _split_conflict_group(group: Dict, max_size_mb: float, max_files: int = 20) -> List[Dict]:
    """
    Split an oversized conflict group into sub-tasks that respect max_size_mb
    and max_files per task.

    Two passes:
    1. File-count split: if build side has > max_files distinct files, chunk build
       files into groups of max_files and replicate probe to each chunk.
    2. Size split: for each resulting group, if total size > max_size_mb, chunk
       probe RGs and replicate build to each chunk.

    This ensures each row is emitted exactly once (the chunked side is never
    duplicated across tasks).
    """
    def _by_size(g: Dict) -> List[Dict]:
        build_size = sum(float(r.get('size_mb') or 0) for r in g['builds'])
        probe_budget = max(max_size_mb - build_size, max_size_mb * 0.3, 1.0)
        chunks, cur, cur_size = [], [], 0.0
        for probe in g['probes']:
            ps = float(probe.get('size_mb') or 0.1)
            if cur and cur_size + ps > probe_budget:
                chunks.append({'builds': g['builds'], 'probes': cur})
                cur, cur_size = [], 0.0
            cur.append(probe)
            cur_size += ps
        if cur:
            chunks.append({'builds': g['builds'], 'probes': cur})
        return chunks

    # Pass 1: file-count split on build side
    build_files = list(dict.fromkeys(r['full_path'] for r in group['builds']))
    if len(build_files) <= max_files:
        return _by_size(group)

    result = []
    for i in range(0, len(build_files), max_files):
        fc = set(build_files[i:i + max_files])
        sub = {
            'builds': [r for r in group['builds'] if r['full_path'] in fc],
            'probes': group['probes'],
        }
        result.extend(_by_size(sub))
    return result


def _pair_build_with_probe_ranges(build_rgs: List[Dict], probe_rgs: List[Dict]) -> List[Dict]:
    if not build_rgs or not probe_rgs:
        return []

    probes_sorted = sorted(probe_rgs, key=lambda x: x["min"])
    probe_max_values = [p["max"] for p in probes_sorted]
    probe_min_values = [p["min"] for p in probes_sorted]

    pairs = []
    for build in build_rgs:
        b_min, b_max = build["min"], build["max"]
        matching = []
        for i, probe in enumerate(probes_sorted):
            try:
                if probe_min_values[i] > b_max:
                    break
                if probe_max_values[i] < b_min:
                    continue
            except TypeError:
                matching.append(probe)
                continue
            matching.append(probe)
        if matching:
            pairs.append({"build": build, "probes": matching})
    return pairs


def _batch_range_pairs(pairs, max_size_mb=50, skew_threshold=20):
    if not pairs:
        return []

    tasks = []
    skewed_count = 0
    split_count = 0

    def _pair_total_size(pair):
        return float(pair["probe"].get("size_mb") or 0) + sum(float(b.get("size_mb") or 0) for b in pair["builds"])

    pair_sizes = [_pair_total_size(p) for p in pairs]
    over_count = sum(1 for s in pair_sizes if s > max_size_mb)

    if over_count / len(pairs) > 0.3:
        sorted_sizes = sorted(pair_sizes)
        p90 = sorted_sizes[int(len(sorted_sizes) * 0.9)]
        new_budget = max(max_size_mb, int(p90 * 1.3))
        print(
            f"  ◆ [range-join] {over_count}/{len(pairs)} pairs exceed {max_size_mb}MB, "
            f"auto-scaling budget to {new_budget}MB"
        )
        max_size_mb = new_budget

    cur_probes = []
    cur_builds = {}
    cur_size = 0.0

    def flush():
        nonlocal cur_probes, cur_builds, cur_size
        if cur_probes:
            tasks.append({
                "probes": list(cur_probes),
                "builds": list(cur_builds.values()),
            })
        cur_probes, cur_builds, cur_size = [], {}, 0.0

    def _split_skewed_pair(pair, budget_mb):
        probe = pair["probe"]
        builds = pair["builds"]
        probe_size = float(probe.get("size_mb") or 0)
        build_budget = max(budget_mb - probe_size, budget_mb * 0.3)

        chunks = []
        current = []
        current_size = 0.0
        for build in sorted(builds, key=lambda x: float(x.get("size_mb") or 0)):
            build_size = float(build.get("size_mb") or 0.1)
            if current_size + build_size > build_budget and current:
                chunks.append(current)
                current = [build]
                current_size = build_size
            else:
                current.append(build)
                current_size += build_size
        if current:
            chunks.append(current)

        return [{"probes": [probe], "builds": chunk} for chunk in chunks]

    for pair in pairs:
        probe = pair["probe"]
        builds = pair["builds"]
        pair_size = _pair_total_size(pair)

        if pair_size > max_size_mb:
            skewed_count += 1
            flush()
            sub_tasks = _split_skewed_pair(pair, max_size_mb)
            split_count += len(sub_tasks)
            tasks.extend(sub_tasks)
            continue

        if cur_size + pair_size > max_size_mb and cur_probes:
            flush()
        cur_probes.append(probe)
        for build in builds:
            key = (build["full_path"], build["rg_id"])
            cur_builds[key] = build
        cur_size += pair_size

    flush()

    if skewed_count:
        print(f"  ◆ [range-join] {skewed_count} over-budget pairs split into {split_count} sub-tasks")

    return tasks


def _batch_build_probe_pairs(pairs, max_size_mb=50, skew_threshold=20):
    if not pairs:
        return []

    tasks = []
    split_count = 0

    def _pair_total_size(pair):
        return float(pair["build"].get("size_mb") or 0) + sum(float(p.get("size_mb") or 0) for p in pair["probes"])

    def _flush_chunks(build, probes, budget_mb):
        nonlocal split_count
        if not probes:
            return

        build_size = float(build.get("size_mb") or 0)
        probe_budget = max(budget_mb - build_size, budget_mb * 0.3)
        probe_budget = max(probe_budget, 1.0)

        chunk = []
        chunk_size = 0.0
        for probe in probes:
            probe_size = float(probe.get("size_mb") or 0.1)
            hit_budget = chunk and (chunk_size + probe_size > probe_budget)
            hit_fanout = chunk and skew_threshold and len(chunk) >= skew_threshold
            if hit_budget or hit_fanout:
                tasks.append({"builds": [build], "probes": list(chunk)})
                split_count += 1
                chunk = []
                chunk_size = 0.0
            chunk.append(probe)
            chunk_size += probe_size

        if chunk:
            tasks.append({"builds": [build], "probes": list(chunk)})
            split_count += 1

    pair_sizes = [_pair_total_size(pair) for pair in pairs]
    over_count = sum(1 for size in pair_sizes if size > max_size_mb)
    if over_count / len(pairs) > 0.3:
        sorted_sizes = sorted(pair_sizes)
        p90 = sorted_sizes[int(len(sorted_sizes) * 0.9)]
        new_budget = max(max_size_mb, int(p90 * 1.3))
        print(
            f"  ◆ [adaptive-range] {over_count}/{len(pairs)} build anchors exceed {max_size_mb}MB, "
            f"auto-scaling budget to {new_budget}MB"
        )
        max_size_mb = new_budget

    for pair in pairs:
        build = pair["build"]
        probes = sorted(pair["probes"], key=lambda x: (x["full_path"], x["rg_id"]))
        _flush_chunks(build, probes, max_size_mb)

    if split_count:
        print(f"  ◆ [adaptive-range] generated {split_count} build-anchored tasks")

    return tasks


def _plan_build_anchored_range_tasks(
        raw_groups: List[Dict],
        max_size_mb: float,
        max_build_file_ratio: float = 0.5,
        max_probe_files: int = 8,
        max_probe_row_groups: int = 32,
) -> Tuple[List[Dict], Dict[str, int]]:
    if not raw_groups:
        return [], {
            "build_file_count": 0,
            "build_chunk_count": 0,
            "probe_chunk_count": 0,
        }

    build_budget_mb = max(max_size_mb * max_build_file_ratio, 1.0)

    def _chunk_build_file(builds: List[Dict]) -> List[List[Dict]]:
        total_size = sum(float(r.get("size_mb") or 0) for r in builds)
        if total_size <= build_budget_mb:
            return [list(builds)]

        chunks: List[List[Dict]] = []
        current: List[Dict] = []
        current_size = 0.0
        for build in sorted(builds, key=lambda x: x["rg_id"]):
            build_size = float(build.get("size_mb") or 0.1)
            if current and current_size + build_size > build_budget_mb:
                chunks.append(current)
                current = []
                current_size = 0.0
            current.append(build)
            current_size += build_size
        if current:
            chunks.append(current)
        return chunks

    def _match_probes(builds: List[Dict], probes: List[Dict]) -> List[Dict]:
        matches = []
        for probe in probes:
            if any(_ranges_overlap(build["min"], build["max"], probe["min"], probe["max"]) for build in builds):
                matches.append(probe)
        return matches

    def _chunk_probes(builds: List[Dict], probes: List[Dict]) -> List[List[Dict]]:
        build_size = sum(float(r.get("size_mb") or 0) for r in builds)
        probe_budget_mb = max(max_size_mb - build_size, max_size_mb * 0.3, 1.0)

        chunks: List[List[Dict]] = []
        current: List[Dict] = []
        current_size = 0.0
        current_files = set()

        for probe in sorted(probes, key=lambda x: (x["full_path"], x["rg_id"])):
            probe_size = float(probe.get("size_mb") or 0.1)
            next_file_count = len(current_files | {probe["full_path"]})
            hit_budget = current and (current_size + probe_size > probe_budget_mb)
            hit_rg_cap = current and max_probe_row_groups and len(current) >= max_probe_row_groups
            hit_file_cap = current and max_probe_files and next_file_count > max_probe_files

            if hit_budget or hit_rg_cap or hit_file_cap:
                chunks.append(current)
                current = []
                current_size = 0.0
                current_files = set()

            current.append(probe)
            current_size += probe_size
            current_files.add(probe["full_path"])

        if current:
            chunks.append(current)
        return chunks

    tasks: List[Dict] = []
    build_file_count = 0
    build_chunk_count = 0
    probe_chunk_count = 0

    for group in raw_groups:
        builds_by_file = defaultdict(list)
        for build in group["builds"]:
            builds_by_file[build["full_path"]].append(build)

        for _, file_builds in sorted(builds_by_file.items()):
            build_file_count += 1
            build_chunks = _chunk_build_file(file_builds)
            build_chunk_count += len(build_chunks)

            for build_chunk in build_chunks:
                matched_probes = _match_probes(build_chunk, group["probes"])
                if not matched_probes:
                    continue

                probe_chunks = _chunk_probes(build_chunk, matched_probes)
                probe_chunk_count += len(probe_chunks)

                for probe_chunk in probe_chunks:
                    tasks.append({
                        "builds": list(build_chunk),
                        "probes": list(probe_chunk),
                    })

    return tasks, {
        "build_file_count": build_file_count,
        "build_chunk_count": build_chunk_count,
        "probe_chunk_count": probe_chunk_count,
    }


def _pairs_to_task_inputs(task: Dict) -> tuple[Dict[str, list], Dict[str, list]]:
    build_paths_rgs = defaultdict(list)
    for build in task["builds"]:
        build_paths_rgs[build["full_path"]].append(build["rg_id"])

    probe_paths_rgs = defaultdict(list)
    for probe in task["probes"]:
        probe_paths_rgs[probe["full_path"]].append(probe["rg_id"])

    return dict(build_paths_rgs), dict(probe_paths_rgs)


def _build_probe_task_to_inputs(task: Dict) -> tuple[Dict[str, list], Dict[str, list]]:
    build_paths_rgs = defaultdict(list)
    for build in task["builds"]:
        build_paths_rgs[build["full_path"]].append(build["rg_id"])

    probe_paths_rgs = defaultdict(list)
    for probe in task["probes"]:
        probe_paths_rgs[probe["full_path"]].append(probe["rg_id"])

    return dict(build_paths_rgs), dict(probe_paths_rgs)


def _extract_where_clause(sql_clauses: str | None) -> str | None:
    if not sql_clauses or not sql_clauses.strip():
        return None
    try:
        wrapped = f"SELECT * FROM _t {sql_clauses}"
        ast = sqlglot.parse_one(wrapped)
        where_node = ast.find(sqlglot_exp.Where)
        if where_node is None:
            return None
        return where_node.this.sql()
    except Exception:
        return None


def _rg_matches_predicate(rg_stats: Dict, where_clause: str) -> bool:
    if not rg_stats or not where_clause:
        return True

    try:
        ast = sqlglot.parse_one(f"SELECT 1 WHERE {where_clause}")
        where = ast.find(sqlglot_exp.Where)
        if where is None:
            return True
        return _eval_predicate_against_stats(where.this, rg_stats)
    except Exception:
        return True


def batch_pairs(pairs: List[Dict], max_size_mb: float = 50) -> List[Dict]:
    tasks = []
    current_probes = []
    current_builds = {}
    current_size = 0.0

    def flush():
        if not current_probes:
            return
        tasks.append({
            "probes": list(current_probes),
            "builds": list(current_builds.values()),
        })

    for pair in pairs:
        probe_size = pair["probe"]["size_mb"]
        if current_size + probe_size > max_size_mb and current_probes:
            flush()
            current_probes, current_builds, current_size = [], {}, 0.0

        current_probes.append(pair["probe"])
        for build in pair["builds"]:
            key = (build["full_path"], build["rg_id"])
            current_builds[key] = build
        current_size += probe_size

    flush()
    return tasks


def _literal_value(node):
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(node, sqlglot_exp.Null):
        return None
    if isinstance(node, sqlglot_exp.Cast):
        inner = node.this
        raw = inner.this if isinstance(inner, sqlglot_exp.Literal) else str(inner)
        dtype = node.to.this
        try:
            if dtype == sqlglot_exp.DataType.Type.DATE:
                return date.fromisoformat(raw)
            if dtype in (sqlglot_exp.DataType.Type.DATETIME, sqlglot_exp.DataType.Type.TIMESTAMP):
                return datetime.fromisoformat(raw)
            if dtype == sqlglot_exp.DataType.Type.DECIMAL:
                return Decimal(raw)
            if dtype in (sqlglot_exp.DataType.Type.INT, sqlglot_exp.DataType.Type.BIGINT, sqlglot_exp.DataType.Type.SMALLINT):
                return int(raw)
            if dtype in (sqlglot_exp.DataType.Type.FLOAT, sqlglot_exp.DataType.Type.DOUBLE):
                return float(raw)
        except Exception:
            return raw
        return raw
    if isinstance(node, sqlglot_exp.Literal):
        raw = node.this
        if node.is_string:
            return raw
        try:
            return int(raw) if "." not in raw else float(raw)
        except Exception:
            return raw
    return None


def _coerce_for_compare(val, reference):
    from datetime import date, datetime
    from decimal import Decimal

    if val is None or reference is None:
        return val
    if type(val) is type(reference):
        return val
    try:
        if isinstance(reference, date) and not isinstance(reference, datetime) and isinstance(val, str):
            return date.fromisoformat(val)
        if isinstance(reference, datetime) and isinstance(val, str):
            return datetime.fromisoformat(val)
        if isinstance(reference, Decimal) and isinstance(val, (int, float, str)):
            return Decimal(str(val))
        if isinstance(reference, (int, float)) and isinstance(val, str):
            return type(reference)(val)
    except Exception:
        pass
    return val


def _eval_predicate_against_stats(node, rg_stats: Dict) -> bool:
    if isinstance(node, sqlglot_exp.And):
        return _eval_predicate_against_stats(node.left, rg_stats) and _eval_predicate_against_stats(node.right, rg_stats)
    if isinstance(node, sqlglot_exp.Or):
        return _eval_predicate_against_stats(node.left, rg_stats) or _eval_predicate_against_stats(node.right, rg_stats)
    if isinstance(node, sqlglot_exp.Not):
        return True

    def _col_and_lit(expression):
        left, right = expression.left, expression.right
        if isinstance(left, sqlglot_exp.Column) and not isinstance(right, sqlglot_exp.Column):
            lit = _literal_value(right)
            return left.name, lit, False
        if isinstance(right, sqlglot_exp.Column) and not isinstance(left, sqlglot_exp.Column):
            lit = _literal_value(left)
            return right.name, lit, True
        return None, None, False

    if isinstance(node, (sqlglot_exp.EQ, sqlglot_exp.NEQ, sqlglot_exp.GT, sqlglot_exp.GTE, sqlglot_exp.LT, sqlglot_exp.LTE)):
        col, lit, flipped = _col_and_lit(node)
        if col is None or lit is None or col not in rg_stats:
            return True
        stats = rg_stats[col]
        mn, mx = stats.get("min"), stats.get("max")
        if mn is None or mx is None:
            return True
        lit = _coerce_for_compare(lit, mn)

        try:
            if isinstance(node, sqlglot_exp.EQ):
                return mn <= lit <= mx
            if isinstance(node, sqlglot_exp.NEQ):
                return not (mn == mx == lit)
            if isinstance(node, sqlglot_exp.GT):
                return (mn > lit) if flipped else (mx > lit)
            if isinstance(node, sqlglot_exp.GTE):
                return (mn >= lit) if flipped else (mx >= lit)
            if isinstance(node, sqlglot_exp.LT):
                return (mx < lit) if flipped else (mn < lit)
            if isinstance(node, sqlglot_exp.LTE):
                return (mx <= lit) if flipped else (mn <= lit)
        except TypeError:
            return True

    if isinstance(node, sqlglot_exp.Between):
        if not isinstance(node.this, sqlglot_exp.Column):
            return True
        col = node.this.name
        if col not in rg_stats:
            return True
        stats = rg_stats[col]
        mn, mx = stats.get("min"), stats.get("max")
        if mn is None or mx is None:
            return True
        lo = _literal_value(node.args["low"])
        hi = _literal_value(node.args["high"])
        if lo is None or hi is None:
            return True
        lo = _coerce_for_compare(lo, mn)
        hi = _coerce_for_compare(hi, mn)
        try:
            return not (mx < lo or mn > hi)
        except TypeError:
            return True

    if isinstance(node, sqlglot_exp.In):
        if not isinstance(node.this, sqlglot_exp.Column):
            return True
        col = node.this.name
        if col not in rg_stats:
            return True
        stats = rg_stats[col]
        mn, mx = stats.get("min"), stats.get("max")
        if mn is None or mx is None:
            return True
        values = [_literal_value(v) for v in node.args.get("expressions", [])]
        values = [_coerce_for_compare(v, mn) for v in values if v is not None]
        if not values:
            return True
        try:
            keep = any(mn <= v <= mx for v in values)
            return not keep if node.args.get("negated") else keep
        except TypeError:
            return True

    if isinstance(node, sqlglot_exp.Is):
        return True

    if isinstance(node, (sqlglot_exp.Like, sqlglot_exp.ILike)):
        return True

    return True


def prune_row_groups_by_stats(files: List[Dict], where_clause: str | None) -> List[Dict]:
    if not where_clause:
        return files

    pruned = []
    for f in files:
        rgs = f.get("row_groups") or []
        if not rgs:
            pruned.append(f)
            continue

        kept_rgs = [rg for rg in rgs if _rg_matches_predicate(rg.get("stats") or {}, where_clause)]
        if not kept_rgs:
            continue

        new_f = dict(f)
        new_f["row_groups"] = kept_rgs
        if rgs and f.get("size_mb"):
            kept_bytes = sum(float(rg.get("total_byte_size_mb") or 0.0) for rg in kept_rgs)
            total_bytes = sum(float(rg.get("total_byte_size_mb") or 0.0) for rg in rgs)
            if total_bytes > 0:
                new_f["size_mb"] = round(f["size_mb"] * (kept_bytes / total_bytes), 2)
        pruned.append(new_f)

    return pruned


@lru_cache(maxsize=None)
def _load_json(path: str):
    return json.loads(Path(path).read_text())


def get_est_elements(task_name: str, json_path: str) -> int | None:
    data = _load_json(json_path)
    return next((item["est_elements"] for item in data if item["task_name"] == task_name), None)


def build_path(temp, task_name):
    return str(temp).replace("$task_name", task_name).replace("$random", uuid.uuid4().hex)


def _rg_size_mb(rg: Dict, columns: List[str] | None) -> float:
    if columns:
        col_map = {c["name"]: float(c.get("compressed_size_mb") or 0.0) for c in rg.get("columns") or []}
        matched = [col_map[c] for c in columns if c in col_map]
        if matched:
            return sum(matched)
    return float(rg.get("total_byte_size_mb") or 0.0)


# Most rows one function reads: max_size_mb counts compressed bytes of the selected columns, and a few well-compressed
# columns let one task take a whole file (Q18 l_orderkey, l_quantity: 60M rows in 177 MB), which runs a 2 GiB function
# out of memory. 44M rows ran fine (Q4). A larger group is split into equal parts. MAX_ROWS_PER_TASK=0 turns it off.
MAX_ROWS_PER_TASK = int(os.environ.get("MAX_ROWS_PER_TASK", 45_000_000))


def _split_by_rows(bucket: List[Dict], max_rows: int) -> List[List[Dict]]:
    rows = sum(item["rows"] for item in bucket)
    if not max_rows or rows <= max_rows or len(bucket) < 2:
        return [bucket]
    k = min(math.ceil(rows / max_rows), len(bucket))
    return [bucket[i * len(bucket) // k:(i + 1) * len(bucket) // k] for i in range(k)]


def partition_tasks(
        files: List[Dict],
        max_size_mb: float = 50,
        split_row_groups: bool = False,
        columns: List[str] | None = None,
        single_file: bool = False,
        max_rows: int | None = None,
) -> List[List[Dict]]:
    max_rows = MAX_ROWS_PER_TASK if max_rows is None else max_rows
    stream = []
    for f in files:
        file_size = float(f.get("size_mb") or 0.0)
        rgs = f.get("row_groups") or []

        if split_row_groups and rgs:
            rg_sizes = [_rg_size_mb(rg, columns) for rg in rgs]
            rg_size_sum = sum(rg_sizes)
            use_file_size = rg_size_sum < 1.0 and file_size > rg_size_sum
            per_rg_size = (file_size / len(rgs)) if use_file_size else None
            for rg, rg_size in zip(rgs, rg_sizes):
                size = per_rg_size if use_file_size else rg_size
                stream.append({"full_path": f["full_path"], "rg_id": rg["id"], "size_mb": size,
                               "rows": int(rg.get("num_rows") or 0)})
        else:
            stream.append({"full_path": f["full_path"], "rg_id": None, "size_mb": file_size, "rows": 0})

    if single_file:
        return [_build_group([item]) for item in stream]

    buckets, bucket, bucket_size = [], [], 0.0
    for item in stream:
        item_size = item["size_mb"]
        if bucket_size + item_size > max_size_mb and bucket:
            buckets.append(bucket)
            bucket, bucket_size = [], 0.0
        bucket.append(item)
        bucket_size += item_size
        if bucket_size >= max_size_mb:
            buckets.append(bucket)
            bucket, bucket_size = [], 0.0
    if bucket:
        buckets.append(bucket)
    return [_build_group(part) for b in buckets for part in _split_by_rows(b, max_rows)]


def _build_group(items: List[Dict]) -> List[Dict]:
    per_file = defaultdict(list)
    for item in items:
        per_file[item["full_path"]].append(item["rg_id"])
    return [
        {"full_path": path, "row_group_ids": None if None in ids else ids}
        for path, ids in per_file.items()
    ]
