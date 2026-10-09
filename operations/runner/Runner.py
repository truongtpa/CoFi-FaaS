import json
import os, sys, platform
import threading
from collections import defaultdict
from operations.runner.EstimateBF import EstimateBF

sys.path.insert(0, os.getcwd())
from operations.libs.HTTPCall import HTTPCall
from operations.libs.S3Metadata import S3Metadata
from operations.libs.Tools import Tools
from operations.libs.Pipeline import Pipeline
from operations.libs import CostLog
from operations.libs.ParquetS3Utils import parse_select_columns
from operations.runner.FlowParser import FlowParser
from operations.runner.Helpers import (
    _batch_build_probe_pairs,
    _batch_range_pairs,
    _build_probe_task_to_inputs,
    _choose_join_strategy,
    _compute_adaptive_skew_threshold,
    _conflict_groups,
    _split_conflict_group,
    _extract_rg_ranges,
    _pair_build_with_probe_ranges,
    _pair_by_range,
    _plan_build_anchored_range_tasks,
    _pairs_to_task_inputs,
    build_path,
    get_est_elements,
    partition_tasks,
)
from datetime import datetime


class Runner:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.parser = FlowParser(yaml_file)
        self.flow_info = self.parser.get_flow_info()
        self.bf_log = []
        self._bf_log_lock = threading.Lock()
        self.predict_est_elements = None

    def _find_summary_clean_step(self):
        for step in self.parser.get_steps():
            if step.get("func") == "summary_clean":
                return step
        return None

    def _write_bf_statistics(self, ctx):
        history_logs = ctx["history_logs"]
        enable_bf = ctx["enable_bf"]
        bucket = ctx["bucket"]
        query_name = ctx["query_name"]

        path_statistic = f"{history_logs}/statistics-bf.json"
        with self._bf_log_lock:
            bf_payload = list(self.bf_log)
        with open(path_statistic, 'w') as f:
            json.dump(bf_payload, f, indent=2, ensure_ascii=False)

        predict_est_elements = self.predict_est_elements or f"./benchmarks/bf-analyze/{bucket}-{query_name}-est-bf-elements.json"
        should_analyze = bool(enable_bf and bf_payload)
        if should_analyze and os.path.exists(predict_est_elements):
            try:
                existing = json.loads(open(predict_est_elements).read())
                should_analyze = existing == []
            except Exception:
                should_analyze = True

        if should_analyze:
            EstimateBF(
                stats_filename=path_statistic,
                history_folder=history_logs,
                result_file=predict_est_elements,
            ).analyze()

    def _cleanup_params(self, ctx):
        summary_step = self._find_summary_clean_step()
        if summary_step:
            params = summary_step.get("params", {})
            return {
                "task_name": summary_step.get("name", "summary-clean"),
                "output_s3": params.get("output_s3", ctx["variables"].get("base_path")),
                "nfs_folder": params.get("nfs_folder", ctx["nfs_folder"]),
                "is_delete": params.get("is_delete", True),
            }

        return {
            "task_name": "__default-summary-clean",
            "output_s3": ctx["variables"].get("base_path"),
            "nfs_folder": ctx["nfs_folder"],
            "is_delete": True,
        }

    def _run_cleanup(self, ctx, reason=None):
        if ctx.get("cleanup_ran"):
            return True

        ctx["cleanup_ran"] = True
        self._write_bf_statistics(ctx)

        params = self._cleanup_params(ctx)
        print(f"[Cleanup] running {params['task_name']}" + (f" ({reason})" if reason else ""))

        executor = Pipeline(name=params["task_name"], max_parallel=1, max_retries=1)
        executor.add_task(
            task_name=params["task_name"],
            func=ctx["func"].summary_clean,
            output_s3=params["output_s3"],
            nfs_folder=params["nfs_folder"],
            is_delete=params["is_delete"],
        )
        result = executor.execute_and_save(output_path=ctx["history_logs"])
        return int(result.get("failed_task", 0)) == 0

    def _prepare_execute_context(self, history_logs=None, k8s=None, knative_func=None, max_parallel=4, func_ver='v1',
                                 tag=None):
        query_name = self.flow_info['name']
        variables = self.flow_info['variables']
        s3_metadata = variables['s3_metadata']
        nfs_folder = variables["nfs_folder"]
        enable_bf = int(variables['enable_bf'])
        bucket = variables['bucket']
        enable_nfs_shuffle = int(variables['enable_nfs_shuffle'])

        max_size_mb_default = float(os.environ.get("MAX_SIZE_MB_DEFAULT", 50))
        shuffle = 'nfs' if enable_nfs_shuffle else 's3'
        system_name = f'bloomfaas-{shuffle}-{func_ver}' if enable_bf == 1 else f'starling-base'
        if tag:   # e.g. the plan name: <time>--bloomfaas-s3-bloomfaas--knee
            system_name = f'{system_name}--{tag}'
        folder_name = f'{history_logs}/{query_name}/{datetime.now().strftime("%Y%m%d-%H%M%s")}--{system_name}'
        self.predict_est_elements = f"./benchmarks/bf-analyze/{bucket}-{query_name}-est-bf-elements.json"
        os.makedirs(folder_name, exist_ok=True)
        history_logs = folder_name

        Tools.export_minio(folder_name)
        s3 = S3Metadata()
        s3.load_json(s3_metadata)
        func = HTTPCall(k8s, knative_func, debug=False, version=func_ver)
        CostLog.dag(folder_name, self.parser.get_steps(), variables, max_parallel)

        return {
            "query_name": query_name,
            "variables": variables,
            "nfs_folder": nfs_folder,
            "enable_bf": enable_bf,
            "bucket": bucket,
            "max_size_mb_default": max_size_mb_default,
            "system_name": system_name,
            "history_logs": history_logs,
            "s3": s3,
            "func": func,
            "max_parallel": max_parallel,
            "cleanup_ran": False,
        }

    def _execute_step(self, step, ctx):
        history_logs = ctx["history_logs"]
        func = ctx["func"]
        s3 = ctx["s3"]
        variables = ctx["variables"]
        nfs_folder = ctx["nfs_folder"]
        enable_bf = ctx["enable_bf"]
        bucket = ctx["bucket"]
        max_size_mb_default = ctx["max_size_mb_default"]
        system_name = ctx["system_name"]
        max_parallel = ctx["max_parallel"]

        task_name = step['name']
        max_parallel = step.get("max_parallel") or max_parallel   # a stage may run with fewer/more functions at once
        executor = Pipeline(name=task_name, max_parallel=max_parallel, max_retries=3)

        if step["func"] == "scan":
            parm = step["params"]
            inputs = step["inputs"]
            from_previous_tasks = step["from_previous_tasks"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default
            previous_task = None
            bf_path = None
            is_filter_bf = False
            default_scan_workers = 1 if 'starling-base' in system_name else 4
            max_workers = parm.get('max_workers') or default_scan_workers
            single_row_group_per_task = bool(parm.get('single_row_group_per_task'))
            source_files = s3.search(inputs)['data']['files']

            if from_previous_tasks:
                previous_task = executor.load_json(f"{history_logs}/{from_previous_tasks}", from_previous_tasks)

            if parm.get('bf_column') and parm.get('filter_by_bf') and previous_task:
                is_filter_bf = True
                bf_path = previous_task[0]['bf_path']['path']

            for file in source_files:
                _raw_cols = parse_select_columns(parm.get('select'), parm.get('sql_clauses'))
                partitions = partition_tasks(
                    [file],
                    max_size_mb=max_size_mb,
                    split_row_groups=True,
                    columns=_raw_cols,
                    single_file=single_row_group_per_task,
                )
                for group in partitions:
                    spec = group[0]
                    executor.add_task(
                        task_name=task_name,
                        func=func.scan,
                        select=parm['select'],
                        location=spec['full_path'],
                        sql_clauses=parm['sql_clauses'],
                        output_file=build_path(parm['output_file'], task_name),
                        row_group_ids=spec['row_group_ids'],
                        bf_column=parm.get('bf_column'),
                        filter_by_bf=bf_path if is_filter_bf else None,
                        max_workers=max_workers,
                        sort_by=parm.get('sort_by'),
                    )

        if step["func"] in ("aggregate", "hash_aggregate"):
            parm = step["params"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default
            from_previous_tasks = step["from_previous_tasks"]
            previous_task = executor.load_json(history_logs + "/" + from_previous_tasks, from_previous_tasks)

            est_elements = get_est_elements(task_name, self.predict_est_elements) if os.path.exists(
                self.predict_est_elements) else None
            est_elements = est_elements or parm.get('est_elements')

            if not previous_task:
                print('[DEBUG] The function combine cannot continue because there are no previous tasks')
                return None

            if step["func"] == "hash_aggregate":
                buckets = defaultdict(list)
                for task_result in previous_task:
                    for bucket in task_result.get("buckets", []):
                        buckets[bucket["bucket_id"]].append(bucket)
                tasks_input = list(buckets.values())
            else:
                tasks_input = partition_tasks(previous_task, max_size_mb=max_size_mb, columns=None)

            for group in tasks_input:
                executor.add_task(
                    task_name=task_name,
                    func=func.aggregate,
                    input_parquet_paths=[spec['full_path'] for spec in group],
                    output_file=build_path(parm['output_file'], task_name),
                    select=parm.get('select'),
                    sql_clauses=parm.get('sql_clauses'),
                    single_file=parm.get('single_file'),
                    bf_column=parm.get('bf_column'),
                    bf_path=build_path(parm.get('bf_path', ''), task_name),
                    est_elements=est_elements,
                    error_rate=0.001,
                    sort_by=parm.get('sort_by'),
                    hash_column=parm.get('hash_column'),
                    num_buckets=parm.get('num_buckets'),
                    partition_output_folder=(
                        build_path(parm['partition_output_folder'], task_name)
                        if parm.get('partition_output_folder') else None
                    ),
                )

        if step["func"] == "merge_bf":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            with self._bf_log_lock:
                self.bf_log.append({
                    'from_previous_tasks': from_previous_tasks,
                    'task_name': task_name
                })
            executor.add_task(
                task_name=task_name,
                func=func.merge_bf,
                folder_bf=f'{nfs_folder}/{from_previous_tasks}',
                file_output=build_path(parm['file_output'], task_name),
                error_rate=0.001,
            )

        if step["func"] == "hash_partition":
            parm = step["params"]
            num_buckets = int(parm.get('num_buckets')) or 10
            from_previous_tasks = step["from_previous_tasks"]
            previous_task = executor.load_json(history_logs + "/" + from_previous_tasks, from_previous_tasks)

            if not previous_task:
                print('[DEBUG] hash_partition cannot continue because there are no previous tasks')
                return None

            for file in previous_task:
                executor.add_task(
                    task_name=task_name,
                    func=func.hash_partition,
                    input_parquet_paths=[file["full_path"]],
                    output_folder=f"{variables['base_path']}/{task_name}",
                    hash_column=parm["hash_column"],
                    num_buckets=num_buckets,
                )

        if step["func"] == "broadcast_join":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default
            join_type = parm.get("join_type") or "inner"
            build_tables = executor.load_json(history_logs + "/" + from_previous_tasks[0], from_previous_tasks[0])
            probe_tables = executor.load_json(history_logs + "/" + from_previous_tasks[1], from_previous_tasks[1])

            for build_group in partition_tasks(build_tables, max_size_mb=max_size_mb):
                for probe_group in partition_tasks(probe_tables, max_size_mb=max_size_mb):
                    executor.add_task(
                        task_name=task_name,
                        func=func.join,
                        build_table_paths=[s['full_path'] for s in build_group],
                        probe_table_paths=[s['full_path'] for s in probe_group],
                        probe_select=parm['probe_select'],
                        join_condition=parm['join_condition'],
                        join_type=join_type,
                        output_file=build_path(parm['output_file'], task_name),
                        sort_by=parm.get('sort_by'),
                    )

        if step["func"] == "broadcast_join_pushdown":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default
            join_type = parm.get("join_type") or "inner"
            build_tables = executor.load_json(history_logs + "/" + from_previous_tasks[0], from_previous_tasks[0])
            probe_tables = executor.load_json(history_logs + "/" + from_previous_tasks[1], from_previous_tasks[1])

            for build_group in partition_tasks(build_tables, max_size_mb=max_size_mb):
                for probe_group in partition_tasks(probe_tables, max_size_mb=max_size_mb):
                    executor.add_task(
                        task_name=task_name,
                        func=func.join_pushdown,
                        build_table_paths=[s['full_path'] for s in build_group],
                        probe_table_paths=[s['full_path'] for s in probe_group],
                        probe_select=parm['probe_select'],
                        join_condition=parm['join_condition'],
                        build_key=parm['build_key'],
                        probe_key=parm['probe_key'],
                        join_type=join_type,
                        output_file=build_path(parm['output_file'], task_name),
                        sort_by=parm.get('sort_by'),
                    )

        if step["func"] == "hash_join":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            join_type = parm.get("join_type") or "inner"

            build_results = executor.load_json(history_logs + "/" + from_previous_tasks[0], from_previous_tasks[0])
            probe_results = executor.load_json(history_logs + "/" + from_previous_tasks[1], from_previous_tasks[1])

            build_buckets: dict[int, list] = defaultdict(list)
            probe_buckets: dict[int, list] = defaultdict(list)

            for task_result in build_results:
                for bucket in task_result.get("buckets", []):
                    build_buckets[bucket["bucket_id"]].append(bucket)

            for task_result in probe_results:
                for bucket in task_result.get("buckets", []):
                    probe_buckets[bucket["bucket_id"]].append(bucket)

            all_bucket_ids = sorted(set(build_buckets) | set(probe_buckets))
            for bucket_id in all_bucket_ids:
                build_group = build_buckets.get(bucket_id, [])
                probe_group = probe_buckets.get(bucket_id, [])

                if not build_group or not probe_group:
                    if join_type in ("left_anti",) and probe_group:
                        executor.add_task(
                            task_name=task_name,
                            func=func.join,
                            build_table_paths=[s['data'] for s in build_group],
                            probe_table_paths=[s['data'] for s in probe_group],
                            probe_select=parm['probe_select'],
                            join_condition=parm['join_condition'],
                            join_type=join_type,
                            output_file=build_path(parm['output_file'], task_name),
                            sort_by=parm.get('sort_by'),
                        )
                    continue

                executor.add_task(
                    task_name=task_name,
                    func=func.join,
                    build_table_paths=[s['data'] for s in build_group],
                    probe_table_paths=[s['data'] for s in probe_group],
                    probe_select=parm['probe_select'],
                    join_condition=parm['join_condition'],
                    join_type=join_type,
                    output_file=build_path(parm['output_file'], task_name),
                    sort_by=parm.get('sort_by'),
                )

        if step["func"] == "range_join":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default
            join_type = parm.get("join_type") or "inner"
            build_key = parm["build_key"]
            probe_key = parm["probe_key"]
            skew_threshold = parm.get("skew_threshold") or 20
            max_files = parm.get("max_files_per_task") or 20

            build_files = executor.load_json(history_logs + "/" + from_previous_tasks[0], from_previous_tasks[0])
            probe_files = executor.load_json(history_logs + "/" + from_previous_tasks[1], from_previous_tasks[1])

            print(f"[range-join][debug] build source json={from_previous_tasks[0]} entries={len(build_files)}")
            print(f"[range-join][debug] probe source json={from_previous_tasks[1]} entries={len(probe_files)}")
            print(f"[range-join][debug] build sample={build_files[:2]}")
            print(f"[range-join][debug] probe sample={probe_files[:2]}")

            build_rgs = _extract_rg_ranges(build_files, build_key)
            probe_rgs = _extract_rg_ranges(probe_files, probe_key)

            if not build_rgs or not probe_rgs:
                print(f"[range-join] missing stats for '{build_key}' or '{probe_key}', " f"falling back to broadcast")
                for build_group in partition_tasks(build_files, max_size_mb=max_size_mb):
                    for probe_group in partition_tasks(probe_files, max_size_mb=max_size_mb):
                        executor.add_task(
                            task_name=task_name,
                            func=func.join,
                            build_table_paths=[s['full_path'] for s in build_group],
                            probe_table_paths=[s['full_path'] for s in probe_group],
                            probe_select=parm['probe_select'],
                            join_condition=parm['join_condition'],
                            join_type=join_type,
                            output_file=build_path(parm['output_file'], task_name),
                            sort_by=parm.get('sort_by'),
                        )
            else:
                user_skew = parm.get("skew_threshold")
                skew_threshold = _compute_adaptive_skew_threshold(
                    build_rgs, probe_rgs,
                    max_task_size_mb=max_size_mb,
                    user_override=user_skew,
                    max_parallel=50
                )
                if user_skew is None:
                    print(f"    skew_threshold auto-computed: {skew_threshold}")

                pairs = _pair_by_range(build_rgs, probe_rgs)
                raw_tasks = _batch_range_pairs(
                    pairs,
                    max_size_mb=max_size_mb,
                    skew_threshold=skew_threshold,
                )

                tasks_input = []
                split_count = 0
                for task in raw_tasks:
                    task_size = (
                        sum(float(r.get("size_mb") or 0) for r in task["builds"]) +
                        sum(float(r.get("size_mb") or 0) for r in task["probes"])
                    )
                    build_file_count = len(set(r["full_path"] for r in task["builds"]))
                    if task_size > max_size_mb or build_file_count > max_files:
                        sub_tasks = _split_conflict_group(task, max_size_mb, max_files=max_files)
                        split_count += len(sub_tasks)
                        tasks_input.extend(sub_tasks)
                    else:
                        tasks_input.append(task)

                total_pair_work = sum(len(t["builds"]) * len(t["probes"]) for t in tasks_input)
                naive_work = len(build_rgs) * len(probe_rgs)
                reduction = 100 * (1 - total_pair_work / naive_work) if naive_work > 0 else 0.0

                print(f"[range-join] {len(tasks_input)} tasks  " f"pairs={total_pair_work}/{naive_work}  reduction={reduction:.1f}%")
                if split_count:
                    print(
                        f"  ◆ [range-join] {split_count} task chunk(s) created "
                        f"to enforce max_files_per_task={max_files}"
                    )
                for task in tasks_input:
                    build_paths_rgs, probe_paths_rgs = _pairs_to_task_inputs(task)
                    print(f"[range-join][debug] build_table_paths={build_paths_rgs}")
                    print(f"[range-join][debug] probe_table_paths={probe_paths_rgs}")
                    executor.add_task(
                        task_name=task_name,
                        func=func.range_join,
                        build_table_paths=build_paths_rgs,
                        probe_table_paths=probe_paths_rgs,
                        probe_select=parm['probe_select'],
                        join_condition=parm['join_condition'],
                        join_type=join_type,
                        output_file=build_path(parm['output_file'], task_name),
                        sort_by=parm.get('sort_by'),
                    )

        if step["func"] == "adaptive_join":
            parm = step["params"]
            from_previous_tasks = step["from_previous_tasks"]
            max_size_mb = step.get("max_size_mb") or max_size_mb_default * 0.1
            join_type = parm.get("join_type") or "inner"
            build_key = parm["build_key"]
            probe_key = parm["probe_key"]
            broadcast_threshold_mb = parm.get("broadcast_threshold_mb") or 100
            force_strategy = parm.get("force_strategy")

            build_files = executor.load_json(history_logs + "/" + from_previous_tasks[0], from_previous_tasks[0])
            probe_files = executor.load_json(history_logs + "/" + from_previous_tasks[1], from_previous_tasks[1])

            try:
                strategy, reason = _choose_join_strategy(
                    build_files, probe_files, build_key, probe_key,
                    broadcast_threshold_mb, force_strategy,
                )
            except ValueError as e:
                print(f"  ✗ [adaptive-join] {e}")
                return {'message': str(e), 'task_name': task_name}

            print(f"  ◆ [adaptive-join] → {strategy}  ({reason})")

            if strategy == 'broadcast':
                build_group = build_files
                for probe_group in partition_tasks(probe_files, max_size_mb=max_size_mb):
                    executor.add_task(
                        task_name=task_name,
                        func=func.join,
                        build_table_paths=[s['full_path'] for s in build_group],
                        probe_table_paths=[s['full_path'] for s in probe_group],
                        probe_select=parm['probe_select'],
                        join_condition=parm['join_condition'],
                        join_type=join_type,
                        output_file=build_path(parm['output_file'], task_name),
                        sort_by=parm.get('sort_by'),
                    )

            elif strategy == 'range':
                build_rgs = _extract_rg_ranges(build_files, build_key)
                probe_rgs = _extract_rg_ranges(probe_files, probe_key)

                if not build_rgs or not probe_rgs:
                    print(f"    stats missing → fallback to broadcast")
                    for build_group in partition_tasks(build_files, max_size_mb=max_size_mb):
                        for probe_group in partition_tasks(probe_files, max_size_mb=max_size_mb):
                            executor.add_task(
                                task_name=task_name,
                                func=func.join,
                                build_table_paths=[s['full_path'] for s in build_group],
                                probe_table_paths=[s['full_path'] for s in probe_group],
                                probe_select=parm['probe_select'],
                                join_condition=parm['join_condition'],
                                join_type=join_type,
                                output_file=build_path(parm['output_file'], task_name),
                                sort_by=parm.get('sort_by'),
                            )
                else:
                    raw_groups = _conflict_groups(build_rgs, probe_rgs)
                    max_probe_files = parm.get('max_probe_files_per_task') or 8
                    max_probe_row_groups = parm.get('max_probe_row_groups_per_task') or 32
                    max_build_file_ratio = parm.get('max_build_file_ratio') or 1

                    groups, plan_stats = _plan_build_anchored_range_tasks(
                        raw_groups,
                        max_size_mb=max_size_mb,
                        max_build_file_ratio=max_build_file_ratio,
                        max_probe_files=max_probe_files,
                        max_probe_row_groups=max_probe_row_groups,
                    )

                    naive_work = len(build_rgs) * len(probe_rgs)
                    total_pair_work = sum(len(g['builds']) * len(g['probes']) for g in groups)
                    reduction = 100 * (1 - total_pair_work / max(naive_work, 1))

                    print(
                        f"    build-anchored range planning: build_files={plan_stats['build_file_count']} "
                        f"build_chunks={plan_stats['build_chunk_count']} probe_chunks={plan_stats['probe_chunk_count']} "
                        f"(max_probe_files={max_probe_files}, max_probe_rgs={max_probe_row_groups}, "
                        f"build_ratio={max_build_file_ratio})"
                    )
                    print(f"    {len(groups)} tasks from {len(raw_groups)} conflict groups  "
                          f"pairs={total_pair_work}/{naive_work}  "
                          f"reduction={reduction:.1f}%")

                    for group in groups:
                        build_paths_rgs, probe_paths_rgs = _build_probe_task_to_inputs(group)
                        executor.add_task(
                            task_name=task_name,
                            func=func.range_join,
                            build_table_paths=build_paths_rgs,
                            probe_table_paths=probe_paths_rgs,
                            probe_select=parm['probe_select'],
                            join_condition=parm['join_condition'],
                            join_type=join_type,
                            output_file=build_path(parm['output_file'], task_name),
                            sort_by=parm.get('sort_by'),
                        )

        if step["func"] == "summary_clean":
            cleanup_ok = self._run_cleanup(ctx, reason=f"terminal step {task_name}")
            return True if cleanup_ok else {
                'message': 'Cleanup failed',
                'task_name': task_name,
            }

        result = executor.execute_and_save(output_path=history_logs)
        status = "✓" if result['failed_task'] == 0 else "✗"
        print(f"{status} {task_name:<60} "
              f"tasks={result['successful_task']}/{result['total_task']} \t\t"
              f"time={result['pipeline_execution_time_s']}s")

        if int(result['failed_task']) > 0:
            if step["func"] != "summary_clean":
                self._run_cleanup(ctx, reason=f"failure in {task_name}")
            return {
                'message': f"The pipeline is stop because it has {result['failed_task']} tasks is failed",
                'task_name': task_name
            }

        return True

    def execute(self, history_logs=None, k8s=None, knative_func=None, max_parallel=4, func_ver='v1', tag=None):
        ctx = self._prepare_execute_context(
            history_logs=history_logs,
            k8s=k8s,
            knative_func=knative_func,
            max_parallel=max_parallel,
            func_ver=func_ver,
            tag=tag,
        )

        for step in self.parser.get_steps():
            rs = self._execute_step(step, ctx)
            if rs is not True:
                return rs

        if self._find_summary_clean_step() is None:
            cleanup_ok = self._run_cleanup(ctx, reason="implicit final cleanup")
            if not cleanup_ok:
                return {
                    'message': 'Cleanup failed',
                    'task_name': '__default-summary-clean'
                }

        return True
