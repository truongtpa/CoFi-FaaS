import glob
import json
import os
import platform
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, os.getcwd())

from operations.Nofityme import sendMessage
from operations.runner.J2FileEditor import J2FileEditor
from operations.runner.Runner import Runner

# Calibration sweep for the cost model (time vs per-function size, fixed cost per task, aggregate bandwidth).
# Sizes run in a shuffled order and 50 MB (the default) both first and last, so cluster drift during the sweep shows
# up as a 50 MB difference instead of being mistaken for a size effect.
MAX_SIZE_MB_SCENARIOS = [50, 10, 100, 25, 200, 15, 70, 35, 150, 20, 50]
HISTORY_DIR_TEMPLATE = "./history-sweep/size-{max_size_mb}/"
NOTE_TEMPLATE = "max_size sweep {max_size_mb}MB; BF-S3; q3 q9 q18"
K8S_ENDPOINT = "http://localhost:8080/" if platform.uname().machine == "arm64" else "http://10.158.1.10:30080/"
KNATIVE_FUNC = "bloomfaas-operation.default.10.158.1.10.nip.io"
BUCKET = "tpch-100"
METADATA_DIR = "benchmarks/metadata"
TOTAL_RUNS = 3
RUN_DELAY_S = 2
TASK_PARALLELISM = 70   # same as run-seek15.py and the plan runs
TMP_YAML = "tmp.yml.j2"

PARAMS_FILE = "./benchmarks/parms-tpch-100.json"
SEEDS = ["seed1", "seed2"]

# q3, q9, q18 were swept first (history-sweep/); the remaining queries with a BF template:
SWEEP_QUERIES = ["q2", "q4", "q5", "q7", "q8", "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q19", "q20", "q21"]
Q_FROM = 1
Q_TO = 22
LOG_FILE = f"./benchmarks/logs/benchmark-run-seek-max-size-series-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
YAML_FILES = sorted(
    [
        f for f in glob.glob("./benchmarks/tpch-parms/q*.yml.j2")
        if Q_FROM <= int(re.search(r"q(\d+)", f).group(1)) <= Q_TO
        and os.path.basename(f).split(".")[0] in SWEEP_QUERIES
    ],
    key=lambda x: int(re.search(r"q(\d+)", x).group(1)),
)

CONFIGS = {
    # "baseline": {"enable_bf": 0, "function_version": "bloomfaas", "enable_nfs_shuffle": 0},
    "bloom_faas_v2": {"enable_bf": 1, "function_version": "bloomfaas", "enable_nfs_shuffle": 0},
    # "bloom_faas_v2_nfs": {"enable_bf": 1, "function_version": "bloomfaas", "enable_nfs_shuffle": 1},
}

PARM_MAP: dict[str, list[str]] = {
    "q1": ["delta"],
    "q2": ["size", "type", "region"],
    "q3": ["segment", "date"],
    "q4": ["date"],
    "q5": ["region", "date"],
    "q6": ["date", "discount", "quantity"],
    "q7": ["nation1", "nation2"],
    "q8": ["nation", "region", "type"],
    "q9": ["color"],
    "q10": ["date"],
    "q11": ["nation", "fraction"],
    "q12": ["shipmode1", "shipmode2", "date"],
    "q13": ["word1", "word2"],
    "q14": ["date"],
    "q15": ["date"],
    "q16": ["brand", "type", "sizes"],
    "q17": ["brand", "container"],
    "q18": ["quantity"],
    "q19": ["brand1", "brand2", "brand3", "quantity1", "quantity2", "quantity3"],
    "q20": ["color", "date", "nation"],
    "q21": ["nation"],
    "q22": ["i1"],
}


def history_dir(max_size_mb: int) -> str:
    return HISTORY_DIR_TEMPLATE.format(max_size_mb=max_size_mb)


def note(max_size_mb: int) -> str:
    return NOTE_TEMPLATE.format(max_size_mb=max_size_mb)


def history_note_file(max_size_mb: int) -> str:
    return os.path.join(history_dir(max_size_mb), "run-seek15-history.txt")


def load_params() -> dict:
    try:
        with open(PARAMS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARN] {PARAMS_FILE} not found — running without TPC-H param substitution")
        return {}


def get_tpch_parms(all_params: dict, query_name: str, seed: str) -> dict[str, str]:
    q_key = query_name.lower()
    keys = PARM_MAP.get(q_key, [])
    params = all_params.get(q_key, {}).get(seed, {})

    result = {}
    for i, key in enumerate(keys, start=1):
        val = params.get(key)
        if val is not None:
            result[f"$parm{i}"] = str(val) if not isinstance(val, list) else ",".join(str(v) for v in val)
    return result


def append_log(entry: dict) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []

    logs.append(entry)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)


def append_history_note(entry: dict) -> None:
    max_size_mb = entry["max_size_mb_default"]
    os.makedirs(history_dir(max_size_mb), exist_ok=True)
    with open(history_note_file(max_size_mb), "a", encoding="utf-8") as f:
        f.write(
            "\n".join([
                f"[{entry['start']}] {entry['query']} | {entry['seed']} | {entry['config']} | run={entry['run']}",
                f"note: {note(max_size_mb)}",
                f"max_size_mb_default={max_size_mb}",
                f"runner={entry['runner']}, status={entry['status']}, duration_s={entry['duration_s']}",
                f"task_parallelism={TASK_PARALLELISM}, total_runs={TOTAL_RUNS}, run_delay_s={RUN_DELAY_S}",
                f"bucket={BUCKET}, metadata_dir={METADATA_DIR}, q_range=Q{Q_FROM}-Q{Q_TO}",
                f"k8s_endpoint={K8S_ENDPOINT}, knative_func={KNATIVE_FUNC}",
                "-" * 80,
            ])
            + "\n"
        )


def render_tmp_yaml(yaml_file: str, cfg: dict, tpch_parms: dict[str, str]) -> None:
    enable_nfs = cfg["enable_nfs_shuffle"]
    base_path = "/mnt/" if enable_nfs else f"s3://{BUCKET}/endpoints/{time.strftime('%Y%m%d%H%M%S')}"

    editor = J2FileEditor(yaml_file)
    for key, value in [
        ("$enable_bf", cfg["enable_bf"]),
        ("$enable_nfs_shuffle", enable_nfs),
        ("$bucket", BUCKET),
        ("$base_path", base_path),
        ("$s3_metadata", METADATA_DIR),
        ("$bf_statistic", True),
    ]:
        editor.replace(key, value)

    for key, value in tpch_parms.items():
        editor.replace(key, value)
        print(f"    {key} = {value}")

    editor.save(TMP_YAML)


def run_benchmark(
    yaml_file: str,
    cfg_name: str,
    cfg: dict,
    query_name: str,
    seed: str,
    tpch_parms: dict[str, str],
    max_size_mb: int,
) -> bool:
    render_tmp_yaml(yaml_file, cfg, tpch_parms)

    for run_idx in range(1, TOTAL_RUNS + 1):
        start = datetime.now().isoformat()
        time.sleep(2)
        result = Runner(TMP_YAML).execute(
            history_logs=history_dir(max_size_mb),
            knative_func=KNATIVE_FUNC,
            k8s=K8S_ENDPOINT,
            max_parallel=TASK_PARALLELISM,
            func_ver=cfg["function_version"],
            tag=f"ms{max_size_mb}",   # run folder <time>--bloomfaas-s3-bloomfaas--ms<size>
        )
        time.sleep(2)
        end = datetime.now().isoformat()

        failed = (not result) or isinstance(result, dict)
        log_entry = {
            "query": query_name,
            "seed": seed,
            "config": cfg_name,
            "runner": "Runner",
            "run": run_idx,
            "start": start,
            "end": end,
            "duration_s": (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
            "status": "failed" if failed else "success",
            "task_parallelism": TASK_PARALLELISM,
            "max_size_mb_default": max_size_mb,
            "history_dir": history_dir(max_size_mb),
            **({
                "failed_task": result.get("task_name"),
                "failed_message": result.get("message"),
            } if isinstance(result, dict) else {}),
        }
        append_log(log_entry)
        append_history_note(log_entry)

        if failed:
            reason = result.get("message", "unknown error") if isinstance(result, dict) else "no result"
            print(f"  [FAILED] run {run_idx} — {reason}")
            return False

        if run_idx < TOTAL_RUNS:
            time.sleep(RUN_DELAY_S)

    return True


def run_scenario(max_size_mb: int, all_params: dict) -> None:
    os.environ["MAX_SIZE_MB_DEFAULT"] = str(max_size_mb)
    os.makedirs(history_dir(max_size_mb), exist_ok=True)

    total_configs = len(CONFIGS)
    total_jobs_per_config = len(YAML_FILES) * len(SEEDS)

    print(f"\n{'#' * 70}")
    print(f"[Scenario] MAX_SIZE_MB_DEFAULT={max_size_mb} | history={history_dir(max_size_mb)}")
    print(f"{'#' * 70}\n")
    sendMessage(
        f"[Scenario] Bắt đầu MAX_SIZE_MB_DEFAULT={max_size_mb}\n"
        f"History: {history_dir(max_size_mb)}\n"
        f"{total_configs} systems × {len(YAML_FILES)} queries × {len(SEEDS)} seeds"
    )

    scenario_start = datetime.now()
    scenario_failed = []

    for cfg_idx, (cfg_name, cfg) in enumerate(CONFIGS.items(), 1):
        system_jobs = [(yaml_file, seed) for yaml_file in YAML_FILES for seed in SEEDS]

        print(f"\n{'=' * 60}")
        print(f"[Scenario {max_size_mb}MB][System {cfg_idx}/{total_configs}] Bắt đầu: {cfg_name}")
        print(f"  Runner = Runner | task_parallel = {TASK_PARALLELISM} | max_size_mb_default = {max_size_mb}")
        print(f"  Tổng queries × seeds = {total_jobs_per_config} jobs")
        print(f"{'=' * 60}\n")
        sendMessage(
            f"[Scenario {max_size_mb}MB][System {cfg_idx}/{total_configs}] Bắt đầu: {cfg_name}\n"
            f"Runner task={TASK_PARALLELISM}, max_size={max_size_mb}MB\n"
            f"Tổng {total_jobs_per_config} jobs (Q{Q_FROM}–Q{Q_TO})"
        )

        system_start = datetime.now()
        failed_queries = []

        for job_idx, (yaml_file, seed) in enumerate(system_jobs, 1):
            query_name = os.path.basename(yaml_file).replace(".yml.j2", "")
            tpch_parms = get_tpch_parms(all_params, query_name, seed)

            print(f"  [{job_idx}/{total_jobs_per_config}] {query_name} | {seed} | {cfg_name} | max_size={max_size_mb}MB")

            try:
                ok = run_benchmark(yaml_file, cfg_name, cfg, query_name, seed, tpch_parms, max_size_mb)
                if not ok:
                    failed_queries.append(f"{query_name}/{seed}")
            except Exception as e:
                print(f"  [ERROR] {query_name}/{seed}: {e}")
                failed_queries.append(f"{query_name}/{seed}")

            if job_idx < total_jobs_per_config:
                print(f"  Waiting {RUN_DELAY_S}s...\n")
                time.sleep(RUN_DELAY_S)

        system_end = datetime.now()
        elapsed = (system_end - system_start).total_seconds()
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        scenario_failed.extend([f"{cfg_name}:{item}" for item in failed_queries])

        fail_summary = f"\nFailed: {', '.join(failed_queries)}" if failed_queries else "\nTất cả thành công"
        msg = (
            f"[Scenario {max_size_mb}MB][System {cfg_idx}/{total_configs}] Xong: {cfg_name}\n"
            f"   Thời gian: {elapsed_str}\n"
            f"   {total_jobs_per_config} jobs{fail_summary}"
        )
        print(f"\n{msg}\n")
        sendMessage(msg)

        if cfg_idx < total_configs:
            print(f"Waiting {RUN_DELAY_S}s trước system tiếp theo...\n")
            time.sleep(RUN_DELAY_S)

    scenario_end = datetime.now()
    elapsed = (scenario_end - scenario_start).total_seconds()
    elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    fail_summary = f"\nFailed: {', '.join(scenario_failed)}" if scenario_failed else "\nTất cả thành công"
    sendMessage(
        f"[Scenario] Hoàn tất MAX_SIZE_MB_DEFAULT={max_size_mb}\n"
        f"Thời gian: {elapsed_str}\n"
        f"History: {history_dir(max_size_mb)}{fail_summary}"
    )


def main() -> None:
    all_params = load_params()
    for idx, max_size_mb in enumerate(MAX_SIZE_MB_SCENARIOS, 1):
        print(f"\n>>> Running scenario {idx}/{len(MAX_SIZE_MB_SCENARIOS)}: {max_size_mb}MB")
        run_scenario(max_size_mb, all_params)
        if idx < len(MAX_SIZE_MB_SCENARIOS):
            print(f"Waiting {RUN_DELAY_S}s trước scenario tiếp theo...\n")
            time.sleep(RUN_DELAY_S)

    sendMessage(
        "Benchmark hoàn tất! Runner | "
        f"{len(MAX_SIZE_MB_SCENARIOS)} scenarios {MAX_SIZE_MB_SCENARIOS} × "
        f"{len(CONFIGS)} systems × {len(YAML_FILES)} queries × {len(SEEDS)} seeds."
    )
    print(f"\nDone. Log: {LOG_FILE}")


if __name__ == "__main__":
    main()
