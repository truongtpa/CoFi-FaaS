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

HISTORY_DIR = "./history/"
PLANS_HISTORY_DIR = "./history-plans/"   # templates from benchmarks/plans-j2/<plan>/ go to history-plans/<plan>/
NOTE = "10MB; COMPRESSION SNAPPY; seed5"
HISTORY_NOTE_FILE = os.path.join(HISTORY_DIR, "run-seek5-history.txt")
K8S_ENDPOINT = "http://localhost:8080/" if platform.uname().machine == "arm64" else "http://10.158.1.10:30080/"
KNATIVE_FUNC = "bloomfaas-operation.default.10.158.1.10.nip.io"
BUCKET = "tpch-100"
METADATA_DIR = "benchmarks/metadata"
TOTAL_RUNS = 3
RUN_DELAY_S = 2
TASK_PARALLELISM = 70
TMP_YAML = "tmp.yml.j2"

PARAMS_FILE = "./benchmarks/parms-tpch-100.json"
SEEDS = [f"seed{i}" for i in range(1, 2)]

Q_FROM = 1
Q_TO = 22
LOG_FILE = f"./benchmarks/logs/benchmark-run-seek-log-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
# YAML_FILES = sorted(
#     [f for p in ("no_bf", "default", "knee", "min_T", "min_C")
#      for f in glob.glob(f"./benchmarks/plans-j2-grid/{p}/q*.yml.j2")],
#     key=lambda x: int(re.search(r"q(\d+)", x).group(1)),
# )
# YAML_FILES = sorted(glob.glob("./benchmarks/plans-j2-validate/*/q18.yml.j2"), key=lambda x: int(re.search(r"q(\d+)", x).group(1)))
# validate experiment (paper, Section 3/4): every plan of the Pareto front of every query in one session
# -> history-plans-validate/<plan>/<query>/
RUNS_NEEDED = {}   # template -> number of runs, to re-run failed ones only (empty = TOTAL_RUNS for every template)
YAML_FILES = sorted(glob.glob("./benchmarks/plans-j2-validate/*/q*.yml.j2"), key=lambda x: int(re.search(r"q(\d+)", x).group(1)))
if RUNS_NEEDED:
    YAML_FILES = [f for f in YAML_FILES if f in RUNS_NEEDED]
CONFIGS = {
    # 'baseline': {'enable_bf': 0, 'function_version': 'bloomfaas', 'enable_nfs_shuffle': 0},
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
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []

    logs.append(entry)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)


def plan_of(yaml_file: str) -> tuple[str, str] | None:
    """(plan set, plan) of a template written by gen-plans.py --emit-j2: benchmarks/plans-j2<-set>/<plan>/<query>.yml.j2,
    e.g. ("-s100", "knee") for plans-j2-s100/knee/q9.yml.j2 and ("", "knee") for plans-j2/knee/q9.yml.j2."""
    m = re.search(r"plans-j2([^/\\]*)[/\\]([^/\\]+)[/\\]", yaml_file)
    return (m.group(1), m.group(2)) if m else None


def history_dir_of(yaml_file: str, cfg: dict) -> tuple[str, str | None]:
    """(history dir, tag): a plan template, or a config with a "tag", gets its own folder and run-name suffix.
    plans-j2<-set>/<plan>/ -> history-plans<-set>/<plan>/, so plan sets from different optimizer runs stay apart."""
    if cfg.get("tag"):
        return os.path.join(PLANS_HISTORY_DIR, cfg["tag"]), cfg["tag"]
    p = plan_of(yaml_file)
    if not p:
        return HISTORY_DIR, None
    plan_set, plan = p
    return os.path.join(PLANS_HISTORY_DIR.rstrip("/") + plan_set, plan), plan


def append_history_note(entry: dict) -> None:
    hist = entry.get("history_dir", HISTORY_DIR)
    os.makedirs(hist, exist_ok=True)
    with open(os.path.join(hist, os.path.basename(HISTORY_NOTE_FILE)), "a", encoding="utf-8") as f:
        f.write(
            "\n".join([
                f"[{entry['start']}] {entry['query']} | {entry['seed']} | {entry['config']}"
                f"{' | plan=' + entry['plan'] if entry.get('plan') else ''} | run={entry['run']}",
                f"note: {NOTE}",
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


def run_benchmark(yaml_file: str, cfg_name: str, cfg: dict, query_name: str, seed: str, tpch_parms: dict[str, str]) -> bool:
    render_tmp_yaml(yaml_file, cfg, tpch_parms)
    history_dir, tag = history_dir_of(yaml_file, cfg)

    total_runs = RUNS_NEEDED.get(yaml_file, TOTAL_RUNS)
    for run_idx in range(1, total_runs + 1):
        start = datetime.now().isoformat()
        time.sleep(2)
        result = Runner(TMP_YAML).execute(
            history_logs=history_dir,
            knative_func=KNATIVE_FUNC,
            k8s=K8S_ENDPOINT,
            max_parallel=TASK_PARALLELISM,
            func_ver=cfg["function_version"],
            tag=tag,
        )
        time.sleep(2)
        end = datetime.now().isoformat()

        failed = (not result) or isinstance(result, dict)
        log_entry = {
            "query": query_name,
            "seed": seed,
            "config": cfg_name,
            "plan": tag,
            "history_dir": history_dir,
            "runner": "Runner",
            "run": run_idx,
            "start": start,
            "end": end,
            "duration_s": (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
            "status": "failed" if failed else "success",
            "task_parallelism": TASK_PARALLELISM,
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

        if run_idx < total_runs:
            time.sleep(RUN_DELAY_S)

    return True


def main() -> None:
    os.makedirs(HISTORY_DIR, exist_ok=True)
    all_params = load_params()
    total_configs = len(CONFIGS)

    for cfg_idx, (cfg_name, cfg) in enumerate(CONFIGS.items(), 1):
        system_jobs = [(yaml_file, seed) for yaml_file in YAML_FILES for seed in SEEDS]
        total_system_jobs = len(system_jobs)

        print(f"\n{'=' * 60}")
        print(f"[System {cfg_idx}/{total_configs}] Bắt đầu: {cfg_name}")
        print(f"  Runner = Runner | task_parallel = {TASK_PARALLELISM}")
        print(f"  Tổng queries × seeds = {total_system_jobs} jobs")
        print(f"{'=' * 60}\n")
        sendMessage(
            f"[System {cfg_idx}/{total_configs}] Bắt đầu: {cfg_name}\n"
            f"Runner task={TASK_PARALLELISM}\n"
            f"Tổng {total_system_jobs} jobs (Q{Q_FROM}–Q{Q_TO})"
        )

        system_start = datetime.now()
        failed_queries = []

        for job_idx, (yaml_file, seed) in enumerate(system_jobs, 1):
            query_name = os.path.basename(yaml_file).replace(".yml.j2", "")
            tpch_parms = get_tpch_parms(all_params, query_name, seed)

            hist, tag = history_dir_of(yaml_file, cfg)
            print(f"  [{job_idx}/{total_system_jobs}] {query_name} | {seed} | {cfg_name}"
                  f"{' | plan=' + tag if tag else ''} | runner=Runner -> {hist}")

            try:
                ok = run_benchmark(yaml_file, cfg_name, cfg, query_name, seed, tpch_parms)
                if not ok:
                    failed_queries.append(f"{query_name}/{seed}")
            except Exception as e:
                print(f"  [ERROR] {query_name}/{seed}: {e}")
                failed_queries.append(f"{query_name}/{seed}")

            if job_idx < total_system_jobs:
                print(f"  Waiting {RUN_DELAY_S}s...\n")
                time.sleep(RUN_DELAY_S)

        system_end = datetime.now()
        elapsed = (system_end - system_start).total_seconds()
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"

        fail_summary = f"\nFailed: {', '.join(failed_queries)}" if failed_queries else "\nTất cả thành công"
        msg = (
            f"[System {cfg_idx}/{total_configs}] Xong: {cfg_name}\n"
            f"   Thời gian: {elapsed_str}\n"
            f"   {total_system_jobs} jobs{fail_summary}"
        )
        print(f"\n{msg}\n")
        sendMessage(msg)

        if cfg_idx < total_configs:
            print(f"Waiting {RUN_DELAY_S}s trước system tiếp theo...\n")
            time.sleep(RUN_DELAY_S)

    sendMessage(f"Benchmark hoàn tất! Runner | {total_configs} systems × {len(YAML_FILES)} queries × {len(SEEDS)} seeds.")
    print(f"\nDone. Log: {LOG_FILE}")


if __name__ == "__main__":
    main()
