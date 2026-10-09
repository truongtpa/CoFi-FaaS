import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Callable
from operations.libs import CostLog


class _StatsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        return super().default(obj)


class PipelineTask:
    def __init__(self, name: str, func: Callable, max_retries: int = 3, **kwargs):
        self.name = name
        self.func = func
        self.max_retries = max_retries
        self.kwargs = kwargs

    @staticmethod
    def _sample_list(values, limit: int = 3):
        if len(values) <= limit * 2:
            return values
        return values[:limit] + ["..."] + values[-limit:]

    def _summarize_param(self, key: str, value):
        if key == "output_file":
            return None

        if key == "row_group_ids" and isinstance(value, list):
            if len(value) <= 12:
                return value
            return {
                "_summary": "row_group_ids",
                "count": len(value),
                "first": value[0],
                "last": value[-1],
                "sample": self._sample_list(value),
            }

        if key in {"input_parquet_paths", "build_table_paths", "probe_table_paths"}:
            if isinstance(value, list):
                if len(value) <= 6:
                    return value
                return {
                    "_summary": "list",
                    "count": len(value),
                    "sample": self._sample_list(value),
                }
            if isinstance(value, dict):
                items = list(value.items())
                sample_items = items[:3]
                return {
                    "_summary": "dict",
                    "count": len(value),
                    "sample": {
                        k: (v if not isinstance(v, list) or len(v) <= 8 else self._sample_list(v))
                        for k, v in sample_items
                    },
                }

        if isinstance(value, list):
            if len(value) <= 12:
                return value
            return {
                "_summary": "list",
                "count": len(value),
                "sample": self._sample_list(value),
            }

        if isinstance(value, dict):
            if len(value) <= 8:
                return value
            sample_items = list(value.items())[:4]
            return {
                "_summary": "dict",
                "count": len(value),
                "sample": {k: v for k, v in sample_items},
            }

        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return str(value)

    def _serialize_params(self) -> Dict[str, Any]:
        serialized = {}
        for key, value in self.kwargs.items():
            summarized = self._summarize_param(key, value)
            if summarized is not None:
                serialized[key] = summarized
        return serialized

    async def execute(self, executor: ThreadPoolExecutor) -> Dict[str, Any]:
        start_time = datetime.now()
        start_ts = time.time()

        task_info = {
            "task_name": self.name,
            "params": self._serialize_params(),
            "output_file": self.kwargs.get("output_file", ""),
            "request_path": None,
            "request_sent_at": None,
            "server_start_time": None,
            "start_time": start_time.isoformat(),
            "end_time": None,
            "execution_time_s": 0,
            "status_code": None,
            "status": "pending",
            "response": None,
            "retries": 0,
        }

        last_error = None
        loop = asyncio.get_running_loop()

        for attempt in range(self.max_retries):
            try:
                kwargs = self.kwargs
                result = await loop.run_in_executor(
                    executor, lambda: self.func(**kwargs)
                )

                execution_time = time.time() - start_ts
                response_data = result.get("response")
                status_code = result.get("status_code", 500)
                status = "success" if 200 <= status_code < 300 else "failed"

                task_info.update({
                    "end_time": datetime.now().isoformat(),
                    "execution_time_s": round(execution_time, 3),
                    "request_path": result.get("request_path"),
                    "request_sent_at": result.get("request_sent_at"),
                    "status_code": status_code,
                    "status": status,
                    "response": response_data,
                    "retries": attempt,
                })
                if isinstance(response_data, dict):
                    task_info["server_start_time"] = response_data.get("server_start_time")

                if status == "success":
                    return task_info

                last_error = response_data

            except Exception as e:
                last_error = {
                    "error": str(e),
                    "error_type": type(e).__name__,
                }

            if attempt < self.max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))

        task_info.update({
            "end_time": datetime.now().isoformat(),
            "execution_time_s": round(time.time() - start_ts, 3),
            "status_code": 500,
            "status": "failed",
            "response": last_error,
            "retries": self.max_retries,
        })

        return task_info


class Pipeline:
    def __init__(self, name: str = "pipeline", max_parallel: int = 20, max_retries: int = 3):
        self.pretty_json = bool(os.environ.get("DEBUG", False))
        self.name = name
        self.tasks: List[PipelineTask] = []
        self.max_parallel = max_parallel
        self.max_retries = max_retries
        self._semaphore: asyncio.Semaphore | None = None
        self._executor = ThreadPoolExecutor(max_workers=max_parallel, thread_name_prefix=name)

    def add_task(self, task_name: str, func: Callable, max_retries: int = None, **kwargs):
        retries = max_retries if max_retries is not None else self.max_retries
        self.tasks.append(PipelineTask(task_name, func, retries, **kwargs))
        return self

    async def _run_task(self, task: PipelineTask):
        async with self._semaphore:
            return await task.execute(executor=self._executor)

    async def execute_async(self) -> Dict[str, Any]:
        self._semaphore = asyncio.Semaphore(self.max_parallel)

        start_time = time.time()
        total = len(self.tasks)
        completed = 0
        results: List[Dict[str, Any]] = []

        tasks = [asyncio.create_task(self._run_task(t)) for t in self.tasks]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            percent = completed / total * 100
            bar_len = 24
            filled = int(bar_len * completed / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(
                f"\033[2K\r\033[94m [{bar}] {percent:5.1f}%  ({completed}/{total})\033[0m",
                end="",
                flush=True,
            )

        print("\033[2K\r", end="", flush=True)

        execution_time = time.time() - start_time
        total_task_time = sum(r.get("execution_time_s", 0) for r in results)

        return {
            "pipeline_execution_time_s": round(execution_time, 3),
            "total_tasks_time_s": round(total_task_time, 3),
            "total_task": total,
            "successful_task": sum(1 for r in results if r["status"] == "success"),
            "failed_task": sum(1 for r in results if r["status"] == "failed"),
            "tasks": results,
        }

    def execute(self) -> Dict[str, Any]:
        try:
            return asyncio.run(self.execute_async())
        finally:
            self._executor.shutdown(wait=False)

    def execute_and_save(self, output_path=".") -> Dict[str, Any]:
        result = self.execute()
        with open(f"{output_path}/{self.name}.json", "w", encoding="utf-8") as f:
            json.dump(
                result, f,
                indent=2 if not self.pretty_json else None,
                separators=None if self.pretty_json else (',', ':'),
                ensure_ascii=False,
                cls=_StatsEncoder,
            )
        CostLog.stage(output_path, self.name, result)
        return result

    @staticmethod
    def load_json(json_path: str, filter_task_name: str) -> List[Dict]:
        if not json_path.endswith(".json"):
            json_path = json_path + ".json"

        if not os.path.isfile(json_path):
            return []

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        successful_outputs = []
        for task in data.get("tasks", []):
            if (
                task.get("status") == "success"
                and task.get("task_name") == filter_task_name
            ):
                output = task.get("response", {}).get("data", "")
                if output:
                    successful_outputs.append(output)

        return successful_outputs
