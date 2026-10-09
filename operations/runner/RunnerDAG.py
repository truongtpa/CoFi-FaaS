from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from operations.runner.Runner import Runner


class RunnerDAG(Runner):
    @staticmethod
    def _is_empty_dep(value) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and value.strip().lower() in {"", "none", "null"}:
            return True
        return False

    def _deps_for_step(self, step, all_step_names=None) -> set[str]:
        deps = step.get("from_previous_tasks")
        if not deps or self._is_empty_dep(deps):
            if step.get("func") == "summary_clean":
                current_name = step.get("name")
                return {name for name in (all_step_names or []) if name != current_name}
            return set()
        if isinstance(deps, str):
            return {deps}
        if isinstance(deps, list):
            return {d for d in deps if not self._is_empty_dep(d)}
        return set()

    def execute(self, history_logs=None, k8s=None, knative_func=None, max_parallel=4, func_ver='v1', max_step_parallel=None):
        ctx = self._prepare_execute_context(
            history_logs=history_logs,
            k8s=k8s,
            knative_func=knative_func,
            max_parallel=max_parallel,
            func_ver=func_ver,
        )

        steps = list(self.parser.get_steps())
        step_by_name = {step["name"]: step for step in steps}
        step_names = [step["name"] for step in steps]
        deps_by_name = {step["name"]: self._deps_for_step(step, step_names) for step in steps}
        completed = set()
        submitted = set()
        running = {}
        max_step_parallel = max_step_parallel or max_parallel

        with ThreadPoolExecutor(max_workers=max_step_parallel, thread_name_prefix="runner-dag") as pool:
            while len(completed) < len(steps):
                ready = [
                    name for name, deps in deps_by_name.items()
                    if name not in submitted and deps.issubset(completed)
                ]

                while ready and len(running) < max_step_parallel:
                    name = ready.pop(0)
                    future = pool.submit(self._execute_step, step_by_name[name], ctx)
                    running[future] = name
                    submitted.add(name)

                if not running:
                    unresolved = sorted(set(step_by_name) - completed - submitted)
                    return {
                        "message": f"Deadlock in DAG scheduling. Unresolved steps: {unresolved}",
                        "task_name": ",".join(unresolved[:3]),
                    }

                done, _ = wait(running.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    step_name = running.pop(future)
                    rs = future.result()
                    if rs is not True:
                        self._run_cleanup(ctx, reason=f"dag failure at {step_name}")
                        try:
                            pool.shutdown(wait=False, cancel_futures=True)
                        except TypeError:
                            pool.shutdown(wait=False)
                        return rs
                    completed.add(step_name)

        if self._find_summary_clean_step() is None:
            cleanup_ok = self._run_cleanup(ctx, reason="implicit final cleanup")
            if not cleanup_ok:
                return {
                    "message": "Cleanup failed",
                    "task_name": "__default-summary-clean",
                }

        return True
