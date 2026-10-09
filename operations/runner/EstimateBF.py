import os, json, struct, math
from pathlib import Path
from functools import lru_cache
import paramiko


@lru_cache(maxsize=None)
def _load_json(path: str):
    return json.loads(Path(path).read_text())


class EstimateBF:
    def __init__(self, history_folder: str, stats_filename: str = "statistics-bf.json", result_file:str = None):
        self.history = history_folder
        self.result_file = result_file
        self.stats_path = stats_filename

    def get_bf_paths(self) -> list[dict]:
        stats = _load_json(self.stats_path)
        combine_to_merge = {i["from_previous_tasks"]: i["task_name"] for i in stats}
        merge_to_combine = {i["task_name"]: i["from_previous_tasks"] for i in stats}

        by_merge = {}
        for file in Path(self.history).glob("*.json"):
            data = _load_json(str(file))
            tasks = data if isinstance(data, list) else data.get("tasks", [])
            for task in tasks:
                bf_path = task.get("response", {}).get("data", {}).get("bf_path")
                if not isinstance(bf_path, dict) or "size_m" not in bf_path:
                    continue

                task_name = task["task_name"]
                is_merge_output = task_name in merge_to_combine
                merge_name = task_name if is_merge_output else combine_to_merge.get(task_name)

                if merge_name and (merge_name not in by_merge or is_merge_output):
                    by_merge[merge_name] = {"task_name": merge_name, "bf_path": bf_path}
        return list(by_merge.values())

    def get_bf_params(self, task_name: str) -> dict:
        from_previous_tasks = next(
            (i["from_previous_tasks"] for i in _load_json(self.stats_path) if i["task_name"] == task_name),
            None
        )
        if not from_previous_tasks:
            raise KeyError(f"task_name not found: {task_name}")
        params = _load_json(str(Path(self.history) / f"{from_previous_tasks}.json"))["tasks"][0]["params"]
        return {"from_previous_tasks": from_previous_tasks, "est_elements": params.get("est_elements"),
                "error_rate": params.get("error_rate")}


    def download_via_sftp(self, host, username, remote_path, local_path, key_path=None, password=None, port=22):
        import logging
        logging.getLogger("paramiko").setLevel(logging.CRITICAL)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {"hostname": host, "username": username, "port": port}
        connect_kwargs["pkey" if key_path else "password"] = paramiko.RSAKey.from_private_key_file(
            key_path) if key_path else password
        try:
            client.connect(**connect_kwargs)
            with client.open_sftp() as sftp:
                sftp.get(remote_path, local_path)
        finally:
            client.close()


    def bf_stats(self, filepath: str, est_elements: int = None, error_rate: float = None) -> dict:
        size = os.path.getsize(filepath)
        with open(filepath, "rb") as f:
            k = struct.unpack(">I", f.read(4))[0]
            bit_bytes = f.read()

        total_bits = len(bit_bytes) * 8
        set_bits = sum(bin(b).count("1") for b in bit_bytes)
        fill_ratio = set_bits / total_bits if total_bits > 0 else 0.0

        try:
            est_inserted = -total_bits / k * math.log(1 - fill_ratio) if fill_ratio < 1.0 and k > 0 else None
        except (ValueError, ZeroDivisionError):
            est_inserted = None

        current_fpr = (1 - math.exp(-k * est_inserted / total_bits)) ** k if est_inserted and total_bits > 0 else None
        capacity_used_pct = round(est_inserted / est_elements * 100, 2) if est_elements and est_inserted else None
        m_theory = -est_elements * math.log(error_rate) / (math.log(2) ** 2) if est_elements and error_rate else None

        return {
            "file_size_kb": round(size / 1024, 2),
            "num_hash_functions_k": k,
            "total_bits_m": total_bits,
            "set_bits": set_bits,
            "fill_ratio_pct": round(fill_ratio * 100, 3),
            "est_inserted_elements": int(est_inserted) if est_inserted is not None else None,
            "current_false_positive_rate": round(current_fpr, 8) if current_fpr is not None else None,
            "est_elements_configured": est_elements,
            "capacity_used_pct": capacity_used_pct,
            "remaining_capacity": int(est_elements - est_inserted) if est_elements and est_inserted else None,
            "m_theory_bits": int(m_theory) if m_theory else None,
            "sizing_ratio": round(total_bits / m_theory, 3) if m_theory else None,
            "is_overloaded": fill_ratio > 0.5,
            "is_near_capacity": (capacity_used_pct > 90) if capacity_used_pct is not None else None,
        }

    def analyze(self):
        import tempfile
        final = []
        host = "10.158.1.40"
        username = "root"
        key_path = "./ansible-setup/private-key-vm-grid5000.pem"
        password = None
        port = 22
        for item in self.get_bf_paths():
            with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tmp:
                local_path = tmp.name

            try:
                self.download_via_sftp(
                    host=host, username=username, port=port,
                    key_path=key_path, password=password,
                    remote_path="/mnt/ramdisk/filters" + item["bf_path"]["path"].replace("/mnt/filters", ""),
                    local_path=local_path
                )
                p = self.get_bf_params(item["task_name"])
                s = self.bf_stats(local_path, est_elements=p["est_elements"], error_rate=p["error_rate"])
                recommended = math.ceil(s["est_inserted_elements"] / 0.90) if s["est_inserted_elements"] else p["est_elements"]
                final.append({"task_name": p["from_previous_tasks"], "est_elements": recommended})
                print(f"[BF] {p['from_previous_tasks']}: capacity={s['capacity_used_pct']}% | {p['est_elements']:,} → {recommended:,}")
            finally:
                if os.path.exists(local_path):
                    os.remove(local_path)

        with open(self.result_file, 'w') as f:
            json.dump(final, f, indent=2, ensure_ascii=False)

        #print(f"  [BF] Saved recommended est_elements → {self.result_file}")