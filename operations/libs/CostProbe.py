import os
import json
import time
import socket
import threading
import pyarrow.parquet as pq
from pyarrow import fs
from flask import request
from operations.libs.ParquetS3Utils import parse_select_columns

_t = threading.local()
_state = {"boot": time.time(), "n": 0, "s3": None}


def _read(path):
    try:
        return open(path).read().strip()
    except Exception:
        return None


def _cgroup():
    cpu = None
    v2 = _read("/sys/fs/cgroup/cpu.max")
    if v2 and not v2.startswith("max"):
        q, p = v2.split()
        cpu = int(q) / int(p)
    q, p = _read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"), _read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if cpu is None and q and p and int(q) > 0:
        cpu = int(q) / int(p)
    mem = _read("/sys/fs/cgroup/memory.max") or _read("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    mem = int(mem) if mem and mem.isdigit() and int(mem) < 2 ** 60 else None
    return cpu or os.cpu_count(), mem


CPU, MEM = _cgroup()


def _s3():
    if _state["s3"] is None:
        _state["s3"] = fs.S3FileSystem(
            endpoint_override=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("ACCESS_KEY", "admin"),
            secret_key=os.environ.get("SECRET_KEY", "lannion-enssat"),
            scheme="http",
        )
    return _state["s3"]


def _cls(path):
    return "s3" if str(path).startswith("s3://") else "nfs"


def _size(path):
    if str(path).startswith("s3://"):
        return _s3().get_file_info(path[5:]).size or 0
    return os.path.getsize(path) if os.path.exists(path) else 0


def _add(rec, side, cls, nbytes, nreq):
    d = rec[side].setdefault(cls, {"bytes": 0, "req": 0})
    d["bytes"] += int(nbytes)
    d["req"] += int(nreq)


def _pq_in(rec, path, rgs=None, cols=None):
    pf = pq.ParquetFile(path[5:], filesystem=_s3()) if path.startswith("s3://") else pq.ParquetFile(path)
    md = pf.metadata
    nbytes = chunks = rows = 0
    for r in (range(md.num_row_groups) if rgs is None else rgs):
        g = md.row_group(r)
        rows += g.num_rows
        for j in range(g.num_columns):
            c = g.column(j)
            if cols is None or c.path_in_schema.split(".")[-1] in cols:
                nbytes += c.total_compressed_size
                chunks += 1
    _add(rec, "in", _cls(path), nbytes, chunks + 2)
    rec["rows_in"] += rows
    return rows


def _bf_io(rec, side, path):
    s = _size(path)
    _add(rec, side, _cls(path), s, 1)
    key = "bf_read" if side == "in" else "bf_write"
    rec[key]["bytes"] += s
    rec[key]["n"] += 1


def _inputs(rec, path, p):
    if path.startswith("/scan"):
        loc = p["location"].replace("FROM", "").strip().strip("'\"")
        cols = None if path == "/scan-starling" else parse_select_columns(p["select"], p.get("bf_column"), p.get("sql_clauses"))
        _pq_in(rec, loc, p.get("row_group_ids"), cols)
        if p.get("filter_by_bf"):
            _bf_io(rec, "in", p["filter_by_bf"])
            rec["bf"] = {"column": p.get("bf_column"), "role": "apply"}
    elif path in ("/join", "/join-pushdown"):
        rec["rows_build"] = sum(_pq_in(rec, x) for x in p.get("build_table_paths") or [] if x)
        rec["rows_probe"] = sum(_pq_in(rec, x) for x in p.get("probe_table_paths") or [] if x)
    elif path == "/range-join":
        rec["rows_build"] = sum(_pq_in(rec, x, r) for x, r in (p.get("build_table_paths") or {}).items())
        rec["rows_probe"] = sum(_pq_in(rec, x, r) for x, r in (p.get("probe_table_paths") or {}).items())
    elif path in ("/aggregate", "/hash-partition"):
        paths = p.get("input_parquet_paths") or []
        for x in paths if isinstance(paths, list) else [paths]:
            _pq_in(rec, x)
        if p.get("bf_column"):
            rec["bf"] = {"column": p["bf_column"], "role": "build", "est_elements": p.get("est_elements"), "error_rate": p.get("error_rate")}
    elif path == "/merge-bf":
        folder = p.get("folder_bf")
        for f in sorted(os.listdir(folder)) if folder and os.path.isdir(folder) else []:
            if f.endswith(".bin"):
                _bf_io(rec, "in", os.path.join(folder, f))
        rec["bf"] = {"role": "merge", "error_rate": p.get("error_rate")}


def _outputs(rec, data):
    if not isinstance(data, dict):
        return
    if data.get("full_path"):
        _add(rec, "out", _cls(data["full_path"]), _size(data["full_path"]), 1)
        rec["rows_out"] += data.get("num_rows") or 0
    for b in data.get("buckets") or (data.get("hash_partitioning") or {}).get("buckets") or []:
        if b.get("data"):
            _add(rec, "out", _cls(b["data"]), float(b.get("size_mb") or 0) * 2 ** 20, 1)
    bf = data.get("bf_path")
    bf = bf.get("path") if isinstance(bf, dict) else bf
    if bf:
        _bf_io(rec, "out", bf)


def _timed(fn, key):
    def wrapper(*args, **kwargs):
        t = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            rec = getattr(_t, "rec", None)
            if rec is not None:
                rec[key] += time.time() - t
    return wrapper


def _before():
    _state["n"] += 1
    now = time.time()
    _t.rec = {"seq": _state["n"], "cold": _state["n"] == 1, "uptime_s": now - _state["boot"],
              "t0": now, "cpu0": time.process_time(), "t_read": 0.0, "t_write": 0.0}


def _after(resp):
    rec = getattr(_t, "rec", None)
    _t.rec = None
    if rec is None or not resp.is_json:
        return resp
    body = resp.get_json(silent=True)
    if not isinstance(body, dict):
        return resp
    rec["t1"] = time.time()
    rec["t_total"] = rec["t1"] - rec["t0"]
    rec["cpu_s"] = time.process_time() - rec.pop("cpu0")
    rec["t_comp"] = max(rec["t_total"] - rec["t_read"] - rec["t_write"], 0.0)
    rec.update({"endpoint": request.path, "host": socket.gethostname(), "cpu": CPU, "mem": MEM,
                "in": {}, "out": {}, "rows_in": 0, "rows_out": 0,
                "bf_read": {"bytes": 0, "n": 0}, "bf_write": {"bytes": 0, "n": 0}})
    try:
        _inputs(rec, request.path, request.get_json(silent=True) or {})
        _outputs(rec, body.get("data"))
    except Exception as e:
        rec["probe_error"] = repr(e)
    body["cost_probe"] = rec
    resp.set_data(json.dumps(body, default=str))
    return resp


def install(app):
    if os.environ.get("COST_PROBE", "1") == "0":
        return
    import operations.libs.ParquetS3Funcs as m
    import operations.libs.ParquetS3Utils as u
    for name, key in (("read_parquet_with_retry", "t_read"), ("read_parquet_with_retry_column", "t_read"),
                      ("load_bloom_filter", "t_read"), ("upload_to_s3_with_retry", "t_write"),
                      ("save_bloom_filter", "t_write")):
        setattr(m, name, _timed(getattr(m, name), key))
    # save_and_get_metadata calls upload_to_s3_with_retry through the ParquetS3Utils module globals
    u.upload_to_s3_with_retry = _timed(u.upload_to_s3_with_retry, "t_write")
    app.before_request(_before)
    app.after_request(_after)
