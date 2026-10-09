import os
import time
import tempfile
import threading
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Tuple, Union
import sqlglot
from fastbloom_rs import BloomFilter
import sqlglot.expressions as sqlglot_exp
from pyarrow import fs

ENABLE_PARQUET_STATS = False

def parse_select_columns(select: str, bf_column: str = None, sql_clauses: str = None) -> Optional[List[str]]:
    select_part = select.strip()
    if select_part.upper().startswith("SELECT"):
        select_part = select_part[6:].strip()

    if select_part == "*":
        return None

    full_query = f"{select} FROM _t"
    if sql_clauses:
        full_query += " " + sql_clauses

    try:
        tree = sqlglot.parse_one(full_query, dialect="spark")
        cols = {col.name for col in tree.find_all(sqlglot_exp.Column)}
    except sqlglot.errors.ParseError:
        return None

    if bf_column:
        cols.add(bf_column)

    return list(cols) if cols else None


def read_parquet_with_retry_column(
        s3: fs.S3FileSystem,
        s3_path: str,
        row_group_ids: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
        read_timeout: int = 300,
        max_retries: int = 3,
        chunk_size: int = 1,
        max_workers: int = 4,
) -> pa.Table:
    def _read_with_retry(read_func, attempt_timeout):
        last_error = None
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                result = read_func()
                elapsed = time.time() - start_time
                if elapsed > attempt_timeout:
                    raise TimeoutError(f"Operation took {elapsed:.2f}s (timeout: {attempt_timeout}s)")
                return result
            except Exception as e:
                last_error = e
                print(f'Retry {attempt + 1}/{max_retries} for {s3_path}: {e}')
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        raise Exception(f"Failed after {max_retries} attempts. Last error: {last_error}")

    _thread_local = threading.local()

    def _get_pf() -> pq.ParquetFile:
        if not hasattr(_thread_local, 'pf'):
            _thread_local.pf = pq.ParquetFile(s3_path, filesystem=s3)
        return _thread_local.pf

    main_pf = _read_with_retry(lambda: pq.ParquetFile(s3_path, filesystem=s3), 30)

    if row_group_ids is None:
        row_group_ids = list(range(main_pf.metadata.num_row_groups))

    target_columns = columns if columns is not None else main_pf.schema_arrow.names
    total_row_groups = len(row_group_ids)

    def _read_single_column(col: str) -> Tuple[int, pa.ChunkedArray]:
        idx = target_columns.index(col)
        pf = _get_pf()
        def _do_read():
            arrays = []
            for i in range(0, total_row_groups, chunk_size):
                rg_ids = row_group_ids[i:i + chunk_size]
                for r in rg_ids:
                    tbl = pf.read_row_group(r, columns=[col])
                    arrays.append(tbl.column(0))
            return (idx, pa.chunked_array([chunk for arr in arrays for chunk in arr.chunks]))
        return _read_with_retry(_do_read, read_timeout)

    if len(target_columns) <= 1 or max_workers <= 1:
        results = [_read_single_column(col) for col in target_columns]
    else:
        results: List[Tuple[int, pa.ChunkedArray]] = []
        with ThreadPoolExecutor(max_workers=len(target_columns)) as executor:
            futures = {executor.submit(_read_single_column, col): col for col in target_columns}
            for future in as_completed(futures):
                col = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    raise Exception(f"Failed reading column '{col}': {e}")

    results.sort(key=lambda x: x[0])

    schema = pa.schema([main_pf.schema_arrow.field(col) for col in target_columns])
    return pa.table({target_columns[i]: arr for i, arr in [(idx, arr) for idx, arr in results]}, schema=schema)


def read_parquet_with_retry(
        s3: fs.S3FileSystem,
        s3_path: str,
        row_group_ids: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
        read_timeout: int = 300,
        max_retries: int = 3,
        chunk_size: int = 1,
        max_workers: int = 4,
) -> pa.Table:

    def _read_with_retry(read_func, attempt_timeout):
        last_error = None
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                result = read_func()
                elapsed = time.time() - start_time
                if elapsed > attempt_timeout:
                    raise TimeoutError(f"Operation took {elapsed:.2f}s (timeout: {attempt_timeout}s)")
                return result
            except Exception as e:
                last_error = e
                print(f'Retry {attempt + 1}/{max_retries} for {s3_path}: {e}')
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        raise Exception(f"Failed after {max_retries} attempts. Last error: {last_error}")

    main_pf = _read_with_retry(
        lambda: pq.ParquetFile(s3_path, filesystem=s3),
        30
    )

    if row_group_ids is None:
        row_group_ids = list(range(main_pf.metadata.num_row_groups))

    total_row_groups = len(row_group_ids)
    if total_row_groups <= 1 or max_workers <= 1:
        all_tables = []
        for i in range(0, total_row_groups, chunk_size):
            chunk_rg_ids = row_group_ids[i:i + chunk_size]
            def _read_seq(rg_ids=chunk_rg_ids):
                if len(rg_ids) == 1:
                    return main_pf.read_row_group(rg_ids[0], columns=columns)
                return pa.concat_tables([main_pf.read_row_group(r, columns=columns) for r in rg_ids])
            all_tables.append(_read_with_retry(_read_seq, read_timeout))
        return pa.concat_tables(all_tables)

    _thread_local = threading.local()

    def _get_pf() -> pq.ParquetFile:
        if not hasattr(_thread_local, 'pf'):
            _thread_local.pf = pq.ParquetFile(s3_path, filesystem=s3)
        return _thread_local.pf

    chunks: List[Tuple[int, List[int]]] = [
        (i, row_group_ids[i:i + chunk_size])
        for i in range(0, total_row_groups, chunk_size)
    ]

    def _read_chunk(chunk_index: int, rg_ids: List[int]) -> Tuple[int, pa.Table]:
        pf = _get_pf()
        def _do_read():
            if len(rg_ids) == 1:
                return pf.read_row_group(rg_ids[0], columns=columns)
            return pa.concat_tables([pf.read_row_group(r, columns=columns) for r in rg_ids])
        table = _read_with_retry(_do_read, read_timeout)
        return (chunk_index, table)

    results: List[Tuple[int, pa.Table]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_read_chunk, idx, rg_ids): (idx, rg_ids)
            for idx, rg_ids in chunks
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                idx, rg_ids = futures[future]
                raise Exception(f"Failed reading row groups {rg_ids} (chunk {idx}): {e}")

    results.sort(key=lambda x: x[0])
    return pa.concat_tables([t for _, t in results])


def save_and_get_metadata(
    conn,
    s3,
    query_or_view: str,
    output_file: str,
    cleanup_views: Optional[List[str]] = None,
    max_retries: int = 3,
    cleanup_local: bool = True,
    sort_by: Optional[Union[str, List[str]]] = None,
    row_group_size: Optional[int] = None,
) -> dict:
    local_temp_file = None
    try:
        if output_file.startswith('s3://'):
            local_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet').name
            save_path = local_temp_file
        else:
            save_path = output_file
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

        query = query_or_view if query_or_view.strip().upper().startswith('SELECT') else f"SELECT * FROM {query_or_view}"

        # Wrap with ORDER BY for tight min/max stats
        if sort_by:
            if isinstance(sort_by, (list, tuple)):
                order_cols = ", ".join(sort_by)
            else:
                order_cols = str(sort_by)
            if order_cols.strip():
                query = f"SELECT * FROM ({query}) ORDER BY {order_cols}"

        # ZSTD; SNAPPY; GZIP; UNCOMPRESSED
        copy_opts = ["FORMAT PARQUET", "COMPRESSION SNAPPY"]
        # copy_opts = ["FORMAT PARQUET"]
        if row_group_size:
            copy_opts.append(f"ROW_GROUP_SIZE {int()}")

        print(f"[DEBUG save_and_get_metadata] sort_by={sort_by!r}, row_group_size={row_group_size!r}")
        final_sql = f"COPY ({query}) TO '{save_path}' ({', '.join(copy_opts)})"
        print(f"[DEBUG SQL] {final_sql[:300]}...")

        conn.execute(f"COPY ({query}) TO '{save_path}' ({', '.join(copy_opts)})")
        metadata = get_parquet_metadata(conn, save_path)

        if output_file.startswith('s3://'):
            upload_to_s3_with_retry(s3, local_temp_file, output_file, max_retries)
            metadata['full_path'] = output_file

        for view in (cleanup_views or []):
            conn.execute(f"DROP VIEW IF EXISTS {view}")

        metadata['local_path'] = save_path
        return metadata
    finally:
        if local_temp_file and cleanup_local:
            Path(local_temp_file).unlink(missing_ok=True)


def upload_to_s3_with_retry(s3: fs.S3FileSystem, local_file: str, s3_path: str, max_retries: int = 3):
    import pyarrow.fs as pafs
    s3_path = s3_path.replace('s3://', '')
    for attempt in range(max_retries):
        try:
            with open(local_file, 'rb') as f, s3.open_output_stream(s3_path) as s3_stream:
                s3_stream.write(f.read())
            if s3.get_file_info(s3_path).type == pafs.FileType.NotFound:
                raise Exception(f"Upload verification failed for {s3_path}")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 ** attempt)
            else:
                raise Exception(f"Failed to upload to S3 after {max_retries} attempts: {e}")


def get_parquet_metadata(conn, parquet_path: str) -> dict:
    try:
        size_bytes = os.path.getsize(parquet_path)
        pf = pq.ParquetFile(parquet_path)
        meta = pf.metadata

        row_groups = []
        for i in range(meta.num_row_groups):
            rg = meta.row_group(i)
            columns = []
            total_bytes = 0

            for j in range(rg.num_columns):
                col = rg.column(j)
                cs = col.total_compressed_size
                total_bytes += cs
                col_name = col.path_in_schema.split(".")[-1]

                columns.append({
                    "name": col_name,
                    "compressed_size": int(cs),
                    "compressed_size_mb": round(cs / (1024 * 1024), 4),
                })

            row_groups.append({
                "id": i,
                "num_rows": int(rg.num_rows),
                "total_byte_size": int(total_bytes),
                "total_byte_size_mb": round(total_bytes / (1024 * 1024), 2),
                "columns": columns,
                "stats": _extract_row_group_stats(rg) if ENABLE_PARQUET_STATS else {},
            })

        return {
            "full_path": parquet_path,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
            "num_rows": int(meta.num_rows),
            "num_row_groups": int(meta.num_row_groups),
            "num_columns": int(meta.num_columns),
            "row_groups": row_groups,
        }
    except Exception as e:
        raise Exception(f"Failed to get metadata: {e}")


def _extract_row_group_stats(rg) -> dict:
    col_stats = {}
    for j in range(rg.num_columns):
        col = rg.column(j)
        col_name = col.path_in_schema.split(".")[-1]
        st = col.statistics
        if st is None or not st.has_min_max:
            continue
        try:
            col_stats[col_name] = {
                "min": st.min,
                "max": st.max,
                "null_count": int(st.null_count) if st.has_null_count and st.null_count is not None else None,
                "distinct_count": int(st.distinct_count) if st.has_distinct_count and st.distinct_count is not None else None,
            }
        except Exception:
            pass
    return col_stats


def duckdb_with_retry(conn, query: str, max_retries: int = 3, description: str = "Query"):
    for attempt in range(max_retries):
        try:
            return conn.execute(query)
        except Exception as e:
            print(f"{description} attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"{description} failed after {max_retries} attempts: {e}") from e
    return None

def save_bloom_filter(bf: BloomFilter, filepath: str, error_rate: float) -> dict:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    bf.save_to_file_with_hashes(filepath)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    return {
        'path': filepath,
        'size_m': size_mb,
    }


def load_bloom_filter(filepath: str) -> BloomFilter:
    if not os.path.exists(filepath):
        raise Exception(f"Bloom Filter {filepath} does not exist")
    return BloomFilter.from_file_with_hashes(filepath)
