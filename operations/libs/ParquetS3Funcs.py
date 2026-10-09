import concurrent
import glob
import os
import shutil
import uuid
import duckdb
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
import platform
import sqlglot
import sqlglot.expressions as sqlglot_exp
from datetime import date, datetime
from decimal import Decimal

from pathlib import Path
from pyarrow import fs
from operations.libs.ParquetS3Utils import (
    parse_select_columns,
    read_parquet_with_retry,
    save_and_get_metadata,
    upload_to_s3_with_retry,
    get_parquet_metadata,
    duckdb_with_retry,
    save_bloom_filter,
    load_bloom_filter,
    read_parquet_with_retry_column,
)


class ParquetS3Funcs:

    def __init__(self):
        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        extension = f"LOAD '/app/operations/extention/x86/bfextension.duckdb_extension'"

        if platform.uname().machine == "arm64":
            extension = f"LOAD './operations/extention/arm/bfextension.duckdb_extension'"

        conn.execute(extension)
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("ACCESS_KEY", "admin")
        secret_key = os.environ.get("SECRET_KEY", "lannion-enssat")

        self.s3 = fs.S3FileSystem(
            endpoint_override=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            scheme="http",
            connect_timeout=5,
            request_timeout=30,
            background_writes=False,
            default_metadata={"cache-control": "max-age=3600"},
        )

        self.conn = conn.execute(f"""
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{access_key}';
            SET s3_secret_access_key='{secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None

        if hasattr(self, 's3'):
            del self.s3
            self.s3 = None

    # ------------------------------------------------------------------
    # Thin wrappers — delegate to ParquetS3Utils
    # ------------------------------------------------------------------
    def _parse_select_columns(self, select, bf_column=None, sql_clauses=None):
        return parse_select_columns(select, bf_column, sql_clauses)

    def _read_parquet_with_retry(self, s3_path, row_group_ids=None, columns=None,
                                  read_timeout=300, max_retries=3, chunk_size=1, max_workers=4):
        return read_parquet_with_retry(self.s3, s3_path, row_group_ids, columns,
                                        read_timeout, max_retries, chunk_size, max_workers)

    def read_parquet_with_retry_column(self, s3_path, row_group_ids=None, columns=None,
                                        read_timeout=300, max_retries=3, chunk_size=1, max_workers=4):
        return read_parquet_with_retry_column(self.s3, s3_path, row_group_ids, columns,
                                               read_timeout, max_retries, chunk_size, max_workers)

    def _save_and_get_metadata(self, query_or_view, output_file, cleanup_views=None,
                                max_retries=3, cleanup_local=True,
                                sort_by=None):
        return save_and_get_metadata(
            self.conn, self.s3, query_or_view, output_file,
            cleanup_views, max_retries, cleanup_local,
            sort_by=sort_by,
        )

    def _upload_to_s3_with_retry(self, local_file, s3_path, max_retries=3):
        return upload_to_s3_with_retry(self.s3, local_file, s3_path, max_retries)

    def _get_parquet_metadata(self, parquet_path):
        return get_parquet_metadata(self.conn, parquet_path)

    def _duckdb_with_retry(self, query, max_retries=3, description="Query"):
        return duckdb_with_retry(self.conn, query, max_retries, description)

    def _save_bloom_filter(self, bf, filepath, error_rate):
        return save_bloom_filter(bf, filepath, error_rate)

    def _load_bloom_filter(self, filepath):
        return load_bloom_filter(filepath)

    def _debug_print_table(self, name: str, table: pa.Table, limit: int = 5):
        if table is None:
            print(f"[range-join][debug] {name}: <empty>")
            return

        preview_rows = table.slice(0, limit).to_pylist()
        print(
            f"[range-join][debug] {name}: rows={table.num_rows}, "
            f"cols={table.num_columns}, schema={table.column_names}"
        )
        print(f"[range-join][debug] {name} preview(0:{limit})={preview_rows}")

    def _resolve_parquet_source(self, path: str):
        if path.startswith("s3://"):
            return path.replace("s3://", ""), self.s3
        return path, None

    @staticmethod
    def _extract_column_min_max_from_row_group(row_group, column_name: str):
        for j in range(row_group.num_columns):
            col = row_group.column(j)
            col_name = col.path_in_schema.split(".")[-1]
            if col_name != column_name:
                continue
            stats = col.statistics
            if stats is None or not stats.has_min_max:
                return None, None
            return stats.min, stats.max
        return None, None

    @staticmethod
    def _ranges_overlap(left_min, left_max, right_min, right_max):
        if left_min is None or left_max is None or right_min is None or right_max is None:
            return True
        try:
            return not (left_max < right_min or left_min > right_max)
        except TypeError:
            return True

    def _prune_probe_row_groups_by_stats(self, probe_table_paths, probe_key: str, build_min, build_max):
        selected = []
        stats = {
            "probe_files": 0,
            "probe_row_groups_total": 0,
            "probe_row_groups_selected": 0,
            "probe_row_groups_skipped": 0,
            "probe_row_groups_without_stats": 0,
        }

        for path in probe_table_paths:
            resolved_path, filesystem = self._resolve_parquet_source(path)
            pf = pq.ParquetFile(resolved_path, filesystem=filesystem)

            stats["probe_files"] += 1
            matched_row_groups = []

            for rg_id in range(pf.metadata.num_row_groups):
                stats["probe_row_groups_total"] += 1
                rg = pf.metadata.row_group(rg_id)
                rg_min, rg_max = self._extract_column_min_max_from_row_group(rg, probe_key)

                if rg_min is None or rg_max is None:
                    stats["probe_row_groups_without_stats"] += 1
                    matched_row_groups.append(rg_id)
                    continue

                if self._ranges_overlap(rg_min, rg_max, build_min, build_max):
                    matched_row_groups.append(rg_id)
                else:
                    stats["probe_row_groups_skipped"] += 1

            stats["probe_row_groups_selected"] += len(matched_row_groups)
            if matched_row_groups:
                selected.append((path, matched_row_groups))

        return selected, stats

    def _read_selected_probe_tables(self, probe_specs):
        tables = []
        for path, row_group_ids in probe_specs:
            resolved_path, filesystem = self._resolve_parquet_source(path)
            if filesystem is self.s3:
                table = self._read_parquet_with_retry(
                    s3_path=resolved_path,
                    row_group_ids=row_group_ids,
                    columns=None,
                    read_timeout=100,
                    max_retries=3,
                    chunk_size=1,
                    max_workers=4,
                )
            else:
                pf = pq.ParquetFile(resolved_path)
                table = pa.concat_tables([pf.read_row_group(rg_id) for rg_id in row_group_ids])
            tables.append(table)

        if not tables:
            return None
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------
    def scan_starling(self, select, location, sql_clauses=None, output_file=None,
                      row_group_ids=None, sort_by=None):
        parquet_path = location.replace("FROM", "").strip().strip("'\"")
        is_s3_input = parquet_path.startswith('s3://')
        if is_s3_input:
            s3_path = parquet_path.replace('s3://', '')
            arrow_table = self._read_parquet_with_retry(
                s3_path=s3_path,
                row_group_ids=row_group_ids,
                read_timeout=100,
                max_retries=3,
                chunk_size=1,
                max_workers=8,
            )
        else:
            if row_group_ids:
                pf = pq.ParquetFile(parquet_path)
                tables = [pf.read_row_group(rg_id) for rg_id in row_group_ids]
                arrow_table = pa.concat_tables(tables)
            else:
                arrow_table = pq.read_table(parquet_path)

        self.conn.register('_rowgroup_data', arrow_table)
        base_query = f"{select} FROM _rowgroup_data"
        if sql_clauses:
            base_query += " " + sql_clauses

        self.conn.execute(f"CREATE OR REPLACE TEMP VIEW _filtered_data AS {base_query}")
        print(f'[DEBUG] {base_query}')

        return self._save_and_get_metadata(
            query_or_view="SELECT * FROM _filtered_data",
            output_file=output_file,
            cleanup_views=["_rowgroup_data"],
            max_retries=3,
            sort_by=sort_by,
        )

    def scan_bloomfaas(self, select, location, sql_clauses=None, output_file=None,
                       row_group_ids=None, bf_column=None, filter_by_bf=None,
                       max_workers=2, sort_by=None):
        parquet_path = location.replace("FROM", "").strip().strip("'\"")
        is_s3_input = parquet_path.startswith('s3://')
        columns = self._parse_select_columns(select, bf_column, sql_clauses)

        if is_s3_input:
            s3_path = parquet_path.replace('s3://', '')
            arrow_table = self._read_parquet_with_retry(
                s3_path=s3_path,
                row_group_ids=row_group_ids,
                columns=columns,
                read_timeout=100,
                max_retries=3,
                chunk_size=1,
                max_workers=max_workers,
            )
        else:
            if row_group_ids:
                pf = pq.ParquetFile(parquet_path)
                tables = [pf.read_row_group(rg_id, columns=columns) for rg_id in row_group_ids]
                arrow_table = pa.concat_tables(tables)
            else:
                arrow_table = pq.read_table(parquet_path, columns=columns)

        self.conn.register('_rowgroup_data', arrow_table)
        base_query = f"{select} FROM _rowgroup_data"

        if filter_by_bf and bf_column:
            self.conn.execute(f"SELECT bloom_load('{filter_by_bf}')")
            bf_condition = f"bloom_contains({bf_column})"
            if sql_clauses and 'WHERE' in sql_clauses.upper():
                sql_clauses = sql_clauses.replace('WHERE', f'WHERE {bf_condition} AND', 1)
            else:
                where_clause = f"WHERE {bf_condition}"
                sql_clauses = where_clause + (" " + sql_clauses if sql_clauses else "")

        if sql_clauses:
            base_query += " " + sql_clauses

        self.conn.execute(f"CREATE OR REPLACE TEMP VIEW _filtered_data AS {base_query}")

        return self._save_and_get_metadata(
            query_or_view="SELECT * FROM _filtered_data",
            output_file=output_file,
            cleanup_views=["_rowgroup_data"],
            max_retries=3,
            sort_by=sort_by,
        )

    def merge_bloom_filter(self, folder, file_output, error_rate):
        global_bf = None
        bf_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.bin')]

        for path_bf in bf_files:
            if global_bf is None:
                global_bf = self._load_bloom_filter(path_bf)
            else:
                global_bf.union(self._load_bloom_filter(path_bf))

        if global_bf is None:
            return {'bf_path': None}

        return {'bf_path': self._save_bloom_filter(global_bf, file_output, error_rate)}

    def join(self, build_table_paths: list[str], probe_table_paths: list[str],
             probe_select: str, join_condition: str, output_file: str,
             join_type: str = "inner",
             sort_by=None):

        SQL_JOIN_MAP = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "left_semi": "SEMI JOIN",
            "left_anti": "ANTI JOIN",
        }

        if isinstance(build_table_paths, list):
            build_table_paths = [p for p in build_table_paths if p is not None]
        if isinstance(probe_table_paths, list):
            probe_table_paths = [p for p in probe_table_paths if p is not None]

        if not probe_table_paths:
            return {"data": None, "size_mb": 0, "row_count": 0}
        if not build_table_paths:
            return {"data": None, "size_mb": 0, "row_count": 0}

        sql_join = SQL_JOIN_MAP.get(join_type.strip().lower())
        if sql_join is None:
            raise ValueError(f"Unsupported join_type: '{join_type}'")

        def to_pattern(paths):
            if isinstance(paths, list):
                quoted = ", ".join(f"'{p}'" for p in paths)
                return f"[{quoted}]"
            return f"'{paths}'"

        self._duckdb_with_retry(f"""
                CREATE OR REPLACE TEMP VIEW build_table AS
                SELECT * FROM read_parquet({to_pattern(build_table_paths)})
            """, description="Create build table")

        self._duckdb_with_retry(f"""
                CREATE OR REPLACE TEMP VIEW probe_table AS
                SELECT * FROM read_parquet({to_pattern(probe_table_paths)})
            """, description="Create probe table")

        join_query = f"""
                {probe_select}
                FROM probe_table
                {sql_join} build_table ON {join_condition}
            """

        return self._save_and_get_metadata(
            query_or_view=join_query,
            output_file=output_file,
            cleanup_views=["build_table", "probe_table"],
            max_retries=3,
            sort_by=sort_by,
        )

    @staticmethod
    def _sql_literal(value):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        return "'" + str(value).replace("'", "''") + "'"

    def join_pushdown(self, build_table_paths: list[str], probe_table_paths: list[str],
                      probe_select: str, join_condition: str, output_file: str,
                      build_key: str, probe_key: str, join_type: str = "inner",
                      sort_by=None):

        safe_pushdown_join_types = {"inner", "left_semi"}
        normalized_join_type = (join_type or "inner").strip().lower()

        if normalized_join_type not in safe_pushdown_join_types:
            return self.join(
                build_table_paths=build_table_paths,
                probe_table_paths=probe_table_paths,
                probe_select=probe_select,
                join_condition=join_condition,
                output_file=output_file,
                join_type=join_type,
                sort_by=sort_by,
            )

        if isinstance(build_table_paths, list):
            build_table_paths = [p for p in build_table_paths if p is not None]
        if isinstance(probe_table_paths, list):
            probe_table_paths = [p for p in probe_table_paths if p is not None]

        if not probe_table_paths or not build_table_paths:
            return {"data": None, "size_mb": 0, "row_count": 0}

        def to_pattern(paths):
            if isinstance(paths, list):
                quoted = ", ".join(f"'{p}'" for p in paths)
                return f"[{quoted}]"
            return f"'{paths}'"

        self._duckdb_with_retry(f"""
                CREATE OR REPLACE TEMP VIEW build_table AS
                SELECT * FROM read_parquet({to_pattern(build_table_paths)})
            """, description="Create build table for pushdown join")

        min_max = self._duckdb_with_retry(
            f"SELECT MIN({build_key}) AS min_key, MAX({build_key}) AS max_key FROM build_table",
            description="Compute build min/max for pushdown join",
        ).fetchone()

        if not min_max or min_max[0] is None or min_max[1] is None:
            return self.join(
                build_table_paths=build_table_paths,
                probe_table_paths=probe_table_paths,
                probe_select=probe_select,
                join_condition=join_condition,
                output_file=output_file,
                join_type=join_type,
                sort_by=sort_by,
            )

        min_key, max_key = min_max
        min_literal = self._sql_literal(min_key)
        max_literal = self._sql_literal(max_key)
        probe_specs, pushdown_stats = self._prune_probe_row_groups_by_stats(
            probe_table_paths=probe_table_paths,
            probe_key=probe_key,
            build_min=min_key,
            build_max=max_key,
        )

        if probe_specs:
            probe_arrow = self._read_selected_probe_tables(probe_specs)
            self.conn.register("_pushdown_probe_data", probe_arrow)
            self._duckdb_with_retry(f"""
                    CREATE OR REPLACE TEMP VIEW probe_table AS
                    SELECT * FROM _pushdown_probe_data
                    WHERE {probe_key} >= {min_literal}
                      AND {probe_key} <= {max_literal}
                """, description="Create probe table for pushdown join")
            cleanup_views = ["build_table", "probe_table", "_pushdown_probe_data"]
        else:
            first_probe_path = probe_table_paths[0]
            self._duckdb_with_retry(f"""
                    CREATE OR REPLACE TEMP VIEW probe_table AS
                    SELECT * FROM read_parquet({to_pattern([first_probe_path])})
                    WHERE 1 = 0
                """, description="Create empty probe table for pushdown join")
            cleanup_views = ["build_table", "probe_table"]

        SQL_JOIN_MAP = {
            "inner": "INNER JOIN",
            "left_semi": "SEMI JOIN",
        }
        sql_join = SQL_JOIN_MAP[normalized_join_type]
        join_query = f"""
                {probe_select}
                FROM probe_table
                {sql_join} build_table ON {join_condition}
            """

        metadata = self._save_and_get_metadata(
            query_or_view=join_query,
            output_file=output_file,
            cleanup_views=cleanup_views,
            max_retries=3,
            sort_by=sort_by,
        )
        metadata["pushdown_stats"] = {
            **pushdown_stats,
            "build_min": str(min_key),
            "build_max": str(max_key),
        }
        return metadata

    def aggregate(self, input_parquet_paths: list[str], output_file: str,
                  select: str = None, sql_clauses: str = None,
                  bf_column: str = None, bf_path: str = None,
                  est_elements: int = None, error_rate=0.001,
                  sort_by=None,
                  hash_column: str = None, num_buckets: int = None,
                  partition_output_folder: str = None):

        def _default_partition_output_folder(path: str) -> str:
            if path.startswith('s3://'):
                base = path[:-8] if path.endswith('.parquet') else path.rstrip('/')
                return f"{base}__hash"
            p = Path(path)
            if p.suffix == '.parquet':
                return str(p.with_suffix('')) + "__hash"
            return str(p) + "__hash"

        num_buckets = int(num_buckets or 0)
        if num_buckets > 0 and not hash_column:
            raise ValueError("aggregate hash partition requires 'hash_column' when 'num_buckets' > 0")

        input_pattern = str(input_parquet_paths) if isinstance(input_parquet_paths, list) else input_parquet_paths
        self._duckdb_with_retry(f"""
            CREATE OR REPLACE TEMP VIEW _combined_input AS
            SELECT * FROM read_parquet({input_pattern})
        """)

        if select and select.strip():
            select_clause = select.strip()
            query = select_clause if select_clause.upper().startswith("SELECT") else f"SELECT {select_clause}"
            query += " FROM _combined_input"
        else:
            query = "SELECT * FROM _combined_input"

        if sql_clauses and sql_clauses.strip():
            query += f" {sql_clauses.strip()}"

        needs_local_output = bool((bf_column and bf_column.strip()) or num_buckets > 0)
        metadata = self._save_and_get_metadata(
            query_or_view=query,
            output_file=output_file,
            cleanup_views=["_combined_input"],
            max_retries=3,
            cleanup_local=not needs_local_output,
            sort_by=sort_by,
        )

        local_path = metadata.get('local_path', output_file)

        if bf_column and bf_column.strip():
            self._duckdb_with_retry(f"""
                SELECT bloom_build('{local_path}', '{bf_column}', '{bf_path}', {est_elements}, {error_rate})
            """)
            metadata['bf_path'] = bf_path

        if num_buckets > 0:
            bucket_output_folder = partition_output_folder or _default_partition_output_folder(output_file)
            hash_metadata = self.hash_partition(
                input_parquet_paths=[local_path],
                output_folder=bucket_output_folder,
                hash_column=hash_column,
                num_buckets=num_buckets,
            )
            metadata['hash_partitioning'] = {
                'hash_column': hash_column,
                'num_buckets': num_buckets,
                'output_folder': bucket_output_folder,
                'buckets': hash_metadata['buckets'],
            }

        if needs_local_output:
            metadata.pop('local_path', None)
            if output_file.startswith('s3://'):
                Path(local_path).unlink(missing_ok=True)
        else:
            metadata.pop('local_path', None)

        return metadata

    def hash_partition(self, input_parquet_paths: list[str], output_folder: str,
                       hash_column: str, num_buckets: int):

        input_pattern = str(input_parquet_paths) if isinstance(input_parquet_paths, list) else input_parquet_paths
        is_s3 = output_folder.startswith("s3://")

        staging_dir = tempfile.mkdtemp(prefix="hash_partition_")
        try:
            self._duckdb_with_retry(f"""
                COPY (
                    SELECT *, hash({hash_column}) % {num_buckets} AS _bucket_id
                    FROM read_parquet({input_pattern})
                )
                TO '{staging_dir}'
                (FORMAT parquet, COMPRESSION SNAPPY, PARTITION_BY (_bucket_id), OVERWRITE_OR_IGNORE true)
            """)

            local_files = glob.glob(f"{staging_dir}/_bucket_id=*/*.parquet")

            def upload_bucket(local_path):
                bucket_id = int(Path(local_path).parent.name.split("=")[1])
                bucket_folder = f"{output_folder.rstrip('/')}/{bucket_id}"
                s3_key_path = f"{bucket_folder}/{uuid.uuid4().hex}.parquet"

                if is_s3:
                    s3_key = s3_key_path.replace("s3://", "")
                    bucket_name = s3_key.split("/")[0]
                    key = "/".join(s3_key.split("/")[1:])

                    if hasattr(self.s3, "upload_fileobj"):
                        with open(local_path, "rb") as f:
                            self.s3.upload_fileobj(f, bucket_name, key)
                    else:
                        with open(local_path, "rb") as f_in:
                            with self.s3.open_output_stream(f"{bucket_name}/{key}") as f_out:
                                f_out.write(f_in.read())
                else:
                    out = Path(s3_key_path)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(local_path, out)

                size_bytes = Path(local_path).stat().st_size
                return {
                    "bucket_id": bucket_id,
                    "data": s3_key_path,
                    "output_folder": bucket_folder,
                    "size_mb": round(size_bytes / 1024 / 1024, 3),
                }

            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
                buckets = list(ex.map(upload_bucket, local_files))

            uploaded_ids = {b["bucket_id"] for b in buckets}
            for i in range(num_buckets):
                if i not in uploaded_ids:
                    buckets.append({
                        "bucket_id": i,
                        "data": None,
                        "output_folder": f"{output_folder.rstrip('/')}/{i}",
                        "size_mb": 0,
                    })

            buckets.sort(key=lambda x: x["bucket_id"])
            return {"buckets": buckets}

        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def summary_clean(self, output_s3=None, nfs_folder=None, is_delete=False):
        import shutil

        results = {
            's3': {'cleaned': False},
            'nfs': {
                'cleaned': False,
                'file_count': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0.0,
                'total_size_gb': 0.0
            },
            'errors': []
        }

        def get_folder_stats(path):
            total_size = 0
            file_count = 0
            for dirpath, _, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total_size += os.path.getsize(fp)
                        file_count += 1
                    except OSError:
                        pass
            return file_count, total_size

        def clean_local(path, key):
            if not path.exists():
                results['errors'].append(f"Path not found: {path}")
            elif not path.is_dir():
                results['errors'].append(f"Not a directory: {path}")
            else:
                file_count, total_size = get_folder_stats(path)
                results[key].update({
                    'file_count': file_count,
                    'total_size_bytes': total_size,
                    'total_size_mb': round(total_size / 1024 ** 2, 2),
                    'total_size_gb': round(total_size / 1024 ** 3, 3),
                })
                if is_delete:
                    def _ignore_missing(func, target, exc_info):
                        err = exc_info[1]
                        if isinstance(err, FileNotFoundError):
                            return
                        raise err

                    try:
                        shutil.rmtree(path, onerror=_ignore_missing)
                        if not path.exists():
                            results[key]['cleaned'] = True
                        else:
                            results['errors'].append(f"Delete folder incomplete {path}: path still exists after cleanup")
                    except Exception as e:
                        results['errors'].append(f"Delete folder failed {path}: {e}")

        if output_s3:
            try:
                if output_s3.startswith('s3://'):
                    s3_path = output_s3.replace('s3://', '').rstrip('/')
                    if is_delete:
                        try:
                            self.s3.delete_dir_contents(s3_path, accept_root_dir=True)
                            results['s3']['cleaned'] = True
                        except Exception as e:
                            file_infos = self.s3.get_file_info(fs.FileSelector(s3_path, recursive=True))
                            for f in file_infos:
                                if f.type == fs.FileType.File:
                                    try:
                                        self.s3.delete_file(f.path)
                                    except Exception as err:
                                        results['errors'].append(f"S3 delete failed {f.path}: {err}")
                            results['s3']['cleaned'] = True
                else:
                    clean_local(Path(output_s3), 's3')
            except Exception as e:
                results['errors'].append(f"S3 error: {e}")

        if nfs_folder:
            try:
                clean_local(Path(nfs_folder), 'nfs')
            except Exception as e:
                results['errors'].append(f"NFS error: {e}")

        return results


    def range_join(self, build_table_paths: dict, probe_table_paths: dict,
                   probe_select: str, join_condition: str, output_file: str,
                   join_type: str = "inner",
                   sort_by=None):

        SQL_JOIN_MAP = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "left_semi": "SEMI JOIN",
            "left_anti": "ANTI JOIN",
        }
        sql_join = SQL_JOIN_MAP[join_type.strip().lower()]

        def _needed_cols() -> set | None:
            try:
                q = f"{probe_select} FROM t1 JOIN t2 ON {join_condition}"
                tree = sqlglot.parse_one(q)
                cols = {c.name for c in tree.find_all(sqlglot_exp.Column)}
                return cols if cols else None
            except Exception:
                return None

        needed = _needed_cols()

        def read_rgs(path_rgs: dict) -> pa.Table:
            tables = []
            for path, rg_ids in path_rgs.items():
                if path.startswith('s3://'):
                    s3_path = path.replace('s3://', '')
                    if needed:
                        pf = pq.ParquetFile(s3_path, filesystem=self.s3)
                        schema_cols = set(pf.schema_arrow.names)
                        cols = [c for c in needed if c in schema_cols] or None
                    else:
                        cols = None
                    tables.append(self._read_parquet_with_retry(
                        s3_path=s3_path, row_group_ids=rg_ids, columns=cols, max_workers=4
                    ))
                else:
                    pf = pq.ParquetFile(path)
                    if needed:
                        schema_cols = set(pf.schema_arrow.names)
                        cols = [c for c in needed if c in schema_cols] or None
                    else:
                        cols = None
                    for r in rg_ids:
                        tables.append(pf.read_row_group(r, columns=cols))
            return pa.concat_tables(tables) if tables else None

        build_tbl = read_rgs(build_table_paths)
        probe_tbl = read_rgs(probe_table_paths)

        if build_tbl is None or probe_tbl is None:
            return {"data": None, "size_mb": 0, "row_count": 0}

        # print(f"[range-join][debug] build_table_paths={build_table_paths}")
        # print(f"[range-join][debug] probe_table_paths={probe_table_paths}")
        # self._debug_print_table("build_table", build_tbl)
        # self._debug_print_table("probe_table", probe_tbl)

        self.conn.register('build_table', build_tbl)
        self.conn.register('probe_table', probe_tbl)

        join_query = f"""
            {probe_select}
            FROM probe_table
            {sql_join} build_table ON {join_condition}
        """

        return self._save_and_get_metadata(
            query_or_view=join_query,
            output_file=output_file,
            cleanup_views=["build_table", "probe_table"],
            max_retries=3,
            sort_by=sort_by,
        )
