import pyarrow.fs as fs
import pyarrow.parquet as pq
import os
import json
from typing import Dict, List, Optional
from datetime import datetime, date
from decimal import Decimal
import re


class _StatsEncoder(json.JSONEncoder):
    """Handle non-serializable types that come from Parquet statistics."""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            # binary stats — encode as hex string for readability
            return obj.hex()
        return super().default(obj)


class S3Metadata:
    """S3 Metadata Scanner - Simplified Version"""

    def __init__(
            self,
            endpoint: str = "localhost:9000",
            access_key: Optional[str] = None,
            secret_key: Optional[str] = None,
            scheme: str = "http"
    ):
        self.endpoint = endpoint
        self.access_key = access_key or os.environ.get("ACCESS_KEY", "admin")
        self.secret_key = secret_key or os.environ.get("SECRET_KEY", "lannion-enssat")
        self.scheme = scheme
        self.catalog = {'metadata': {}, 'datasets': {}}
        self._s3 = None

    def _get_s3_client(self) -> fs.S3FileSystem:
        """Get S3 client"""
        if self._s3 is None:
            self._s3 = fs.S3FileSystem(
                endpoint_override=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                scheme=self.scheme,
            )
        return self._s3

    def _should_exclude(self, path: str, exclude_patterns: List[str]) -> bool:
        """Check if path should be excluded"""
        if not exclude_patterns:
            return False
        for pattern in exclude_patterns:
            if pattern in path or re.search(pattern, path):
                return True
        return False

    def scan_folder(
            self,
            s3_path: str,
            file_extension: str = '.parquet',
            recursive: bool = False,
            exclude_dirs: Optional[List[str]] = None,
            include_schema: bool = False
    ) -> Dict:
        """Scan một S3 folder và trả về metadata"""
        s3 = self._get_s3_client()
        exclude_dirs = exclude_dirs or []

        dataset_name = s3_path.rstrip('/').split('/')[-1]
        path = s3_path.replace('s3://', '')

        file_info = s3.get_file_info(fs.FileSelector(path, recursive=recursive))

        files = []
        total_rows = 0
        total_size = 0

        for info in file_info:
            if info.type != fs.FileType.File:
                continue
            if self._should_exclude(info.path, exclude_dirs):
                continue
            if file_extension and not info.path.endswith(file_extension):
                continue

            file_path = f's3://{info.path}'  # Cho hiển thị
            parquet_path = info.path  # Cho đọc file (không có s3://)

            file_meta = {
                'filename': info.path.split('/')[-1],
                'full_path': file_path,
                'size_mb': round(info.size / (1024 * 1024), 2),
                'num_rows': 0,
                'num_row_groups': 0,
                'num_columns': 0,
                'row_groups': [],
            }

            if file_extension == '.parquet':
                try:
                    pf = pq.ParquetFile(parquet_path, filesystem=s3)
                    meta = pf.metadata

                    # Row groups - dùng compressed size để khớp với row_group_compressed_bytes
                    row_groups = []
                    for i in range(meta.num_row_groups):
                        rg = meta.row_group(i)
                        compressed_bytes = sum(
                            rg.column(j).total_compressed_size
                            for j in range(rg.num_columns)
                        )

                        # Column-level metadata: compressed size + min/max stats
                        col_stats = {}
                        col_sizes = []
                        for j in range(rg.num_columns):
                            col = rg.column(j)
                            col_name = col.path_in_schema.split('.')[-1]
                            col_compressed = col.total_compressed_size  # bytes

                            # min/max stats
                            st = col.statistics
                            if st is not None and st.has_min_max:
                                try:
                                    col_stats[col_name] = {'min': st.min, 'max': st.max}
                                except Exception:
                                    # decimal128, byte_array, etc. — has_min_max=True but unextractable
                                    pass

                            col_sizes.append({
                                'path_in_schema': col.path_in_schema,
                                'name': col_name,
                                'compressed_size': col_compressed,
                                'compressed_size_mb': round(col_compressed / (1024 * 1024), 4),
                            })

                        row_groups.append({
                            'id': i,
                            'num_rows': rg.num_rows,
                            'total_byte_size': compressed_bytes,
                            'total_byte_size_mb': round(compressed_bytes / (1024 * 1024), 2),
                            'columns': col_sizes,
                            'stats': col_stats,
                        })

                    file_meta.update({
                        'num_rows': int(meta.num_rows),
                        'num_row_groups': int(meta.num_row_groups),
                        'num_columns': int(meta.num_columns),
                        'row_groups': row_groups,
                    })

                    # Schema (optional)
                    if include_schema:
                        schema = {}
                        for i in range(meta.num_columns):
                            col = meta.schema.column(i)
                            schema[col.name] = {
                                'type': str(col.physical_type),
                                'logical_type': str(col.logical_type) if col.logical_type else None,
                            }
                        file_meta['schema'] = schema

                    total_rows += meta.num_rows
                except Exception as e:
                    file_meta['error'] = str(e)

            files.append(file_meta)
            total_size += info.size

        num_files = len(files)
        return {
            'name': dataset_name,
            'path': s3_path,
            'num_files': num_files,
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'total_size_gb': round(total_size / (1024 ** 3), 2),
            'avg_rows_per_file': round(total_rows / num_files) if num_files > 0 else 0,
            'avg_size_mb_per_file': round(total_size / (1024 * 1024) / num_files, 2) if num_files > 0 else 0,
            'files': files
        }

    def scan_multiple(
            self,
            s3_paths: List[str],
            file_extension: str = '.parquet',
            recursive: bool = False,
            exclude_dirs: Optional[List[str]] = None,
            include_schema: bool = False
    ) -> Dict:
        """Scan nhiều folders và build catalog"""
        self.catalog = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'endpoint': self.endpoint,
                'total_datasets': 0,
                'total_files': 0,
                'total_rows': 0,
                'total_size_bytes': 0,
                'total_size_gb': 0.0
            },
            'datasets': {}
        }

        for s3_path in s3_paths:
            dataset = self.scan_folder(s3_path, file_extension, recursive, exclude_dirs, include_schema)
            self.catalog['datasets'][dataset['name']] = dataset
            self.catalog['metadata']['total_files'] += dataset['num_files']
            self.catalog['metadata']['total_rows'] += dataset['total_rows']
            self.catalog['metadata']['total_size_bytes'] += dataset['total_size_bytes']

        self.catalog['metadata']['total_datasets'] = len(self.catalog['datasets'])
        self.catalog['metadata']['total_size_gb'] = round(
            self.catalog['metadata']['total_size_bytes'] / (1024 ** 3), 2
        )

        return self.catalog

    def scan_bucket(
            self,
            bucket_name: str,
            file_extension: str = '.parquet',
            exclude_dirs: Optional[List[str]] = None,
            include_schema: bool = False
    ) -> Dict:
        """Scan toàn bộ bucket - auto discover datasets"""
        s3 = self._get_s3_client()
        exclude_dirs = exclude_dirs or []

        # List tất cả items trong bucket (không filter extension ở đây)
        file_info = s3.get_file_info(fs.FileSelector(bucket_name, recursive=False))

        s3_paths = []
        for info in file_info:
            # Chỉ lấy directories, KHÔNG filter theo extension
            if info.type == fs.FileType.Directory:
                if not self._should_exclude(info.path, exclude_dirs):
                    s3_paths.append(f's3://{info.path}/')

        # Scan từng folder (lúc này mới filter files theo extension)
        return self.scan_multiple(s3_paths, file_extension, False, exclude_dirs, include_schema)

    def export_json(self, output_file: str) -> str:
        """Xuất catalog ra JSON"""
        with open(output_file, 'w') as f:
            json.dump(self.catalog, f, indent=2, cls=_StatsEncoder)
        return output_file

    def load_json(self, input_file: str) -> Dict:
        """Load catalog từ JSON"""
        with open(input_file, 'r') as f:
            self.catalog = json.load(f)
        return self.catalog

    def get_catalog(self) -> Dict:
        """Trả về catalog"""
        return self.catalog

    def query_dataset(self, dataset_name: str) -> Optional[Dict]:
        """Query dataset"""
        return self.catalog['datasets'].get(dataset_name)

    def query_file(self, dataset_name: str, filename: str) -> Optional[Dict]:
        """Query file trong dataset"""
        dataset = self.query_dataset(dataset_name)
        if dataset:
            for f in dataset['files']:
                if f['filename'] == filename:
                    return f
        return None

    def search(self, search_path: str) -> Optional[Dict]:
        """
        Search theo full path (file hoặc dataset)

        Args:
            search_path: Full path để search
                        - Dataset: 's3://tpch-50/customer.parquet/'
                        - File: 's3://tpch-50/customer.parquet/part-0.parquet'

        Returns:
            Dict với thông tin file hoặc dataset, hoặc None nếu không tìm thấy
        """
        search_path = search_path.rstrip('/')

        # Search trong datasets
        for dataset_name, dataset in self.catalog['datasets'].items():
            dataset_path = dataset['path'].rstrip('/')

            # Match dataset path
            if search_path == dataset_path or search_path == f"s3://{dataset_path}":
                return {
                    'type': 'dataset',
                    'data': dataset
                }

            # Search trong files
            for file_meta in dataset['files']:
                if search_path == file_meta['full_path']:
                    return {
                        'type': 'file',
                        'dataset': dataset_name,
                        'data': file_meta
                    }

        return None

    def search_pattern(self, pattern: str) -> List[Dict]:
        """
        Search theo pattern (hỗ trợ regex)

        Args:
            pattern: Pattern để search (regex)
                    - 'part-0' -> tìm tất cả file có 'part-0'
                    - '.*lineitem.*' -> tìm tất cả có 'lineitem'
                    - '.*\\.parquet$' -> tìm tất cả kết thúc .parquet

        Returns:
            List các kết quả tìm được
        """
        results = []

        for dataset_name, dataset in self.catalog['datasets'].items():
            # Search dataset path
            if re.search(pattern, dataset['path']):
                results.append({
                    'type': 'dataset',
                    'match': dataset['path'],
                    'data': dataset
                })

            # Search files
            for file_meta in dataset['files']:
                if re.search(pattern, file_meta['full_path']) or re.search(pattern, file_meta['filename']):
                    results.append({
                        'type': 'file',
                        'dataset': dataset_name,
                        'match': file_meta['full_path'],
                        'data': file_meta
                    })

        return results

    def close(self):
        """Đóng connection"""
        if self._s3:
            del self._s3
            self._s3 = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()