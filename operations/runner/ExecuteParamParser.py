from typing import Dict, Any
from operations.runner.BaseParamParser import BaseParamParser


def _str_to_none(value):
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "none":
        return None
    return value


def _to_list(value):
    if value is None:
        return []
    elif isinstance(value, list):
        return value
    elif isinstance(value, str):
        if ',' in value:
            return [v.strip() for v in value.split(',')]
        else:
            return [value]
    else:
        return [value]


def _to_int_or_none(value):
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "none":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _to_float_or_none(value):
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "none":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_bool_or_none(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "on"):
            return True
        if lowered in ("false", "0", "no", "n", "off", "none", ""):
            return False
    return bool(value)


def _to_sort_by(value):
    """Normalize sort_by to list[str] or None"""
    if value is None:
        return None
    if isinstance(value, str):
        if value.lower() == "none" or not value.strip():
            return None
        if ',' in value:
            return [v.strip() for v in value.split(',') if v.strip()]
        return [value.strip()]
    if isinstance(value, (list, tuple)):
        cleaned = [str(v).strip() for v in value if str(v).strip()]
        return cleaned or None
    return None


class ScanParamParser(BaseParamParser):
    required_fields = ["select", "output_file"]
    optional_fields = ["sql_clauses", "bf_path", "sort_by",
                       "max_workers", "single_row_group_per_task"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "select": self.raw["select"].strip(),
            "location": self.raw.get("location"),
            "sql_clauses": _str_to_none(self.raw.get("sql_clauses")),
            "output_file": self.raw["output_file"],
            "bf_column": _str_to_none(self.raw.get("bf_column")),
            "bf_path": _str_to_none(self.raw.get("bf_path")),
            "est_elements": self.raw.get("est_elements"),
            "filter_by_bf": _str_to_none(self.raw.get("filter_by_bf")),
            "max_workers": _str_to_none(self.raw.get("max_workers")),
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
            "single_row_group_per_task": _to_bool_or_none(self.raw.get("single_row_group_per_task")),
        }


class AggregateParamParser(BaseParamParser):
    required_fields = ["output_file"]
    optional_fields = ["sort_by", "hash_column", "num_buckets", "partition_output_folder"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "input_parquet_paths": self.raw.get("input_parquet_paths"),
            "output_file": self.raw["output_file"],
            "select": self.raw.get("select"),
            "sql_clauses": self.raw.get("sql_clauses"),
            "single_file": self.raw.get("single_file"),
            "bf_column": self.raw.get("bf_column"),
            "bf_path": self.raw.get("bf_path"),
            "est_elements": self.raw.get("est_elements"),
            "error_rate": self.raw.get("error_rate"),
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
            "hash_column": _str_to_none(self.raw.get("hash_column")),
            "num_buckets": _to_int_or_none(self.raw.get("num_buckets")),
            "partition_output_folder": _str_to_none(self.raw.get("partition_output_folder")),
        }


class MergeBFParamParser(BaseParamParser):
    required_fields = ["file_output", "error_rate"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "folder_bf": self.raw.get("folder_bf"),
            "file_output": self.raw["file_output"],
            "error_rate": self.raw["error_rate"],
        }


class BroadcastJoinParamParser(BaseParamParser):
    required_fields = ["probe_select", "join_condition", "output_file"]
    optional_fields = ["build_table_paths", "probe_table_paths", "join_type",
                       "sort_by"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "build_table_paths": self.raw.get('build_table_paths'),
            "probe_table_paths": self.raw.get("probe_table_paths"),
            "probe_select": self.raw["probe_select"],
            "join_condition": self.raw["join_condition"],
            "output_file": self.raw["output_file"],
            "join_type": self.raw.get("join_type"),
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
        }


class BroadcastJoinPushdownParamParser(BaseParamParser):
    required_fields = ["probe_select", "join_condition", "output_file", "build_key", "probe_key"]
    optional_fields = ["build_table_paths", "probe_table_paths", "join_type", "sort_by"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "build_table_paths": self.raw.get('build_table_paths'),
            "probe_table_paths": self.raw.get("probe_table_paths"),
            "probe_select": self.raw["probe_select"],
            "join_condition": self.raw["join_condition"],
            "output_file": self.raw["output_file"],
            "join_type": self.raw.get("join_type"),
            "build_key": self.raw["build_key"],
            "probe_key": self.raw["probe_key"],
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
        }


class HashJoinParamParser(BaseParamParser):
    required_fields = ["probe_select", "join_condition", "output_file"]
    optional_fields = ["build_table_paths", "probe_table_paths", "join_type",
                       "sort_by"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "build_table_paths": self.raw.get('build_table_paths'),
            "probe_table_paths": self.raw.get("probe_table_paths"),
            "probe_select": self.raw["probe_select"],
            "join_condition": self.raw["join_condition"],
            "output_file": self.raw["output_file"],
            "join_type": self.raw.get("join_type"),
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
        }


class SummaryCleanParamParser(BaseParamParser):
    required_fields = []
    optional_fields = ["output_s3", "nfs_folder", "is_delete", "exclude_patterns"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "output_s3": self.raw.get('output_s3'),
            "nfs_folder": self.raw.get("nfs_folder"),
            "is_delete": self.raw.get("is_delete"),
            "exclude_patterns": _to_list(self.raw.get("exclude_patterns")),
        }


class CreateBFParamParser(BaseParamParser):
    required_fields = []
    optional_fields = ['input_paths', 'bf_column', 'bf_path', 'est_elements', 'error_rate']

    def _normalize(self) -> Dict[str, Any]:
        return {
            "input_paths": self.raw.get('input_paths'),
            "bf_column": self.raw.get("bf_column"),
            "bf_path": self.raw.get("bf_path"),
            "est_elements": self.raw.get("est_elements"),
            "error_rate": self.raw.get("error_rate"),
        }


class HashPartitionParamParser(BaseParamParser):
    required_fields = []
    optional_fields = ['input_parquet_paths', 'output_folder', 'hash_column', 'num_buckets']

    def _normalize(self) -> Dict[str, Any]:
        return {
            "input_parquet_paths": self.raw.get('input_parquet_paths'),
            "output_folder": self.raw.get("output_folder"),
            "hash_column": self.raw.get("hash_column"),
            "num_buckets": self.raw.get("num_buckets"),
        }


class RangeJoinParamParser(BaseParamParser):
    required_fields = ["probe_select", "join_condition", "output_file",
                       "build_key", "probe_key"]
    optional_fields = ["build_table_paths", "probe_table_paths", "join_type",
                       "sort_by", "skew_threshold",
                       "max_files_per_task"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "build_table_paths": self.raw.get('build_table_paths'),
            "probe_table_paths": self.raw.get("probe_table_paths"),
            "probe_select": self.raw["probe_select"],
            "join_condition": self.raw["join_condition"],
            "output_file": self.raw["output_file"],
            "join_type": self.raw.get("join_type"),
            "build_key": self.raw["build_key"],
            "probe_key": self.raw["probe_key"],
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
            "skew_threshold": _to_int_or_none(self.raw.get("skew_threshold")),
            "max_files_per_task": _to_int_or_none(self.raw.get("max_files_per_task")),
        }

class AdaptiveJoinParamParser(BaseParamParser):
    required_fields = ["probe_select", "join_condition", "output_file",
                       "build_key", "probe_key"]
    optional_fields = ["build_table_paths", "probe_table_paths", "join_type",
                       "sort_by",
                       "broadcast_threshold_mb", "skew_threshold",
                       "force_strategy", "max_files_per_task",
                       "max_probe_files_per_task", "max_probe_row_groups_per_task",
                       "max_build_file_ratio"]

    def _normalize(self) -> Dict[str, Any]:
        return {
            "build_table_paths": self.raw.get('build_table_paths'),
            "probe_table_paths": self.raw.get("probe_table_paths"),
            "probe_select": self.raw["probe_select"],
            "join_condition": self.raw["join_condition"],
            "output_file": self.raw["output_file"],
            "join_type": self.raw.get("join_type"),
            "build_key": self.raw["build_key"],
            "probe_key": self.raw["probe_key"],
            "sort_by": _to_sort_by(self.raw.get("sort_by")),
            "broadcast_threshold_mb": _to_int_or_none(self.raw.get("broadcast_threshold_mb")),
            "skew_threshold": _to_int_or_none(self.raw.get("skew_threshold")),
            "force_strategy": self.raw.get("force_strategy"),
            "max_files_per_task": _to_int_or_none(self.raw.get("max_files_per_task")),
            "max_probe_files_per_task": _to_int_or_none(self.raw.get("max_probe_files_per_task")),
            "max_probe_row_groups_per_task": _to_int_or_none(self.raw.get("max_probe_row_groups_per_task")),
            "max_build_file_ratio": _to_float_or_none(self.raw.get("max_build_file_ratio")),
        }
