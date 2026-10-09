from operations import app
from operations.libs.ParquetS3Funcs import ParquetS3Funcs
from flask import request, jsonify
from datetime import datetime
import logging
import socket
import sys

class SafeFormatter(logging.Formatter):
    def format(self, record):
        for key in ("method", "json"):
            if not hasattr(record, key):
                setattr(record, key, None)
        return super().format(record)

formatter = SafeFormatter(
    '%(asctime)s - %(levelname)s - %(message)s - method=%(method)s json=%(json)s'
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def ok_response(result):
    return jsonify({
        "status": "OK",
        "hostname": socket.gethostname(),
        "server_start_time": getattr(request, "_server_start_time", None),
        "data": result,
    }), 200


def mark_request_start():
    request._server_start_time = datetime.now().isoformat()

@app.route("/scan-starling", methods=['POST'])
def scan_v1():
    mark_request_start()
    logger.info("HTTP request /scan-starling", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        select = data.get("select")
        location = data.get("location")
        output_file = data.get("output_file")
        sql_clauses = data.get('sql_clauses')
        row_group_ids = data.get('row_group_ids')

        with ParquetS3Funcs() as funcs:
            result = funcs.scan_starling(
                select=select,
                location=location,
                sql_clauses=sql_clauses,
                output_file=output_file,
                row_group_ids=row_group_ids
            )

            return ok_response(result)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/scan-bloomfaas", methods=['POST'])
def scan_v2():
    mark_request_start()
    logger.info("HTTP request /scan-bloomfaas", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        select = data.get("select")
        location = data.get("location")
        output_file = data.get("output_file")
        sql_clauses = data.get('sql_clauses')
        row_group_ids = data.get('row_group_ids')
        bf_column = data.get('bf_column')
        filter_by_bf = data.get('filter_by_bf')
        max_workers = data.get('max_workers')
        sort_by = data.get('sort_by')

        with ParquetS3Funcs() as funcs:
            result = funcs.scan_bloomfaas(
                select=select,
                location=location,
                sql_clauses=sql_clauses,
                output_file=output_file,
                row_group_ids=row_group_ids,
                bf_column=bf_column,
                filter_by_bf=filter_by_bf,
                max_workers=max_workers,
                sort_by=sort_by,
            )

            return ok_response(result)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/join", methods=['POST'])
def join():
    mark_request_start()
    logger.info("HTTP request join", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        build_table_paths = data.get("build_table_paths")
        probe_table_paths = data.get("probe_table_paths")
        probe_select = data.get("probe_select")
        join_condition = data.get('join_condition')
        output_file = data.get('output_file')
        join_type = data.get('join_type')
        sort_by = data.get('sort_by')

        with ParquetS3Funcs() as faas:
            result = faas.join(
                build_table_paths=build_table_paths,
                probe_table_paths=probe_table_paths,
                probe_select=probe_select,
                join_condition=join_condition,
                output_file=output_file,
                join_type=join_type,
                sort_by=sort_by,
            )

            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /execute")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/join-pushdown", methods=['POST'])
def join_pushdown():
    mark_request_start()
    logger.info("HTTP request /join-pushdown", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        build_table_paths = data.get("build_table_paths")
        probe_table_paths = data.get("probe_table_paths")
        probe_select = data.get("probe_select")
        join_condition = data.get('join_condition')
        output_file = data.get('output_file')
        build_key = data.get('build_key')
        probe_key = data.get('probe_key')
        join_type = data.get('join_type')
        sort_by = data.get('sort_by')

        with ParquetS3Funcs() as faas:
            result = faas.join_pushdown(
                build_table_paths=build_table_paths,
                probe_table_paths=probe_table_paths,
                probe_select=probe_select,
                join_condition=join_condition,
                output_file=output_file,
                build_key=build_key,
                probe_key=probe_key,
                join_type=join_type,
                sort_by=sort_by,
            )

            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /join-pushdown")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/merge-bf", methods=['POST'])
def merge_bf():
    mark_request_start()
    logger.info("HTTP request /merge-bf", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        folder_bf = data.get("folder_bf")
        file_output = data.get("file_output")
        error_rate = data.get("error_rate")

        with ParquetS3Funcs() as faas:
            result = faas.merge_bloom_filter(
                folder=folder_bf,
                file_output=file_output,
                error_rate=float(error_rate)
            )

            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /execute")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/aggregate", methods=['POST'])
def aggregate():
    mark_request_start()
    logger.info("HTTP request /aggregate", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        input_parquet_paths = data.get("input_parquet_paths")
        output_file = data.get("output_file")
        select = data.get("select")
        sql_clauses = data.get("sql_clauses")
        bf_column = data.get('bf_column')
        bf_path = data.get('bf_path')
        est_elements = data.get('est_elements')
        error_rate = data.get('error_rate')
        sort_by = data.get('sort_by')
        hash_column = data.get('hash_column')
        num_buckets = data.get('num_buckets')
        partition_output_folder = data.get('partition_output_folder')

        with ParquetS3Funcs() as faas:
            result = faas.aggregate(
                input_parquet_paths=input_parquet_paths,
                output_file=output_file,
                select=select,
                sql_clauses=sql_clauses,
                bf_column=bf_column,
                bf_path=bf_path,
                est_elements=est_elements,
                error_rate=error_rate,
                sort_by=sort_by,
                hash_column=hash_column,
                num_buckets=num_buckets,
                partition_output_folder=partition_output_folder,
            )

            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /aggregate")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500

@app.route("/hash-partition", methods=['POST'])
def hash_partition():
    mark_request_start()
    logger.info("HTTP request /hash-partition", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        input_parquet_paths = data.get("input_parquet_paths")
        output_folder = data.get("output_folder")
        hash_column = data.get("hash_column")
        num_buckets = data.get("num_buckets")

        with ParquetS3Funcs() as faas:
            result = faas.hash_partition(
                input_parquet_paths=input_parquet_paths,
                output_folder=output_folder,
                hash_column=hash_column,
                num_buckets=num_buckets,
            )
            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /hash-partition")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500


@app.route("/range-join", methods=['POST'])
def range_join():
    mark_request_start()
    logger.info("HTTP request /range-join", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        if not data:
            raise ValueError("Empty JSON body")

        build_table_paths = data.get("build_table_paths")
        probe_table_paths = data.get("probe_table_paths")
        probe_select = data.get("probe_select")
        join_condition = data.get('join_condition')
        output_file = data.get('output_file')
        join_type = data.get('join_type')
        sort_by = data.get('sort_by')

        with ParquetS3Funcs() as faas:
            result = faas.range_join(
                build_table_paths=build_table_paths,
                probe_table_paths=probe_table_paths,
                probe_select=probe_select,
                join_condition=join_condition,
                output_file=output_file,
                join_type=join_type,
                sort_by=sort_by,
            )
            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /range-join")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500

@app.route("/summary-clean", methods=['POST'])
def summary_clean():
    mark_request_start()
    logger.info("HTTP request /summary-clean", extra={
        "method": request.method,
        "json": request.get_json(),
    })
    try:
        data = request.get_json()
        output_s3 = data.get("output_s3")
        nfs_folder = data.get("nfs_folder")
        is_delete = data.get("is_delete")

        with ParquetS3Funcs() as faas:
            result = faas.summary_clean(output_s3=output_s3, nfs_folder=nfs_folder, is_delete=is_delete)
            return ok_response(result)

    except Exception as e:
        logger.exception("Unhandled exception in /execute")
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e)
        }, 500
