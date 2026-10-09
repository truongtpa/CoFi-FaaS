import json
import requests
from datetime import datetime


class HTTPCall:
    def __init__(self, url, host, debug=False, version='v1'):
        self.base = url
        self.host = host
        self.timeout = 60
        self.debug = debug
        self.version = version

    def __call(self, path, payload):
        url = self.base + path
        headers = {
            "Host": self.host,
            "Content-Type": "application/json"
        }
        request_sent_at = datetime.now().isoformat()

        if self.debug:
            print(f"URL: {url}")
            print(f"Headers: {json.dumps(headers, indent=2)}")
            print(f"Payload: {json.dumps(payload, indent=2, default=str)}")

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            return {
                'request_sent_at': request_sent_at,
                'request_path': path,
                'status_code': 599,
                'response': {
                    'error': f'Network error: {type(e).__name__}: {e}',
                    'error_type': 'NetworkError',
                    'url': url,
                    'host_header': self.host,
                },
            }

        try:
            body = response.json()
        except (json.JSONDecodeError, ValueError):
            body = {
                'error': f'Non-JSON response (HTTP {response.status_code})',
                'error_type': 'InvalidResponse',
                'url': url,
                'host_header': self.host,
                'raw_body_preview': response.text[:500] if response.text else '<empty>',
                'content_type': response.headers.get('Content-Type', ''),
            }

        return {
            'request_sent_at': request_sent_at,
            'request_path': path,
            'status_code': response.status_code,
            'response': body,
        }

    def scan(self, select, location, sql_clauses=None, output_file=None,
             row_group_ids=None, bf_column=None, bf_path=None, est_elements=None,
             filter_by_bf=None, max_workers=6,
             sort_by=None):
        payload = {
            'select': select,
            'location': location,
            'sql_clauses': sql_clauses,
            'output_file': output_file,
            'row_group_ids': row_group_ids,
            'bf_column': bf_column,
            'bf_path': bf_path,
            'est_elements': est_elements,
            'filter_by_bf': filter_by_bf,
            'max_workers': max_workers,
            'sort_by': sort_by,
        }
        return self.__call(f'/scan-{self.version}', payload)

    def join(self, build_table_paths: list[str], probe_table_paths: list[str],
             probe_select: str, join_condition: str, output_file: str, join_type: str,
             sort_by=None):
        payload = {
            'build_table_paths': build_table_paths,
            'probe_table_paths': probe_table_paths,
            'probe_select': probe_select,
            'join_condition': join_condition,
            'output_file': output_file,
            'join_type': join_type,
            'sort_by': sort_by,
        }
        return self.__call('/join', payload)

    def join_pushdown(self, build_table_paths: list[str], probe_table_paths: list[str],
                      probe_select: str, join_condition: str, output_file: str,
                      build_key: str, probe_key: str, join_type: str,
                      sort_by=None):
        payload = {
            'build_table_paths': build_table_paths,
            'probe_table_paths': probe_table_paths,
            'probe_select': probe_select,
            'join_condition': join_condition,
            'output_file': output_file,
            'build_key': build_key,
            'probe_key': probe_key,
            'join_type': join_type,
            'sort_by': sort_by,
        }
        return self.__call('/join-pushdown', payload)

    def merge_bf(self, folder_bf: str, file_output: str, error_rate: float):
        payload = {
            'folder_bf': folder_bf,
            'file_output': file_output,
            'error_rate': error_rate,
        }
        return self.__call('/merge-bf', payload)

    def aggregate(self, input_parquet_paths: list[str], output_file: str, select: str,
                  sql_clauses: str, single_file: str, bf_column: str, bf_path: str,
                  est_elements: int, error_rate: float,
                  sort_by=None,
                  hash_column=None, num_buckets=None, partition_output_folder=None):
        payload = {
            'input_parquet_paths': input_parquet_paths,
            'output_file': output_file,
            'select': select,
            'sql_clauses': sql_clauses,
            'single_file': single_file,
            'bf_column': bf_column,
            'bf_path': bf_path,
            'est_elements': est_elements,
            'error_rate': error_rate,
            'sort_by': sort_by,
            'hash_column': hash_column,
            'num_buckets': num_buckets,
            'partition_output_folder': partition_output_folder,
        }
        return self.__call(f'/aggregate', payload)

    def summary_clean(self, output_s3: str, nfs_folder: str, is_delete: bool):
        payload = {
            'output_s3': output_s3,
            'nfs_folder': nfs_folder,
            'is_delete': is_delete
        }
        return self.__call('/summary-clean', payload)

    def create_bf(self, input_paths: list[str], bf_column: str, bf_path: str,
                  est_elements: int, error_rate: float):
        payload = {
            'input_paths': input_paths,
            'bf_column': bf_column,
            'bf_path': bf_path,
            'est_elements': est_elements,
            'error_rate': error_rate
        }
        return self.__call('/create-bf', payload)

    def hash_partition(self, input_parquet_paths: list[str], output_folder: str,
                       hash_column: str, num_buckets: int):
        payload = {
            'input_parquet_paths': input_parquet_paths,
            'output_folder': output_folder,
            'hash_column': hash_column,
            'num_buckets': num_buckets,
        }
        return self.__call('/hash-partition', payload)


    def range_join(self, build_table_paths: dict, probe_table_paths: dict,
                   probe_select: str, join_condition: str, output_file: str,
                   join_type: str = "inner",
                   sort_by=None):
        payload = {
            'build_table_paths': build_table_paths,
            'probe_table_paths': probe_table_paths,
            'probe_select': probe_select,
            'join_condition': join_condition,
            'output_file': output_file,
            'join_type': join_type,
            'sort_by': sort_by,
        }
        return self.__call('/range-join', payload)
