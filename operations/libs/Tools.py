import os
import platform
import uuid, requests
from pathlib import Path

import pyarrow.parquet as pq
import os
from pyarrow import fs
from typing import List, Tuple, Dict
from contextlib import contextmanager

@contextmanager
def s3_connection():

    endpoint = os.environ.get("MINIO_ENDPOINT", "10.158.1.20:9000")
    access_key = os.environ.get("ACCESS_KEY", "admin")
    secret_key = os.environ.get("SECRET_KEY", "lannion-enssat")

    s3 = fs.S3FileSystem(
        endpoint_override=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        scheme="http",
    )
    try:
        yield s3
    finally:
        del s3

class Tools:

    @staticmethod
    def export_minio(history_folder):
        host = "10.158.1.100"
        headers = {
            'Authorization': 'Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwcm9tZXRoZXVzIiwic3ViIjoiYWRtaW4iLCJleHAiOjQ5MjMxOTg5NjB9.KefE8cccDt1dhnJeF8L1c08Hl4lxVcMmWZ4YxRBv6AyTdyRAP8b25o8H5LDy9OVLKM4EMPp7fDHu5aAyc_QiXg'
        }
        response = requests.request("GET", f"http://{host}:9000/minio/v2/metrics/bucket", headers=headers, data={})
        folder = history_folder + '/minio.txt'
        with open(folder, "w", encoding="utf-8") as f:
            f.write(response.text)