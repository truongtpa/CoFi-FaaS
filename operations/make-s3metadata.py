import json
import os, sys, sys, uuid
sys.path.insert(0, os.getcwd())
from libs.S3Metadata import S3Metadata

bucket_name = 'tpch-100'
s3 = S3Metadata(endpoint='10.158.1.100:9000')
s3.scan_bucket(bucket_name=bucket_name)
s3.export_json(f'/Users/truongtpa/Sites/BLOOM-FaaS/benchmarks/metadata/{bucket_name}.json')