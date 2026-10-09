## PUMA Benchmark for Hadoop
```
https://engineering.purdue.edu/~puma/datasets.htm
```

## Convert to parquet
```
cat > single_column.py << 'EOF'
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")

conn.execute("""
    SET s3_endpoint='10.158.1.20:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='lannion-enssat';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Read entire line as single column
conn.execute("""
    COPY (
        SELECT UNNEST(string_split(line, ',')) as value
        FROM read_csv(
            's3://puma30/file1',
            delim='\n',
            header=FALSE,
            columns={'line': 'VARCHAR'}
        )
    )
    TO 's3://puma30/file1.parquet' 
    (FORMAT PARQUET, COMPRESSION 'SNAPPY')
""")

print("Converted s3://puma30/file1 -> s3://puma30/file1.parquet (single column)")

conn.close()
EOF
```

```
cat > single_column.py << 'EOF'
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")

conn.execute("""
    SET s3_endpoint='10.158.1.20:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='lannion-enssat';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Read entire line as single column
conn.execute("""
    COPY (
        SELECT line as column0
        FROM read_csv(
            's3://puma30/file1',
            delim='\n',
            header=FALSE,
            columns={'line': 'VARCHAR'}
        )
    )
    TO 's3://puma30/file1.parquet' 
    (FORMAT PARQUET, COMPRESSION 'SNAPPY')
""")

print("Converted s3://puma30/file1 -> s3://puma30/file1.parquet")

conn.close()
EOF
```

```bash
cat > all-file.py << 'EOF'
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")

conn.execute("""
    SET s3_endpoint='10.158.1.20:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='lannion-enssat';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

bucket = 'puma30'
success = 0
failed = 0

print("Starting conversion of file1 to file28...\n")

for i in range(1, 29):  # 1 to 28
    filename = f'file{i}'
    input_path = f's3://{bucket}/{filename}'
    output_path = f's3://{bucket}/{filename}.parquet'
    
    try:
        conn.execute(f"""
            COPY (
                SELECT line as column0
                FROM read_csv(
                    '{input_path}',
                    delim='\n',
                    header=FALSE,
                    columns={{'line': 'VARCHAR'}}
                )
            )
            TO '{output_path}' 
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """)
        
        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        print(f"[{i}/28] {filename} -> {filename}.parquet ({count:,} rows)")
        success += 1
        
    except Exception as e:
        print(f"[{i}/28] {filename}: {e}")
        failed += 1

print(f"\n{'='*50}")
print(f"Summary: {success} succeeded, {failed} failed")
print(f"{'='*50}")

conn.close()
EOF
```

```bash
for i in {15..28}; do
  mc mv minio/puma30/file${i}.parquet minio/puma30/dataset02/file${i}.parquet
  echo "Moved file${i}.parquet -> dataset02/"
done
```