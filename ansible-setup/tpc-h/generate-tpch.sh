#!/bin/bash
# Script: generate_tpch.sh
# Usage: ./generate_tpch.sh <scale_factor> <partitions>
set -e  # Exit on error

# Parse arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <scale_factor> <partitions>"
    echo "Example: $0 10 4"
    exit 1
fi

SF=$1
PAR=$2

# Paths
CARGO_PATH="/mnt/ssd/tpctools"
DBGEN_PATH="/mnt/ssd/tpctools/tpch-dbgen"
OUTPUT_DIR="/mnt/ssd/tpch${SF}"
PARQUET_DIR="/mnt/ssd/tpch${SF}-parquet"

echo "========================================="
echo "TPC-H Data Generation and Conversion"
echo "========================================="
echo "Scale Factor: ${SF}"
echo "Partitions: ${PAR}"
echo "Output Directory: ${OUTPUT_DIR}"
echo "Parquet Directory: ${PARQUET_DIR}"
echo "========================================="

# Clean up existing directories
echo "Cleaning up existing directories..."
rm -rf "${OUTPUT_DIR}"
rm -rf "${PARQUET_DIR}"

# Create output directories with 777 permissions
echo "Creating output directories with full permissions..."
mkdir -p "${OUTPUT_DIR}"
mkdir -p "${PARQUET_DIR}"
chmod -R 777 "${OUTPUT_DIR}"
chmod -R 777 "${PARQUET_DIR}"

# Navigate to cargo project
cd "${CARGO_PATH}"

# Generate TPC-H data
echo ""
echo "Step 1: Generating TPC-H data (SF=${SF}, Partitions=${PAR})..."
cargo run --release -- generate \
  --benchmark tpch \
  --scale "${SF}" \
  --partitions "${PAR}" \
  --generator-path "${DBGEN_PATH}/" \
  --output "${OUTPUT_DIR}"

echo "Generation completed!"

# Set permissions for generated files
echo "Setting permissions for generated files..."
chmod -R 777 "${OUTPUT_DIR}"

# Convert to Parquet
echo ""
echo "Step 2: Converting to Parquet format..."
cargo run --release -- convert \
  --benchmark tpch \
  --input "${OUTPUT_DIR}" \
  --output "${PARQUET_DIR}"

echo ""
echo "========================================="
echo "Conversion completed!"
echo "========================================="

# Set final permissions for parquet files
echo "Setting final permissions..."
chmod -R 777 "${PARQUET_DIR}"

echo "Raw data: ${OUTPUT_DIR}"
echo "Parquet data: ${PARQUET_DIR}"

# Show size information
echo ""
echo "Size information:"
du -sh "${OUTPUT_DIR}"
du -sh "${PARQUET_DIR}"

# Verify permissions
echo ""
echo "Permissions:"
ls -ld "${OUTPUT_DIR}"
ls -ld "${PARQUET_DIR}"

echo ""
echo "Done!"