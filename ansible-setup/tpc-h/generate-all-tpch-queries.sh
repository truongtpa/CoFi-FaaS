#!/bin/bash
set -e

SF=${1:-100}
OUT_DIR="/mnt/ssd/tpch${SF}-queries"
DBGEN_PATH="/mnt/ssd/tpctools/tpch-dbgen"

generate_query() {
    local SF=$1 QUERY=$2
    cd ${DBGEN_PATH}
    export DSS_QUERY=${DBGEN_PATH}/queries
    ./qgen -s ${SF} ${QUERY} > ${QUERY}.sql
}

mkdir -p "${OUT_DIR}"
echo "SF=${SF}, output=${OUT_DIR}"

for idx in $(seq 1 15); do
    for Q in $(seq 1 22); do
        mkdir -p "${OUT_DIR}/q${Q}"
        generate_query "${SF}" "${Q}" "${idx}"
        cp "${DBGEN_PATH}/${Q}.sql" "${OUT_DIR}/q${Q}/seed${idx}.sql"
        printf "%s\n" "${OUT_DIR}/q${Q}/seed${idx}.sql"
    done
    sleep 5
done

echo "Done → ${OUT_DIR}"