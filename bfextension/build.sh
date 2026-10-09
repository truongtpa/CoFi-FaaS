#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update && sudo apt-get install -y build-essential && \
if ! command -v cargo &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    source "$HOME/.cargo/env"
fi

cargo duckdb-ext-build -- --release

cp target/release/bfextension.duckdb_extension ../operations/extention