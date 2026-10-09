#!/bin/bash

ROOT=/mnt/ssd
sudo-g5k chmod -R 777 $ROOT
cd $ROOT
git clone https://github.com/truongtpa/tpctools
cd tpctools
git clone https://github.com/electrum/tpch-dbgen
cd tpch-dbgen
make
curl https://sh.rustup.rs -sSf | sh