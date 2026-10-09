#!/usr/bin/env bash

apt update
apt install -y podman
export DOCKER_HOST=unix:///run/podman/podman.sock

yes | apt install python3-pip
pip3 install -r benchmarks/requirements.txt
pip install --upgrade requests urllib3 chardet