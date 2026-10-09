#!/bin/bash

REGISTRY="10.158.1.10:32000"
IMAGE="python-3.11-knative-based"
TAG="alpine"

podman build -t $REGISTRY/$IMAGE:$TAG -f Dockerfile .
podman push --tls-verify=false $REGISTRY/$IMAGE:$TAG

echo "Uploaded success: $REGISTRY/$IMAGE:$TAG"