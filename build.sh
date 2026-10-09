#!/usr/bin/env bash
set -euo pipefail

REGISTRY="10.158.1.10:32000"
IMAGE="bloomfaas-operation"
TAG=$(uuidgen | cut -d- -f1)

IMAGE_FULL="$REGISTRY/$IMAGE:$TAG"

echo "▶ Building image: $IMAGE_FULL"
podman build -t "$IMAGE_FULL" .

echo "▶ Pushing image"
podman push --tls-verify=false "$IMAGE_FULL"

echo "▶ Uploaded success: $IMAGE_FULL"

echo "▶ Update image tag in workflow.yaml"
sed -i "s|\($IMAGE:\)[a-zA-Z0-9._-]*|\1$TAG|" deploy.yaml

echo "▶ Redeploy workload"

kubectl apply -f deploy.yaml

echo "Done"
