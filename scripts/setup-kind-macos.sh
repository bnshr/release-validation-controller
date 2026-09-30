#!/bin/bash
set -euo pipefail

cluster=release-validation
context=kind-$cluster
image=release-validation-controller:local
repo_dir=$(cd "$(dirname "$0")/.." && pwd)

if [[ $(uname -s) != Darwin ]]; then
  echo "This setup script requires macOS." >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Install Homebrew from https://brew.sh, then rerun this script." >&2
  exit 1
fi

for tool in kind kubectl docker; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    brew install "$tool"
  fi
done

if ! docker info >/dev/null 2>&1; then
  if ! command -v colima >/dev/null 2>&1; then
    brew install colima
  fi
  colima start --cpu 4 --memory 6
  if ! docker info >/dev/null 2>&1; then
    echo "Docker is still unavailable. Check Docker Desktop or Colima, then retry." >&2
    exit 1
  fi
fi

cd "$repo_dir"
if ! kind get clusters | grep -Fxq "$cluster"; then
  kind create cluster --name "$cluster" --wait 2m
fi

docker build -t "$image" .
kind load docker-image "$image" --name "$cluster"
kubectl --context "$context" apply -f config/install.yaml
kubectl --context "$context" rollout restart deployment/release-validation-controller
kubectl --context "$context" rollout status deployment/release-validation-controller --timeout=2m
kubectl --context "$context" apply -f config/example.yaml
kubectl --context "$context" wait validationrun/example-qualification --for=jsonpath='{.status.phase}'=Passed --timeout=2m
kubectl --context "$context" get validationrun example-qualification -o jsonpath='{.status.report}{"\n"}'
