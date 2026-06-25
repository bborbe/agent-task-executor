#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

THIS_PKG="github.com/bborbe/agent/task/executor"

# Locate kube_codegen.sh in the Go module cache (shell scripts are not vendored by go mod vendor).
CODEGEN_PKG=$(cd "${SCRIPT_ROOT}" && go list -m -f '{{.Dir}}' k8s.io/code-generator 2>/dev/null || echo "")

if [[ -z "${CODEGEN_PKG}" ]]; then
    echo "k8s.io/code-generator not found in go.mod. Run: go get k8s.io/code-generator"
    exit 1
fi

source "${CODEGEN_PKG}/kube_codegen.sh"

kube::codegen::gen_helpers \
    --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    "${SCRIPT_ROOT}/k8s/apis"

kube::codegen::gen_client \
    --with-watch \
    --with-applyconfig \
    --output-dir "${SCRIPT_ROOT}/k8s/client" \
    --output-pkg "${THIS_PKG}/k8s/client" \
    --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    "${SCRIPT_ROOT}/k8s/apis"
