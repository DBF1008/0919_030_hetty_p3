#!/usr/bin/env bash
#
# Runs all unit tests with the race detector enabled.
#
# Usage:
#   ./test.sh           # run all unit tests
#   ./test.sh ./pkg/... # run tests for specific packages only

set -euo pipefail

cd "$(dirname "$0")"

PACKAGES="${@:-./...}"

echo "==> go vet ${PACKAGES}"
go vet ${PACKAGES}

echo "==> go test -race -count=1 ${PACKAGES}"
go test -race -count=1 ${PACKAGES}

echo "==> all tests passed"
