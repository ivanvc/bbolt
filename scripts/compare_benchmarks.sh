#!/usr/bin/env bash
# https://github.com/kubernetes/kube-state-metrics/blob/main/tests/compare_benchmarks.sh (originally written by mxinden)

# exit immediately when a command fails
set -e
# only exit with zero if all commands of the pipeline exit successfully
set -o pipefail
# error on unset variables
set -u

[[ "$#" -eq 1 ]] || echo "One argument required, $# provided."

REF_CURRENT="$(git rev-parse --abbrev-ref HEAD)"
REF_TO_COMPARE=$1

RESULT_CURRENT="$(mktemp)-${REF_CURRENT}"
RESULT_TO_COMPARE="$(mktemp)-${REF_TO_COMPARE}"

TIMEOUT=${TIMEOUT:-30m}
BENCH_COUNT=${BENCH_COUNT:-3}
BENCHSTAT_CONFIDENCE_LEVEL=${BENCHSTAT_CONFIDENCE_LEVEL:-0.75}
BENCHSTAT_FORMAT=${BENCHSTAT_FORMAT:-"text"}

if [[ "${BENCHSTAT_FORMAT}" == "csv" ]] && [[ -z "${BENCHSTAT_OUTPUT_FILE}" ]]; then
  echo "BENCHSTAT_FORMAT is set to csv, but BENCHSTAT_OUTPUT_FILE is not set."
  exit 1
fi

echo ""
echo "### Testing ${REF_CURRENT}"

go test -timeout="${TIMEOUT}" -count="${BENCH_COUNT}" -benchmem -run=NONE -bench=. ./... | tee "${RESULT_CURRENT}"

# Filter benchark lines, so benchstat can parse the output.
grep ^Benchmark "${RESULT_CURRENT}" > "${RESULT_CURRENT}".tmp && mv "${RESULT_CURRENT}".tmp "${RESULT_CURRENT}"

echo ""
echo "### Done testing ${REF_CURRENT}"

echo ""
echo "### Testing ${REF_TO_COMPARE}"

git checkout "${REF_TO_COMPARE}"

go test -timeout="${TIMEOUT}" -count="${BENCH_COUNT}" -benchmem -run=NONE -bench=. ./... | tee "${RESULT_TO_COMPARE}"

# Filter benchark lines, so benchstat can parse the output.
grep ^Benchmark "${RESULT_TO_COMPARE}" > "${RESULT_TO_COMPARE}".tmp && mv "${RESULT_TO_COMPARE}".tmp "${RESULT_TO_COMPARE}"

echo ""
echo "### Done testing ${REF_TO_COMPARE}"

git checkout -

echo ""
echo "### Result"
echo "old=${REF_TO_COMPARE} new=${REF_CURRENT}"

if [[ "${BENCHSTAT_FORMAT}" == "csv" ]]; then
  benchstat -format=csv -confidence="${BENCHSTAT_CONFIDENCE_LEVEL}" BASE="${RESULT_TO_COMPARE}" HEAD="${RESULT_CURRENT}" 2>/dev/null 1>"${BENCHSTAT_OUTPUT_FILE}"
else
  benchstat -confidence="${BENCHSTAT_CONFIDENCE_LEVEL}" BASE="${RESULT_TO_COMPARE}" HEAD="${RESULT_CURRENT}"
fi
