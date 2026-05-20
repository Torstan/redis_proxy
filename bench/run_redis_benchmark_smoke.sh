#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
proxy_bin="${PROXY_BIN:-${build_dir}/redis_proxy}"
backend_host="${REDIS_BACKEND_HOST:-127.0.0.1}"
backend_port="${REDIS_BACKEND_PORT:-8888}"
proxy_host="${PROXY_HOST:-127.0.0.1}"
proxy_port="${PROXY_PORT:-6391}"
requests="${REQUESTS:-100000}"
clients="${CLIENTS:-50}"
pipeline="${PIPELINE:-16}"
backend_conns="${BACKEND_CONNS:-1}"
min_proxy_qps="${MIN_PROXY_QPS:-100000}"
min_direct_qps="${MIN_DIRECT_QPS:-125000}"
out_dir="${BENCH_OUT_DIR:-${build_dir}/bench-results}"

mkdir -p "${out_dir}"

if [[ ! -x "${proxy_bin}" ]]; then
  echo "missing redis_proxy binary: ${proxy_bin}" >&2
  exit 1
fi
if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "redis-benchmark not found" >&2
  exit 1
fi
if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli not found" >&2
  exit 1
fi

redis-cli -h "${backend_host}" -p "${backend_port}" PING >/dev/null

"${proxy_bin}" \
  --listen "${proxy_host}:${proxy_port}" \
  --redis "${backend_host}:${backend_port}" \
  --workers 1 \
  --backend-conns "${backend_conns}" \
  >"${out_dir}/redis-benchmark-smoke-proxy.log" 2>&1 &
proxy_pid=$!
trap 'kill "${proxy_pid}" >/dev/null 2>&1 || true' EXIT
sleep 0.5

extract_qps() {
  awk -F'[:, ]+' '/requests per second/ { print $2; exit }'
}

direct_out="${out_dir}/direct-ping-c${clients}-p${pipeline}.txt"
proxy_out="${out_dir}/proxy-ping-c${clients}-p${pipeline}.txt"

redis-benchmark -h "${backend_host}" -p "${backend_port}" \
  -n "${requests}" -c "${clients}" -P "${pipeline}" -q PING \
  | tee "${direct_out}"

redis-benchmark -h "${proxy_host}" -p "${proxy_port}" \
  -n "${requests}" -c "${clients}" -P "${pipeline}" -q PING \
  | tee "${proxy_out}"

direct_qps="$(extract_qps <"${direct_out}")"
proxy_qps="$(extract_qps <"${proxy_out}")"

awk -v qps="${direct_qps}" -v min="${min_direct_qps}" 'BEGIN { exit(qps >= min ? 0 : 1) }' || {
  echo "direct Redis qps ${direct_qps} is below ${min_direct_qps}; environment cannot prove 100k proxy target" >&2
  exit 2
}

awk -v qps="${proxy_qps}" -v min="${min_proxy_qps}" 'BEGIN { exit(qps >= min ? 0 : 1) }' || {
  echo "proxy qps ${proxy_qps} is below ${min_proxy_qps}" >&2
  exit 3
}

echo "direct_qps=${direct_qps}"
echo "proxy_qps=${proxy_qps}"
