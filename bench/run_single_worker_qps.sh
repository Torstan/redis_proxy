#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
redis_port="${REDIS_PORT:-6380}"
proxy_port="${PROXY_PORT:-6390}"
seconds="${SECONDS_PER_CASE:-10}"
out_dir="${BENCH_OUT_DIR:-${build_dir}/bench-results}"
mkdir -p "${out_dir}"

if ! command -v redis-server >/dev/null 2>&1; then
  echo "redis-server not found; skipping benchmark"
  exit 0
fi

redis-server --port "${redis_port}" --save "" --appendonly no >"${out_dir}/redis.log" 2>&1 &
redis_pid=$!
trap 'kill ${proxy_pid:-0} ${redis_pid:-0} >/dev/null 2>&1 || true' EXIT
sleep 0.3

"${build_dir}/redis_proxy" --listen "127.0.0.1:${proxy_port}" --redis "127.0.0.1:${redis_port}" --workers 1 --backend-conns "${BACKEND_CONNS:-1}" >"${out_dir}/proxy.log" 2>&1 &
proxy_pid=$!
sleep 0.5

csv="${out_dir}/single-worker-qps.csv"
echo "target,backend_conns,clients,pipeline,command,qps" >"${csv}"

for backend_conns in ${BENCH_BACKEND_CONNS:-1 2 4}; do
  kill "${proxy_pid}" >/dev/null 2>&1 || true
  "${build_dir}/redis_proxy" --listen "127.0.0.1:${proxy_port}" --redis "127.0.0.1:${redis_port}" --workers 1 --backend-conns "${backend_conns}" >"${out_dir}/proxy-${backend_conns}.log" 2>&1 &
  proxy_pid=$!
  sleep 0.5
  for clients in ${BENCH_CLIENTS:-1 16 64 128}; do
    for pipeline in ${BENCH_PIPELINES:-1 16 64 128}; do
      for command in ${BENCH_COMMANDS:-PING GET SET}; do
        proxy_json=$("${build_dir}/proxy_bench" --port "${proxy_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}")
        direct_json=$("${build_dir}/proxy_bench" --port "${redis_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}")
        proxy_qps=$(echo "${proxy_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')
        direct_qps=$(echo "${direct_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')
        echo "proxy,${backend_conns},${clients},${pipeline},${command},${proxy_qps}" | tee -a "${csv}"
        echo "direct,${backend_conns},${clients},${pipeline},${command},${direct_qps}" | tee -a "${csv}"
      done
    done
  done
done
