#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
redis_port="${REDIS_PORT:-6380}"
proxy_port="${PROXY_PORT:-6390}"
seconds="${SECONDS_PER_CASE:-10}"
out_dir="${BENCH_OUT_DIR:-${build_dir}/bench-results}"

if [[ "${USE_REDIS_BENCHMARK_SMOKE:-0}" == "1" ]]; then
  smoke_env=(
    BUILD_DIR="${build_dir}"
    REDIS_BACKEND_PORT="${REDIS_PORT:-8888}"
    BACKEND_CONNS="${BACKEND_CONNS:-1}"
  )
  if [[ -v PROXY_PORT ]]; then
    smoke_env+=(PROXY_PORT="${PROXY_PORT}")
  fi
  env "${smoke_env[@]}" bench/run_redis_benchmark_smoke.sh
  exit $?
fi

mkdir -p "${out_dir}"

if ! command -v redis-server >/dev/null 2>&1; then
  echo "redis-server not found; skipping benchmark"
  exit 0
fi

# Clean up any existing processes on these ports
echo "Cleaning up existing processes..."
fuser -k "${redis_port}/tcp" "${proxy_port}/tcp" 2>/dev/null || true
sleep 1

redis-server --port "${redis_port}" --save "" --appendonly no >"${out_dir}/redis.log" 2>&1 &
redis_pid=$!
trap 'kill ${proxy_pid:-0} ${redis_pid:-0} >/dev/null 2>&1 || true' EXIT

# Wait for redis-server to be ready
echo "Waiting for redis-server to start..."
for i in {1..30}; do
  if timeout 1 bash -c "echo > /dev/tcp/127.0.0.1/${redis_port}" 2>/dev/null; then
    echo "redis-server is ready"
    break
  fi
  sleep 0.1
done

backend_conns="${BACKEND_CONNS:-4}"
"${build_dir}/redis_proxy" --listen "127.0.0.1:${proxy_port}" --redis "127.0.0.1:${redis_port}" --workers 1 --backend-conns "${backend_conns}" >"${out_dir}/proxy.log" 2>&1 &
proxy_pid=$!

# Wait for redis_proxy to be ready
echo "Waiting for redis_proxy to start..."
for i in {1..30}; do
  if timeout 1 bash -c "echo > /dev/tcp/127.0.0.1/${proxy_port}" 2>/dev/null; then
    echo "redis_proxy is ready"
    break
  fi
  sleep 0.1
done

# Verify proxy is actually working with a real test
echo "Verifying redis_proxy is responding..."
if ! "${build_dir}/proxy_bench" --port "${proxy_port}" --clients 1 --pipeline 1 --seconds 1 --command PING >/dev/null 2>&1; then
  echo "ERROR: redis_proxy failed to start or is not responding"
  cat "${out_dir}/proxy.log"
  exit 1
fi
echo "redis_proxy verification passed"

csv="${out_dir}/single-worker-qps.csv"
echo "target,clients,pipeline,command,qps" >"${csv}"

echo "Running benchmark with backend_conns=${backend_conns}"

for clients in ${BENCH_CLIENTS:-16 64 128}; do
  for pipeline in ${BENCH_PIPELINES:-1 16 64 128}; do
    for command in ${BENCH_COMMANDS:-PING GET SET}; do
      # Run proxy benchmark with retry logic
      proxy_qps=0
      for attempt in 1 2; do
        proxy_json=$("${build_dir}/proxy_bench" --port "${proxy_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}" 2>&1)
        proxy_qps=$(echo "${proxy_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')

        # If QPS is 0 or empty, retry after a delay
        if [[ "$proxy_qps" == "0" ]] || [[ -z "$proxy_qps" ]]; then
          if [[ $attempt -eq 1 ]]; then
            echo "Warning: proxy test failed (clients=${clients}, pipeline=${pipeline}, command=${command}), retrying..."
            sleep 2
          else
            echo "Error: proxy test failed after retry"
            proxy_qps=0
          fi
        else
          break
        fi
      done

      # Delay between proxy and direct tests
      sleep 1

      # Run direct benchmark
      direct_json=$("${build_dir}/proxy_bench" --port "${redis_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}")
      direct_qps=$(echo "${direct_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')

      echo "proxy,${clients},${pipeline},${command},${proxy_qps}" | tee -a "${csv}"
      echo "direct,${clients},${pipeline},${command},${direct_qps}" | tee -a "${csv}"

      # Delay before next test case
      sleep 1
    done
  done
done
