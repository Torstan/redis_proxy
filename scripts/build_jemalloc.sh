#!/usr/bin/env bash
set -euo pipefail

repo_root="$1"
build_root="$2"
src_dir="${repo_root}/thirdparty/jemalloc"
prefix="${build_root}/jemalloc"
stamp="${prefix}/.built"

if [[ -f "${stamp}" && -f "${prefix}/lib/libjemalloc.a" ]]; then
  exit 0
fi

mkdir -p "${prefix}"
cd "${src_dir}"

if [[ ! -x ./configure ]]; then
  ./autogen.sh
fi

./configure --prefix="${prefix}" --disable-shared --enable-static
make -j"$(nproc)"
make install
touch "${stamp}"
