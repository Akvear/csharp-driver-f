# Benchmarking With SDB

This directory contains backend and benchmark configuration files for
[ScyllaDB Drivers Benchmarker (SDB)](https://github.com/scylladb-drivers-benchmarker/scylladb-drivers-benchmarker).

This README only explains the local run + plot flow.
For full explanation and advanced options, see the SDB repository docs.

## Prerequisites

- `scylladb-drivers-benchmarker` cloned in the same parent directory as this repo
- A running ScyllaDB cluster reachable via `SCYLLA_URI`
- Driver-specific runtime/build dependencies (dotnet, cargo, node, npm)

## Environment Variables

Benchmark binaries read configuration from environment variables.
Some are set directly by your shell, others are injected by backend config files in
`benchmark/runner-config/*/config.yml` via `run-command: env ...`.

- `SCYLLA_URI`: ScyllaDB contact point used by benchmark code.
  Default in code when unset: `172.47.0.2`.
- `CNT`: Number of benchmark iterations.
  This is set by SDB for each benchmark step and passed through `run-csharp-benchmark.sh`.
- `N_WORKERS`: Number of concurrent workers.
  Used by: `insert`, `select`, `ser`, `deser` (and their concurrent variants).
  Default in code: `1`.
- `N_ROWS`: Number of rows preinserted for select benchmarks.
  Used by: `select`, `large_select` (and their concurrent variants).
  Default in code: `10`.
- `PAGE_SIZE`: Query page size.
  Used by: `select`, `large_select`, `deser`, and `paging`.
  Defaults in code: `5000` for `select`/`deser`, `1` for `paging`.

## 1) Run Insert Benchmark Into A Fresh DB

Create a new database file and run `insert` for each backend config.

```bash
export SCYLLA_URI=172.46.0.2:9042
export REPOS_DIR="$HOME/repos"
export CSHARP_REPO="$REPOS_DIR/csharp-driver"
export NODEJS_REPO="$REPOS_DIR/nodejs-rs-driver"
export SDB_MANIFEST="$REPOS_DIR/scylladb-drivers-benchmarker/Cargo.toml"
export DB="$CSHARP_REPO/benchmark/results/results-database.db"
```

Run from `csharp-driver`:

```bash
cd "$CSHARP_REPO"

cargo run --manifest-path "$SDB_MANIFEST" -- \
  -d "$DB" run \
  -B benchmark/runner-config/datastax/config.yml \
  -b benchmark/runner-config/config.yml \
  insert time

cargo run --manifest-path "$SDB_MANIFEST" -- \
  -d "$DB" run \
  -B benchmark/runner-config/csharp-rs/config.yml \
  -b benchmark/runner-config/config.yml \
  -M force-rerun insert time
```

Run from `nodejs-rs-driver` (example backend):

```bash
cd "$NODEJS_REPO"

cargo run --manifest-path "$SDB_MANIFEST" -- \
  -d "$DB" run \
  -B benchmark/runner-config/scylladb-driver/config.yml \
  -b benchmark/runner-config/config.yml \
  insert time
```

Repeat the same `run` command pattern for any additional backend config.

## 2) Plot Insert Comparison

Use `plot insert` with one `--series` per backend name stored in the DB.

```bash
cd "$CSHARP_REPO"

cargo run --manifest-path "$SDB_MANIFEST" -- \
  -d "$DB" plot insert \
  -b benchmark/runner-config/config.yml \
  --series datastax@"$CSHARP_REPO":HEAD=datastax \
  --series csharp-rs@"$CSHARP_REPO":HEAD=csharp-rs \
  --series scylladb-driver@"$NODEJS_REPO":HEAD=scylladb-nodejs-rs \
  -o benchmark/results/insert-compare.svg \
  series
```

Output plot: `benchmark/results/insert-compare.svg`

## 3) Generate Flamegraphs

This requires some extra setup:

- Clone [FlameGraph](https://github.com/brendangregg/FlameGraph) to a known location
- Set `perf_event_paranoid` to `-1` to allow perf profiling:
  ```bash
  sudo sysctl kernel.perf_event_paranoid=-1
  ```
- You will need to install dotnet directly from Microsoft
  ```bash
mkdir -p "$HOME/dotnet-ms"
curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --version latest --channel 9.0 --install-dir "$HOME/dotnet-ms"
curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --runtime dotnet --channel 8.0 --install-dir "$HOME/dotnet-ms"

MS_RUNTIME_DIR=$(dirname $(find $HOME/dotnet-ms/ -name "libcoreclr.so" | grep "8.0"))

dotnet tool install --global dotnet-symbol
dotnet symbol --symbols --output "$MS_RUNTIME_DIR" \
    "$MS_RUNTIME_DIR/libcoreclr.so" \
    "$MS_RUNTIME_DIR/libclrjit.so"
  ```

You can use this example flame graph generation script:
```bash
#!/bin/bash
set -euo pipefail

: "${BENCH:=ParametrizedSelect}"
: "${SCYLLA_URI=172.47.0.2:9042}"
: "${CNT:=1000}"
: "${N_WORKERS:=1}"
: "${N_ROWS:=1000}"
: "${PAGE_SIZE:=1000}"
: "${FLAMEGRAPH_DIR:=$HOME/repos/FlameGraph}"

DOTNET_BIN="$HOME/dotnet-ms/dotnet"
SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/results/flamegraph"
ENTRY_POINT="${BENCH}BenchmarkEntryPoint"
OUTPUT_NAME="flamegraph-${BENCH}"
PROJECT_PATH="$SCRIPT_DIR/csharp-driver/csharp-rs/ScyllaCSharpBench.csproj"
BUILD_OUTPUT_DIR="$SCRIPT_DIR/csharp-driver/csharp-rs/bin/Release/$ENTRY_POINT/net8.0"

mkdir -p "$OUTPUT_DIR"

$DOTNET_BIN build "$PROJECT_PATH" -c Release \
    -p:BenchmarkEntryPoint="$ENTRY_POINT" \
    -p:DebugSymbols=true \
    -p:DebugType=full

sudo \
    SCYLLA_URI="$SCYLLA_URI" \
    CNT="$CNT" \
    N_WORKERS="$N_WORKERS" \
    N_ROWS="$N_ROWS" \
    PAGE_SIZE="$PAGE_SIZE" \
    DOTNET_PerfMapEnabled=1 \
    DOTNET_PerfMapIgnoreSingleFileMapping=1 \
    DOTNET_EnableWriteXorExecute=0 \
    DOTNET_PreserveFramePointer=1 \
    perf record -F 1000 -g -- "$DOTNET_BIN" "$BUILD_OUTPUT_DIR/ScyllaCSharpBench.dll"

sudo perf script > "$OUTPUT_DIR/out.perf"

"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$OUTPUT_DIR/out.perf" > "$OUTPUT_DIR/out.folded"
"$FLAMEGRAPH_DIR/flamegraph.pl" --width 1000 "$OUTPUT_DIR/out.folded" > "$OUTPUT_DIR/$OUTPUT_NAME.svg"

cp "$OUTPUT_DIR/out.folded" "$OUTPUT_DIR/$OUTPUT_NAME.speedscope.folded"
```
The flamegraph is in the .svg file.
Alternatively you can use speedscope.folded files in [Speedscope](https://www.speedscope.app/).
This script is designed to be placed in csharp-driver/benchmark

## Notes

- `--series` backend names must match the backend names saved by each config.
- SDB stores results with the git commit hash of the repository where `run` was executed. This is how runs from different revisions are distinguished in one DB.
- To compare specific revisions on one chart, pass refs in `--series`, for example: `--series csharp-rs@"$CSHARP_REPO":HEAD=main --series csharp-rs@"$CSHARP_REPO":<commit-sha>=candidate`.
