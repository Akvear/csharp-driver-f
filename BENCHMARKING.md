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
export SCYLLA_URI=172.45.0.2:9042
export REPOS_DIR="$HOME/Repos"
export CSHARP_REPO="~/Repos/csharp-driver"
export NODEJS_REPO="~/Repos/nodejs-rs-driver"
export SDB_MANIFEST="~/Repos/scylladb-drivers-benchmarker/Cargo.toml"
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

## Notes

- `--series` backend names must match the backend names saved by each config.
- SDB stores results with the git commit hash of the repository where `run` was executed. This is how runs from different revisions are distinguished in one DB.
- To compare specific revisions on one chart, pass refs in `--series`, for example: `--series csharp-rs@"$CSHARP_REPO":HEAD=main --series csharp-rs@"$CSHARP_REPO":<commit-sha>=candidate`.
