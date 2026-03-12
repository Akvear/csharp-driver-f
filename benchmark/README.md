# Driver Benchmark Suite

Compares three ScyllaDB driver implementations using a single, simple query benchmark:
**`SELECT host_id FROM system.local WHERE key='local'`**

## Drivers Under Test

| Driver | Language | Directory |
|---|---|---|
| ScyllaDB C# Driver (this project, Rust-backed) | C# | `csharp-bench/scylla-csharp/` |
| ScyllaDB C# Driver (official) | C# | `csharp-bench/datastax-csharp/` |
| ScyllaDB Rust Driver (scylla-rs) | Rust | `rust-bench/` |

## Benchmark: `SimpleQuery`

Measures steady-state latency and throughput for a single prepared `SELECT from system.local`.

- **Setup:** connect to cluster, prepare statement once
- **Measured operation:** execute the prepared statement (single row result)
- **Metric:** operations per second, mean latency, stddev

## Prerequisites

- A running ScyllaDB node reachable at `127.0.0.1:9042` (override via env var `SCYLLA_URI`)
- .NET 8 SDK (for C# benchmarks)
- Rust toolchain (for Rust benchmark)
- The native Rust wrapper library built: run `cargo build --release` inside `rust/`
  and ensure `libcsharp_wrapper.so` is on `LD_LIBRARY_PATH`

## Running the C# Benchmarks

### ScyllaDB C# Driver (Rust-backed)

```bash
cd benchmark/csharp-bench/scylla-csharp
dotnet run -c Release
```

### ScyllaDB C# Driver (official, TCP-backed)

```bash
cd benchmark/csharp-bench/datastax-csharp
dotnet run -c Release
```

Both produce BenchmarkDotNet HTML/CSV/markdown reports in `BenchmarkDotNet.Artifacts/`.

## Running the Rust Benchmark

```bash
cd benchmark/rust-bench
cargo run --release
```

Results are printed to stdout as operations/second and mean latency.

## Configuring the Contact Point

All benchmarks read the contact point from the environment:

| Variable | Default |
|---|---|
| `SCYLLA_URI` | `127.0.0.1:9042` |

Set it before running:

```bash
export SCYLLA_URI=192.168.1.10:9042
dotnet run -c Release
```

## Result Comparison

Collect the `Mean` and `Op/s` columns from each run and compare side by side.
Example table (fill in with real numbers):

| Driver | Mean latency | Throughput (op/s) |
|---|---|---|
| ScyllaDB C# (Rust-backed) | ? ms | ? |
| ScyllaDB C# (official) | ? ms | ? |
| scylla-rs (Rust) | ? ms | ? |
