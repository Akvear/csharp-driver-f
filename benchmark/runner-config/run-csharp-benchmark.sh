#!/usr/bin/env bash
# Wrapper on the command for benchmark runner.
# Usage: run-csharp-benchmark.sh <driver> <entry-point> <N>
set -euo pipefail

DRIVER="${1:-}"
ENTRY_POINT="${2:-}"
N="${3:-}"

if [[ -z "$DRIVER" || -z "$ENTRY_POINT" || -z "$N" ]]; then
  echo "Usage: $0 <driver> <entry-point> <N>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../.."

case "$DRIVER" in
  datastax)
    PROJECT_PATH="$REPO_ROOT/benchmark/csharp-driver/datastax/DatastaxCSharpBench.csproj"
    ;;
  csharp-rs)
    PROJECT_PATH="$REPO_ROOT/benchmark/csharp-driver/csharp-rs/ScyllaCSharpBench.csproj"
    ;;
  *)
    echo "Unsupported driver '$DRIVER'. Expected one of: datastax, csharp-rs" >&2
    exit 1
    ;;
esac

if [[ ! -f "$PROJECT_PATH" ]]; then
  echo "Benchmark project not found at '$PROJECT_PATH'." >&2
  exit 1
fi

CNT="$N" dotnet run --no-build -c Release --project "$PROJECT_PATH" -p:BenchmarkEntryPoint="$ENTRY_POINT"
