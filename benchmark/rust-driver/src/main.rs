/// Simple throughput / latency benchmark for the official ScyllaDB Rust driver.
///
/// Benchmark: execute `SELECT host_id FROM system.local WHERE key='local'`
/// (prepared statement) `ITERATIONS` times sequentially, then report:
///   - total time
///   - mean latency per query
///   - throughput (operations per second)
///
/// Usage:
///   cargo run --release
///
/// Configuration via environment variables:
///   SCYLLA_URI   — contact point, default "172.42.0.2:9042"
///   ITERATIONS   — number of measured iterations, default 10_000
use std::env;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

mod benchmarks;

const DEFAULT_URI: &str = "172.42.0.2:9042";
const DEFAULT_ITERATIONS: u64 = 10_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| DEFAULT_URI.to_string());
    let benchmark = env::var("BENCHMARK").unwrap_or_else(|_| "insert".to_string());
    let iterations: u64 = env::var("ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS);

    println!("Driver    : scylla-rs (official Rust driver)");
    println!("Benchmark : {benchmark}");
    println!("Connecting to {uri}...");
    let session: Session = SessionBuilder::new().known_node(&uri).build().await?;

    match benchmark.as_str() {
        "insert" => benchmarks::insert::run(&session, iterations).await?,
        // TODO: wire up benchmarks::select::run and benchmarks::concurrent_insert::run
        "select" => todo!("select benchmark not yet implemented"),
        "concurrent_insert" => todo!("concurrent_insert benchmark not yet implemented"),
        other => {
            eprintln!("Unknown benchmark '{other}'. Choose: insert, select, concurrent_insert");
            std::process::exit(1);
        }
    }

    Ok(())
}
