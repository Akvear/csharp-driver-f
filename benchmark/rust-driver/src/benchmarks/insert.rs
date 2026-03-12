/// INSERT benchmark.
///
/// Schema:
///   CREATE KEYSPACE IF NOT EXISTS benchmarks ...
///   CREATE TABLE benchmarks.basic (id int PRIMARY KEY, value text)
///
/// Measured operation:
///   INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)
///   with a random i32 id and a 100-byte text value, prepared statement.
use std::time::Instant;

use scylla::client::session::Session;
use uuid::Uuid;

pub async fn run(
    session: &Session,
    iterations: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    session.query_unpaged(
        "CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = \
         {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
        &[],
    ).await?;
    session.query_unpaged("DROP TABLE IF EXISTS benchmarks.basic", &[]).await?;
    session.query_unpaged(
        "CREATE TABLE benchmarks.basic (id int PRIMARY KEY, value text)",
        &[],
    ).await?;

    let prepared = session
        .prepare("INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)")
        .await?;

    // Measured run.
    println!("Running benchmark ({iterations} iterations)...");
    let start = Instant::now();
    for _ in 0..iterations {
        let id = Uuid::new_v4().as_u128() as i32;
        let value = "x".repeat(100);
        session.execute_unpaged(&prepared, &(id, value)).await?;
    }
    let elapsed = start.elapsed();

    let mean_us = elapsed.as_micros() as f64 / iterations as f64;
    let throughput = iterations as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("Driver       : scylla-rs (official Rust driver)");
    println!("Iterations   : {iterations}");
    println!("Total time   : {:.3} s", elapsed.as_secs_f64());
    println!("Mean latency : {mean_us:.1} µs");
    println!("Throughput   : {throughput:.0} op/s");

    Ok(())
}
