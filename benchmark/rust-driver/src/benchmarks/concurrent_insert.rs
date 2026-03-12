/// Concurrent INSERT benchmark — same INSERT as insert.rs but issues multiple
/// requests concurrently to saturate throughput.
///
/// Measured operation:
///   INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)
///   with CONCURRENCY futures joined via futures::future::join_all per batch.
///
/// Extra env var:
///   CONCURRENCY — number of parallel in-flight requests per batch, default 64
use scylla::client::session::Session;

pub async fn run(
    session: &Session,
    iterations: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: read CONCURRENCY from environment (default 64)

    // TODO: create/reset keyspace and table (same as insert.rs)

    // TODO: prepare INSERT statement

    // TODO: warmup — fire one batch of CONCURRENCY futures, not measured

    // TODO: measured loop
    //   - divide iterations into batches of CONCURRENCY
    //   - each batch: build CONCURRENCY futures, join_all, await
    //   - measure total wall-clock time across all batches

    // TODO: print Results block including CONCURRENCY level
    todo!("implement concurrent_insert benchmark")
}
