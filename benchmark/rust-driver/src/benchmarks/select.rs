/// SELECT benchmark — read path equivalent of insert.rs.
///
/// Schema (reuses table from insert benchmark):
///   benchmarks.basic (id int PRIMARY KEY, value text)
///
/// Measured operation:
///   SELECT id, value FROM benchmarks.basic WHERE id = ?
///   using a randomly chosen id from a pre-seeded data set, prepared statement.
use scylla::client::session::Session;

pub async fn run(
    session: &Session,
    iterations: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: ensure table exists and has seed data (run insert benchmark first, or seed here)

    // TODO: collect/store seed ids to use as query parameters (e.g. Vec<i32>)

    // TODO: prepare SELECT statement

    // TODO: warmup loop (warmup iterations, not measured)

    // TODO: measured loop (iterations iterations)
    //   - pick a random id from the seed set
    //   - session.execute_unpaged(&prepared, &(id,)).await?

    // TODO: print Results block (driver name, iterations, total time, mean latency µs, throughput op/s)
    todo!("implement select benchmark")
}
