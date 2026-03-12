using System.Diagnostics;
using Cassandra;

/// <summary>
/// Concurrent INSERT benchmark — same INSERT as InsertBenchmark but fires
/// multiple requests in-flight simultaneously to measure throughput under
/// parallelism.
///
/// Measured operation:
///   INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)
///   with CONCURRENCY tasks running in parallel via Task.WhenAll batches.
///
/// Extra env var:
///   CONCURRENCY — number of parallel requests per batch, default 64
/// </summary>
public static class ConcurrentInsertBenchmark
{
    public static async Task Run(ISession session, int iterations)
    {
        // TODO: read CONCURRENCY from environment (default 64)

        // TODO: create/reset keyspace and table (same as InsertBenchmark)

        // TODO: prepare INSERT statement

        // TODO: warmup — fire one batch of CONCURRENCY requests, not measured

        // TODO: measured loop
        //   - divide iterations into batches of CONCURRENCY
        //   - each batch: build CONCURRENCY tasks with random ids, await Task.WhenAll
        //   - measure total wall-clock time across all batches

        // TODO: print Results block including CONCURRENCY level
        throw new NotImplementedException("TODO: implement ConcurrentInsertBenchmark");
    }
}
