using System.Diagnostics;
using Cassandra;

/// <summary>
/// SELECT benchmark — read path equivalent of InsertBenchmark.
///
/// Schema (reuses table created by InsertBenchmark):
///   benchmarks.basic (id int PRIMARY KEY, value text)
///
/// Measured operation:
///   SELECT id, value FROM benchmarks.basic WHERE id = ?
///   using a randomly chosen id from the pre-inserted data set, prepared statement.
/// </summary>
public static class SelectBenchmark
{
    public static async Task Run(ISession session, int iterations)
    {
        // TODO: ensure table exists and contains data (run InsertBenchmark first, or insert
        //       a seed data set here)

        // TODO: collect existing ids to use as query parameters

        // TODO: prepare SELECT statement

        // TODO: measured loop (iterations iterations)
        //   - pick a random id from the seed set
        //   - execute prepared statement
        //   - dispose RowSet

        // TODO: print Results block (driver name, iterations, total time, mean latency µs, throughput op/s)
        throw new NotImplementedException("TODO: implement SelectBenchmark");
    }
}
