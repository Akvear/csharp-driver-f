using System.Diagnostics;
using Cassandra;

/// <summary>
/// INSERT benchmark — mirrors the Rust bench in rust-bench/src/benchmarks/insert.rs.
///
/// Schema:
///   CREATE KEYSPACE IF NOT EXISTS benchmarks ...
///   CREATE TABLE benchmarks.basic (id int PRIMARY KEY, value text)
///
/// Measured operation:
///   INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)
///   with a random int id and a 100-byte text value, using a prepared statement.
/// </summary>
public static class InsertBenchmark
{
    public static async Task Run(ISession session, int iterations)
    {
        await session.ExecuteAsync(new SimpleStatement(
            "CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = " +
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"));
        await session.ExecuteAsync(new SimpleStatement(
            "DROP TABLE IF EXISTS benchmarks.basic"));
        await session.ExecuteAsync(new SimpleStatement(
            "CREATE TABLE benchmarks.basic (id int PRIMARY KEY, value text)"));

        var prepared = await session.PrepareAsync(
            "INSERT INTO benchmarks.basic (id, value) VALUES (?, ?)");

        var rng = Random.Shared;
        var value = new string('x', 100);

        // Measured run.
        Console.WriteLine($"Running benchmark ({iterations} iterations)...");
        var sw = Stopwatch.StartNew();
        for (int n = 0; n < iterations; n++)
        {
            await session.ExecuteAsync(prepared.Bind(rng.Next(), value));
        }
        sw.Stop();

        var meanUs     = sw.Elapsed.TotalMicroseconds / iterations;
        var throughput = iterations / sw.Elapsed.TotalSeconds;

        Console.WriteLine();
        Console.WriteLine("=== Results ===");
        Console.WriteLine("Driver       : ScyllaDB C# Driver (official)");
        Console.WriteLine($"Iterations   : {iterations}");
        Console.WriteLine($"Total time   : {sw.Elapsed.TotalSeconds:F3} s");
        Console.WriteLine($"Mean latency : {meanUs:F1} µs");
        Console.WriteLine($"Throughput   : {throughput:F0} op/s");
    }
}
