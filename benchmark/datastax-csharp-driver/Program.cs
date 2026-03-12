using Cassandra;

/// Entry point — pick which benchmark to run via the BENCHMARK env var.
///
/// Available values for BENCHMARK:
///   insert            — sequential INSERT (matches rust-bench default)
///   select            — sequential SELECT by primary key
///   concurrent_insert — parallel INSERT with configurable concurrency
///
/// Other env vars (all benchmarks):
///   SCYLLA_URI   — contact point, default "127.0.0.1"
///   ITERATIONS   — measured iterations,  default 10_000

var benchmark  = Environment.GetEnvironmentVariable("BENCHMARK") ?? "insert";
var scyllaUri  = Environment.GetEnvironmentVariable("SCYLLA_URI") ?? "127.0.0.1";
var iterations = int.TryParse(Environment.GetEnvironmentVariable("ITERATIONS"), out var i) ? i : 10_000;

var lastColon = scyllaUri.LastIndexOf(':');
var host = lastColon >= 0 ? scyllaUri[..lastColon] : scyllaUri;
var port = lastColon >= 0 && int.TryParse(scyllaUri[(lastColon + 1)..], out var p) ? p : 9042;

Console.WriteLine($"Driver    : ScyllaDB C# Driver (official Datastax C# Driver)");
Console.WriteLine($"Benchmark : {benchmark}");
Console.WriteLine($"Connecting to {host}:{port}...");

var cluster = Cluster.Builder().AddContactPoint(host).WithPort(port).Build();
using ISession session = await cluster.ConnectAsync();

switch (benchmark)
{
    case "insert":
        await InsertBenchmark.Run(session, iterations);
        break;
    case "select":
        await SelectBenchmark.Run(session, iterations);
        break;
    case "concurrent_insert":
        await ConcurrentInsertBenchmark.Run(session, iterations);
        break;
    default:
        Console.Error.WriteLine($"Unknown benchmark '{benchmark}'. Choose: insert, select, concurrent_insert");
        Environment.Exit(1);
        break;
}

session.Dispose();
cluster.Dispose();