using Cassandra;

/// <summary>
/// Executes insert operations with configurable concurrency.
///
/// Configuration:
///   N_WORKERS: number of concurrent insert workers (default: 1)
///
/// Measured operation:
///   INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)
/// </summary>
public static class ParametrizedInsertBenchmark
{
    private const int DefaultWorkers = 1;

    private static async Task InsertData(ISession session, PreparedStatement prepared, int startIndex, int iterations, int workers)
    {
        var index = startIndex;
        while (index < iterations)
        {
            await session.ExecuteAsync(prepared.Bind(Guid.NewGuid(), 100));
            index += workers;
        }
    }

    public static async Task Run(ISession session, int iterations)
    {
        var workers = Common.GetPositiveIntEnvOrDefault("N_WORKERS", DefaultWorkers);

        var sharedPrepared = await session.PrepareAsync(Common.BasicInsertQuery);
        var tasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            var startIndex = i;
            tasks[i] = InsertData(session, sharedPrepared, startIndex, iterations, workers);
        }

        await Task.WhenAll(tasks);
        await Common.CheckRowCnt(session, iterations);
    }
}

public static class ParametrizedInsertBenchmarkEntryPoint
{
    public static async Task Main()
    {
        var n = Common.GetCnt();
        await using var context = await Common.InitSimpleTable();
        await ParametrizedInsertBenchmark.Run(context.Session, n);
    }
}
