using Cassandra;

/// <summary>
/// Executes complex-row insert operations to benchmark serialization with configurable concurrency.
///
/// Configuration:
///   N_WORKERS: number of concurrent insert workers (default: 1)
/// </summary>
public static class ParametrizedSerBenchmark
{
    private const int DefaultWorkers = 1;

    private static async Task InsertData(ISession session, PreparedStatement prepared, int startIndex, int iterations, int workers)
    {
        var index = startIndex;
        while (index < iterations)
        {
            await session.ExecuteAsync(prepared.Bind(Common.GetDeserData()));
            index += workers;
        }
    }

    public static async Task Run(ISession session, int iterations)
    {
        var workers = Common.GetPositiveIntEnvOrDefault("N_WORKERS", DefaultWorkers);
        var totalInserts = iterations * iterations;

        Common.DefineDeserUdt(session);

        var sharedPrepared = await session.PrepareAsync(Common.DeserInsertQuery);
        var tasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            var startIndex = i;
            tasks[i] = InsertData(session, sharedPrepared, startIndex, totalInserts, workers);
        }

        await Task.WhenAll(tasks);
        await Common.CheckRowCnt(session, totalInserts);
    }
}

public static class ParametrizedSerBenchmarkEntryPoint
{
    public static async Task Main()
    {
        var n = Common.GetCnt();
        await using var context = await Common.InitDeserTable();
        await ParametrizedSerBenchmark.Run(context.Session, n);
    }
}
