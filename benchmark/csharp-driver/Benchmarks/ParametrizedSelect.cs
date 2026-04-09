using Cassandra;

/// <summary>
/// Executes full-table select operations with configurable table size and concurrency.
///
/// Configuration:
///   N_ROWS: number of rows preinserted into benchmarks.basic (default: 10)
///   N_WORKERS: number of concurrent select workers (default: 1)
///   PAGE_SIZE: page size for SELECT * reads (default: 5000)
///
/// Measured operation:
///   SELECT * FROM benchmarks.basic
/// </summary>
public static class ParametrizedSelectBenchmark
{
    private const int DefaultRows = 10;
    private const int DefaultWorkers = 1;
    private const int DefaultPageSize = 5000;

    private static async Task SelectData(
        ISession session,
        PreparedStatement prepared,
        int startIndex,
        int iterations,
        int workers,
        int expectedRows,
        int pageSize)
    {
        var index = startIndex;
        while (index < iterations)
        {
            var statement = prepared.Bind().SetPageSize(pageSize);
            var result = await session.ExecuteAsync(statement);
            var rowCount = 0;
            foreach (var row in result)
            {
                _ = row.GetValue<Guid>("id");
                _ = row.GetValue<int>("val");
                rowCount++;
            }

            if (rowCount != expectedRows)
            {
                throw new InvalidOperationException(
                    $"Expected {expectedRows} selected rows, but found {rowCount}.");
            }

            index += workers;
        }
    }

    public static async Task Run(ISession session, int iterations)
    {
        var rows = Common.GetPositiveIntEnvOrDefault("N_ROWS", DefaultRows);
        var workers = Common.GetPositiveIntEnvOrDefault("N_WORKERS", DefaultWorkers);
        var pageSize = Common.GetPositiveIntEnvOrDefault("PAGE_SIZE", DefaultPageSize);

        await Common.InsertDataIntoSimpleTable(session, rows);

        var preparedSelect = await session.PrepareAsync(Common.BasicSelectQuery);

        var tasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            var startIndex = i;
            tasks[i] = SelectData(session, preparedSelect, startIndex, iterations, workers, rows, pageSize);
        }

        await Task.WhenAll(tasks);
    }
}

public static class ParametrizedSelectBenchmarkEntryPoint
{
    public static async Task Main()
    {
        var n = Common.GetCnt();
        await using var context = await Common.InitSimpleTable();
        await ParametrizedSelectBenchmark.Run(context.Session, n);
    }
}
