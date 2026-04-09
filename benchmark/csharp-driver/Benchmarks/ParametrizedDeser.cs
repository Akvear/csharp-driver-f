using Cassandra;

/// <summary>
/// Executes complex-row insert and full-table select operations to benchmark deserialization with configurable concurrency.
///
/// Configuration:
///   N_WORKERS: number of concurrent workers for insert and select phases (default: 1)
///   PAGE_SIZE: page size for select reads in deserialization phase (default: 5000)
/// </summary>
public static class ParametrizedDeserBenchmark
{
    private const int DefaultWorkers = 1;
    private const int DefaultPageSize = 5000;

    private static void ReadDeserRow(Row row)
    {
        _ = row.GetValue<Guid>("id");
        _ = row.GetValue<int>("val");
        _ = row.GetValue<TimeUuid>("tuuid");
        _ = row.GetValue<System.Net.IPAddress>("ip");
        _ = row.GetValue<LocalDate>("date");
        _ = row.GetValue<LocalTime>("time");
        _ = row.GetValue<Tuple<string, int>>("tuple");
        _ = row.GetValue<DeserUdt1>("udt");
        _ = row.GetValue<ISet<int>>("set1");
        _ = row.GetValue<Duration>("duration");
    }

    private static async Task InsertData(ISession session, PreparedStatement prepared, int startIndex, int iterations, int workers)
    {
        var index = startIndex;
        while (index < iterations)
        {
            await session.ExecuteAsync(prepared.Bind(Common.GetDeserData()));
            index += workers;
        }
    }

    private static async Task SelectData(
        ISession session,
        PreparedStatement prepared,
        int startIndex,
        int iterations,
        int workers,
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
                ReadDeserRow(row);
                rowCount++;
            }

            if (rowCount != iterations)
            {
                throw new InvalidOperationException(
                    $"Expected {iterations} selected rows, but found {rowCount}.");
            }

            index += workers;
        }
    }

    public static async Task Run(ISession session, int iterations)
    {
        var workers = Common.GetPositiveIntEnvOrDefault("N_WORKERS", DefaultWorkers);
        var pageSize = Common.GetPositiveIntEnvOrDefault("PAGE_SIZE", DefaultPageSize);

        Common.DefineDeserUdt(session);

        var preparedInsert = await session.PrepareAsync(Common.DeserInsertQuery);
        var insertTasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            var startIndex = i;
            insertTasks[i] = InsertData(session, preparedInsert, startIndex, iterations, workers);
        }

        await Task.WhenAll(insertTasks);

        var preparedSelect = await session.PrepareAsync(Common.BasicSelectQuery);
        var selectTasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            var startIndex = i;
            selectTasks[i] = SelectData(session, preparedSelect, startIndex, iterations, workers, pageSize);
        }

        await Task.WhenAll(selectTasks);
    }
}

public static class ParametrizedDeserBenchmarkEntryPoint
{
    public static async Task Main()
    {
        var n = Common.GetCnt();
        await using var context = await Common.InitDeserTable();
        await ParametrizedDeserBenchmark.Run(context.Session, n);
    }
}
