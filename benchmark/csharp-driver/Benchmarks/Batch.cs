using Cassandra;

/// <summary>
/// Executes logged batch insert operations in sequence.
/// </summary>
public static class BatchBenchmark
{
    // Empirically determined max batch size, that doesn't cause database error.
    private const int Step = 3971;

    public static async Task Run(ISession session, int iterations)
    {
        var prepared = await session.PrepareAsync(Common.BasicInsertQuery);

        var chunks = (iterations + Step - 1) / Step;
        for (var i = 0; i < chunks; i++)
        {
            var cLen = Math.Min(iterations - (i * Step), Step);

            var batch = new BatchStatement()
                .SetBatchType(BatchType.Logged);

            for (var j = 0; j < cLen; j++)
            {
                batch.Add(prepared.Bind(Guid.NewGuid(), 1));
            }

            await session.ExecuteAsync(batch);
        }

        await Common.CheckRowCnt(session, iterations);
    }
}

public static class BatchBenchmarkEntryPoint
{
    public static async Task Main()
    {
#if RUST_BACKED_DRIVER
    throw new NotSupportedException("Batches are not yet supported by the csharp-rs driver.");
#endif
        var n = Common.GetCnt();
        await using var context = await Common.InitSimpleTable();
        await BatchBenchmark.Run(context.Session, n);
    }
}
