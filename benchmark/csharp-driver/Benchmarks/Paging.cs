using Cassandra;

/// <summary>
/// Executes paged full-table reads in sequence.
///
/// Configuration:
///   PAGE_SIZE: page size for paged reads (default: 1)
/// </summary>
public static class PagingBenchmark
{
	private const int DefaultPageSize = 1;

	public static async Task Run(ISession session, int iterations)
	{
		var pageSize = Common.GetPositiveIntEnvOrDefault("PAGE_SIZE", DefaultPageSize);

		await Common.InsertDataIntoSimpleTable(session, 500);

		var preparedSelect = await session.PrepareAsync(Common.BasicSelectQuery);

		for (var index = 0; index < iterations; index++)
		{
			byte[]? pagingState = null;
			var sum = 0;

			while (true)
			{
				var statement = preparedSelect.Bind().SetAutoPage(false).SetPageSize(pageSize);
				if (pagingState is { Length: > 0 })
				{
					statement = statement.SetPagingState(pagingState);
				}

				var result = await session.ExecuteAsync(statement);
				foreach (var row in result)
				{
					sum += row.GetValue<int>("val");
				}

				if (result.PagingState == null || result.PagingState.Length == 0)
				{
					break;
				}

				pagingState = result.PagingState;
			}

			if (sum != 500)
			{
				throw new InvalidOperationException($"Expected sum 500, but found {sum}.");
			}
		}
	}
}

public static class PagingBenchmarkEntryPoint
{
	public static async Task Main()
	{
#if RUST_BACKED_DRIVER
	throw new NotSupportedException("Paging is not supported yet in the csharp-rs driver.");
#endif
		var n = Common.GetCnt();
		await using var context = await Common.InitSimpleTable();
		await PagingBenchmark.Run(context.Session, n);
	}
}
