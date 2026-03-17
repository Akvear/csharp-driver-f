using Cassandra;
using System.Net;

/// <summary>
/// Holds initialized cluster and session objects used by benchmark scenarios.
/// </summary>
public sealed class SessionContext : IAsyncDisposable
{
    public SessionContext(ICluster cluster, ISession session)
    {
        Cluster = cluster;
        Session = session;
    }

    public ICluster Cluster { get; }
    public ISession Session { get; }

#if RUST_BACKED_DRIVER
    public async ValueTask DisposeAsync()
    {
        await Session.ShutdownAsync();
        Cluster.Dispose();
    }
#else
    public ValueTask DisposeAsync()
    {
        Session.Dispose();
        Cluster.Dispose();
        return ValueTask.CompletedTask;
    }
#endif
}

public sealed class DeserUdt1
{
    public string Field1 { get; init; } = string.Empty;

    public int Field2 { get; init; }
}

public static class Common
{
    public const string BasicInsertQuery = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)";

    public const string BasicSelectQuery = "SELECT * FROM benchmarks.basic";

    public const string DeserInsertQuery =
        "INSERT INTO benchmarks.basic (id, val, tuuid, ip, date, time, tuple, udt, set1, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// <summary>
    /// Reads the number of benchmark iterations from the CNT environment variable.
    /// </summary>
    /// <returns>Parsed CNT value.</returns>
    public static int GetCnt()
    {
        return Environment.GetEnvironmentVariable("CNT")
            is { } raw && int.TryParse(raw, out var cnt)
            ? cnt
            : throw new InvalidOperationException("CNT parameter is required.");
    }

    /// <summary>
    /// Reads a positive integer benchmark parameter from an environment variable or returns a default value.
    /// </summary>
    /// <param name="name">Environment variable name.</param>
    /// <param name="defaultValue">Fallback value when the variable is not set.</param>
    public static int GetPositiveIntEnvOrDefault(string name, int defaultValue)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (raw is null)
        {
            return defaultValue;
        }

        if (int.TryParse(raw, out var value) && value > 0)
        {
            return value;
        }

        throw new InvalidOperationException($"{name} must be a positive integer, got '{raw}'.");
    }

    /// <summary>
    /// Initializes the basic benchmark table.
    /// </summary>
    /// <returns>A ready-to-use benchmark session context.</returns>
    public static async Task<SessionContext> InitSimpleTable()
    {
        return await InitCommon("CREATE TABLE benchmarks.basic (id uuid PRIMARY KEY, val int)");
    }

    /// <summary>
    /// Initializes the complex benchmark table used by serialization and deserialization scenarios.
    /// </summary>
    /// <returns>A ready-to-use benchmark session context.</returns>
    public static async Task<SessionContext> InitDeserTable()
    {
        var context = await InitSimpleTable();
        var session = context.Session;

        await session.ExecuteAsync(new SimpleStatement(
            "CREATE TYPE IF NOT EXISTS benchmarks.udt1 (field1 text, field2 int)"));
        await session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS benchmarks.basic"));
        await session.ExecuteAsync(new SimpleStatement(
            "CREATE TABLE benchmarks.basic (" +
            "id uuid, val int, tuuid timeuuid, ip inet, date date, time time, " +
            "tuple frozen<tuple<text, int>>, udt frozen<udt1>, set1 set<int>, duration duration, PRIMARY KEY(id))"));

        return context;
    }

    public static async Task InsertDataIntoSimpleTable(ISession session, int n)
    {
        var preparedInsert = await session.PrepareAsync(BasicInsertQuery);
        for (var index = 0; index < n; index++)
        {
            await session.ExecuteAsync(preparedInsert.Bind(Guid.NewGuid(), 100));
        }
    }

    /// <summary>
    /// Registers UDT mapping used by complex benchmark rows.
    /// </summary>
    /// <param name="session">Session used for UDT registration.</param>
    public static void DefineDeserUdt(ISession session)
    {
        session.UserDefinedTypes.Define(
            UdtMap.For<DeserUdt1>("udt1", keyspace: "benchmarks")
                .Map(v => v.Field1, "field1")
                .Map(v => v.Field2, "field2"));
    }

    /// <summary>
    /// Creates a payload row matching the complex benchmark table schema.
    /// </summary>
    public static object[] GetDeserData()
    {
        var now = DateTime.Now;
        return
        [
            Guid.NewGuid(),
            100,
            TimeUuid.Parse("8e14e760-7fa8-11eb-bc66-000000000001"),
            IPAddress.Parse("192.168.0.1"),
            new LocalDate(now.Year, now.Month, now.Day),
            new LocalTime(now.Hour, now.Minute, now.Second, now.Millisecond * 1_000_000),
            new Tuple<string, int>(
                "Litwo! Ojczyzno moja! ty jestes jak zdrowie: Ile cie trzeba cenic, ten tylko sie dowie, Kto cie stracil. Dzis pieknosc twa w calej ozdobie Widze i opisuje, bo tesknie po tobie.\n",
                1),
            new DeserUdt1
            {
                Field1 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis congue egestas sapien id maximus eget.",
                Field2 = 4321
            },
            new HashSet<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 11 },
            new Duration(1, 2, 3)
        ];
    }

    /// <summary>
    /// Verifies that the benchmark table contains exactly the expected number of rows.
    /// </summary>
    /// <param name="session">Session used to query row count.</param>
    /// <param name="n">Expected number of rows.</param>
    public static async Task CheckRowCnt(ISession session, int n)
    {
        var preparedSelect = await session.PrepareAsync("SELECT COUNT(1) FROM benchmarks.basic");
        var countResult = await session.ExecuteAsync(preparedSelect.Bind());

        // Result sets are enumerable; read the first row to get the aggregate value.
        Row? countRow = null;
        foreach (var row in countResult)
        {
            countRow = row;
            break;
        }

        if (countRow == null)
        {
            throw new InvalidOperationException("Failed to retrieve count of rows.");
        }

        var rows = countRow.GetValue<long>(0);
        if (rows != n)
        {
            throw new InvalidOperationException($"Expected {n} rows, but found {rows}.");
        }
    }

    /// <summary>
    /// Creates a new benchmark keyspace and table based on the provided schema.
    /// Existing basic table data is dropped before creation.
    /// </summary>
    /// <param name="schema">CREATE TABLE statement for benchmarks.basic.</param>
    /// <returns>A ready-to-use benchmark session context.</returns>
    private static async Task<SessionContext> InitCommon(string schema)
    {
        var uri = Environment.GetEnvironmentVariable("SCYLLA_URI") ?? "172.47.0.2";
        var clusterBuilder = Cluster.Builder();

#if DATASTAX_DRIVER
        var (host, port) = ParseHostAndPort(uri);
        clusterBuilder = clusterBuilder.AddContactPoint(host).WithPort(port);
#else
        clusterBuilder = clusterBuilder.AddContactPoint(uri);
#endif

        var cluster = clusterBuilder.Build();
        var session = await cluster.ConnectAsync();

        await session.ExecuteAsync(new SimpleStatement(
            "CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = " +
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"));

        await session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS benchmarks.basic"));
        await session.ExecuteAsync(new SimpleStatement(schema));

        return new SessionContext(cluster, session);
    }

#if DATASTAX_DRIVER
    /// <summary>
    /// Parses host and optional port in the form host[:port] for the DataStax driver.
    /// </summary>
    /// <param name="input">Host or host:port value.</param>
    /// <returns>Tuple containing parsed host and port.</returns>
    private static (string Host, int Port) ParseHostAndPort(string input)
    {
        var host = input;
        var port = 9042;
        var parts = input.Split(':', 2);
        if (parts.Length == 2 && int.TryParse(parts[1], out var parsedPort))
        {
            host = parts[0];
            port = parsedPort;
        }

        return (host, port);
    }
#endif
}
