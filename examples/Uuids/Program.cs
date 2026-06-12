using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Uuids
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "log";
        private static readonly string InsertCql = $"INSERT INTO {Keyspace}.{Table} (key, time, entry) VALUES (?, ?, ?)";
        private static readonly string SelectCql = $"SELECT * FROM {Keyspace}.{Table} WHERE key = ?";

        private static void Main(string[] args)
        {
            new Program().RunAsync(args).GetAwaiter().GetResult();
        }

        private async Task RunAsync(string[] args)
        {
            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            try
            {
                using var cluster = Cluster.Builder().AddContactPoint(host).Build();
                using var session = await cluster.ConnectAsync().ConfigureAwait(false);

                await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
                await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (key text, time timeuuid, entry text, PRIMARY KEY (key, time))")).ConfigureAwait(false);

                var insert = await session.PrepareAsync(InsertCql).ConfigureAwait(false);
                for (int i = 1; i <= 4; i++)
                {
                    await session.ExecuteAsync(insert.Bind("test", TimeUuid.NewId(), $"Log entry #{i}")).ConfigureAwait(false);
                }

                var select = await session.PrepareAsync(SelectCql).ConfigureAwait(false);
                var rs = await session.ExecuteAsync(select.Bind("test")).ConfigureAwait(false);

                foreach (var row in rs)
                {
                    Console.WriteLine($"Key: {row.GetValue<string>("key")} | Time: {row.GetValue<TimeUuid>("time")} | Entry: {row.GetValue<string>("entry")}");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
