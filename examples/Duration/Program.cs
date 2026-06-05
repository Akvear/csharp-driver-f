using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace DurationExample
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "duration_table";
        private static readonly string InsertCql = $"INSERT INTO {Keyspace}.{Table} (id, duration_val) VALUES (?, ?)";
        private static readonly string SelectCql = $"SELECT id, duration_val FROM {Keyspace}.{Table} WHERE id = ?";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning Duration example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create keyspace and table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (id int PRIMARY KEY, duration_val duration)")).ConfigureAwait(false);

            // Prepare the insert statement, bind a duration and execute
            var insert = await session.PrepareAsync(InsertCql).ConfigureAwait(false);
            var duration = new Duration(2, 15, 123456789L);
            await session.ExecuteAsync(insert.Bind(1, duration)).ConfigureAwait(false);

            // Retrieve the duration
            var select = await session.PrepareAsync(SelectCql).ConfigureAwait(false);
            var row = (await session.ExecuteAsync(select.Bind(1)).ConfigureAwait(false)).FirstOrDefault();

            if (row != null)
            {
                var retrievedDuration = row.GetValue<Duration>("duration_val");
                Console.WriteLine($"Retrieved Duration -> Months: {retrievedDuration.Months}, Days: {retrievedDuration.Days}, Nanoseconds: {retrievedDuration.Nanoseconds}");
            }
        }
    }
}
