using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Simple
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "simple_table";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning Simple example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create keyspace and table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (id int PRIMARY KEY, value text)")).ConfigureAwait(false);

            // Insert a row
            await session.ExecuteAsync(new SimpleStatement($"INSERT INTO {Keyspace}.{Table} (id, value) VALUES (1, 'Hello World')")).ConfigureAwait(false);

            // Retrieve the row
            var row = (await session.ExecuteAsync(new SimpleStatement($"SELECT id, value FROM {Keyspace}.{Table} WHERE id = 1")).ConfigureAwait(false)).FirstOrDefault();
            if (row != null)
            {
                Console.WriteLine($"Row found -> id: {row.GetValue<int>("id")}, value: {row.GetValue<string>("value")}");
            }
        }
    }
}
