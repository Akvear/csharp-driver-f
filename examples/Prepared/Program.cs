using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Prepared
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "basic";
        private static readonly string InsertCql = $"INSERT INTO {Keyspace}.{Table} (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?)";
        private static readonly string SelectCql = $"SELECT * FROM {Keyspace}.{Table} WHERE key = ?";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning Prepared example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create keyspace and table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (key text PRIMARY KEY, bln boolean, flt float, dbl double, i32 int, i64 bigint)")).ConfigureAwait(false);

            // Insert a row using a simple statement with values
            await session.ExecuteAsync(new SimpleStatement(InsertCql, "my_key", true, 1.5f, 3.14159, 42, 1234567890L)).ConfigureAwait(false);

            // Prepare the select statement
            var select = await session.PrepareAsync(SelectCql).ConfigureAwait(false);

            // Execute the prepared statement with bound parameters
            var row = (await session.ExecuteAsync(select.Bind("my_key")).ConfigureAwait(false)).FirstOrDefault();
            if (row != null)
            {
                Console.WriteLine($"Retrieved -> key: {row.GetValue<string>("key")}, bln: {row.GetValue<bool>("bln")}, flt: {row.GetValue<float>("flt")}, dbl: {row.GetValue<double>("dbl")}, i32: {row.GetValue<int>("i32")}, i64: {row.GetValue<long>("i64")}");
            }
        }
    }
}
