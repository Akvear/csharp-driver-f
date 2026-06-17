using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace GetByNameExample
{
    internal class Program
    {
        private const string Keyspace = "examples_ks";
        private const string Table = "get_by_name";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            Console.WriteLine($"Connecting to {host} ...");

            // Configure the cluster and connect
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create Keyspace and Table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).ConfigureAwait(false);
            
            // Change keyspace context
            session.ChangeKeyspace(Keyspace);

            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Table} (pk int, ck int, value text, primary key (pk, ck))")).ConfigureAwait(false);

            // Insert data using positional parameters
            var insertQuery = $"INSERT INTO {Table} (pk, ck, value) VALUES (?, ?, ?)";
            await session.ExecuteAsync(new SimpleStatement(insertQuery, 3, 4, "def")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement(insertQuery, 1, 2, "abc")).ConfigureAwait(false);

            // Retrieve rows
            var rowSet = await session.ExecuteAsync(new SimpleStatement($"SELECT pk, ck, value FROM {Table}")).ConfigureAwait(false);

            Console.WriteLine("ck           |  value");
            Console.WriteLine("---------------------");

            foreach (var row in rowSet)
            {
                // Accessing columns by name using the GetValue<T> method.
                // This method handles finding the correct column index automatically based on the name provided.
                int ck = row.GetValue<int>("ck");
                string value = row.GetValue<string>("value");

                Console.WriteLine($"{ck,-12} | {value}");
            }
        }
    }
}
