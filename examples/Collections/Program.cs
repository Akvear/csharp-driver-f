using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Collections
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "collections_table";
        private static readonly string InsertCql = $"INSERT INTO {Keyspace}.{Table} (id, set_col, list_col, map_col) VALUES (?, ?, ?, ?)";
        private static readonly string SelectCql = $"SELECT * FROM {Keyspace}.{Table} WHERE id = ?";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning Collections example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create keyspace and table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (id int PRIMARY KEY, set_col set<text>, list_col list<text>, map_col map<text, int>)")).ConfigureAwait(false);

            var mySet = new HashSet<string> { "apple", "banana", "cherry" };
            var myList = new List<string> { "first", "second", "third" };
            var myMap = new Dictionary<string, int> { { "one", 1 }, { "two", 2 } };

            // Prepare the insert statement and execute with collection parameters
            var insert = await session.PrepareAsync(InsertCql).ConfigureAwait(false);
            await session.ExecuteAsync(insert.Bind(1, mySet, myList, myMap)).ConfigureAwait(false);

            // Retrieve the row with the collections
            var select = await session.PrepareAsync(SelectCql).ConfigureAwait(false);
            var row = (await session.ExecuteAsync(select.Bind(1)).ConfigureAwait(false)).FirstOrDefault();

            if (row != null)
            {
                var retrievedSet = row.GetValue<HashSet<string>>("set_col");
                var retrievedList = row.GetValue<List<string>>("list_col");
                var retrievedMap = row.GetValue<Dictionary<string, int>>("map_col");

                Console.WriteLine("Set elements: " + string.Join(", ", retrievedSet));
                Console.WriteLine("List elements: " + string.Join(", ", retrievedList));
                Console.WriteLine("Map elements: " + string.Join(", ", retrievedMap.Select(kvp => $"{kvp.Key}={kvp.Value}")));
            }
        }
    }
}
