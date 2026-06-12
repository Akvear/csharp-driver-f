using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace SchemaMetadata
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning SchemaMetadata example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Query the metadata about a keyspace
            var keyspaceMeta = cluster.Metadata.GetKeyspace("system");
            Console.WriteLine($"Tables in '{keyspaceMeta.Name}' keyspace:");
            
            var tables = cluster.Metadata.GetTables("system");
            foreach (var tableName in tables)
            {
                Console.Write($"{tableName}, ");
            }
            Console.WriteLine("\n\nColumns in 'system.local' table:");

            // Read metadata about a specific table's columns
            var localTable = cluster.Metadata.GetTable("system", "local");
            if (localTable != null)
            {
                foreach (var column in localTable.TableColumns)
                {
                    Console.WriteLine($" - {column.Name} (Type: {column.TypeCode})");
                }
            }
        }
    }
}
