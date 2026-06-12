using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace UserDefinedTypes
{
    internal class Program
    {
        private const string Keyspace = "examples";
        private const string Table = "udt_table";
        private const string UdtName = "address";

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine("Beginning UserDefinedTypes example!");

            var host = args.Length > 0 ? args[0] : "127.0.0.1";

            // Build a cluster and connect a session
            using var cluster = Cluster.Builder().AddContactPoint(host).Build();
            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            // Create keyspace, UDT, and table
            await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TYPE IF NOT EXISTS {Keyspace}.{UdtName} (street text, city text, zip int)")).ConfigureAwait(false);
            await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table} (id int PRIMARY KEY, addr frozen<{UdtName}>)")).ConfigureAwait(false);

            // Define the UDT mapping for the driver so it knows how to map to the Address class
            session.UserDefinedTypes.Define(
                UdtMap.For<Address>(UdtName, Keyspace)
                    .Map(a => a.Street, "street")
                    .Map(a => a.City, "city")
                    .Map(a => a.Zip, "zip")
            );

            // Prepare the insert statement and bind an Address object
            var insert = await session.PrepareAsync($"INSERT INTO {Keyspace}.{Table} (id, addr) VALUES (?, ?)").ConfigureAwait(false);
            var myAddress = new Address { Street = "123 Main St", City = "Springfield", Zip = 12345 };
            await session.ExecuteAsync(insert.Bind(1, myAddress)).ConfigureAwait(false);

            // Retrieve the row containing the mapped UDT
            var select = await session.PrepareAsync($"SELECT id, addr FROM {Keyspace}.{Table} WHERE id = ?").ConfigureAwait(false);
            var row = (await session.ExecuteAsync(select.Bind(1)).ConfigureAwait(false)).FirstOrDefault();

            if (row != null)
            {
                var retrievedAddress = row.GetValue<Address>("addr");
                Console.WriteLine($"Retrieved Address -> Street: {retrievedAddress.Street}, City: {retrievedAddress.City}, Zip: {retrievedAddress.Zip}");
            }
        }
    }

    // The C# class that maps to the User-Defined Type (UDT)
    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public int Zip { get; set; }
    }
}