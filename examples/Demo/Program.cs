using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Mapping;

namespace DemoExample
{
    // Custom C# class to map to a User-Defined Type (UDT)
    public class UserAddress
    {
        public string Street { get; set; }
        public string City { get; set; }
        public int ZipCode { get; set; }

        public override string ToString() => $"{Street}, {City} {ZipCode}";
    }

    internal class Program
    {
        private const string Keyspace = "demo_ks";
        private static ISession _session;
        private static ICluster _cluster;

        private static async Task Main(string[] args)
        {
            var host = args.Length > 0 ? args[0] : "127.0.2.1";

            Console.Clear();
            Console.WriteLine("=== ScyllaDB C#-RS Driver Live Demo ===");
            Console.WriteLine("================================================");

            try
            {
                await RunDemoAsync(host);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[ERROR] Demo failed: {ex.Message}");
            }
            finally
            {
                _session?.Dispose();
                _cluster?.Dispose();
            }
        }

        private static async Task RunDemoAsync(string host)
        {
            // STEP 1: Cluster Configuration & Socket Options
            await Step("Configure Cluster with Socket Options", async () =>
            {
                // We configure SocketOptions to control low-level TCP settings and timeouts.
                // These are passed down to the underlying Rust driver implementation.
                _cluster = Cluster.Builder()
                    .AddContactPoint(host)
                    .WithSocketOptions(new SocketOptions()
                        .SetConnectTimeoutMillis(5000)
                        .SetKeepAlive(true)
                        .SetTcpNoDelay(true)
                        .SetReuseAddress(true)
                        .SetKeepAliveIntervalMillis(2000)
                        .SetReceiveBufferSize(1024 * 64)
                        .SetSendBufferSize(1024 * 64)
                        .SetSoLinger(0))
                    .Build();

                _session = await _cluster.ConnectAsync();
                Console.WriteLine($"Connected to Cluster at {host}");
            });

            // STEP 2: Cleanup
            await Step("Cleanup (Drop Keyspace if exists)", async () =>
            {
                await _session.ExecuteAsync(new SimpleStatement($"DROP KEYSPACE IF EXISTS {Keyspace}"));
                Console.WriteLine($"Keyspace '{Keyspace}' dropped to ensure a fresh start.");
            });

            // STEP 3: Schema Creation (Keyspace, UDT, Table)
            await Step("Initialize Schema (Keyspace, UDT, and Table)", async () =>
            {
                await _session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}"));
                _session.ChangeKeyspace(Keyspace);

                // Define a User-Defined Type
                await _session.ExecuteAsync(new SimpleStatement("CREATE TYPE IF NOT EXISTS user_address (street text, city text, zip_code int)"));
                
                // Create a table using the UDT
                await _session.ExecuteAsync(new SimpleStatement("CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text, address frozen<user_address>)"));

                // Map the C# class to the UDT explicitly to handle the snake_case vs PascalCase mismatch
                _session.UserDefinedTypes.Define(
                    UdtMap.For<UserAddress>("user_address")
                        .Map(a => a.Street, "street")
                        .Map(a => a.City, "city")
                        .Map(a => a.ZipCode, "zip_code"));

                Console.WriteLine("Schema created and UDT mapped.");
            });

            // STEP 4: Simple Statement
            await Step("Execute a Simple Statement", async () =>
            {
                var address = new UserAddress { Street = "123 Scylla Way", City = "Cloud City", ZipCode = 99999 };
                
                // SimpleStatements are great for one-off queries or schema changes.
                var insert = new SimpleStatement("INSERT INTO users (id, name, address) VALUES (?, ?, ?)", 1, "Alice", address);
                await _session.ExecuteAsync(insert);
                Console.WriteLine("Inserted Alice using SimpleStatement.");
            });

            // STEP 5: Prepared Statement
            await Step("Execute a Prepared Statement", async () =>
            {
                // Prepared statements are parsed once by the server and reused.
                // This is the recommended way for high-performance data paths.
                var ps = await _session.PrepareAsync("INSERT INTO users (id, name, address) VALUES (?, ?, ?)");
                
                var address = new UserAddress { Street = "456 Driver Ave", City = "DotNetTown", ZipCode = 12345 };

                var bound1 = ps.Bind(2, "Bob", address);
                var bound2 = ps.Bind(3, "Charlie", address);
                
                await _session.ExecuteAsync(bound1);
                await _session.ExecuteAsync(bound2);
                
                Console.WriteLine("Inserted Bob and Charlie using PreparedStatement.");
            });

            // STEP 6: Metadata Retrieval
            await Step("Retrieve Cluster Metadata", async () =>
            {
                var metadata = _cluster.Metadata;
                Console.WriteLine($"Nodes in cluster: {metadata.AllHosts().Count}");
                foreach (var hostInfo in metadata.AllHosts())
                {
                    Console.WriteLine($"- Host: {hostInfo.Address}, DC: {hostInfo.Datacenter}, Rack: {hostInfo.Rack}");
                }

                var tableMetadata = metadata.GetTable(Keyspace, "users");
                Console.WriteLine($"Table '{tableMetadata.Name}' has {tableMetadata.TableColumns.Length} columns.");
                foreach (var column in tableMetadata.TableColumns)
                {                    
                    Console.WriteLine($"- Column: {column.Name}, Type: {column.Type}");
                }
            });

            // STEP 7: Query and Read UDT Data
            await Step("Query Data and verify UDT mapping", async () =>
            {
                var row = (await _session.ExecuteAsync(new SimpleStatement("SELECT * FROM users WHERE id = 1"))).FirstOrDefault();
                if (row != null)
                {
                    var name = row.GetValue<string>("name");
                    var addr = row.GetValue<UserAddress>("address");
                    Console.WriteLine($"Retrieved: User={name}, Address=[{addr}]");
                }
            });

            Console.WriteLine("\nDemo Complete! Press any key to exit.");
            Console.ReadKey();
        }

        private static async Task Step(string title, Func<Task> action)
        {
            Console.WriteLine($"\n[NEXT STEP]: {title}");
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.Write("Press ENTER to execute...");
            Console.ResetColor();
            Console.ReadLine();
            
            await action();
        }
    }
}
