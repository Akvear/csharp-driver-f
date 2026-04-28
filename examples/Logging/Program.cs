using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Cassandra;

namespace LoggingExample
{
    /// <summary>
    /// This example demonstrates how to configure logging in the C# driver.
    /// </summary>
    internal class Program
    {
        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            string logFilePath = "driver.log";

            // Log configuration *MUST* be done before any other driver call.
            // Set the minimum log level.
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

            // We use a StreamWriter to write logs to a file.
            using (StreamWriter logFile = new StreamWriter(logFilePath, append: false))
            {
                TextWriterTraceListener fileListener = new TextWriterTraceListener(logFile);
                ConsoleTraceListener consoleListener = new ConsoleTraceListener();
                
                // Add the listener to the global Trace listeners collection.
                Trace.Listeners.Add(fileListener);
                
                // Optional: Also log to the console to see progress.
                Trace.Listeners.Add(consoleListener);

                try
                {
                    // Initialize the cluster and session.
                    // Internal logging messages from both C# and the Rust core 
                    // will now be captured by our listener.
                    using (var cluster =
                        Cluster.Builder()
                        .AddContactPoint("172.42.0.2")
                        .Build())
                    {
                        var session = cluster.Connect();

                        // Execute a query to generate some log activity.
                        var s = new SimpleStatement("SELECT host_id FROM system.peers");
                        await session.ExecuteAsync(s);
                    }
                }
                finally
                {
                    // Cleanup: Ensure logs are flushed to the file and remove the listener.
                    Trace.Flush();
                    Trace.Listeners.Remove(fileListener);
                    Trace.Listeners.Remove(consoleListener);
                    Console.WriteLine($"Example finished. Check '{logFilePath}' for detailed logs.");
                }
            }
        }
    }
}
