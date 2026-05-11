using System;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Logging;

namespace FactoryBasedLogging
{
    /// <summary>
    /// This example demonstrates how to use LoggerFactory with a custom provider and
    /// how to register that provider with the driver in the ScyllaDB C# RS Driver.
    /// </summary>
    internal class Program
    {
        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            // This sample only configures the driver's ILogger-based logging path.
            // Registering a provider here tells the driver to route its logs through ILogger.
            Diagnostics.AddLoggerProvider(new ConsoleLoggerProvider());

            // Build a normal cluster and session.
            // Any driver logs emitted during connect or query execution are forwarded through the provider above.
            using (var cluster =
                Cluster.Builder()
                .AddContactPoint("172.42.0.1")
                .Build())
            {
                using var session = cluster.Connect();

                // Run a query so the example produces driver logs through ILogger.
                await session.ExecuteAsync(new SimpleStatement("SELECT host_id FROM system.peers"));
            }
        }

        private sealed class ConsoleLoggerProvider : ILoggerProvider
        {
            public ILogger CreateLogger(string categoryName)
            {
                return new ConsoleLogger(categoryName);
            }

            public void Dispose()
            {
            }
        }

        private sealed class ConsoleLogger : ILogger
        {
            private readonly string _categoryName;

            public ConsoleLogger(string categoryName)
            {
                _categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return NullScope.Instance;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                Console.WriteLine($"[{logLevel}] {_categoryName}: {formatter(state, exception)}");
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new NullScope();

            public void Dispose()
            {
            }
        }
    }
}
