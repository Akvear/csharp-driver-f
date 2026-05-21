# Logging in the C# RS Driver

This guide explains how logging is handled in the C# RS Driver, which wraps the underlying Rust driver using FFI. Logging covers both the C# API layers and the internal Rust driver logic, providing a unified view of what's happening.

## Overview

The driver supports two primary logging mechanisms, configurable through the `Cassandra.Diagnostics` class:
1. **`Microsoft.Extensions.Logging` (Recommended):** The modern, extensible logging framework standard in .NET Core and .NET 5+.
2. **`System.Diagnostics.Trace` (Legacy):** The older trace-based logging common in .NET Framework.

Regardless of which mechanism you choose, the logging configuration **must** be done very early in your application lifecycle—typically before any calls to `Cluster.Builder()`. This guarantees the configuration is correctly wired up before the Rust driver is initialized.

---

## 1. Using `Microsoft.Extensions.Logging` (Factory-Based Logging)

This is the recommended approach for modern .NET applications.

To route both C# and Rust logger outputs through an `ILoggerFactory`, use the `Diagnostics.AddLoggerProvider()` method. This will set an internal flag (`UseLoggerFactory = true`) causing the driver to abandon standard `Trace` listeners and use the provided `ILoggerProvider` instead.

### Example

```csharp
class Program
{
    static async Task Main(string[] args)
    {
        // 1. Register a provider. This tells the driver to route its logs through ILogger.
        Diagnostics.AddLoggerProvider(new ConsoleLoggerProvider());

        // 2. Build the cluster and session.
        // Any driver logs emitted around connection or query execution are forwarded.
        using (var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build())
        {
            using var session = cluster.Connect();
            await session.ExecuteAsync(new SimpleStatement("SELECT host_id FROM system.peers"));
        }
    }
}
```

(The `ConsoleLoggerProvider` implementation is omitted for brevity, but any standard extension like `AddConsole()` or third-party loggers like Serilog will supply standard `ILoggerProvider` objects).

---

## 2. Using `System.Diagnostics.Trace` (Trace-Based Logging)

If you don't supply an `ILoggerProvider`, the driver defaults to classic `Trace`-based logging.

You need to explicitly configure the verbosity via `Diagnostics.CassandraTraceSwitch.Level`. Also, ensure you attach `TraceListener`s like `ConsoleTraceListener` or `TextWriterTraceListener` to actually capture the output. The default log level for trace-based logging is Off.

### Environmental Configuration
You can also configure the trace level via environment variable without changing the code, note that this means logs will be outputted to the console:
```bash
export CASSANDRA_LOG_LEVEL=Verbose
```
Acceptable values are: `Off`, `Error`, `Warning`, `Info`, `Verbose`.

### Example

```csharp
class Program
{
    static async Task Main(string[] args)
    {
        // Set the minimum log level (MUST be done before driver interaction)
        Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

        string logFilePath = "driver.log";
        using (StreamWriter logFile = new StreamWriter(logFilePath, append: false))
        {
            TextWriterTraceListener fileListener = new TextWriterTraceListener(logFile);
            Trace.Listeners.Add(fileListener);
            Trace.Listeners.Add(new ConsoleTraceListener());

            try
            {
                using (var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build())
                {
                    var session = cluster.Connect();
                    await session.ExecuteAsync(new SimpleStatement("SELECT host_id FROM system.peers"));
                }
            }
            finally
            {
                Trace.Flush();
            }
        }
    }
}
```

---

## The Rust-to-C# Logging Bridge

The C# RS Driver tightly integrates with the underlying Rust driver's logging (`tracing` crate). Rust logs are automatically forwarded to your configured C# logger (either `ILoggerFactory` or `Trace`). There is no difference from user perspective whether a log originated natively in C# or in Rust.

### Log Level Mapping

The internal Rust log levels map to the C# `Diagnostics.CassandraTraceSwitch` (represented internally as `CsharpLogLevel`) as follows:

| Rust (`tracing::Level`) | C# (`TraceLevel` / `CsharpLogLevel`) |
|-------------------------|--------------------------------------|
| `Level::ERROR`          | `Error`                              |
| `Level::WARN`           | `Warning`                            |
| `Level::INFO`           | `Info`                               |
| `Level::DEBUG`          | `Verbose`                            |
| `Level::TRACE`          | `Verbose`                            |

*(Note: C# does not have a separate log level for trace vs debug, so both are coalesced into `Verbose`.)*
