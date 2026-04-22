using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Cassandra.LoggingTests
{
    [TestFixture, Category("logging"), NonParallelizable]
    public class RustLoggingTests
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            SimulacronManager.DefaultInstance.Start();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            SimulacronManager.DefaultInstance.Stop();
        }

        [Test]
        public void Should_Forward_Rust_Log_Entries_Using_LoggerFactory()
        {
            AssertLoggerFactoryRustLogs(TraceLevel.Off, expectedMessageCount: 5);
        }

        [Test]
        public void Should_Forward_Rust_Log_On_Connect()
        {
            AssertRustLogs(TraceLevel.Verbose, expectedMessageCount: null, assertConnectLog: true);
        }

        [Test]
        public void Should_Filter_Rust_Log_Entries_At_Off()
        {
            AssertRustLogs(TraceLevel.Off, expectedMessageCount: 0);
        }

        [Test]
        public void Should_Filter_Rust_Log_Entries_At_Error()
        {
            AssertRustLogs(TraceLevel.Error, expectedMessageCount: 1);
        }

        [Test]
        public void Should_Filter_Rust_Log_Entries_At_Warning()
        {
            AssertRustLogs(TraceLevel.Warning, expectedMessageCount: 2);
        }

        [Test]
        public void Should_Filter_Rust_Log_Entries_At_Info()
        {
            AssertRustLogs(TraceLevel.Info, expectedMessageCount: 3);
        }

        [Test]
        public void Should_Filter_Rust_Log_Entries_At_Verbose()
        {
            AssertRustLogs(TraceLevel.Verbose, expectedMessageCount: 5);
        }

        private static void AssertRustLogs(TraceLevel level, int? expectedMessageCount, bool assertConnectLog = false)
        {
            var writer = new StringWriter();
            var listener = new TextWriterTraceListener(writer);
            SimulacronCluster testCluster = null;
            ICluster cluster = null;

            Trace.Listeners.Add(listener);
            try
            {
                Cassandra.Diagnostics.CassandraTraceSwitch.Level = level;

                testCluster = SimulacronCluster.CreateNew(1);
                cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build();
                var session = cluster.Connect();

                if (assertConnectLog)
                {
                    var hasRustLog = writer.ToString().Contains("Rust:");
                    Assert.That(hasRustLog, Is.True, "The trace listener should have captured at least one log message forwarded from the Rust bridge.");
                    return;
                }

                writer.GetStringBuilder().Clear();

                emit_all_log_levels();
                Trace.Flush();

                var traceOutput = writer.ToString();
                var traceCount = traceOutput.Split("This is a", StringSplitOptions.None).Length - 1;
                Assert.That(traceCount, Is.EqualTo(expectedMessageCount), $"Failed for level {level}. Messages captured: {traceOutput}");
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                listener.Dispose();
                writer.Dispose();
                cluster?.Dispose();
                testCluster?.Dispose();
            }
        }

        private static void AssertLoggerFactoryRustLogs(TraceLevel level, int? expectedMessageCount)
        {
            var provider = new TestLoggerProvider();
            SimulacronCluster testCluster = null;
            ICluster cluster = null;

            Cassandra.Diagnostics.CassandraTraceSwitch.Level = level;
            Cassandra.Diagnostics.AddLoggerProvider(provider);

            try
            {
                testCluster = SimulacronCluster.CreateNew(1);
                cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build();
                var session = cluster.Connect();

                provider.Clear();
                emit_all_log_levels();

                var rustLogCount = provider.Records.Count(record =>
                    record.Contains("Rust") &&
                    record.Contains("This is a"));

                Assert.That(rustLogCount, Is.EqualTo(expectedMessageCount), $"Expected logger factory to capture {expectedMessageCount} Rust log entries. Captured: {string.Join(" | ", provider.Records)}");
            }
            finally
            {
                cluster?.Dispose();
                testCluster?.Dispose();
            }
        }

        private sealed class TestLoggerProvider : ILoggerProvider
        {
            public ConcurrentQueue<string> Records { get; } = new ConcurrentQueue<string>();

            public ILogger CreateLogger(string categoryName)
            {
                return new TestLogger(categoryName, Records);
            }

            public void Dispose()
            {
            }

            public void Clear()
            {
                while (Records.TryDequeue(out _)) { }
            }
        }

        private sealed class TestLogger : ILogger
        {
            private readonly string _categoryName;
            private readonly ConcurrentQueue<string> _records;

            public TestLogger(string categoryName, ConcurrentQueue<string> records)
            {
                _categoryName = categoryName;
                _records = records;
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
                _records.Enqueue($"{_categoryName} [{logLevel}] {formatter(state, exception)}");
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new NullScope();

            public void Dispose()
            {
            }
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void emit_all_log_levels();
    }
}
