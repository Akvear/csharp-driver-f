using System.Diagnostics;
using System.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class RustLoggingIntegrationTests : SharedClusterTest
    {
        public RustLoggingIntegrationTests() : base(amountOfNodes: 1, createSession: false)
        {
        }

        [OneTimeSetUp]
        public override void OneTimeSetUp()
        {
            Diagnostics.UseLoggerFactory = false;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

            base.OneTimeSetUp();
        }

        [Test]
        public void Should_Emit_Rust_Log_On_Connect()
        {
            var listener = new TestTraceListener();
            Trace.Listeners.Add(listener);

            try
            {
                using (var cluster = GetNewTemporaryCluster())
                {
                    cluster.Connect();
                }

                // Log messages originating from the Rust driver are formatted in 
                // RustBridge.ForwardRustLog using the pattern "[target] message".
                var hasRustLog = listener.Queue.Any(m => m != null && m.Contains("Rust"));
                Assert.That(hasRustLog, Is.True, "The trace listener should have captured at least one log message forwarded from the Rust bridge (formatted with '[target]').");
            }
            finally
            {
                Trace.Listeners.Remove(listener);
            }
        }
    }
}
