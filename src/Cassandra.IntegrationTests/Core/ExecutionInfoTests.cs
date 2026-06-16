using System.Linq;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class ExecutionInfoTests : SharedClusterTest
    {
        public ExecutionInfoTests() : base(1, true)
        {
        }

        [Test]
        public void QueriedHost_Is_Set_After_Simple_Query()
        {
            var rs = Session.Execute("SELECT key FROM system.local WHERE key='local'");
            Assert.IsNotNull(rs.Info.QueriedHost);
        }

        [Test]
        public void QueriedHost_Matches_Cluster_Host()
        {
            var rs = Session.Execute("SELECT key FROM system.local WHERE key='local'");
            var knownAddresses = Cluster.AllHosts().Select(h => h.Address).ToList();
            Assert.That(knownAddresses.Contains(rs.Info.QueriedHost),
                $"QueriedHost {rs.Info.QueriedHost} not found in known hosts: [{string.Join(", ", knownAddresses)}]");
        }

        [Test]
        public void QueriedHost_Is_Set_After_Prepared_Statement()
        {
            var ps = Session.Prepare("SELECT key FROM system.local WHERE key='local'");
            var rs = Session.Execute(ps.Bind());
            Assert.IsNotNull(rs.Info.QueriedHost);
        }

        [Test]
        public void TriedHosts_Is_Populated_After_Query()
        {
            var rs = Session.Execute("SELECT key FROM system.local WHERE key='local'");
            Assert.IsNotNull(rs.Info.TriedHosts);
            Assert.Greater(rs.Info.TriedHosts.Count, 0, "TriedHosts should contain at least one entry");
        }

        [Test]
        public void QueriedHost_Matches_Last_TriedHost()
        {
            var rs = Session.Execute("SELECT key FROM system.local WHERE key='local'");
            var triedHosts = rs.Info.TriedHosts;
            Assert.IsTrue(triedHosts.Count > 0);
            Assert.AreEqual(triedHosts[triedHosts.Count - 1], rs.Info.QueriedHost);
        }
    }
}
