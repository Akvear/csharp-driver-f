//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Order(1)]
    public class LoadBalancingPolicyShortTests : SharedClusterTest
    {
        public LoadBalancingPolicyShortTests() : base(3, false, new TestClusterOptions { UseVNodes = true })
        {
        }

        /// <summary>
        /// Validate that two sessions connected to the same DC use separate Policy instances
        /// </summary>
        [Test]
        public void TwoSessionsConnectedToSameDcUseSeparatePolicyInstances()
        {
            var builder = ClusterBuilder();

            using (var cluster1 = builder.WithConnectionString($"Contact Points={TestCluster.ClusterIpPrefix}1").Build())
            using (var cluster2 = builder.WithConnectionString($"Contact Points={TestCluster.ClusterIpPrefix}2").Build())
            {
                var session1 = (Session)cluster1.Connect();
                var session2 = (Session)cluster2.Connect();
                Assert.AreNotSame(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy, "Load balancing policy instances should be different");
                Assert.AreNotSame(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy, "Reconnection policy instances should be different");
                Assert.AreNotSame(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy, "Retry policy instances should be different");
            }
        }

        /// <summary>
        /// With RF=2 and a fixed prepared-statement key, the Rust driver rotates between
        /// both replicas (<c>maybe_shuffled_replicas</c> in the Rust LBP). The test confirms
        /// the driver routes only to replicas — not all 3 nodes — by asserting exactly 2
        /// distinct coordinators appear across many executions.
        /// <para>
        /// Note: this test cannot verify that those 2 coordinators are the <em>correct</em>
        /// replicas for this token; that requires <c>GetReplicas</c>, which is not yet implemented.
        /// </para>
        /// </summary>
        [Test]
        public void TokenAware_RF2_BothReplicasUsedAsCoordinators()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");

            // Use a fixed key so every execution targets the same token and the same replica set
            var ps = session.Prepare("SELECT v FROM tbl WHERE k = ?");
            var coordinators = new HashSet<IPEndPoint>();
            for (var i = 0; i < 30; i++)
            {
                var rs = session.Execute(ps.Bind(42));
                coordinators.Add(rs.Info.QueriedHost);
            }
            // With RF=2 on a 3-node cluster, token-aware routing must use exactly 2 coordinators.
            // If routing were random (not token-aware) we would see all 3; if broken we would see 1.
            Assert.AreEqual(2, coordinators.Count,
                "With RF=2 and token-aware routing, all queries for the same key must go to exactly the 2 owning replicas");
        }

        /// <summary>
        /// With RF=1, every partition key has exactly one replica, so a token-aware
        /// policy must always route the same key to the same coordinator.
        /// </summary>
        [Test]
        public void TokenAware_SameKey_AlwaysSameCoordinator()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            session.Execute("INSERT INTO tbl (k, v) VALUES (42, 1)");

            var ps = session.Prepare("SELECT v FROM tbl WHERE k = ?");
            var coordinators = new HashSet<IPEndPoint>();
            for (var i = 0; i < 10; i++)
            {
                var rs = session.Execute(ps.Bind(42));
                coordinators.Add(rs.Info.QueriedHost);
            }
            Assert.AreEqual(1, coordinators.Count, "With RF=1, the same partition key must always route to the same coordinator");
        }

        /// <summary>
        /// With RF=1 and 3 nodes, 100 distinct partition keys should be distributed
        /// across more than one coordinator.
        /// </summary>
        [Test]
        public void TokenAware_DifferentKeys_MultipleCoordinators()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            var ps = session.Prepare("INSERT INTO tbl (k, v) VALUES (?, ?)");
            for (var i = 0; i < 100; i++)
                session.Execute(ps.Bind(i, i));

            var selectPs = session.Prepare("SELECT v FROM tbl WHERE k = ?");
            var coordinators = new HashSet<IPEndPoint>();
            for (var i = 0; i < 100; i++)
            {
                var rs = session.Execute(selectPs.Bind(i));
                coordinators.Add(rs.Info.QueriedHost);
            }
            Assert.Greater(coordinators.Count, 1, "100 distinct partition keys across 3 nodes should use multiple coordinators");
        }

        /// <summary>
        /// Validate that DCAwareRoundRobinPolicy with the correct datacenter allows queries to succeed.
        /// Tests that the DC-aware config is correctly translated to the Rust layer.
        /// </summary>
        [Test]
        public void DcAware_CorrectDc_QueriesSucceed()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("datacenter1")));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            session.Execute("INSERT INTO tbl (k, v) VALUES (1, 42)");
            var rs = session.Execute("SELECT v FROM tbl WHERE k = 1");
            Assert.AreEqual(42, rs.First().GetValue<int>("v"));
        }

        [Test]
        public void DcAware_ParameterlessConstructor_Throws()
        {
#pragma warning disable 618
            NUnit.Framework.Assert.Throws<ArgumentException>(() => new DCAwareRoundRobinPolicy());
#pragma warning restore 618
        }

        [Test]
        public void DcAware_PermitDcFailover_QueriesSucceed()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("datacenter1", permitDcFailover: true)));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            session.Execute("INSERT INTO tbl (k, v) VALUES (1, 42)");
            var rs = session.Execute("SELECT v FROM tbl WHERE k = 1");
            Assert.AreEqual(42, rs.First().GetValue<int>("v"));
        }

        [Test]
        public void DcAware_WrongDc_Throws()
        {
            NUnit.Framework.Assert.Throws<ArgumentException>(() =>
            {
                var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("nonexistent_dc")));
                cluster.Connect();
            });
        }

        /// <summary>
        /// Validate that TokenAwarePolicy wrapping RoundRobinPolicy allows queries to succeed.
        /// Tests that token-aware config is correctly translated to the Rust layer.
        /// </summary>
        [Test]
        public void TokenAware_RoundRobin_QueriesSucceed()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            for (var i = 0; i < 5; i++)
                session.Execute($"INSERT INTO tbl (k, v) VALUES ({i}, {i * 10})");
            var count = session.Execute("SELECT COUNT(*) FROM tbl").First().GetValue<long>(0);
            Assert.AreEqual(5, count);
        }

        /// <summary>
        /// Validate that an explicit RoundRobinPolicy allows queries to succeed.
        /// Tests that the round-robin config is correctly translated to the Rust layer.
        /// </summary>
        [Test]
        public void RoundRobin_QueriesSucceed()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new RoundRobinPolicy()));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            for (var i = 0; i < 5; i++)
                session.Execute($"INSERT INTO tbl (k, v) VALUES ({i}, {i * 10})");
            var count = session.Execute("SELECT COUNT(*) FROM tbl").First().GetValue<long>(0);
            Assert.AreEqual(5, count);
        }
    }

    /// <summary>
    /// DC-aware and token-aware + DC-aware load balancing tests.
    /// Uses a separate 2-DC cluster (1 node per DC) and must run after
    /// <see cref="LoadBalancingPolicyShortTests"/> to avoid destroying its shared cluster.
    /// </summary>
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Order(2)]
    public class LoadBalancingPolicyMultiDcTests : SharedClusterTest
    {
        public LoadBalancingPolicyMultiDcTests()
            : base(1, false, new TestClusterOptions { Dc2NodeLength = 1 }) { }

        /// <summary>
        /// With a 2-DC cluster, DCAwareRoundRobinPolicy should route all queries to
        /// the local DC while it is available.
        /// </summary>
        [Test]
        public void DcAware_AllQueriesGoToLocalDc()
        {
            var localDcNode = TestCluster.ClusterIpPrefix + "1";
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc1")));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            session.Execute("INSERT INTO tbl (k, v) VALUES (1, 42)");

            var coordinators = new HashSet<IPEndPoint>();
            for (var i = 0; i < 20; i++)
            {
                var rs = session.Execute("SELECT v FROM tbl WHERE k = 1");
                coordinators.Add(rs.Info.QueriedHost);
            }
            Assert.IsTrue(
                coordinators.All(ep => ep.Address.ToString() == localDcNode),
                $"All queries should go to the local DC node ({localDcNode}), but got: {string.Join(", ", coordinators)}");
        }

        /// <summary>
        /// With a 2-DC cluster, TokenAwarePolicy wrapping DCAwareRoundRobinPolicy should
        /// keep all coordinators inside the local DC.
        /// </summary>
        [Test]
        public void TokenAware_DcAware_CoordinatorsStayInLocalDc()
        {
            var localDcNode = TestCluster.ClusterIpPrefix + "1";
            var cluster = GetNewTemporaryCluster(b => b.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1"))));
            var session = cluster.Connect();
            var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            session.Execute($"CREATE KEYSPACE \"{ks}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}");
            session.ChangeKeyspace(ks);
            session.Execute("CREATE TABLE tbl (k int PRIMARY KEY, v int)");
            var ps = session.Prepare("INSERT INTO tbl (k, v) VALUES (?, ?)");
            for (var i = 0; i < 20; i++)
                session.Execute(ps.Bind(i, i));

            var selectPs = session.Prepare("SELECT v FROM tbl WHERE k = ?");
            var coordinators = new HashSet<IPEndPoint>();
            for (var i = 0; i < 20; i++)
            {
                var rs = session.Execute(selectPs.Bind(i));
                coordinators.Add(rs.Info.QueriedHost);
            }
            Assert.IsTrue(
                coordinators.All(ep => ep.Address.ToString() == localDcNode),
                $"All queries should stay in the local DC ({localDcNode}), but got: {string.Join(", ", coordinators)}");
        }
    }
}
