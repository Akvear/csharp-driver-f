using System;
using System.Linq;
using System.Collections.Generic;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class MetadataGetReplicasTests : SharedClusterTest
    {
        private readonly string _keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

        public MetadataGetReplicasTests() : base(3)
        {
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            var tabletsClause = TestClusterManager.IsScylla ? " AND tablets = { 'enabled' : false }" : "";
            Session.Execute(
                $"CREATE KEYSPACE {_keyspaceName} WITH replication = " +
                $"{{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}{tabletsClause}");
        }

        #region Simple Partition Key Tests

        [TestCase("text", "simple_text_key", TestName = "GetReplicas_SimpleKey_Text")]
        [TestCase("int", 12345, TestName = "GetReplicas_SimpleKey_Int")]
        [TestCase("bigint", 9876543210L, TestName = "GetReplicas_SimpleKey_BigInt")]
        [TestCase("boolean", true, TestName = "GetReplicas_SimpleKey_Boolean")]
        [TestCase("float", 3.14f, TestName = "GetReplicas_SimpleKey_Float")]
        [TestCase("double", 2.718281828, TestName = "GetReplicas_SimpleKey_Double")]
        public void GetReplicas_ReturnsValidReplicas_ForSimplePartitionKey(string cqlType, object keyValue)
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk {cqlType} PRIMARY KEY, value text)");
            var allHosts = Cluster.AllHosts();

            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { keyValue });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
            foreach (var replica in replicas)
            {
                Assert.IsNotNull(replica.Host);
                Assert.IsTrue(allHosts.Any(h => h.Address.Equals(replica.Host.Address)));
            }
        }

        [Test]
        public void GetReplicas_ReturnsConsistentReplicas_ForSimpleKey_UUID()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk uuid PRIMARY KEY, value text)");
            var uuid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");

            var replicas1 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { uuid });
            var replicas2 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { uuid });

            Assert.AreEqual(replicas1.Count, replicas2.Count);
            var addresses1 = replicas1.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var addresses2 = replicas2.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            CollectionAssert.AreEqual(addresses1, addresses2);
        }

        [TestCase("", TestName = "GetReplicas_SimpleKey_EmptyString")]
        [TestCase("a", TestName = "GetReplicas_SimpleKey_SingleChar")]
        [TestCase("very_long_key_with_many_characters_to_test_edge_cases_" +
                  "abcdefghijklmnopqrstuvwxyz_0123456789_ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                  TestName = "GetReplicas_SimpleKey_LongString")]
        [TestCase("key with spaces and special chars: !@#$%", TestName = "GetReplicas_SimpleKey_SpecialChars")]
        public void GetReplicas_HandlesEdgeCases_ForTextKeys(string keyValue)
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk text PRIMARY KEY, value text)");
            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { keyValue });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
        }

        [TestCase(0, TestName = "GetReplicas_SimpleKey_Int_Zero")]
        [TestCase(-1, TestName = "GetReplicas_SimpleKey_Int_Negative")]
        [TestCase(int.MaxValue, TestName = "GetReplicas_SimpleKey_Int_MaxValue")]
        [TestCase(int.MinValue, TestName = "GetReplicas_SimpleKey_Int_MinValue")]
        public void GetReplicas_HandlesEdgeCases_ForIntKeys(int keyValue)
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk int PRIMARY KEY, value text)");
            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { keyValue });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
        }

        #endregion

        #region Composite Partition Key Tests

        [TestCase("text", "int", "key1", 100, TestName = "GetReplicas_CompositeKey_TextInt")]
        [TestCase("int", "text", 200, "key2", TestName = "GetReplicas_CompositeKey_IntText")]
        [TestCase("bigint", "boolean", 9999999L, true, TestName = "GetReplicas_CompositeKey_BigIntBoolean")]
        [TestCase("text", "text", "pk1", "pk2", TestName = "GetReplicas_CompositeKey_TextText")]
        public void GetReplicas_ReturnsValidReplicas_ForCompositePartitionKey_TwoComponents(
            string type1, string type2, object value1, object value2)
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 {type1}, pk2 {type2}, value text, PRIMARY KEY ((pk1, pk2)))");
            var allHosts = Cluster.AllHosts();

            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { value1, value2 });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
            foreach (var replica in replicas)
            {
                Assert.IsNotNull(replica.Host);
                Assert.IsTrue(allHosts.Any(h => h.Address.Equals(replica.Host.Address)));
            }
        }

        [Test]
        public void GetReplicas_ReturnsValidReplicas_ForCompositeKey_ThreeComponents()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 text, pk2 int, pk3 bigint, value text, PRIMARY KEY ((pk1, pk2, pk3)))");
            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { "component1", 42, 9876543210L });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
        }

        [Test]
        public void GetReplicas_ReturnsValidReplicas_ForCompositeKey_FourComponents()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 text, pk2 int, pk3 boolean, pk4 double, value text, " +
                           $"PRIMARY KEY ((pk1, pk2, pk3, pk4)))");
            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { "part1", 100, false, 3.14159 });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
        }

        [Test]
        public void GetReplicas_ReturnsConsistentReplicas_ForSameCompositeKey()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 text, pk2 int, value text, PRIMARY KEY ((pk1, pk2)))");
            IReadOnlyList<object> key = new object[] { "consistency_test", 999 };

            var replicas1 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, key);
            var replicas2 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, key);
            var replicas3 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, key);

            Assert.AreEqual(replicas1.Count, replicas2.Count);
            Assert.AreEqual(replicas1.Count, replicas3.Count);
            var addresses1 = replicas1.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var addresses2 = replicas2.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var addresses3 = replicas3.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            CollectionAssert.AreEqual(addresses1, addresses2);
            CollectionAssert.AreEqual(addresses1, addresses3);
        }

        [TestCase("", 0, TestName = "GetReplicas_CompositeKey_EmptyStringAndZero")]
        [TestCase("key", int.MaxValue, TestName = "GetReplicas_CompositeKey_IntMaxValue")]
        [TestCase("key", int.MinValue, TestName = "GetReplicas_CompositeKey_IntMinValue")]
        public void GetReplicas_HandlesEdgeCases_ForCompositeKeys(string textValue, int intValue)
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 text, pk2 int, value text, PRIMARY KEY ((pk1, pk2)))");
            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { textValue, intValue });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
        }

        [Test]
        public void GetReplicas_DifferentKeys_ReturnValidReplicas()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (pk1 text, pk2 int, value text, PRIMARY KEY ((pk1, pk2)))");
            var keys = new object[][]
            {
                new object[] { "key_a", 1 },
                new object[] { "key_b", 2 },
                new object[] { "key_c", 3 },
                new object[] { "different_prefix", 1000 },
            };

            var allReplicas = keys.Select(k => Cluster.Metadata.GetReplicas(_keyspaceName, tableName, k)).ToList();

            foreach (var replicas in allReplicas)
            {
                Assert.IsNotNull(replicas);
                Assert.Greater(replicas.Count, 0);
            }
        }

        #endregion

        [Test]
        public void GetReplicas_ReturnsHosts_ForGivenPartitionKey()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (id text PRIMARY KEY, value text)");
            var allHosts = Cluster.AllHosts();

            var replicas = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { "key1" });

            Assert.IsNotNull(replicas);
            Assert.Greater(replicas.Count, 0);
            foreach (var replica in replicas)
            {
                Assert.IsNotNull(replica.Host);
                Assert.IsTrue(allHosts.Any(h => h.Address.Equals(replica.Host.Address)));
            }
        }

        [Test]
        public void GetReplicas_ReturnsSameReplicas_ForSameKey()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (id text PRIMARY KEY, value text)");
            var replicas1 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { "consistent_key" });
            var replicas2 = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, new object[] { "consistent_key" });

            Assert.AreEqual(replicas1.Count, replicas2.Count);
            var addresses1 = replicas1.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var addresses2 = replicas2.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            CollectionAssert.AreEqual(addresses1, addresses2);
        }

        #region NetworkTopologyStrategy / tablet-aware path

        [Test]
        public void GetReplicas_ReturnsCorrectReplicaCount_ForNetworkTopologyStrategy()
        {
            var dc = Cluster.AllHosts().First().Datacenter;
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            Session.Execute($"CREATE KEYSPACE {keyspaceName} WITH replication = " +
                            $"{{ 'class' : 'NetworkTopologyStrategy', '{dc}' : 2 }}");
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {keyspaceName}.{tableName} (id text PRIMARY KEY, value text)");
            var allHosts = Cluster.AllHosts();

            var replicas = Cluster.Metadata.GetReplicas(keyspaceName, tableName, new object[] { "key1" });

            Assert.IsNotNull(replicas);
            Assert.AreEqual(2, replicas.Count);
            foreach (var replica in replicas)
            {
                Assert.IsNotNull(replica.Host);
                Assert.IsTrue(allHosts.Any(h => h.Address.Equals(replica.Host.Address)));
            }
        }

        #endregion

        #region Cluster facade delegation

        [Test]
        public void GetReplicas_Cluster_DelegatesToMetadata()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            Session.Execute($"CREATE TABLE {_keyspaceName}.{tableName} (id text PRIMARY KEY, value text)");
            IReadOnlyList<object> key = new object[] { "facade_key" };

            var viaCluster = Cluster.GetReplicas(_keyspaceName, tableName, key);
            var viaMetadata = Cluster.Metadata.GetReplicas(_keyspaceName, tableName, key);

            Assert.IsNotNull(viaCluster);
            var clusterAddresses = viaCluster.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            var metadataAddresses = viaMetadata.Select(r => r.Host.Address).OrderBy(a => a.ToString()).ToList();
            CollectionAssert.AreEqual(metadataAddresses, clusterAddresses);
        }

        #endregion

        #region Argument validation

        [Test]
        public void GetReplicas_Throws_WhenKeyspaceIsNull()
        {
            NUnit.Framework.Assert.Throws<ArgumentNullException>(
                () => Cluster.Metadata.GetReplicas(null, "table", new object[] { "key" }));
        }

        [Test]
        public void GetReplicas_Throws_WhenTableIsNull()
        {
            NUnit.Framework.Assert.Throws<ArgumentNullException>(
                () => Cluster.Metadata.GetReplicas("keyspace", null, new object[] { "key" }));
        }

        [Test]
        public void GetReplicas_Throws_WhenPartitionKeyValuesIsNull()
        {
            NUnit.Framework.Assert.Throws<ArgumentNullException>(
                () => Cluster.Metadata.GetReplicas("keyspace", "table", null));
        }

        [Test]
        public void GetReplicas_Throws_WhenPartitionKeyValuesIsEmpty()
        {
            NUnit.Framework.Assert.Throws<ArgumentException>(
                () => Cluster.Metadata.GetReplicas("keyspace", "table", Array.Empty<object>()));
        }

        #endregion
    }

    // Parametrized by node count only: replication factor is a per-keyspace setting, not a
    // topology, so all RF cases for a given cluster size share a single CCM cluster instead of
    // spinning up one cluster per (nodeCount, RF) pair. We cap at 3 nodes to keep CI affordable;
    // 3 nodes already covers the structurally distinct cases (single owner, partial, all nodes).
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    [TestFixture(1)]
    [TestFixture(3)]
    public class MetadataGetReplicasReplicationFactorTests : SharedClusterTest
    {
        public MetadataGetReplicasReplicationFactorTests(int nodeCount) : base(nodeCount)
        {
        }

        [Test]
        public void GetReplicas_ReturnsReplicaCountMatchingReplicationFactor()
        {
            for (var rf = 1; rf <= AmountOfNodes; rf++)
            {
                var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant() + $"_rf{rf}_n{AmountOfNodes}";
                var tabletsClause = TestClusterManager.IsScylla ? " AND tablets = { 'enabled' : false }" : "";
                Session.Execute($"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : {rf} }}{tabletsClause}");
                var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
                Session.Execute($"CREATE TABLE {keyspaceName}.{tableName} (id text PRIMARY KEY, value text)");

                var replicas = Cluster.Metadata.GetReplicas(keyspaceName, tableName, new object[] { "key1" });

                Assert.IsNotNull(replicas);
                Assert.AreEqual(rf, replicas.Count, $"RF={rf} on {AmountOfNodes}-node cluster");
            }
        }
    }
}
