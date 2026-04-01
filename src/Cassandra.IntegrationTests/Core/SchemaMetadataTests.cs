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

using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class SchemaMetadataTests : SharedClusterTest
    {
        private const int DefaultNodeCount = 3;

        [Test]
        public void SchemaMetadata_GetKeyspaceThatDoesNotExist()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var metadata = testCluster.Cluster.Metadata;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();

            KeyspaceMetadata ksmd = metadata.GetKeyspace(keyspaceName);
            Assert.IsNull(ksmd, $"Keyspace '{keyspaceName}' should not exist in cluster metadata");
        }

        [Test]
        public void SchemaMetadata_GetTables()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            string tableName1 = TestUtils.GetUniqueTableName().ToLower();
            string tableName2 = TestUtils.GetUniqueTableName().ToLower();
            string tableName3 = TestUtils.GetUniqueTableName().ToLower();
            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(1),
                true
            );

            Assert.NotNull(session.Cluster.Metadata.GetKeyspace(keyspaceName),
                $"Keyspace '{keyspaceName}' should be available in metadata after creation");

            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName1} (a int, b int, c text, primary key (a, b))");
            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName2} (a int, b int, c text, primary key (a, b))");
            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName3} (a int, b int, c text, primary key (a, b))");

            // Fetch tables from metadata
            var tables = session.Cluster.Metadata.GetTables(keyspaceName);
            Assert.IsNotNull(tables);
            Assert.AreEqual(3, tables.Count, $"GetTables() should return 3 tables for keyspace '{keyspaceName}'");
            Assert.IsTrue(tables.Contains(tableName1), $"GetTables() should contain table '{tableName1}'");
            Assert.IsTrue(tables.Contains(tableName2), $"GetTables() should contain table '{tableName2}'");
            Assert.IsTrue(tables.Contains(tableName3), $"GetTables() should contain table '{tableName3}'");

            // Fetch tables from keyspace metadata
            var keyspaceMetadata = session.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.IsNotNull(keyspaceMetadata, $"Keyspace metadata for '{keyspaceName}' should not be null");
            var tablesFromKeyspace = keyspaceMetadata.GetTablesNames();
            Assert.IsNotNull(tablesFromKeyspace);
            Assert.AreEqual(3, tablesFromKeyspace.Count, $"GetTablesNames() should return 3 tables for keyspace '{keyspaceName}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName1), $"GetTablesNames() should contain table '{tableName1}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName2), $"GetTablesNames() should contain table '{tableName2}'");
            Assert.IsTrue(tablesFromKeyspace.Contains(tableName3), $"GetTablesNames() should contain table '{tableName3}'");
        }

        [Test]
        public void GetTable_With_Keyspace_And_Table_Not_Found()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;

            var table1 = session.Cluster.Metadata.GetTable("ks_does_not_exist", "t1");
            Assert.Null(table1);
            var table2 = session.Cluster.Metadata.GetTable("system", "table_does_not_exist");
            Assert.Null(table2);
        }

        private void CheckTableMetadataColumns(TableMetadata tableMetadata, string expectedTableName, string expectedKeyspaceName, string[] expectedColumnNames)
        {
            Assert.AreEqual(expectedColumnNames.Length, tableMetadata.TableColumns.Length, $"Table '{expectedKeyspaceName}.{expectedTableName}' should have {expectedColumnNames.Length} columns in metadata");
            foreach (string columnName in expectedColumnNames)
            {
                var column = tableMetadata.TableColumns.FirstOrDefault(c => c.Name == columnName);
                Assert.IsNotNull(column, $"Column '{columnName}' should be present in metadata for table '{expectedKeyspaceName}.{expectedTableName}'");
                Assert.IsTrue(tableMetadata.ColumnsByName.ContainsKey(columnName),
                                $"ColumnsByName should contain key '{columnName}' for table '{expectedKeyspaceName}.{expectedTableName}'");
                Assert.AreEqual(column, tableMetadata.ColumnsByName[columnName],
                                $"ColumnsByName should return the correct column metadata for column '{columnName}' in table '{expectedKeyspaceName}.{expectedTableName}'");
                Assert.AreEqual(expectedTableName, column.Table,
                                $"Column '{columnName}' should have table name '{expectedTableName}' in metadata");
                Assert.AreEqual(expectedKeyspaceName, column.Keyspace,
                                $"Column '{columnName}' should have keyspace name '{expectedKeyspaceName}' in metadata");
                Assert.AreEqual(columnName, column.Name,
                                $"Column name should be '{columnName}' in metadata for table '{expectedKeyspaceName}.{expectedTableName}'");
            }
        }

        [Test]
        public void SchemaMetadata_GetTableMetadata_Partition_Keys()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(1),
                true
            );

            Assert.NotNull(session.Cluster.Metadata.GetKeyspace(keyspaceName),
                $"Keyspace '{keyspaceName}' should be available in metadata after creation");

            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName} (a int, b int, c text, primary key ((a, b), c))");

            // Fetch table metadata
            var tableMetadata = session.Cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.IsNotNull(tableMetadata, $"Table metadata for '{keyspaceName}.{tableName}' should not be null");
            Assert.AreEqual(tableName, tableMetadata.Name, $"Table name in metadata should be '{tableName}'");

            CheckTableMetadataColumns(tableMetadata, tableName, keyspaceName, new[] { "a", "b", "c" });

            // Verify partition keys
            Assert.AreEqual(2, tableMetadata.PartitionKeys.Length, $"Table '{keyspaceName}.{tableName}' should have 2 partition keys in metadata");
            Assert.IsTrue(tableMetadata.PartitionKeys.Any(pk => pk.Name == "a"), $"Partition keys should contain 'a' for table '{keyspaceName}.{tableName}'");
            Assert.IsTrue(tableMetadata.PartitionKeys.Any(pk => pk.Name == "b"), $"Partition keys should contain 'b' for table '{keyspaceName}.{tableName}'");
        }

        [Test]
        public void SchemaMetadata_TableColumns_Types_And_Flags()
        {
            ITestCluster testCluster = TestClusterManager.CreateNew(DefaultNodeCount);
            testCluster.InitClient();
            var session = testCluster.Session;

            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            string tableName = TestUtils.GetUniqueTableName().ToLower();

            session.CreateKeyspace(
                keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(1),
                true
            );

            Assert.NotNull(session.Cluster.Metadata.GetKeyspace(keyspaceName),
                $"Keyspace '{keyspaceName}' should be available in metadata after creation");

            session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName} (pk int, ck int, s int static, fl frozen<list<text>>, t text, primary key (pk, ck))");

            var tableMetadata = session.Cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.IsNotNull(tableMetadata, $"Table metadata for '{keyspaceName}.{tableName}' should not be null");
            Assert.AreEqual(tableName, tableMetadata.Name, $"Table name in metadata should be '{tableName}'");

            CheckTableMetadataColumns(tableMetadata, tableName, keyspaceName, new[] { "pk", "ck", "s", "fl", "t" });

            var columns = tableMetadata.TableColumns.ToDictionary(c => c.Name);

            Assert.IsNull(columns["pk"].TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, columns["pk"].TypeCode);
            Assert.False(columns["pk"].IsStatic);

            Assert.IsNull(columns["ck"].TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, columns["ck"].TypeCode);
            Assert.True(columns["s"].IsStatic, "Static column should have IsStatic=true");

            var frozenList = columns["fl"];
            Assert.True(frozenList.IsFrozen, "Frozen collection should have IsFrozen=true");
            Assert.NotNull(frozenList.TypeInfo, "Frozen collection should include element type info");
            Assert.IsInstanceOf<ListColumnInfo>(frozenList.TypeInfo);
            var listInfo = (ListColumnInfo)frozenList.TypeInfo;
            Assert.AreEqual(ColumnTypeCode.Text, listInfo.ValueTypeCode, "List element type should be text");

            Assert.AreEqual(ColumnTypeCode.Text, columns["t"].TypeCode);
            Assert.IsNull(columns["t"].TypeInfo);
        }
    }
}
