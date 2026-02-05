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
using System.Collections.Concurrent;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;
using System.Net;
using System.Collections;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class PreparedStatementsTests : SharedClusterTest
    {
        private readonly string _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();
        private const string AllTypesTableName = "all_types_table_prepared";
        private readonly List<ICluster> _privateClusterInstances = new List<ICluster>();

        protected override ICluster GetNewTemporaryCluster(Action<Builder> build = null)
        {
            var builder = ClusterBuilder()
                          .AddContactPoint(TestCluster.InitialContactPoint)
                          .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000).SetReadTimeoutMillis(22000));
            build?.Invoke(builder);
            var cluster = builder.Build();
            _privateClusterInstances.Add(cluster);
            return cluster;
        }

        public override void TearDown()
        {
            foreach (var c in _privateClusterInstances)
            {
                try
                {
                    c.Dispose();
                }
                catch
                {
                    // ignored
                }
            }
            _privateClusterInstances.Clear();
            base.TearDown();
        }

        public PreparedStatementsTests() : base(3)
        {
            //A 3 node cluster
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, AllTypesTableName));
            CreateTable(_tableName);
        }

        [Test]
        public void PreparedStatement_With_Changing_Schema()
        {
            byte[] originalResultMetadataId = null;
            // Use 2 different clusters as the prepared statement cache should be different
            using (var cluster1 = GetNewTemporaryCluster())
            using (var cluster2 = GetNewTemporaryCluster())
            {
                var session1 = cluster1.Connect();
                var session2 = cluster2.Connect();

                // Create schema and insert data
                session1.Execute(
                    "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
                session1.ChangeKeyspace("ks1");
                session2.ChangeKeyspace("ks1");
                session1.Execute("CREATE TABLE table1 (id int PRIMARY KEY, a text, c text)");
                var insertPs = session1.Prepare("INSERT INTO table1 (id, a, c) VALUES (?, ?, ?)");
                session1.Execute(insertPs.Bind(1, "a value", "c value"));

                // Prepare and execute a few requests
                var selectPs1 = session1.Prepare("SELECT * FROM table1");
                var selectPs2 = session2.Prepare("SELECT * FROM table1");

                var protocolVersion = (ProtocolVersion)session1.BinaryProtocolVersion;
                if (protocolVersion.SupportsResultMetadataId())
                {
                    originalResultMetadataId = selectPs1.ResultMetadata.ResultMetadataId;
                    Assert.That(selectPs2.ResultMetadata.ResultMetadataId, Is.EquivalentTo(selectPs1.ResultMetadata.ResultMetadataId));
                }

                for (var i = 0; i < 10; i++)
                {
                    var rs1 = session1.Execute(selectPs1.Bind());
                    var rs2 = session2.Execute(selectPs2.Bind());
                    Assert.That(rs1.Columns.Select(c => c.Name), Does.Contain("a").And.Contain("c"));
                    Assert.That(rs2.Columns.Select(c => c.Name), Does.Contain("a").And.Contain("c"));
                }

                // Alter table, adding a new column
                session1.Execute("ALTER TABLE table1 ADD b text");

                // Execute on all nodes on a single session1, causing UNPREPARE->PREPARE flow
                for (var i = 0; i < 10; i++)
                {
                    var rs1 = session1.Execute(selectPs1.Bind());
                    Assert.That(rs1.Columns.Select(c => c.Name), Does.Contain("a").And.Contain("b").And.Contain("c"));
                }

                // Execute on a different session causing metadata change
                for (var i = 0; i < 10; i++)
                {
                    var rs2 = session2.Execute(selectPs2.Bind());
                    Assert.That(rs2.Columns.Select(c => c.Name), Does.Contain("a").And.Contain("b").And.Contain("c"));
                }

                if (protocolVersion.SupportsResultMetadataId())
                {
                    // The ResultMetadataId changed and it's updated on both PreparedStatement instances
                    Assert.That(selectPs1.ResultMetadata.ResultMetadataId, Is.Not.EquivalentTo(originalResultMetadataId));
                    Assert.That(selectPs2.ResultMetadata.ResultMetadataId, Is.EquivalentTo(selectPs1.ResultMetadata.ResultMetadataId));
                }
            }
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Prepared_SetTimestamp()
        {
            var timestamp = new DateTimeOffset(1999, 12, 31, 1, 2, 3, TimeSpan.Zero);
            var id = Guid.NewGuid();
            var insertStatement = Session.Prepare(string.Format("INSERT INTO {0} (id, text_sample) VALUES (?, ?)", AllTypesTableName));
            Session.Execute(insertStatement.Bind(id, "sample text").SetTimestamp(timestamp));
            var row = Session.Execute(new SimpleStatement(string.Format("SELECT id, text_sample, writetime(text_sample) FROM {0} WHERE id = ?", AllTypesTableName), id)).First();
            Assert.NotNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParamsOrder()
        {
            var query = string.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(query);
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.LessThan))
            {
                //For older versions, there is no way to determine that my_id is actually id column
                Assert.Null(preparedStatement.RoutingIndexes);
            }
            Assert.AreEqual(preparedStatement.Variables.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, my_id", String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters()
        {
            var insertQuery = string.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new[] { 3 }, preparedStatement.RoutingIndexes);
            Assert.AreEqual(preparedStatement.Variables.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, id", String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new { my_int = 100, my_bigint = -500L, id = id, my_text = "named params ftw!" }));

            var row = Session.Execute(string.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(-500L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("named params ftw!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_Nulls()
        {
            var insertQuery = string.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new { my_bigint = (long?)null, my_int = 100, my_id = id }));

            var row = Session.Execute(string.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.IsNull(row.GetValue<long?>("bigint_sample"));
            Assert.IsNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_CaseInsensitive()
        {
            var insertQuery = string.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_TeXt, :my_int, :my_bigint, :id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);
            //The routing key is at position 3
            CollectionAssert.AreEqual(new[] { 3 }, preparedStatement.RoutingIndexes);

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new { MY_int = -100, MY_BigInt = 1511L, ID = id, MY_text = "yeah!" }));

            var row = Session.Execute(string.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(-100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(1511L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("yeah!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Paging()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, table));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var rsWithoutPaging = Session.Execute("SELECT * FROM " + table, int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var ps = Session.Prepare("SELECT * FROM " + table);
            var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rs.InnerQueueCount);

            //Use Linq to iterate through all the rows
            var allTheRowsPaged = rs.ToList();

            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Paging_Parallel()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, table));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }
            var ps = Session.Prepare(string.Format("SELECT * FROM {0} LIMIT 10000", table));
            var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
            Assert.AreEqual(pageSize, rs.GetAvailableWithoutFetching());
            var counterList = new ConcurrentBag<int>();
            Action iterate = () =>
            {
                var counter = rs.Count();
                counterList.Add(counter);
            };

            //Iterate in parallel the RowSet
            Parallel.Invoke(iterate, iterate, iterate, iterate);

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength, counterList.Sum());
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Paging_MultipleTimesOverTheSameStatement()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var times = 10;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, table));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var ps = Session.Prepare(string.Format("SELECT * FROM {0} LIMIT 10000", table));

            var counter = 0;
            for (var i = 0; i < times; i++)
            {
                var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
                Assert.AreEqual(pageSize, rs.InnerQueueCount);
                counter += rs.Count();
            }

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Manual_Paging()
        {
            const int pageSize = 15;
            const int totalRowLength = 20;
            var table = "tbl" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, table));
            var insertPs = Session.Prepare(string.Format("INSERT INTO {0} (id) VALUES (?)", table));
            //Insert the rows
            TestHelper.Invoke(() => Session.Execute(insertPs.Bind(Guid.NewGuid())), totalRowLength);

            var ps = Session.Prepare(string.Format("SELECT * FROM {0} LIMIT 10000", table));
            var rs = Session.Execute(ps.Bind().SetAutoPage(false).SetPageSize(pageSize));
            Assert.False(rs.AutoPage);
            Assert.NotNull(rs.PagingState);
            //Dequeue all via Linq
            var ids = rs.Select(r => r.GetValue<Guid>("id")).ToList();
            Assert.AreEqual(pageSize, ids.Count);
            //Retrieve the next page
            var rs2 = Session.Execute(ps.Bind().SetAutoPage(false).SetPagingState(rs.PagingState));
            Assert.Null(rs2.PagingState);
            var ids2 = rs2.Select(r => r.GetValue<Guid>("id")).ToList();
            Assert.AreEqual(totalRowLength - pageSize, ids2.Count);
            Assert.AreEqual(totalRowLength, ids.Union(ids2).Count());
        }

        [Test, TestTimeout(180000)]
        public void Bound_With_ChangingKeyspace()
        {
            using (var localCluster = ClusterBuilder()
                .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(15000))
                .AddContactPoint(TestCluster.InitialContactPoint)
                .Build())
            {
                var session = localCluster.Connect("system");
                session.Execute("CREATE KEYSPACE bound_changeks_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
                TestUtils.WaitForSchemaAgreement(localCluster);
                var ps = session.Prepare("SELECT * FROM system.local WHERE key='local'");
                session.ChangeKeyspace("bound_changeks_test");
                Assert.DoesNotThrow(() => TestHelper.Invoke(() => session.Execute(ps.Bind()), 10));
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_With_Named_Parameters_Routing_Key()
        {
            Func<string, string, byte[]> calculateKey = (id1, id2) =>
            {
                return new byte[0]
                .Concat(new byte[] { 0, (byte)id1.Length })
                .Concat(System.Text.Encoding.UTF8.GetBytes(id1))
                .Concat(new byte[] { 0 })
                .Concat(new byte[] { 0, (byte)id2.Length })
                .Concat(System.Text.Encoding.UTF8.GetBytes(id2))
                .Concat(new byte[] { 0 }).ToArray();
            };
            Session.Execute("CREATE TABLE tbl_ps_multiple_pk_named (a uuid, b text, c text, d text, primary key ((a, b), c))");
            Thread.Sleep(3000);
            var ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk_named WHERE a = :a AND b = :b AND c = :ce");
            //Parameters at position 1 and 0 are part of the routing key
            CollectionAssert.AreEqual(new[] { 0, 1 }, ps.RoutingIndexes);
            var anon = new { ce = "hello ce2", a = "aValue1", b = "bValue1" };
            var statement = ps.Bind(anon);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon.a, anon.b), statement.RoutingKey.RawRoutingKey);
            //Now with another parameters
            anon = new { ce = "hello ce2", a = "aValue2", b = "bValue2" };
            statement = ps.Bind(anon);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon.a, anon.b), statement.RoutingKey.RawRoutingKey);

            //With another query, named parameters are different
            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk_named WHERE b = :nice_name_b AND a = :nice_name_a AND c = :nice_name_c");
            //Parameters names are different from partition keys
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.LessThan))
            {
                //For older versions, there is no way to determine that nice_name_a is actually partition column
                Assert.Null(ps.RoutingIndexes);
            }
            ps.SetRoutingNames("nice_name_a", "nice_name_b");
            var anon2 = new { nice_name_b = "b", nice_name_a = "a", nice_name_c = "c" };
            statement = ps.Bind(anon2);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon2.nice_name_a, anon2.nice_name_b), statement.RoutingKey.RawRoutingKey);
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void Bound_Date_Tests()
        {
            Session.Execute("CREATE TABLE tbl_date_prep (id int PRIMARY KEY, v date)");
            var insert = Session.Prepare("INSERT INTO tbl_date_prep (id, v) VALUES (?, ?)");
            var select = Session.Prepare("SELECT * FROM tbl_date_prep WHERE id = ?");
            var values = new[] { new LocalDate(2010, 4, 29), new LocalDate(0, 1, 1), new LocalDate(-1, 12, 31) };
            var index = 0;
            foreach (var v in values)
            {
                Session.Execute(insert.Bind(index, v));
                var rs = Session.Execute(select.Bind(index)).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<LocalDate>("v"));
                index++;
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void Bound_Time_Tests()
        {
            Session.Execute("CREATE TABLE tbl_time_prep (id int PRIMARY KEY, v time)");
            var insert = Session.Prepare("INSERT INTO tbl_time_prep (id, v) VALUES (?, ?)");
            var select = Session.Prepare("SELECT * FROM tbl_time_prep WHERE id = ?");
            var values = new[] { new LocalTime(0, 0, 0, 0), new LocalTime(12, 11, 1, 10), new LocalTime(0, 58, 31, 991809111) };
            var index = 0;
            foreach (var v in values)
            {
                Session.Execute(insert.Bind(index, v));
                var rs = Session.Execute(select.Bind(index)).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<LocalTime>("v"));
                index++;
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void Bound_SmallInt_Tests()
        {
            Session.Execute("CREATE TABLE tbl_smallint_prep (id int PRIMARY KEY, v smallint)");
            var insert = Session.Prepare("INSERT INTO tbl_smallint_prep (id, v) VALUES (?, ?)");
            var select = Session.Prepare("SELECT * FROM tbl_smallint_prep WHERE id = ?");
            var values = new short[] { Int16.MinValue, -31000, -1, 0, 1, 2, 0xff, 0x0101, Int16.MaxValue };
            foreach (var v in values)
            {
                Session.Execute(insert.Bind(Convert.ToInt32(v), v));
                var rs = Session.Execute(select.Bind(Convert.ToInt32(v))).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<short>("v"));
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void Bound_TinyInt_Tests()
        {
            Session.Execute("CREATE TABLE tbl_tinyint_prep (id int PRIMARY KEY, v tinyint)");
            var insert = Session.Prepare("INSERT INTO tbl_tinyint_prep (id, v) VALUES (?, ?)");
            var select = Session.Prepare("SELECT * FROM tbl_tinyint_prep WHERE id = ?");
            var values = new sbyte[] { sbyte.MinValue, -4, -1, 0, 1, 2, 126, sbyte.MaxValue };
            foreach (var v in values)
            {
                Session.Execute(insert.Bind(Convert.ToInt32(v), v));
                var rs = Session.Execute(select.Bind(Convert.ToInt32(v))).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<sbyte>("v"));
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        [TestCassandraVersion(4, 0)]
        public void Session_Prepare_With_Keyspace_Defined_On_Protocol_Greater_Than_4(bool usePayload)
        {
            if (Session.Cluster.Metadata.ControlConnection.Serializer.CurrentProtocolVersion < ProtocolVersion.V5)
            {
                Assert.Ignore("This test requires protocol v5+");
                return;
            }

            Assert.AreNotEqual("system", Session.Keyspace);
            PreparedStatement ps;
            if (!usePayload)
            {
                ps = Session.Prepare("SELECT key FROM local", "system");
            }
            else
            {
                ps = Session.Prepare("SELECT key FROM local", "system",
                    new Dictionary<string, byte[]> { { "a", new byte[] { 0, 0, 0, 1 } } });
            }
            Assert.AreEqual("system", ps.Keyspace);

            for (var i = 0; i < Cluster.AllHosts().Count; i++)
            {
                var boundStatement = ps.Bind();
                Assert.AreEqual("system", boundStatement.Keyspace);
                var row = Session.Execute(boundStatement).First();
                Assert.NotNull(row.GetValue<string>("key"));
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        [TestCassandraVersion(4, 0)]
        public async Task Session_PrepareAsync_With_Keyspace_Defined_On_Protocol_Greater_Than_4(bool usePayload)
        {
            if (Session.Cluster.Metadata.ControlConnection.Serializer.CurrentProtocolVersion < ProtocolVersion.V5)
            {
                Assert.Ignore("This test requires protocol v5+");
                return;
            }

            Assert.AreNotEqual("system", Session.Keyspace);
            PreparedStatement ps;
            if (!usePayload)
            {
                ps = await Session.PrepareAsync("SELECT key FROM local", "system").ConfigureAwait(false);
            }
            else
            {
                ps = await Session.PrepareAsync("SELECT key FROM local", "system",
                    new Dictionary<string, byte[]> { { "a", new byte[] { 0, 0, 0, 1 } } }).ConfigureAwait(false);
            }
            Assert.AreEqual("system", ps.Keyspace);

            await TestHelper.TimesLimit(async () =>
            {
                var boundStatement = ps.Bind();
                Assert.AreEqual("system", boundStatement.Keyspace);
                var rs = await Session.ExecuteAsync(boundStatement).ConfigureAwait(false);
                Assert.NotNull(rs.First().GetValue<string>("key"));
                return rs;
            }, Cluster.AllHosts().Count, Cluster.AllHosts().Count).ConfigureAwait(false);
        }

        [Test]
        [TestCassandraVersion(4, 0)]
        public void Session_Prepare_With_Keyspace_Defined_On_Protocol_V4()
        {
            TestKeyspaceInPrepareNotSupported(true);
        }

        [Test]
        [TestCassandraVersion(4, 0, Comparison.LessThan)]
        public void Session_Prepare_With_Keyspace_Defined_On_Previuos_Cassandra_Versions()
        {
            TestKeyspaceInPrepareNotSupported(false);
        }

        private void TestKeyspaceInPrepareNotSupported(bool specifyProtocol)
        {
            using (var cluster = GetNewTemporaryCluster(builder =>
            {
                if (specifyProtocol)
                {
                    builder.WithMaxProtocolVersion(ProtocolVersion.V4);
                }
            }))
            {
                var session = cluster.Connect(KeyspaceName);

                // Specifying the keyspace on lower protocol versions is explicitly not supported
                Assert.Throws<NotSupportedException>(() => session.Prepare("SELECT key FROM local", "system"));
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Prepared_With_Composite_Routing_Key()
        {
            Session.Execute("CREATE TABLE tbl_ps_multiple_pk (a uuid, b text, c text, d text, primary key ((a, b), c))");
            // Parameters at position 2 and 0 are part of the routing key
            var ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = ? AND c = ? AND a = ?");
            CollectionAssert.AreEqual(new[] { 2, 0 }, ps.RoutingIndexes);
            Assert.AreEqual(ps.Keyspace, KeyspaceName);
            var bound = ps.Bind("a", "c", Guid.NewGuid());
            Assert.NotNull(bound.RoutingKey);
            Assert.AreEqual(bound.Keyspace, KeyspaceName);

            // The same as before but with keyspace specified
            var fullyQualifiedQuery = string.Format(
                "SELECT * FROM {0}.tbl_ps_multiple_pk WHERE b = ? AND c = ? AND a = ?", KeyspaceName);
            ps = Session.Prepare(fullyQualifiedQuery);
            bound = ps.Bind("a", "c", Guid.NewGuid());
            Assert.NotNull(bound.RoutingKey);
            Assert.AreEqual(bound.Keyspace, KeyspaceName);
            Assert.AreEqual(KeyspaceName, bound.Keyspace);

            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = :b AND a = :a AND c = :ce");
            // Parameters at position 1 and 0 are part of the routing key
            CollectionAssert.AreEqual(new[] { 1, 0 }, ps.RoutingIndexes);
            bound = ps.Bind("a", Guid.NewGuid());
            Assert.NotNull(bound.RoutingKey);
            Assert.AreEqual(bound.Keyspace, KeyspaceName);

            // Parameters names are different from partition keys
            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = :nice_name1 AND a = :nice_name2 AND c = :nice_name3");
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.LessThan))
            {
                //For older versions, there is no way to determine that nice_name_a is actually partition column
                Assert.Null(ps.RoutingIndexes);
            }

            // using a different session, not using any keyspace
            var otherSession = Cluster.Connect();
            ps = otherSession.Prepare(fullyQualifiedQuery);
            // It was not prepared in any keyspace, so it should be null
            // edit: after CASSANDRA-17248 and/or CASSANDRA-15252, this can be the qualified keyspace
            Assert.IsTrue(ps.Keyspace == null || ps.Keyspace == KeyspaceName, ps.Keyspace);
            bound = ps.Bind("a", "c", Guid.NewGuid());
            Assert.AreEqual(bound.Keyspace, KeyspaceName);
        }

        [Test]
        public void Prepared_SelectOne()
        {
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"
                    CREATE TABLE {0}(
                    tweet_id int PRIMARY KEY,
                    numb double,
                    label text);", tableName));
                TestUtils.WaitForSchemaAgreement(Session.Cluster);
            }
            catch (AlreadyExistsException)
            {
            }

            for (int i = 0; i < 10; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (tweet_id, numb, label) VALUES({1}, 0.01,'{2}')", tableName, i, "row" + i));
            }

            var prepSelect = QueryTools.PrepareQuery(Session, string.Format("SELECT * FROM {0} WHERE tweet_id = ?;", tableName));

            var rowId = 5;
            var result = QueryTools.ExecutePreparedSelectQuery(Session, prepSelect, new object[] { rowId });
            foreach (var row in result)
            {
                Assert.True((string)row.GetValue(typeof(int), "label") == "row" + rowId);
            }
            Assert.True(result.Columns != null);
            Assert.True(result.Columns.Length == 3);
        }

        [Test]
        public void Prepared_IpAddress()
        {
            InsertingSingleValuePrepared(typeof(IPAddress));
        }

        //////////////////////////////
        // Test Helpers
        //////////////////////////////

        private void AssertValid(ISession session, PreparedStatement ps, object value)
        {
            try
            {
                session.Execute(ps.Bind(Guid.NewGuid(), value));
            }
            catch (Exception e)
            {
                string assertFailMsg = string.Format("Exception was thrown, but shouldn't have been! \nException message: {0}, Exception StackTrace: {1}", e.Message, e.StackTrace);
                Assert.Fail(assertFailMsg);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_With_Unprepared_Flow()
        {
            // It should be unprepared on some of the nodes, we use a different table from the rest of the tests
            CreateTable("tbl_unprepared_flow");

            // Use a dedicated cluster and table
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false)).Build())
            {
                var session = cluster.Connect(KeyspaceName);
                var ps1 = session.Prepare("INSERT INTO tbl_unprepared_flow (id, label) VALUES (?, ?)");
                var ps2 = session.Prepare("UPDATE tbl_unprepared_flow SET label = ? WHERE id = ?");
                session.Execute(new BatchStatement()
                    .Add(ps1.Bind(1, "label1_u"))
                    .Add(ps2.Bind("label2_u", 2)));
                // Execute in multiple nodes
                session.Execute(new BatchStatement()
                    .Add(ps1.Bind(3, "label3_u"))
                    .Add(ps2.Bind("label4_u", 4)));
                var result = session.Execute("SELECT id, label FROM tbl_unprepared_flow")
                                    .Select(r => new object[] { r.GetValue<int>(0), r.GetValue<string>(1) })
                                    .OrderBy(arr => (int)arr[0])
                                    .ToArray();
                Assert.AreEqual(Enumerable.Range(1, 4).Select(i => new object[] { i, $"label{i}_u" }), result);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0, Comparison.Equal)]
        public void Batch_PreparedStatements_FlagsNotSupportedInC2_0()
        {
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            batch.Add(ps.Bind(1, "label1", 1));
            Assert.Throws<NotSupportedException>(() => Session.Execute(batch.SetTimestamp(DateTime.Now)));
        }

        [Test]
        [TestCassandraVersion(1, 9, Comparison.LessThan)]
        public void Batch_PreparedStatements_NotSupportedInC1_2()
        {
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            batch.Add(ps.Bind(1, "label1", 1));
            try
            {
                Session.Execute(batch);
                Assert.Fail("Cassandra version below 2.0, should not execute batches of prepared statements");
            }
            catch (NotSupportedException ex)
            {
                //This is OK
                Assert.True(ex.Message.ToLower().Contains("batch"));
            }
        }

        [Test]
        [TestCassandraVersion(4, 0)]
        public void BatchStatement_With_Keyspace_Defined_On_Protocol_Greater_Than_4()
        {
            using (var cluster = ClusterBuilder().AddContactPoint(TestClusterManager.InitialContactPoint).Build())
            {
                if (cluster.Metadata.ControlConnection.Serializer.CurrentProtocolVersion < ProtocolVersion.V5)
                {
                    Assert.Ignore("This test requires protocol v5+");
                    return;
                }
                var session = cluster.Connect("system");
                var value = new Random().Next();
                var query = new SimpleStatement($@"INSERT INTO {_tableName} (id, number) VALUES (?, ?)", -1000, value);

                // Use the keyspace specified in the BatchStatement
                var batchStatement = new BatchStatement().Add(query).SetKeyspace(KeyspaceName);
                Assert.AreEqual(KeyspaceName, batchStatement.Keyspace);
                session.Execute(batchStatement);
                var selectStatement = new SimpleStatement(
                    $"SELECT number FROM {KeyspaceName}.{_tableName} WHERE id = ?", -1000);
                var row = session.Execute(selectStatement).First();
                Assert.AreEqual(value, row.GetValue<int>("number"));
            }
        }

        [Test]
        [TestCassandraVersion(4, 0, Comparison.LessThan)]
        public void BatchStatement_With_Keyspace_Defined_On_Lower_Protocol_Versions()
        {
            using (var cluster = GetNewTemporaryCluster())
            {
                var session = cluster.Connect("system");
                var query = new SimpleStatement(
                    $@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)", -1000, "label", 1);

                // It should fail as the keyspace from the session will be used
                Assert.Throws<InvalidQueryException>(() =>
                    session.Execute(new BatchStatement().Add(query).SetKeyspace(KeyspaceName)));
            }
        }

        /// <summary>
        /// This test relies on CASSANDRA-15252 to reproduce the error condition. If it gets fixed in
        /// Cassandra, we'll need to add a version restriction.
        /// See https://issues.apache.org/jira/browse/CASSANDRA-15252.
        /// </summary>
        [Test]
        public void Should_FailFast_When_PreparedStatementIdChangesOnReprepare()
        {
            if (TestClusterManager.CheckCassandraVersion(false, new Version(3, 0, 0), Comparison.GreaterThanOrEqualsTo) &&
                TestClusterManager.CheckCassandraVersion(false, new Version(3, 11, 0), Comparison.LessThan))
            {
                if (TestClusterManager.CheckCassandraVersion(false, new Version(3, 0, 26), Comparison.GreaterThanOrEqualsTo))
                {
                    Assert.Ignore("This test relies on a bug that is fixed in this server version.");
                    return;
                }
            }

            if (TestClusterManager.CheckCassandraVersion(false, new Version(3, 11, 12), Comparison.GreaterThanOrEqualsTo))
            {
                Assert.Ignore("This test relies on a bug that is fixed in this server version.");
                return;
            }

            var tableName = TestUtils.GetUniqueTableName();
            using (var cluster =
                   GetNewTemporaryCluster(builder => builder.WithQueryTimeout(500000)))
            {
                var session = cluster.Connect();
                session.Execute($"CREATE TABLE {KeyspaceName}.{tableName} (a int PRIMARY KEY, b int, c int)");
                var ps = session.Prepare($"SELECT * FROM {KeyspaceName}.{tableName} WHERE a = ?");
                session.ChangeKeyspace(KeyspaceName);
                session.Execute($"DROP TABLE {tableName}");
                session.Execute($"CREATE TABLE {tableName} (a int PRIMARY KEY, b int, c int)");
                var ex = Assert.Throws<PreparedStatementIdMismatchException>(
                    () => session.Execute(ps.Bind(1)));
                Assert.IsTrue(ex.Id.SequenceEqual(ps.Id));
                Assert.IsTrue(ex.Message.Contains("ID mismatch while trying to reprepare"));
                Assert.IsTrue(ex.Message.Contains($"expected {BitConverter.ToString(ps.Id).Replace("-", "")}"));
            }
        }

        public void InsertingSingleValuePrepared(Type tp, object value = null)
        {
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            var tableName = "table" + Guid.NewGuid().ToString("N");

            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                tweet_id uuid PRIMARY KEY,
                value {1}
                );", tableName, cassandraDataTypeName));

            TestUtils.WaitForSchemaAgreement(Session.Cluster);

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof(string))
                val = "'" + val.ToString().Replace("'", "''") + "'";

            var row1 = new[] { Guid.NewGuid(), val };

            toInsert.Add(row1);

            var prep = QueryTools.PrepareQuery(Session,
                                                             string.Format("INSERT INTO {0}(tweet_id, value) VALUES ({1}, ?);", tableName,
                                                                           toInsert[0][0]));
            if (value == null)
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new[] { toInsert[0][1] });
            }
            else
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new[] { value });
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert);
        }

        private void CreateTable(string tableName)
        {
            CreateTable(Session, tableName);
        }

        private void CreateTable(ISession session, string tableName)
        {
            QueryTools.ExecuteSyncNonQuery(session, $@"CREATE TABLE {tableName}(
                                                                id int PRIMARY KEY,
                                                                label text,
                                                                number int
                                                                );");
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }
    }
}
