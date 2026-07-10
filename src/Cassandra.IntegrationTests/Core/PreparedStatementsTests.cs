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
        public void Bound_AllSingleTypesDifferentValues()
        {
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample,
                    blob_sample, boolean_sample, timestamp_sample, inet_sample)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new[] { 0 }, preparedStatement.RoutingIndexes);

            var firstRowValues = new object[]
            {
                Guid.NewGuid(), "first", 10, Int64.MaxValue - 1, 1.999F, 32.002D, 1.101010M,
                new byte[] {255, 255}, true, new DateTimeOffset(new DateTime(2005, 8, 5)), new IPAddress(new byte[] {192, 168, 0, 100})
            };
            var secondRowValues = new object[]
            {
                Guid.NewGuid(), "second", 0, 0L, 0F, 0D, 0M,
                new byte[] {0, 0}, true, new DateTimeOffset(new DateTime(1970, 9, 18)), new IPAddress(new byte[] {0, 0, 0, 0})
            };
            var thirdRowValues = new object[]
            {
                Guid.NewGuid(), "third", -100, Int64.MinValue + 1, -150.111F, -5.12342D, -8.101010M,
                new byte[] {1, 1}, true, new DateTimeOffset(new DateTime(1543, 5, 24)), new IPAddress(new byte[] {255, 128, 12, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
            };

            Session.Execute(preparedStatement.Bind(firstRowValues));
            Session.Execute(preparedStatement.Bind(secondRowValues));
            Session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = string.Format(@"
            SELECT
                id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample,
                    blob_sample, boolean_sample, timestamp_sample, inet_sample
            FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = Session.Execute(selectQuery).ToList();
            //Check that they were inserted and retrieved
            Assert.AreEqual(3, rowList.Count);

            //Create a dictionary with the inserted values to compare with the retrieved values
            var insertedValues = new Dictionary<Guid, object[]>()
            {
                {(Guid)firstRowValues[0], firstRowValues},
                {(Guid)secondRowValues[0], secondRowValues},
                {(Guid)thirdRowValues[0], thirdRowValues}
            };

            foreach (var retrievedRow in rowList)
            {
                var inserted = insertedValues[retrievedRow.GetValue<Guid>("id")];
                for (var i = 0; i < inserted.Length; i++)
                {
                    var insertedValue = inserted[i];
                    var retrievedValue = retrievedRow[i];
                    Assert.AreEqual(insertedValue, retrievedValue);
                }
            }
        }

        [Test]
        public void Bound_AllSingleTypesNullValues()
        {
            const string columns = "id, text_sample, int_sample, bigint_sample, float_sample, double_sample, " +
                                   "decimal_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample";
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                ({1})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName, columns);

            var preparedStatement = Session.Prepare(insertQuery);
            Assert.AreEqual(columns, String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));
            var nullRowValues = new object[]
            {
                Guid.NewGuid(), null, null, null, null, null, null, null, null, null, null
            };

            Session.Execute(preparedStatement.Bind(nullRowValues));

            var rs = Session.Execute(string.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, nullRowValues[0]));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(1, row.Count(v => v != null));
            Assert.IsTrue(row.Count(v => v == null) > 5, "The rest of the row values must be null");
        }

        [Test]
        public void Bound_String_Empty()
        {
            const string columns = "id, text_sample";
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                ({1})
                VALUES (?, ?)", AllTypesTableName, columns);

            var preparedStatement = Session.Prepare(insertQuery);
            Assert.AreEqual(columns, String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));
            var nullRowValues = new object[]
            {
                Guid.NewGuid(), ""
            };

            Session.Execute(preparedStatement.Bind(nullRowValues));

            var rs = Session.Execute(string.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, nullRowValues[0]));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual("", row.GetValue<string>("text_sample"));
        }

        [Test]
        public void PreparedStatement_With_Changing_Schema()
        {
            // Use 2 different clusters as the prepared statement cache should be different
            using (var cluster1 = GetNewTemporaryCluster())
            using (var cluster2 = GetNewTemporaryCluster())
            {
                var session1 = cluster1.Connect();
                var session2 = cluster2.Connect();

                // Create schema and insert data
                session1.Execute(
                    "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 1}");
                session1.ChangeKeyspace("ks1");
                session2.ChangeKeyspace("ks1");
                session1.Execute("CREATE TABLE table1 (id int PRIMARY KEY, a text, c text)");
                var insertPs = session1.Prepare("INSERT INTO table1 (id, a, c) VALUES (?, ?, ?)");
                session1.Execute(insertPs.Bind(1, "a value", "c value"));

                // Prepare and execute a few requests
                var selectPs1 = session1.Prepare("SELECT * FROM table1");
                var selectPs2 = session2.Prepare("SELECT * FROM table1");

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
            }
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Bound_Unset_Specified_Tests()
        {
            const string columns = "id, text_sample, int_sample";
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                ({1})
                VALUES (?, ?, ?)", AllTypesTableName, columns);

            var preparedStatement = Session.Prepare(insertQuery);
            Assert.AreEqual(columns, String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));
            var id = Guid.NewGuid();

            Session.Execute(preparedStatement.Bind(id, Unset.Value, Unset.Value));

            var rs = Session.Execute(string.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, id));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(id, row.GetValue<Guid>("id"));
            Assert.Null(row.GetValue<string>("text_sample"));
            Assert.Null(row.GetValue<int?>("int_sample"));
        }

        /// Test for implicit UNSET values
        ///
        /// Bound_Unset_Not_Specified_Tests tests that implicit UNSET values are properly inserted by the driver when there are
        /// missing parameters in a bound statement. It first creates a prepared statement with three parameters. If run on a Cassandra
        /// version less than 2.2, it verifies that binding only a subset of the parameters with arguments raises an InvalidQueryException.
        /// If run on a Cassandra version greater than or equal to 2.2, it verifies that binding less than the required number of parameters
        /// causes the driver to implicitly insert UNSET values into the missing parameters.
        ///
        /// @since 3.0.0
        /// @jira_ticket CSHARP-356
        /// @expected_result In Cassandra &lt; 2.2 should throw an error, while in Cassandra >= 2.2 the driver should set UNSET values.
        ///
        /// @test_category data_types:unset
        [Test]
        public void Bound_Unset_Not_Specified_Tests()
        {
            const string columns = "id, text_sample, int_sample";
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                ({1})
                VALUES (?, ?, ?)", AllTypesTableName, columns);

            var preparedStatement = Session.Prepare(insertQuery);
            Assert.AreEqual(columns, String.Join(", ", preparedStatement.Variables.Columns.Select(c => c.Name)));
            var id = Guid.NewGuid();

            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.LessThan))
            {
                //For previous Cassandra versions, all parameters must be specified
                Assert.Throws<InvalidQueryException>(() => Session.Execute(preparedStatement.Bind(id)));
                return;
            }
            // Bind just 1 value, the others should be set automatically to "Unset"
            Session.Execute(preparedStatement.Bind(id));

            var rs = Session.Execute(string.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, id));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(id, row.GetValue<Guid>("id"));
            Assert.Null(row.GetValue<string>("text_sample"));
            Assert.Null(row.GetValue<int?>("int_sample"));
        }

        private void Check_Expected(PreparedStatement select, object[] expected)
        {
            var row = Session.Execute(select.Bind(0)).First();
            Assert.IsNotNull(row);
            Assert.AreEqual(expected[1], row.GetValue<int?>("v0"));
            Assert.AreEqual(expected[2], row.GetValue<int?>("v1"));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Bound_Unset_Values_Tests()
        {
            Session.Execute("CREATE TABLE IF NOT EXISTS test_unset_values (k int PRIMARY KEY, v0 int, v1 int)");
            var insert = Session.Prepare("INSERT INTO test_unset_values (k, v0, v1) VALUES (?, ?, ?)");
            var select = Session.Prepare("SELECT * FROM test_unset_values WHERE k=?");

            // initial condition
            Session.Execute(insert.Bind(0, 0, 0));
            Check_Expected(select, new object[] { 0, 0, 0 });

            // explicit unset
            Session.Execute(insert.Bind(0, 1, Unset.Value));
            Check_Expected(select, new object[] { 0, 1, 0 });
            Session.Execute(insert.Bind(0, Unset.Value, 2));
            Check_Expected(select, new object[] { 0, 1, 2 });

            Session.Execute(insert.Bind(new { k = 0, v0 = 3, v1 = Unset.Value }));
            Check_Expected(select, new object[] { 0, 3, 2 });
            Session.Execute(insert.Bind(new { k = 0, v0 = Unset.Value, v1 = 4 }));
            Check_Expected(select, new object[] { 0, 3, 4 });

            // nulls still work
            Session.Execute(insert.Bind(0, null, null));
            Check_Expected(select, new object[] { 0, null, null });

            // PKs cannot be UNSET
            Assert.Throws(Is.InstanceOf<InvalidQueryException>(), () => Session.Execute(insert.Bind(Unset.Value, 0, 0)));

            Session.Execute("DROP TABLE test_unset_values");
        }

        [Test]
        public void Bound_CollectionTypes()
        {
            var insertQuery = string.Format(@"
                INSERT INTO {0}
                (id, map_sample, list_sample, set_sample)
                VALUES (?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new[] { 0 }, preparedStatement.RoutingIndexes);

            var firstRowValues = new object[]
            {
                Guid.NewGuid(),
                new Dictionary<string, string> {{"key1", "value1"}, {"key2", "value2"}},
                new List<string> (new [] {"one", "two", "three", "four", "five"}),
                new List<string> (new [] {"set_1one", "set_2two", "set_3three", "set_4four", "set_5five"})
            };
            var secondRowValues = new object[]
            {
                Guid.NewGuid(),
                new Dictionary<string, string>(),
                new List<string>(),
                new List<string>()
            };
            var thirdRowValues = new object[]
            {
                Guid.NewGuid(),
                null,
                null,
                null
            };

            Session.Execute(preparedStatement.Bind(firstRowValues));
            Session.Execute(preparedStatement.Bind(secondRowValues));
            Session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = string.Format(@"
                SELECT
                    id, map_sample, list_sample, set_sample
                FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = Session.Execute(selectQuery).ToList();
            //Check that they were inserted and retrieved
            Assert.AreEqual(3, rowList.Count);

            //Create a dictionary with the inserted values to compare with the retrieved values
            var insertedValues = new Dictionary<Guid, object[]>()
            {
                {(Guid)firstRowValues[0], firstRowValues},
                {(Guid)secondRowValues[0], secondRowValues},
                {(Guid)thirdRowValues[0], thirdRowValues}
            };

            foreach (var retrievedRow in rowList)
            {
                var inserted = insertedValues[retrievedRow.GetValue<Guid>("id")];
                for (var i = 1; i < inserted.Length; i++)
                {
                    var insertedValue = inserted[i];
                    var retrievedValue = retrievedRow[i];
                    if (retrievedValue == null)
                    {
                        //Empty collections are retrieved as nulls
                        Assert.True(insertedValue == null || ((ICollection)insertedValue).Count == 0);
                        continue;
                    }
                    if (insertedValue != null)
                    {
                        Assert.AreEqual(((ICollection)insertedValue).Count, ((ICollection)retrievedValue).Count);
                    }
                    Assert.AreEqual(insertedValue, retrievedValue);
                }
            }
        }

        [Test]
        public void Prepared_NoParams()
        {
            var preparedStatement = Session.Prepare("SELECT id FROM " + AllTypesTableName);
            //No parameters => no routing indexes
            Assert.Null(preparedStatement.RoutingIndexes);
            //Just check that it works
            var rs = Session.Execute(preparedStatement.Bind());
            Assert.NotNull(rs);
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
        public void Bound_With_Parameters_That_Can_Not_Be_Encoded()
        {
            var ps = Session.Prepare("SELECT * FROM system.local WHERE key = ?");
            Assert.Throws<InvalidTypeException>(() => ps.Bind(new Object()));
        }

        [Test]
        public void Bound_Int_Valids()
        {
            var psInt32 = Session.Prepare(string.Format("INSERT INTO {0} (id, int_sample) VALUES (?, ?)", AllTypesTableName));

            //Int: only int and blob valid
            AssertValid(Session, psInt32, 100);
            AssertValid(Session, psInt32, new byte[] { 0, 0, 0, 1 });
        }

        [Test]
        public void Bound_Double_Valids()
        {
            var psDouble = Session.Prepare(string.Format("INSERT INTO {0} (id, double_sample) VALUES (?, ?)", AllTypesTableName));

            //Double: Only doubles, longs and blobs (8 bytes)
            AssertValid(Session, psDouble, 1D);
            AssertValid(Session, psDouble, 1L);
            AssertValid(Session, psDouble, new byte[8]);
        }

        [Test]
        public void Bound_Decimal_Valids()
        {
            var psDecimal = Session.Prepare(string.Format("INSERT INTO {0} (id, decimal_sample) VALUES (?, ?)", AllTypesTableName));

            //decimal: There is type conversion, all numeric types are valid
            AssertValid(Session, psDecimal, 1L);
            AssertValid(Session, psDecimal, 1F);
            AssertValid(Session, psDecimal, 1D);
            AssertValid(Session, psDecimal, 1);
            AssertValid(Session, psDecimal, new byte[16]);
        }

        [Test]
        public void Bound_Collections_List_Valids()
        {
            var session = GetNewTemporarySession(KeyspaceName);
            PreparedStatement psList = session.Prepare(string.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)", AllTypesTableName));

            // Valid cases -- NOTE: Only types List and blob are valid
            AssertValid(session, psList, new List<string>(new[] { "one", "two", "three" })); // parameter type = List<string>
            AssertValid(session, psList, new List<string>(new[] { "one", "two" }).Select(s => s)); // parameter type = IEnumerable
            // parameter type = long fails for C* 2.0.x, passes for C* 2.1.x
            // AssertValid(Session, psList, 123456789L);
        }

        [Test]
        public void Bound_Collections_Map_Valid()
        {
            var session = GetNewTemporarySession(KeyspaceName);
            PreparedStatement psMap = session.Prepare(string.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)", AllTypesTableName));
            AssertValid(session, psMap, new Dictionary<string, string> { { "one", "1" }, { "two", "2" } });
        }


        [Test]
        public void Bound_ExtraParameter()
        {
            var session = GetNewTemporarySession(KeyspaceName);
            var ps = session.Prepare(string.Format("INSERT INTO {0} (id, list_sample, int_sample) VALUES (?, ?, ?)", AllTypesTableName));
            Assert.Throws(Is
                .InstanceOf<ArgumentException>().Or
                .InstanceOf<InvalidQueryException>().Or
                .InstanceOf<ServerErrorException>(),
                () => session.Execute(ps.Bind(Guid.NewGuid(), null, null, "yeah, this is extra")));
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
                session.Execute("CREATE KEYSPACE bound_changeks_test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}");
                var ps = session.Prepare("SELECT * FROM system.local WHERE key='local'");
                session.ChangeKeyspace("bound_changeks_test");
                Assert.DoesNotThrow(() => TestHelper.Invoke(() => session.Execute(ps.Bind()), 10));
            }
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

        [Test]
        public void Prepared_SelectOne()
        {
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                Session.Execute(string.Format("CREATE TABLE {0} (tweet_id int PRIMARY KEY, numb double, label text);", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            for (int i = 0; i < 10; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (tweet_id, numb, label) VALUES({1}, 0.01,'{2}')", tableName, i, "row" + i));
            }

            var prepSelect = Session.Prepare(string.Format("SELECT * FROM {0} WHERE tweet_id = ?;", tableName));

            var rowId = 5;
            var result = Session.Execute(prepSelect.Bind(rowId));
            foreach (var row in result)
            {
                Assert.True((string)row.GetValue(typeof(int), "label") == "row" + rowId);
            }
            Assert.True(result.Columns != null);
            Assert.True(result.Columns.Length == 3);
        }

        [Test]
        public void Prepared_Bound_Overrides_Are_Local()
        {
            var preparedStatement = Session.Prepare("SELECT key FROM system.local WHERE key = ?");

            preparedStatement.SetConsistencyLevel(ConsistencyLevel.One);
            preparedStatement.SetIdempotence(false);

            var boundWithOverrides = preparedStatement.Bind("local")
                                                    .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                                                    .SetIdempotence(true);
            Session.Execute(boundWithOverrides);

            Assert.AreEqual(ConsistencyLevel.One, preparedStatement.ConsistencyLevel);
            Assert.AreEqual(false, preparedStatement.IsIdempotent);

            var nextBound = preparedStatement.Bind("local");
            Assert.AreEqual(ConsistencyLevel.One, nextBound.ConsistencyLevel);
            Assert.AreEqual(false, nextBound.IsIdempotent);
        }

        [Test]
        public void Prepared_Bind_Snapshots_Prepared_Options()
        {
            var preparedStatement = Session.Prepare("SELECT key FROM system.local WHERE key = ?");

            preparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            preparedStatement.SetIdempotence(true);
            var firstBound = preparedStatement.Bind("local");

            preparedStatement.SetConsistencyLevel(ConsistencyLevel.EachQuorum);
            preparedStatement.SetIdempotence(false);
            var secondBound = preparedStatement.Bind("local");

            Assert.AreEqual(ConsistencyLevel.LocalQuorum, firstBound.ConsistencyLevel);
            Assert.AreEqual(true, firstBound.IsIdempotent);

            Assert.AreEqual(ConsistencyLevel.EachQuorum, secondBound.ConsistencyLevel);
            Assert.AreEqual(false, secondBound.IsIdempotent);
        }

        [Test]
        public void Prepared_Nullability_Preserved()
        {
            var preparedStatement = Session.Prepare("SELECT key FROM system.local WHERE key = ?");

            Assert.IsNull(preparedStatement.ConsistencyLevel);
            Assert.IsNull(preparedStatement.IsIdempotent);

            var bound = preparedStatement.Bind("local");
            Assert.IsNull(bound.ConsistencyLevel);
            Assert.IsNull(bound.IsIdempotent);
        }

        [Test]
        public void Prepared_Bound_Invalid_ConsistencyLevel()
        {
            var preparedStatement = Session.Prepare("SELECT key FROM system.local WHERE key = ?");
            var bound = preparedStatement.Bind("local").SetConsistencyLevel((ConsistencyLevel)999);

            Assert.Throws<InvalidArgumentException>(() => Session.Execute(bound));
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

        private void CreateTable(string tableName)
        {
            CreateTable(Session, tableName);
        }

        // FIXME: Currently ExecuteSyncNonQuery requires RowSet to have acccess to Info.QueriedHost,
        // and WaitForSchemaAgreement is not implemented in TestUtils. Use Execute instead.
        private void CreateTable(ISession session, string tableName)
        {
            // QueryTools.ExecuteSyncNonQuery(session, $@"CREATE TABLE {tableName}(
            //                                                     id int PRIMARY KEY,
            //                                                     label text,
            //                                                     number int
            //                                                     );");
            // TestUtils.WaitForSchemaAgreement(session.Cluster);

            var cql = $@"CREATE TABLE {tableName} (
                    id int PRIMARY KEY,
                    label text,
                    number int
                );";

            session.Execute(cql, session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}