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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class CollectionsTests : SharedClusterTest
    {
        private const string AllTypesTableName = "all_types_table_collections";

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            Session.Execute(string.Format(TestUtils.CreateTableAllTypes, AllTypesTableName));
        }

        [Test]
        public void DecodeCollectionTest()
        {
            var id = "c9850ed4-c139-4b75-affe-098649f9de93";
            var insertQuery = string.Format("INSERT INTO {0} (id, map_sample, list_sample, set_sample) VALUES ({1}, {2}, {3}, {4})",
                AllTypesTableName,
                id,
                "{'fruit': 'apple', 'band': 'Beatles'}",
                "['one', 'two']",
                "{'set_1one', 'set_2two'}");

            Session.Execute(insertQuery);
            var row = Session.Execute(string.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, id)).First();
            var expectedMap = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", "Beatles" } };
            var expectedList = new List<string> { "one", "two" };
            var expectedSet = new List<string> { "set_1one", "set_2two" };
            Assert.AreEqual(expectedMap, row.GetValue<IDictionary<string, string>>("map_sample"));
            Assert.AreEqual(expectedList, row.GetValue<List<string>>("list_sample"));
            Assert.AreEqual(expectedSet, row.GetValue<List<string>>("set_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void TimeUuid_Collection_Insert_Get_Test()
        {
            var session = GetNewTemporarySession(KeyspaceName);
            session.Execute("CREATE TABLE tbl_timeuuid_collections (id int PRIMARY KEY, set_value set<timeuuid>, list_value list<timeuuid>)");
            const string selectQuery = "SELECT * FROM tbl_timeuuid_collections WHERE id = ?";
            const string insertQuery = "INSERT INTO tbl_timeuuid_collections (id, set_value, list_value) VALUES (?, ?, ?)";
            var psInsert = session.Prepare(insertQuery);
            var set1 = new SortedSet<TimeUuid> { TimeUuid.NewId() };
            var list1 = new List<TimeUuid> { TimeUuid.NewId() };
            session.Execute(psInsert.Bind(1, set1, list1));
            var row1 = session.Execute(new SimpleStatement(selectQuery, 1)).First();
            CollectionAssert.AreEqual(set1, row1.GetValue<SortedSet<TimeUuid>>("set_value"));
            CollectionAssert.AreEqual(set1, row1.GetValue<ISet<TimeUuid>>("set_value"));
            CollectionAssert.AreEqual(set1, row1.GetValue<TimeUuid[]>("set_value"));
            CollectionAssert.AreEqual(list1, row1.GetValue<List<TimeUuid>>("list_value"));
            CollectionAssert.AreEqual(list1, row1.GetValue<TimeUuid[]>("list_value"));
        }

        [Test]
        public void Encode_Map_With_NullValue_Should_Throw()
        {
            var id = Guid.NewGuid();
            var localSession = GetNewTemporarySession(KeyspaceName);
            var insertQuery = localSession.Prepare(string.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)",
                AllTypesTableName));

            var map = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", null } };
            var stmt = insertQuery.Bind(id, map);
            Assert.Throws<ArgumentNullException>(() => localSession.Execute(stmt));
        }

        [Test]
        public void Encode_List_With_NullValue_Should_Throw()
        {
            var id = Guid.NewGuid();
            var localSession = GetNewTemporarySession(KeyspaceName);
            var insertQuery = localSession.Prepare(string.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)",
                AllTypesTableName));
            var map = new List<string> { "fruit", null };
            var stmt = insertQuery.Bind(id, map);
            Assert.Throws<ArgumentNullException>(() => localSession.Execute(stmt));
        }

        [Test]
        public void Decode_NestedList_And_Different_CollectionTypes_Should_Return_Correct_Results()
        {
            var id = Guid.NewGuid();
            var localSession = GetNewTemporarySession(KeyspaceName);
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var typeName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            localSession.Execute($"CREATE TYPE {typeName} (id uuid)");
            localSession.Execute($"CREATE TABLE {tableName} (id uuid PRIMARY KEY, nested_list list<frozen<list<list<int>>>>, list list<int>)");
            var insertQuery = localSession.Prepare($"INSERT INTO {tableName} (id, nested_list, list) VALUES (?, ?, ?)");
            var list = new List<int> { 0, 1 };
            var nestedList = new List<List<List<int>>> { new List<List<int>> { new List<int> { 3, 4 } } };
            var stmt = insertQuery.Bind(id, nestedList, list);
            localSession.Execute(stmt);
            var rs = localSession.Execute(new SimpleStatement($"SELECT * FROM {tableName} WHERE id = ?", id)).ToList().Single();
            CollectionAssert.AreEqual(nestedList.Single(), rs.GetValue<IEnumerable<IEnumerable<IEnumerable<int>>>>("nested_list").Single());
            CollectionAssert.AreEqual(nestedList.Single(), rs.GetValue<IReadOnlyCollection<ICollection<IList<int>>>>("nested_list").Single());
            CollectionAssert.AreEqual(list, rs.GetValue<IEnumerable<long>>("list"));
            CollectionAssert.AreEqual(list, rs.GetValue<IEnumerable<int>>("list"));
            CollectionAssert.AreEqual(list, rs.GetValue<IReadOnlyList<int>>("list"));
            CollectionAssert.AreEqual(list, rs.GetValue<int[]>("list"));
        }
    }
}
