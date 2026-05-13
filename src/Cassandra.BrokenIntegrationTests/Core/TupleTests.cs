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

using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [TestCassandraVersion(2, 1)]
    public class TupleTests : SimulacronTest
    {
        private const string TableName = "users_tuples";

        [Test]
        public void EncodeDecodeTupleAsNestedTest()
        {
            var achievements = new List<Tuple<string, int>>
            {
                new Tuple<string, int>("What", 1),
                new Tuple<string, int>(null, 100),
                new Tuple<string, int>(@"¯\_(ツ)_/¯", 150)
            };

            var insert = new SimpleStatement("INSERT INTO " + TableName + " (id, achievements) values (?, ?)", 31, achievements);
            Session.Execute(insert);

            VerifyStatement(
                QueryType.Query,
                "INSERT INTO " + TableName + " (id, achievements) values (?, ?)",
                1,
                31, achievements);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 31")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Varchar, DataType.Ascii, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Varchar, DataType.Int)))
                          },
                          r => r.WithRow(
                              31,
                              null,
                              achievements)));

            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 31").First();

            Assert.AreEqual(achievements, row.GetValue<List<Tuple<string, int>>>("achievements"));
        }
    }
}