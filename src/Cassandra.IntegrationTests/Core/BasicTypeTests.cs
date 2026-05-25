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
using System.Linq;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using System.Reflection;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Validates the (de)serialization of CLR types and CQL types.
    /// Each test will upsert a value on specific CQL type and expect the correspondent CLR type. Should_Get(CLR type)_When_Upsert(CQL data type).
    /// </summary>
    public class BasicTypeTests : SimulacronTest
    {
        /// <summary>
        /// Test the convertion of a decimal value ( with a negative scale) stored in a column.
        /// 
        /// @jira CSHARP-453 https://datastax-oss.atlassian.net/browse/CSHARP-453
        /// 
        /// </summary>
        [Test]
        public void DecimalWithNegativeScaleTest()
        {
            const string insertQuery = @"INSERT INTO decimal_neg_scale (id, value) VALUES (?, ?)";
            var preparedStatement = Session.Prepare(insertQuery);

            const int scale = -1;
            var scaleBytes = BitConverter.GetBytes(scale);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(scaleBytes);
            }

            var bytes = new byte[scaleBytes.Length + 1];
            Array.Copy(scaleBytes, bytes, scaleBytes.Length);

            bytes[scaleBytes.Length] = 5;

            var firstRowValues = new object[] { Guid.NewGuid(), bytes };
            Session.Execute(preparedStatement.Bind(firstRowValues));

            VerifyBoundStatement(
                insertQuery,
                1,
                firstRowValues);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM decimal_neg_scale")
                      .ThenRowsSuccess(new[] { "id", "value" }, r => r.WithRow(firstRowValues.First(), (decimal)50)));

            var row = Session.Execute("SELECT * FROM decimal_neg_scale").First();
            var decValue = row.GetValue<decimal>("value");

            Assert.AreEqual(50, decValue);
        }
    }
}
