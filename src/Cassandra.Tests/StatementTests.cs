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
using Cassandra.Serialization;
using Moq;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;
#pragma warning disable 618

namespace Cassandra.Tests
{
    [TestFixture]
    public class StatementTests
    {
        private const string Query = "SELECT * ...";

        [Test]
        public void SimpleStatement_Constructor_Positional_Values()
        {
            var stmt = new SimpleStatement(Query, 1, "value 2", 3L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 3L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_Positional_Values()
        {
            var stmt = new SimpleStatement(Query, 1, "value 2", 10030L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 10030L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_No_Values()
        {
            var stmt = new SimpleStatement(Query);
            Assert.AreEqual(null, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_No_Values()
        {
            var stmt = new SimpleStatement(Query, new object[0]);
            Assert.AreEqual(new object[0], stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query, values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }

        [Test]
        public void SimpleStatement_Constructor_Dictionary_Named_Test()
        {
            var valuesDictionary = new Dictionary<string, object>
            {
                {"Name", "Futurama"},
                {"Description", "In Stereo where available"},
                {"Time", DateTimeOffset.Parse("1963-08-28")}
            };
            var stmt = new SimpleStatement(valuesDictionary, Query);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(valuesDictionary["Name"], actualValues["name"]);
            Assert.AreEqual(valuesDictionary["Description"], actualValues["description"]);
            Assert.AreEqual(valuesDictionary["Time"], actualValues["time"]);
        }

        [Test]
        public void SimpleStatement_Bind_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query, values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }
    }
}
