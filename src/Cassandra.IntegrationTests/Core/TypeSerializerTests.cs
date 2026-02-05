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
using System.IO;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using Cassandra.Tests;
using Cassandra.Tests.Extensions.Serializers;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using StringAssert = NUnit.Framework.Legacy.StringAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class TypeSerializersTests : SharedClusterTest
    {
        public TypeSerializersTests() : base(1, true)
        {
        }

        // Re-add once Session.UserDefinedTypes is set in Session and can be used in tests
        // private static long vectorSchemaSetUp = 0;

        private static IEnumerable VectorTestCaseData()
        {
            var r = new Random();
            Action<object, object> defaultAssert = Assert.AreEqual;
            Action<object, object> listVectorAssert = (expected, actual) => CollectionAssert.AreEqual((IEnumerable)expected, (IEnumerable)actual);
            Action<object, object> setMapAssert = (expected, actual) => CollectionAssert.AreEquivalent((IEnumerable)expected, (IEnumerable)actual);
            var buf = new byte[128];
            return new[]
            {
                    new TestCaseData("int", (Func<int>)(()=>r.Next()), defaultAssert),
                    new TestCaseData("bigint", (Func<long>)(()=>(long)r.NextDouble()), defaultAssert),
                    new TestCaseData("smallint", (Func<short>)(()=>(short)r.Next()), defaultAssert),
                    new TestCaseData("tinyint", (Func<sbyte>)(()=>(sbyte)r.Next()), defaultAssert),
                    new TestCaseData("varint", (Func<BigInteger>)(()=>new BigInteger((long)r.NextDouble())), defaultAssert),

                    new TestCaseData("float", (Func<float>)(()=>(float)r.NextDouble()), defaultAssert),
                    new TestCaseData("double", (Func<double>)(()=>(double)r.NextDouble()), defaultAssert),
                    new TestCaseData("decimal", (Func<decimal>)(()=>(decimal)r.NextDouble()), defaultAssert),

                    new TestCaseData("ascii", (Func<string>)(()=>r.Next().ToString(CultureInfo.InvariantCulture)), defaultAssert),
                    new TestCaseData("text", (Func<string>)(()=>r.Next().ToString(CultureInfo.InvariantCulture)), defaultAssert),

                    new TestCaseData("date", (Func<LocalDate>)(()=>new LocalDate((uint)(r.Next()%100000))), defaultAssert),
                    new TestCaseData("time", (Func<LocalTime>)(()=>new LocalTime(r.Next())), defaultAssert),
                    new TestCaseData("timestamp", (Func<DateTimeOffset>)(()=>DateTimeOffset.FromUnixTimeMilliseconds(r.Next() % 10000000)), defaultAssert),

                    new TestCaseData("uuid", (Func<Guid>)(Guid.NewGuid), defaultAssert),
                    new TestCaseData("timeuuid", (Func<TimeUuid>)(TimeUuid.NewId), defaultAssert),

                    new TestCaseData("boolean", (Func<bool>)(()=>r.Next()%2==0), defaultAssert),
                    new TestCaseData("inet", (Func<IPAddress>)(()=> IPAddress.Parse($"{(r.Next()%255) + 1}.{r.Next()%255}.{r.Next()%255}.{(r.Next()%255) + 1}")), defaultAssert),
                    new TestCaseData("blob", (Func<byte[]>)(()=>
                    {
                        r.NextBytes(buf);
                        return buf;
                    }), defaultAssert),

                    new TestCaseData("list<int>", (Func<int[]>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToArray()), listVectorAssert),
                    new TestCaseData("list<int>", (Func<IEnumerable<int>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToList()), listVectorAssert),
                    new TestCaseData("list<int>", (Func<List<int>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToList()), listVectorAssert),
                    new TestCaseData("list<varint>", (Func<IEnumerable<BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => new BigInteger((long)r.NextDouble())).ToList()), listVectorAssert),

                    new TestCaseData("set<int>", (Func<ISet<int>>)(()=>new HashSet<int>(Enumerable.Range(0, r.Next()%200).Distinct().Select(i => i))), setMapAssert),
                    new TestCaseData("set<int>", (Func<HashSet<int>>)(()=>new HashSet<int>(Enumerable.Range(0, r.Next()%200).Distinct().Select(i => i))), setMapAssert),

                    new TestCaseData("map<int,int>", (Func<IDictionary<int, int>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => i, i => r.Next())), setMapAssert),
                    new TestCaseData("map<int,varint>", (Func<IDictionary<int, BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i =>i, i => new BigInteger((long)r.NextDouble()))), setMapAssert),
                    new TestCaseData("map<varint,int>", (Func<IDictionary<BigInteger, int>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => new BigInteger(i), i => r.Next())), setMapAssert),
                    new TestCaseData("map<varint,varint>", (Func<IDictionary<BigInteger, BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => new BigInteger(i), i => new BigInteger((long)r.NextDouble()))), setMapAssert),

                    new TestCaseData("vector<int,2>", (Func<CqlVector<int>>)(()=>new CqlVector<int>(Enumerable.Range(0, 2).Select(i => r.Next()).ToArray())), listVectorAssert),
                    new TestCaseData("vector<int,2>", (Func<CqlVector<int>>)(()=>new CqlVector<int>(r.Next(), r.Next())), listVectorAssert),
                    new TestCaseData("vector<varint,2>", (Func<CqlVector<BigInteger>>)(()=>new CqlVector<BigInteger>(Enumerable.Range(0, 2).Select(i => new BigInteger((long)r.NextDouble())).ToArray())), listVectorAssert),

                    new TestCaseData("tuple<int,int>", (Func<Tuple<int,int>>)(()=>new Tuple<int, int>(r.Next(), r.Next())), defaultAssert),
                    new TestCaseData("tuple<int,varint>", (Func<Tuple<int,BigInteger>>)(()=>new Tuple<int, BigInteger>(r.Next(), new BigInteger((long)r.NextDouble()))), defaultAssert),
                    new TestCaseData("tuple<varint,int>", (Func<Tuple<BigInteger, int>>)(()=>new Tuple<BigInteger, int>(new BigInteger((long)r.NextDouble()), r.Next())), defaultAssert),
                    new TestCaseData("tuple<varint,varint>", (Func<Tuple<BigInteger, BigInteger>>)(()=>new Tuple<BigInteger, BigInteger>(new BigInteger((long)r.NextDouble()), new BigInteger((long)r.NextDouble()))), defaultAssert),

                    // Re-add once Session.UserDefinedTypes is set in Session and can be used in tests
                    // new TestCaseData("fixed_type", (Func<FixedType>)(()=>new FixedType { a = r.Next(), b = r.Next()}), defaultAssert),
                    // new TestCaseData("mixed_type_one", (Func<MixedTypeOne>)(()=>new MixedTypeOne { a = r.Next(), b = (long)r.NextDouble()}), defaultAssert),
                    // new TestCaseData("mixed_type_two", (Func<MixedTypeTwo>)(()=>new MixedTypeTwo { a = (long)r.NextDouble(), b = r.Next()}), defaultAssert),
                    // new TestCaseData("var_type", (Func<VarType>)(()=>new VarType { a = (long)r.NextDouble(), b = (long)r.NextDouble()}), defaultAssert),
                    // new TestCaseData("complex_vector_udt", (Func<ComplexVectorUdt>)(()=>new ComplexVectorUdt { a = new CqlVector<int>(r.Next(), r.Next(), r.Next()), b = new CqlVector<BigInteger>(r.Next(), r.Next(), r.Next())}), defaultAssert),
                };
        }

        [Test, TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorSimpleStatementTest<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableName = baseName + "isH";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)");

            Action<Func<int, CqlVector<T>, SimpleStatement>> vectorSimpleStmtTestFn = simpleStmtFn =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                Session.Execute(simpleStmtFn(i, vector));
                var rs = Session.Execute($"SELECT * FROM {tableName} WHERE i = {i}");
                AssertSimpleVectorTest(vector, rs, assertFn);
            };

            vectorSimpleStmtTestFn((i, v) => new SimpleStatement($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", i, v));
            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(new Dictionary<string, object> { { "idx", i }, { "vec", v } }, $"INSERT INTO {tableName} (i, j) VALUES (:idx, :vec)"));
        }

        [Test, TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorSimpleStatementTestComplex<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableNameComplex = baseName + "_complex";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableNameComplex} (i int PRIMARY KEY, k vector<vector<{cqlSubType}, 3>, 3>, l vector<list<vector<{cqlSubType}, 3>>, 3>)");

            Action<Func<int, List<CqlVector<T>>, SimpleStatement>> vectorSimpleStmtTestFn = simpleStmtFn =>
            {
                var vectorList = new List<CqlVector<T>>
                {
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                };
                var i = new Random().Next();
                Session.Execute(simpleStmtFn(i, vectorList));
                var rs = Session.Execute($"SELECT * FROM {tableNameComplex} WHERE i = {i}");
                AssertComplexVectorTest(vectorList, rs, assertFn);
            };

            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (?, ?, ?)",
                i,
                new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })));
            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(
                new Dictionary<string, object>
                {
                    { "idx", i },
                    { "vec", new CqlVector<CqlVector<T>>(v[0], v[1], v[2]) },
                    { "vecc", new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] }) }
                },
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (:idx, :vec, :vecc)"));
        }

        [Test, TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorPreparedStatementTest<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_prep_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableName = baseName + "isH";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)");

            Action<string, Func<int, CqlVector<T>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vector, ps));
                ps = Session.Prepare($"SELECT * FROM {tableName} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                AssertSimpleVectorTest(vector, rs, assertFn);
            };

            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", (i, v, ps) => ps.Bind(i, v));
            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (:idx, :vec)", (i, v, ps) => ps.Bind(new { idx = i, vec = v }));
        }

        [Test, TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorPreparedStatementTestComplex<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_prep_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableNameComplex = baseName + "_complex";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableNameComplex} (i int PRIMARY KEY, k vector<vector<{cqlSubType}, 3>, 3>, l vector<list<vector<{cqlSubType}, 3>>, 3>)");

            Action<string, Func<int, List<CqlVector<T>>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vectorList = new List<CqlVector<T>>
                {
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                };
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vectorList, ps));
                ps = Session.Prepare($"SELECT * FROM {tableNameComplex} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                AssertComplexVectorTest(vectorList, rs, assertFn);
            };

            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (?, ?, ?)",
                (i, v, ps) =>
                    ps.Bind(
                        i,
                        new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                        new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })));
            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (:idx, :vec, :vecc)",
                (i, v, ps) =>
                    ps.Bind(
                        new
                        {
                            idx = i,
                            vec = new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                            vecc = new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })
                        }
                ));
        }

        [Test]
        public void VectorFloatTest()
        {
            var tableName = TestUtils.GetUniqueTableName();
            Session.Execute($"CREATE TABLE {tableName} (i int PRIMARY KEY, j vector<float, 3>)");

            var vector = new CqlVector<float>(1.1f, 2.2f, 3.3f);

            // Simple insert and select
            Session.Execute(new SimpleStatement($"INSERT INTO {tableName} (i, j) VALUES (1, ?)", vector));
            var rs = Session.Execute($"SELECT * FROM {tableName} WHERE i = 1");
            AssertSimpleVectorTest(vector, rs, Assert.AreEqual);

            // Prepared insert and select
            var ps = Session.Prepare($"INSERT INTO {tableName} (i, j) VALUES (?, ?)");
            Session.Execute(ps.Bind(2, vector));
            rs = Session.Execute($"SELECT * FROM {tableName} WHERE i = 2");
            AssertSimpleVectorTest(vector, rs, Assert.AreEqual);

            // throw when length is not 3
            Assert.Throws<InvalidQueryException>(() =>
            {
                var shortVector = new CqlVector<float>(1.1f, 2.2f);
                Session.Execute(new SimpleStatement($"INSERT INTO {tableName} (i, j) VALUES (3, ?)", shortVector));
            });
        }

        private void AssertSimpleVectorTest<T>(CqlVector<T> expected, RowSet rs, Action<object, object> assertFn)
        {
            var rowList = rs.ToList();
            Assert.AreEqual(1, rowList.Count);
            var retrievedVector = rowList[0].GetValue<CqlVector<T>>("j");
            Assert.AreEqual(3, retrievedVector.Count);
            for (var idx = 0; idx < retrievedVector.Count; idx++)
            {
                assertFn(expected[idx], retrievedVector[idx]);
            }
        }

        private void AssertSimpleVectorEquals<T>(CqlVector<T> expected, IEnumerable<T> actual, Action<object, object> assertFn)
        {
            var list = actual.ToList();
            Assert.AreEqual(3, list.Count);
            for (var idx = 0; idx < list.Count; idx++)
            {
                assertFn(expected[idx], list[idx]);
            }
        }

        private void AssertComplexVectorTest<T>(List<CqlVector<T>> vectorList, RowSet rs, Action<object, object> assertFn)
        {
            var rowList = rs.ToList();
            Assert.AreEqual(1, rowList.Count);

            var retrievedVector1 = rowList[0].GetValue<CqlVector<CqlVector<T>>>("k");
            Assert.AreEqual(3, retrievedVector1.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                Assert.AreEqual(3, retrievedVector1[idx].Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], retrievedVector1[idx][idxj]);
                }
            }
            var retrievedVector2 = rowList[0].GetValue<CqlVector<List<CqlVector<T>>>>("l");
            Assert.AreEqual(3, retrievedVector2.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                Assert.AreEqual(1, retrievedVector2[idx].Count);
                Assert.AreEqual(3, retrievedVector2[idx][0].Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], retrievedVector2[idx][0][idxj]);
                }
            }
        }

        private void AssertComplexVectorEquals<T>(
            List<CqlVector<T>> vectorList,
            IEnumerable<IEnumerable<T>> actual1,
            IEnumerable<IEnumerable<IEnumerable<T>>> actual2,
            Action<object, object> assertFn)
        {
            var retrievedVector1 = actual1.ToList();
            Assert.AreEqual(3, retrievedVector1.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                var elem = retrievedVector1[idx].ToList();
                Assert.AreEqual(3, elem.Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], elem[idxj]);
                }
            }

            var retrievedVector2 = actual2.ToList();
            Assert.AreEqual(3, retrievedVector2.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                var elem = retrievedVector2[idx].ToList();
                Assert.AreEqual(1, elem.Count);
                var subElem = elem[0].ToList();
                Assert.AreEqual(3, subElem.Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], subElem[idxj]);
                }
            }
        }

        private void SetupVectorUdtSchema()
        {
            // Re-add once Session.UserDefinedTypes is set in Session and can be used in tests
            // if (Interlocked.CompareExchange(ref vectorSchemaSetUp, 1, 0) == 0)
            // {
            //     Session.Execute("create type if not exists fixed_type (a int, b int)");
            //     Session.Execute("create type if not exists mixed_type_one (a int, b varint)");
            //     Session.Execute("create type if not exists mixed_type_two (a varint, b int)");
            //     Session.Execute("create type if not exists var_type (a varint, b varint)");
            //     Session.Execute("create type if not exists complex_vector_udt (a vector<int, 3>, b vector<varint, 3>)");
            // }
            // Session.UserDefinedTypes.Define(
            //     UdtMap.For<FixedType>("fixed_type"),
            //     UdtMap.For<MixedTypeOne>("mixed_type_one"),
            //     UdtMap.For<MixedTypeTwo>("mixed_type_two"),
            //     UdtMap.For<VarType>("var_type"),
            //     UdtMap.For<ComplexVectorUdt>("complex_vector_udt"));
        }

        public class FixedType
        {
            protected bool Equals(FixedType other)
            {
                return a == other.a && b == other.b;
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((FixedType)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a * 397) ^ b;
                }
            }

            public int a { get; set; }

            public int b { get; set; }
        }

        public class MixedTypeOne
        {
            protected bool Equals(MixedTypeOne other)
            {
                return a == other.a && b.Equals(other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((MixedTypeOne)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a * 397) ^ b.GetHashCode();
                }
            }

            public int a { get; set; }

            public BigInteger b { get; set; }
        }

        public class MixedTypeTwo
        {
            protected bool Equals(MixedTypeTwo other)
            {
                return a.Equals(other.a) && b == other.b;
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((MixedTypeTwo)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a.GetHashCode() * 397) ^ b;
                }
            }

            public BigInteger a { get; set; }

            public int b { get; set; }
        }

        public class VarType
        {
            protected bool Equals(VarType other)
            {
                return a.Equals(other.a) && b.Equals(other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((VarType)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a.GetHashCode() * 397) ^ b.GetHashCode();
                }
            }

            public BigInteger a { get; set; }

            public BigInteger b { get; set; }
        }

        public class ComplexVectorUdt
        {
            protected bool Equals(ComplexVectorUdt other)
            {
                return Equals(a, other.a) && Equals(b, other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((ComplexVectorUdt)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((a != null ? a.GetHashCode() : 0) * 397) ^ (b != null ? b.GetHashCode() : 0);
                }
            }

            public CqlVector<int> a { get; set; }

            public CqlVector<BigInteger> b { get; set; }
        }
    }
}
