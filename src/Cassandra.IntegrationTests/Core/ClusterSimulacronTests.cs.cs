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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Cassandra.Tests;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    public class ClusterSimulacronTests : SimulacronTest
    {
        public ClusterSimulacronTests() : base(false, new SimulacronOptions { Nodes = "3" }, false)
        {
        }

        [Test]
        public void Cluster_Should_Ignore_IpV6_Addresses_For_Not_Valid_Hosts()
        {
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(IPAddress.Parse("::1"))
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                Assert.DoesNotThrow(() =>
                {
                    var session = cluster.Connect();
                    session.Execute("SELECT * FROM system.local WHERE key='local'");
                });
            }
        }

        [Test]
        public void Cluster_Connect_With_Wrong_Keyspace_Name_Test()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"MY_WRONG_KEYSPACE\"").ThenServerError(ServerError.Invalid, "msg"));
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"ANOTHER_THAT_DOES_NOT_EXIST\"").ThenServerError(ServerError.Invalid, "msg"));

            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace that does not exists
                                        .WithDefaultKeyspace("MY_WRONG_KEYSPACE")
                                        .Build())
            {
                Assert.Throws<InvalidQueryException>(() => cluster.Connect());
                Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES_NOT_EXIST"));
            }
        }
    }
}