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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category(TestCategory.Long)]
    public class LoadBalancingPolicyTests : TestGlobals
    {

        /// <summary>
        /// Validates that the driver fails as expected with a non-existing datacenter is specified via DCAwareRoundRobinPolicy 
        /// 
        /// @test_category load_balancing:dc_aware
        /// </summary>
        [Test]
        public void RoundRobin_DcAware_BuildClusterWithNonExistentDc()
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(1);
            testCluster.Builder = ClusterBuilder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            try
            {
                testCluster.InitClient();
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (ArgumentException e)
            {
                string expectedErrMsg = "Datacenter dc2 does not match any of the nodes, available datacenters: datacenter1.";
                Assert.IsTrue(e.Message.Contains(expectedErrMsg));
            }
        }
    }
}
