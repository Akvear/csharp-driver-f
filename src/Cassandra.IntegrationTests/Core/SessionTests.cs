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

using System.Diagnostics;

using Cassandra.Tests;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class SessionTests : SharedClusterTest
    {
        public SessionTests() : base(3, false)
        {
        }

        [Test]
        public void Session_Keyspace_Does_Not_Exist_On_Connect_Throws()
        {
            var localCluster = GetNewTemporaryCluster();
            var ex = Assert.Throws<InvalidQueryException>(() => localCluster.Connect("THIS_KEYSPACE_DOES_NOT_EXIST"));
            Assert.True(ex.Message.ToLower().Contains("keyspace"));
        }

        [Test]
        public void Session_Keyspace_Empty_On_Connect()
        {
            var localCluster = GetNewTemporaryCluster();
            Assert.DoesNotThrow(() =>
            {
                var localSession = localCluster.Connect("");
                localSession.Execute("SELECT * FROM system.local WHERE key='local'");
            });
        }

        [Test]
        public void Session_Keyspace_Connect_Case_Sensitive()
        {
            var localCluster = GetNewTemporaryCluster();
            Assert.Throws<InvalidQueryException>(() => localCluster.Connect("SYSTEM"));
        }

        [Test]
        public void Session_Use_Statement_Changes_Keyspace()
        {
            var localCluster = GetNewTemporaryCluster();
            var localSession = localCluster.Connect();
            localSession.Execute("USE system");
            //The session should be using the system keyspace now
            Assert.DoesNotThrow(() =>
            {
                for (var i = 0; i < 5; i++)
                {
                    localSession.Execute("select * from local");
                }
            });
            Assert.That(localSession.Keyspace, Is.EqualTo("system"));
        }

        [Test]
        public void Session_Use_Statement_Changes_Keyspace_Case_Insensitive()
        {
            var localCluster = GetNewTemporaryCluster();
            var localSession = localCluster.Connect();
            //The statement is case insensitive by default, as no quotes were specified
            localSession.Execute("USE SyStEm");
            //The session should be using the system keyspace now
            Assert.DoesNotThrow(() =>
            {
                for (var i = 0; i < 5; i++)
                {
                    localSession.Execute("select * from local");
                }
            });
        }

        [Test]
        public void Session_Execute_Logging_With_Verbose_Level_Test()
        {
            var originalLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            try
            {
                Assert.DoesNotThrow(() =>
                {
                    using (var localCluster = ClusterBuilder().AddContactPoint(TestCluster.InitialContactPoint).Build())
                    {
                        var localSession = localCluster.Connect("system");
                        var ps = localSession.Prepare("SELECT * FROM local");
                        TestHelper.ParallelInvoke(() => localSession.Execute(ps.Bind()), 100);
                    }
                });
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = originalLevel;
            }
        }
        
        [Test]
        public void Session_Keyspace_Does_Not_Exist_On_Change_Throws()
        {
            var localCluster = GetNewTemporaryCluster();
            var localSession = localCluster.Connect();
            var ex = Assert.Throws<InvalidQueryException>(() => localSession.ChangeKeyspace("THIS_KEYSPACE_DOES_NOT_EXIST_EITHER"));
            Assert.True(ex.Message.ToLower().Contains("keyspace"));
        }
    }
}
