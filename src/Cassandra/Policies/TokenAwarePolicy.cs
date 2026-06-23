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

namespace Cassandra
{
    /// <summary>
    /// A wrapper load balancing policy that adds token awareness to a child policy.
    /// <para> This policy encapsulates another policy. The resulting policy works in the following way:
    /// </para>
    /// <list type="number">
    /// <item>The <see cref="Distance(Host)"/> method is inherited  from the child policy.</item>
    /// </list>
    /// </summary>
    public class TokenAwarePolicy : ILoadBalancingPolicy
    {
        /// <summary>
        ///  Creates a new <c>TokenAware</c> policy that wraps the provided child
        ///  load balancing policy.
        /// </summary>
        /// <param name="childPolicy"> the load balancing policy to wrap with token
        ///  awareness.</param>
        public TokenAwarePolicy(ILoadBalancingPolicy childPolicy)
        {
            ChildPolicy = childPolicy ?? throw new ArgumentNullException(nameof(childPolicy));
        }

        public ILoadBalancingPolicy ChildPolicy { get; }

        [Obsolete("Initialize is not supported. Load balancing is handled by the Rust driver internally.")]
        public void Initialize(ICluster cluster)
        {
            throw new NotSupportedException(
                "Initialize is not supported. Load balancing is handled by the Rust driver internally.");
        }

        /// <summary>
        ///  Return the HostDistance for the provided host.
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        ///
        /// <returns>the HostDistance to <c>host</c> as returned by the wrapped
        ///  policy.</returns>
        public HostDistance Distance(Host host)
        {
            return ChildPolicy.Distance(host);
        }

        /// <summary>
        /// <b> This function is not supported. All query routing is handled by the Rust driver internally.</b>
        ///  Returns the hosts to use for a new query. <p> The returned plan will first
        ///  return replicas (whose <c>HostDistance</c> for the child policy is
        ///  <c>Local</c>) for the query if it can determine them (i.e. mainly if
        ///  <c>IStatement.RoutingKey</c> is not <c>null</c>). Following what
        ///  it will return the plan of the child policy.</p>
        /// </summary>
        /// <param name="loggedKeyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>the new query plan.</returns>
        [Obsolete("NewQueryPlan is not supported. Load balancing is handled by the Rust driver internally.")]
        public IEnumerable<HostShard> NewQueryPlan(string loggedKeyspace, IStatement query)
        {
            throw new NotSupportedException(
                "NewQueryPlan is not supported. Query routing is handled by the Rust driver internally.");
        }
    }
}
