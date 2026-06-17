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
    public class RetryLoadBalancingPolicy : ILoadBalancingPolicy
    {
        public RetryLoadBalancingPolicy(ILoadBalancingPolicy loadBalancingPolicy, IReconnectionPolicy reconnectionPolicy)
        {
            ReconnectionPolicy = reconnectionPolicy;
            LoadBalancingPolicy = loadBalancingPolicy;
        }

        public IReconnectionPolicy ReconnectionPolicy { get; }

        public ILoadBalancingPolicy LoadBalancingPolicy { get; }

        [Obsolete("Initialize is not supported.")]
        public void Initialize(ICluster cluster)
        {
            throw new NotSupportedException(
                "RetryLoadBalancingPolicy is not supported. " +
                "The Rust driver handles node reconnection internally.");
        }

        public HostDistance Distance(Host host)
        {
            return LoadBalancingPolicy.Distance(host);
        }

        [Obsolete("NewQueryPlan is not supported.")]
        public IEnumerable<HostShard> NewQueryPlan(string keyspace, IStatement query)
        {
            throw new NotSupportedException(
                "RetryLoadBalancingPolicy is not supported. " +
                "The Rust driver handles node reconnection internally.");
        }
    }
}
