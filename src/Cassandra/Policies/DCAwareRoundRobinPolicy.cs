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
    /// A data-center aware Round-robin load balancing policy.
    /// <para>
    /// This policy provides queries over the nodes of the local datacenter. Also, if the `PermitDcFailover` flag is set, it includes in the query plans
    /// hosts in remote datacenters (which are always tried after the local nodes).
    /// See the comments on <see cref="DCAwareRoundRobinPolicy(string, int)"/> for more information.
    /// </para>
    /// </summary>
    public class DCAwareRoundRobinPolicy : ILoadBalancingPolicy
    {
        private const string UsedHostsPerRemoteDcObsoleteMessage =
            "The usedHostsPerRemoteDc parameter will be removed in the next major release of the driver. " +
            "DC failover should not be done in the driver, which does not have the necessary context to know " +
            "what makes sense considering application semantics. See https://datastax-oss.atlassian.net/browse/CSHARP-722";

        private string _localDc;
        private readonly int _usedHostsPerRemoteDc;

        /// <summary>
        /// Creates a new datacenter aware round robin policy that auto-discover the local data-center.
        /// <para>
        /// If this constructor is used, the data-center used as local will the
        /// data-center of the first Cassandra node the driver connects to. This
        /// will always be ok if all the contact points use at <see cref="Cluster"/>
        /// creation are in the local data-center. If it's not the case, you should
        /// provide the local data-center name yourself by using one of the other
        /// constructor of this class.
        /// </para>
        /// </summary>
        [Obsolete("Automatic local datacenter detection is not supported. Specify the local datacenter explicitly: new DCAwareRoundRobinPolicy(localDc).")]
        public DCAwareRoundRobinPolicy()
        {
            throw new ArgumentException(
                "Automatic local datacenter detection is no longer supported. " +
                "Please specify the local datacenter explicitly: new DCAwareRoundRobinPolicy(localDc).");
        }

        /// <summary>
        ///  Creates a new datacenter aware round robin policy given the name of the local
        ///  datacenter. <p> The name of the local datacenter provided must be the local
        ///  datacenter name as known by Cassandra. </p><p> The policy created will ignore all
        ///  remote hosts. In other words, this is equivalent to
        ///  <c>new DCAwareRoundRobinPolicy(localDc, 0)</c>.</p>
        /// </summary>
        /// <param name="localDc"> the name of the local datacenter (as known by Cassandra).</param>
#pragma warning disable 618
        public DCAwareRoundRobinPolicy(string localDc) : this(localDc, 0, false)
#pragma warning restore 618
        {
        }

        ///<summary>
        /// Obsolete constructor. Creates a new DCAwareRoundRobin policy given the name of the local
        /// datacenter and that uses the provided number of host per remote
        /// datacenter as failover for the local hosts. 
        /// Setting the number of hosts per remote DC does not change anything. It is a deprecated parameter.
        /// Use DCAwareRoundRobinPolicy(string localDc, bool permitDcFailover) instead, and set permitDcFailover to true to allow DC failover. 
        /// <p>
        /// The name of the local datacenter provided must be the local
        /// datacenter name as known by Cassandra.</p>
        ///</summary>
        /// <param name="localDc">The name of the local datacenter (as known by
        /// Cassandra).</param>
        /// <param name="usedHostsPerRemoteDc">Obsolete. The number of host per remote
        /// datacenter that policies created by the returned factory should
        /// consider. Created policies <c>distance</c> method will return a
        /// <c>HostDistance.Remote</c> distance for only <c>usedHostsPerRemoteDc</c>
        /// hosts per remote datacenter. Other hosts of the remote datacenters will be ignored
        /// (and thus no connections to them will be maintained).
        /// <para>Note that setting this parameter does not change anything and it will be removed in the next major release of
        /// the driver.</para></param>
        [Obsolete(DCAwareRoundRobinPolicy.UsedHostsPerRemoteDcObsoleteMessage)]
        public DCAwareRoundRobinPolicy(string localDc, int usedHostsPerRemoteDc)
            : this(localDc, usedHostsPerRemoteDc, false)
        {
        }

        /// <summary>
        /// Creates a new datacenter aware round robin policy given the name of the local
        /// datacenter and whether to permit datacenter failover.
        /// <p>
        /// The name of the local datacenter provided must be the local
        /// datacenter name as known by Cassandra.</p>
        /// </summary>
        /// <param name="localDc">The name of the local datacenter (as known by Cassandra).</param>
        /// <param name="permitDcFailover">Whether to permit failover to remote datacenters.</param>
        public DCAwareRoundRobinPolicy(string localDc, bool permitDcFailover)
            : this(localDc, 0, permitDcFailover)
        {
        }

        private DCAwareRoundRobinPolicy(string localDc, int usedHostsPerRemoteDc, bool permitDcFailover)
        {
            _localDc = localDc;
            if (string.IsNullOrWhiteSpace(localDc))
            {
                throw new ArgumentException("Local datacenter cannot be null or empty.", nameof(localDc));
            }
            _usedHostsPerRemoteDc = usedHostsPerRemoteDc;
            PermitDcFailover = permitDcFailover;
        }

        /// <summary>
        /// Gets the Local Datacenter. This value is provided in the constructor.
        /// </summary>
        public string LocalDc => _localDc;

        /// <summary>
        /// Gets the number of hosts per remote datacenter that should be considered. This value is provided in the constructor.
        /// </summary>
        [Obsolete(DCAwareRoundRobinPolicy.UsedHostsPerRemoteDcObsoleteMessage)]
        public int UsedHostsPerRemoteDc => _usedHostsPerRemoteDc;

        /// <summary>
        /// Gets whether this policy permits failover to remote datacenters.
        /// </summary>
        public bool PermitDcFailover { get; }

        [Obsolete("Initialize is not supported. Load balancing is handled by the Rust driver internally.")]
        public void Initialize(ICluster cluster)
        {
            throw new NotSupportedException(
                "Initialize is not supported. Load balancing is handled by the Rust driver internally.");
        }

        /// <summary>
        ///  Return the HostDistance for the provided host. <p> This policy consider nodes
        ///  in the local datacenter as <c>Local</c>. For each remote datacenter, it
        ///  considers a configurable number of hosts as <c>Remote</c> and the rest
        ///  is <c>Ignored</c>. </p><p> To configure how many host in each remote
        ///  datacenter is considered <c>Remote</c>.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <c>host</c>.</returns>
        public HostDistance Distance(Host host)
        {
            var dc = host.Datacenter ?? _localDc;
            if (dc == _localDc)
            {
                return HostDistance.Local;
            }
            return HostDistance.Remote;
        }

        /// <summary>
        ///  <b>This function is not supported. All query routing is handled by the Rust driver internally.</b>
        ///  Returns the hosts to use for a new query. <p> The returned plan will always
        ///  try each known host in the local datacenter first, and then, if none of the
        ///  local host is reachable, will try up to a configurable number of other host
        ///  per remote datacenter. The order of the local node in the returned query plan
        ///  will follow a Round-robin algorithm.</p>
        /// </summary>
        /// <param name="keyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        [Obsolete("NewQueryPlan is not supported. Load balancing is handled by the Rust driver internally.")]
        public IEnumerable<HostShard> NewQueryPlan(string keyspace, IStatement query)
        {
            throw new NotSupportedException(
                "NewQueryPlan is not supported. Query routing is handled by the Rust driver internally.");
        }
    }
}
