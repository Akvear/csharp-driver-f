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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
#pragma warning disable CS0067
        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;
#pragma warning restore CS0067

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        /// <summary>
        /// Determines whether the cluster is provided as a service.
        /// </summary>
        public bool IsDbaas { get; private set; } = false;

        /// <summary>
        /// Gets the configuration associated with this instance.
        /// </summary>
        internal Configuration Configuration { get; private set; }

        // Function to get an active session from the cluster for FFI calls.
        // Provided by Cluster during construction. It never returns null.
        // It either returns a valid Session or throws InvalidOperationException.
        private readonly Func<Session> _getActiveSessionOrThrow;

        // Pointer to the last cluster state used to detect changes. This is a raw pointer 
        // stored only for comparison purposes - it does not extend the lifetime of the ClusterState. 
        // Volatile ensures visibility of updates across threads for the lock-free read in AllHosts().
        private volatile BridgedClusterState _lastClusterState = null;

        internal class RefreshContext(IReadOnlyDictionary<Guid, Host> oldHosts)
        {
            private readonly Dictionary<Guid, Host> _newHosts = new Dictionary<Guid, Host>();
            private readonly Dictionary<IPEndPoint, Guid> _newHostIdsByIp = new Dictionary<IPEndPoint, Guid>();

            internal IReadOnlyDictionary<Guid, Host> OldHosts { get; } = oldHosts;

            internal void AddHost(Host host)
            {
                _newHosts[host.HostId] = host;
                _newHostIdsByIp[host.Address] = host.HostId;
            }

            internal HostRegistry ToNewRegistry() => new HostRegistry(_newHosts, _newHostIdsByIp);
        }

        // HostRegistry groups both maps so they can be swapped atomically.
        internal sealed class HostRegistry(
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyDictionary<IPEndPoint, Guid> hostIdsByIp)
        {
            internal readonly IReadOnlyDictionary<Guid, Host> HostsById =
                hostsById ?? new Dictionary<Guid, Host>();

            internal readonly IReadOnlyDictionary<IPEndPoint, Guid> HostIdsByIp =
                hostIdsByIp ?? new Dictionary<IPEndPoint, Guid>();
        }

        // Active host registry reference; swapped atomically on refresh.
        // NOTE: Do not access this field directly; use GetRegistry() instead, since the accessor covers the
        // refreshment logic, with a compromise between limited data staleness and performance.
        private volatile HostRegistry _hostRegistry =
            new HostRegistry(new Dictionary<Guid, Host>(), new Dictionary<IPEndPoint, Guid>());

        private readonly object _hostLock = new object();

        internal Metadata(Configuration configuration, Func<Session> getActiveSessionOrThrow)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
        }

        public void Dispose()
        {
            lock (_hostLock)
            {
                _lastClusterState?.Dispose();
                _lastClusterState = null;
            }
        }

        public Host GetHost(IPEndPoint address)
        {
            var registry = GetRegistry();

            return !registry.HostIdsByIp.TryGetValue(address, out var hostId) ? null : registry.HostsById.GetValueOrDefault(hostId);
        }

        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
            if (GetRegistry().HostIdsByIp.TryGetValue(address, out var hostId))
            {
                return hostId;
            }

            return null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            // Return a snapshot copy of the values as ICollection<Host>
            return new List<Host>(GetRegistry().HostsById.Values);
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        // for tests
        internal KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => throw new NotImplementedException();

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a registry instance, refreshing topology if needed.
        /// </summary>
        private HostRegistry GetRegistry()
        {
            var session = _getActiveSessionOrThrow();

            try
            {
                // First, try to perform a lock-free read.
                // This is the fast path in the common case where the cluster state has not changed.
                using (var clusterState = session.GetClusterState())
                {
                    // If a cached cluster state exists, try to increase its reference count
                    // to use it for comparison without taking the lock.
                    if (_lastClusterState != null && _lastClusterState.TryIncreaseReferenceCount())
                    {
                        try
                        {
                            if (_lastClusterState.Equals(clusterState))
                            {
                                return _hostRegistry;
                            }
                        }
                        finally
                        {
                            // Release the reference acquired above.
                            _lastClusterState.DecreaseReferenceCount();
                        }
                    }
                }

                // Acquire the host lock to perform update if needed.
                lock (_hostLock)
                {
                    // Acquire fresh pointer inside lock - the cluster state may have changed while we waited for lock.
                    var clusterState = session.GetClusterState();
                    // Double-check: another thread may have updated the cache while we waited for lock.
                    if (_lastClusterState != null && _lastClusterState.Equals(clusterState))
                    {
                        clusterState.Dispose();
                        return _hostRegistry;
                    }

                    // If cluster state changed, and cache is stale, refresh it.
                    RefreshTopologyCache(clusterState);

                    // Dispose the old state as we don't need it anymore.
                    var oldState = Interlocked.Exchange(ref _lastClusterState, clusterState);
                    oldState?.Dispose();

                    return _hostRegistry;
                }
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow. 
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        /// Updates the cached topology if the cluster state has changed.
        /// </summary>
        private void RefreshTopologyCache(BridgedClusterState clusterState)
        {
            var context = new RefreshContext(_hostRegistry.HostsById);
            clusterState.FillHostCache(context);

            // Atomically replace the host registry reference with the new one.
            Interlocked.Exchange(ref _hostRegistry, context.ToNewRegistry());
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public ICollection<string> GetTables(string keyspace)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            // No-op for now - metadata shutdown not yet implemented
            // throw new NotImplementedException();
        }

        public Task Init()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Initiates a schema agreement check.
        /// <para/>
        /// Schema changes need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// This method does not perform retries so
        /// <see cref="ProtocolOptions.MaxSchemaAgreementWaitSeconds"/> does not apply.
        /// </summary>
        /// <returns>True if schema agreement was successful and false if it was not successful.</returns>
        public Task<bool> CheckSchemaAgreementAsync()
        {
            throw new NotImplementedException();
        }
    }
}