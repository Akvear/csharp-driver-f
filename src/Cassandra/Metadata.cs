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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Serialization;
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

        // Cluster's serializer manager — carries protocol version, custom type serializers,
        // UDT mappings, and column encryption policy. Used wherever the partition key (or
        // other CLR values) must be encoded to the same bytes the server would see on the wire.
        private readonly ISerializerManager _serializerManager;

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
        internal readonly struct HostRegistry(
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyDictionary<IPEndPoint, Guid> hostIdsByIp)
        {
            internal readonly IReadOnlyDictionary<Guid, Host> HostsById =
                hostsById ?? new Dictionary<Guid, Host>();

            internal readonly IReadOnlyDictionary<IPEndPoint, Guid> HostIdsByIp =
                hostIdsByIp ?? new Dictionary<IPEndPoint, Guid>();
        }

        // ClusterSnapshot couples a BridgedClusterState with the HostRegistry built from it.
        // Each instance owns a reference count on the underlying BridgedClusterState (SafeHandle),
        // analogous to Arc in Rust. The native resource is freed when the last refcount drops.
        //
        // Disposal is split between this class (the *primary*) and the nested ClusterSnapshotClone:
        //   - Primary's Dispose -> State.Dispose()                  (sets Disposed bit, suppresses finalizer)
        //   - Clone's Dispose   -> State.DecreaseReferenceCount()   (pure refcount decrement)
        //
        // Why the split: SafeHandle.Dispose() is idempotent — subsequent calls early-return
        // without decrementing. If every ClusterSnapshot routed disposal through State.Dispose(),
        // the first call would set the Disposed bit and the rest would silently leak refcount.
        // Routing clones through DangerousRelease (which ignores the Disposed bit) keeps every
        // increment matched by a real decrement, while the primary's single State.Dispose() call
        // takes care of suppressing the finalizer.
        private class ClusterSnapshot : IDisposable
        {
            internal BridgedClusterState State { get; }
            internal HostRegistry Registry { get; }
            private bool _disposed;

            /// <summary>
            /// Clones an existing snapshot, incrementing the refcount on the underlying state.
            /// Analogous to <c>Arc::clone</c> in Rust.
            /// </summary>
            /// <exception cref="ObjectDisposedException">The source snapshot's state has already been freed.</exception>
            internal static ClusterSnapshot CloneByRef(ClusterSnapshot other)
            {
                if (!other.State.TryIncreaseReferenceCount())
                    throw new ObjectDisposedException(nameof(ClusterSnapshot),
                        "Cannot clone a snapshot whose native state has already been freed.");
                return new ClusterSnapshotClone(other.State, other.Registry);
            }

            /// <summary>
            /// Takes ownership of an existing refcount on <paramref name="state"/>.
            /// Used when building from a freshly acquired BridgedClusterState.
            /// </summary>
            private ClusterSnapshot(BridgedClusterState state, HostRegistry registry)
            {
                State = state;
                Registry = registry;
            }

            /// <summary>
            /// Builds a new snapshot from a freshly acquired <paramref name="state"/>,
            /// taking ownership of its refcount. Reuses existing Host instances from
            /// <paramref name="oldRegistry"/> where possible.
            /// </summary>
            internal static ClusterSnapshot BuildFromFreshState(
                BridgedClusterState state, HostRegistry oldRegistry)
            {
                RefreshContext context;
                try
                {
                    // oldRegistry may be default(HostRegistry) (null maps) when there is no cache yet.
                    var hostsById = oldRegistry.HostsById ?? new Dictionary<Guid, Host>();
                    context = new(hostsById);
                    state.FillHostCache(context);
                }
                catch (Exception)
                {
                    // Fully dispose (not just decrement): driving the refcount to 0 via
                    // DangerousRelease alone would leave the SafeHandle's finalizer registered,
                    // and it would throw later when ~SafeHandle runs on a 0-refcount handle.
                    state.Dispose();
                    throw;
                }
                return new ClusterSnapshot(state, context.ToNewRegistry());
            }

            public void Dispose()
            {
                // Each snapshot has a single owner and is disposed exactly once: clones are
                // per-caller, and the cached primary is evicted via Interlocked.Exchange. So
                // `_disposed` is only ever touched by that one disposing thread — no volatile
                // or atomic is needed. This guard is not load-bearing; it only catches a
                // regression that breaks the single-owner invariant, where a second dispose
                // would over-release the native refcount. Debug.Assert keeps that loud in
                // debug/test builds and compiles out of release, where it stays a no-op.
                if (_disposed)
                {
                    System.Diagnostics.Debug.Assert(false,
                        "Double dispose of ClusterSnapshot would over-release the native refcount.");
                    return;
                }
                _disposed = true;
                DisposeCore();
            }

            protected virtual void DisposeCore() => State.Dispose();

            /// <summary>
            /// An additional reference to a <see cref="ClusterSnapshot"/>'s underlying state,
            /// produced by <see cref="CloneByRef"/>. Disposing decrements the refcount via
            /// <see cref="RustResource.DecreaseReferenceCount"/> without touching the
            /// SafeHandle's Disposed bit.
            /// </summary>
            private sealed class ClusterSnapshotClone : ClusterSnapshot
            {
                internal ClusterSnapshotClone(BridgedClusterState state, HostRegistry registry)
                    : base(state, registry) { }

                protected override void DisposeCore() => State.DecreaseReferenceCount();
            }
        }

        private volatile ClusterSnapshot _cachedSnapshot = null;

        private readonly object _hostLock = new object();

        internal Metadata(
            Configuration configuration,
            Func<Session> getActiveSessionOrThrow,
            ISerializerManager serializerManager)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
            _serializerManager = serializerManager ?? throw new ArgumentNullException(nameof(serializerManager));
        }

        public void Dispose()
        {
            lock (_hostLock)
            {
                var old = Interlocked.Exchange(ref _cachedSnapshot, null);
                old?.Dispose();
            }
        }

        public Host GetHost(IPEndPoint address)
        {
            using var snapshot = GetSnapshot();
            return !snapshot.Registry.HostIdsByIp.TryGetValue(address, out var hostId)
                ? null
                : snapshot.Registry.HostsById.GetValueOrDefault(hostId);
        }

        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
            using var snapshot = GetSnapshot();
            return snapshot.Registry.HostIdsByIp.TryGetValue(address, out var hostId)
                ? hostId
                : null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            using var snapshot = GetSnapshot();
            return new List<Host>(snapshot.Registry.HostsById.Values);
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a registry instance, refreshing topology if needed.
        /// </summary>
        private ClusterSnapshot GetSnapshot()
        {
            var session = _getActiveSessionOrThrow();

            try
            {
                // Fast path: lock-free read.
                // Probe the current cluster state and compare against the cached snapshot.
                using (var probeState = session.GetClusterState())
                {
                    var cached = _cachedSnapshot;
                    if (cached != null)
                    {
                        try
                        {
                            // Clone the snapshot (increments refcount). If the state was
                            // already disposed by a concurrent replacement, this throws
                            // and we fall through to the slow path.
                            var borrowed = ClusterSnapshot.CloneByRef(cached);
                            if (borrowed.State.Equals(probeState))
                                return borrowed;

                            // Not a match — release the clone.
                            borrowed.Dispose();
                        }
                        catch (ObjectDisposedException) { }
                    }
                }

                // Slow path: cluster state changed (or no cache exists). Take the lock and rebuild.
                lock (_hostLock)
                {
                    var freshState = session.GetClusterState();
                    var cached = _cachedSnapshot;

                    // Double-check: another thread may have already updated the cache.
                    if (cached != null && cached.State.Equals(freshState))
                    {
                        freshState.Dispose();
                        return ClusterSnapshot.CloneByRef(cached); // Clone for the caller.
                    }

                    var newSnapshot = ClusterSnapshot.BuildFromFreshState(
                        freshState, cached?.Registry ?? default);

                    var old = Interlocked.Exchange(ref _cachedSnapshot, newSnapshot);
                    old?.Dispose(); // Release the cache's old refcount.

                    return ClusterSnapshot.CloneByRef(newSnapshot); // Clone for the caller.
                }
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        private ClusterStateLease AcquireClusterState()
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                return new ClusterStateLease(session, session.GetClusterState());
            }
            catch
            {
                session.DecreaseReferenceCount();
                throw;
            }
        }

        // Pairs a borrowed ClusterState handle with the session reference taken to obtain it, so a
        // single `using` releases both. A struct, so `using var` adds no heap allocation.
        private readonly struct ClusterStateLease : IDisposable
        {
            private readonly Session _session;
            internal BridgedClusterState State { get; }

            internal ClusterStateLease(Session session, BridgedClusterState state)
            {
                _session = session;
                State = state;
            }

            public void Dispose()
            {
                try
                {
                    State.Dispose();
                }
                finally
                {
                    _session.DecreaseReferenceCount();
                }
            }
        }

        /// <summary>
        /// When the caller doesn't specify a keyspace (either by passing `null` or using
        /// the overload that omits the keyspace), we send this empty sentinel value to
        /// the Rust bridge. No keyspace matches the empty name in cluster metadata, so the
        /// bridge applies its fallback replication strategy (LocalStrategy), which resolves
        /// to the single primary token owner.
        /// </summary>
        private const string NoSpecifiedKeyspace = "";

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        /// <remarks>
        /// This overload is not table-aware and always uses the Murmur3 partitioner.
        /// It does not support tablet-aware routing. Prefer
        /// <see cref="GetReplicas(string, string, IReadOnlyList{object})"/> instead.
        /// The <paramref name="partitionKey"/> must already be serialized in routing-key format.
        /// There is no dedicated public serializer for this legacy API; existing code can often reuse
        /// a routing key already computed by the driver, for example from
        /// <c>prepared.Bind(values).RoutingKey.RawRoutingKey</c>.
        /// </remarks>
        [Obsolete("This overload does not support tablet routing. Use GetReplicas(keyspace, table, partitionKeyValues) instead.")]
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            ArgumentNullException.ThrowIfNull(partitionKey);

            using var snapshot = GetSnapshot();

            // This legacy overload takes no table name, so token computation is forced to
            // Murmur3. The original driver resolved the partitioner from system.local instead;
            // FIXME: bridge that lookup from the Rust driver so non-Murmur3 clusters are handled.

            // Coalesce null keyspace to the empty sentinel; with no matching keyspace the Rust
            // side falls back to LocalStrategy, returning only the primary token owner.
            return snapshot.State.GetReplicasLegacyMurmur3(
                keyspaceName ?? NoSpecifiedKeyspace, snapshot.Registry.HostsById, partitionKey);
        }

        [Obsolete("This overload does not support tablet routing. Use GetReplicas(keyspace, table, partitionKeyValues) instead.")]
        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
#pragma warning disable CS0618
            return GetReplicas(NoSpecifiedKeyspace, partitionKey);
#pragma warning restore CS0618
        }

        /// <summary>
        /// Gets replicas for a partition key using table-aware routing.
        /// Each partition key column value is serialized individually and passed to the Rust bridge,
        /// enabling tablet-aware replica lookup and proper partitioner selection.
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspace, string table, IReadOnlyList<object> partitionKeyValues)
        {
            ArgumentNullException.ThrowIfNull(keyspace);
            ArgumentNullException.ThrowIfNull(table);
            ArgumentNullException.ThrowIfNull(partitionKeyValues);

            if (partitionKeyValues.Count == 0)
                throw new ArgumentException("Partition key values cannot be empty", nameof(partitionKeyValues));

            // The borrowed clone keeps the native state's refcount elevated for the duration
            // of the FFI call, so a concurrent cache swap on another thread can only bring it
            // down to 1, never to 0 — the native pointer stays valid until `using` disposes
            // the clone here.
            using var snapshot = GetSnapshot();
            return snapshot.State.GetReplicas(
                keyspace, table, snapshot.Registry.HostsById, partitionKeyValues,
                _serializerManager.GetCurrentSerializer());
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
            var session = _getActiveSessionOrThrow();
            try
            {
                var clusterState = session.GetClusterState();
                try
                {
                    var keyspaceMetadata = clusterState.GetKeyspaceMetadata(keyspace);
                    if (keyspaceMetadata == null)
                    {
                        clusterState.Dispose();
                    }
                    return keyspaceMetadata;
                }
                catch
                {
                    clusterState.Dispose();
                    throw;
                }
            }
            finally
            {
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            using var lease = AcquireClusterState();
            return lease.State.GetKeyspaceNames();
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
            using var lease = AcquireClusterState();
            return lease.State.GetTableNames(keyspace);
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            using var lease = AcquireClusterState();
            return lease.State.GetTableMetadata(keyspace, tableName);
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
            using var lease = AcquireClusterState();
            return lease.State.GetUdtMetadata(keyspace, typeName);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            return Task.FromResult(GetUdtDefinition(keyspace, typeName));
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
