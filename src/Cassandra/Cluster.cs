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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Collections;
using Cassandra.Helpers;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="ICluster" />
    public class Cluster : ICluster
    {
        private const string DefaultVersionString = "N/A";
        private const string DefaultProductString = "ScyllaDB C# Driver";

        private static ProtocolVersion _maxProtocolVersion = ProtocolVersion.MaxSupported;
        internal static readonly Logger Logger = new Logger(typeof(Cluster));
        private readonly CopyOnWriteList<ISession> _connectedSessions = new CopyOnWriteList<ISession>();

        private readonly Metadata _metadata;
        private readonly IEnumerable<string> _contactPoints;

        // Disable unused event warnings, because they are part of the public API, so we can't remove them.
#pragma warning disable CS0067
        /// <inheritdoc />
        public event Action<Host> HostAdded;

        /// <inheritdoc />
        public event Action<Host> HostRemoved;
#pragma warning restore CS0067

        /// <summary>
        ///  Build a new cluster based on the provided initializer. <p> Note that for
        ///  building a cluster programmatically, Cluster.NewBuilder provides a slightly less
        ///  verbose shortcut with <link>NewBuilder#Build</link>. </p><p> Also note that that all
        ///  the contact points provided by <c>initializer</c> must share the same
        ///  port.</p>
        /// </summary>
        /// <param name="initializer">the Cluster.Initializer to use</param>
        /// <returns>the newly created Cluster instance </returns>
        public static Cluster BuildFrom(IInitializer initializer)
        {
            return BuildFrom(initializer, null, null);
        }

        internal static Cluster BuildFrom(IInitializer initializer, IReadOnlyList<object> nonIpEndPointContactPoints)
        {
            return BuildFrom(initializer, nonIpEndPointContactPoints, null);
        }

        internal static Cluster BuildFrom(IInitializer initializer, IReadOnlyList<object> nonIpEndPointContactPoints, Configuration config)
        {
            nonIpEndPointContactPoints = nonIpEndPointContactPoints ?? new object[0];
            if (initializer.ContactPoints.Count == 0 && nonIpEndPointContactPoints.Count == 0)
            {
                throw new ArgumentException("Cannot build a cluster without contact points");
            }

            return new Cluster(
                initializer.ContactPoints.Concat(nonIpEndPointContactPoints),
                config ?? initializer.GetConfiguration());
        }

        /// <summary>
        ///  Creates a new <link>Cluster.NewBuilder</link> instance. <p> This is a shortcut
        ///  for <c>new Cluster.NewBuilder()</c></p>.
        /// </summary>
        /// <returns>the new cluster builder.</returns>
        public static Builder Builder()
        {
            return new Builder();
        }

        /// <summary>
        /// Gets or sets the maximum protocol version used by this driver.
        /// <para>
        /// While property value is maintained for backward-compatibility,
        /// use <see cref="ProtocolOptions.SetMaxProtocolVersion(ProtocolVersion)"/> to set the maximum protocol version used by the driver.
        /// </para>
        /// <para>
        /// Protocol version used can not be higher than <see cref="ProtocolVersion.MaxSupported"/>.
        /// </para>
        /// </summary>
        public static int MaxProtocolVersion
        {
            get { return (int)_maxProtocolVersion; }
            set
            {
                if (value > (int)ProtocolVersion.MaxSupported)
                {
                    // Ignore
                    return;
                }
                _maxProtocolVersion = (ProtocolVersion)value;
            }
        }

        /// <summary>
        ///  Gets the cluster configuration.
        /// </summary>
        public Configuration Configuration { get; private set; }

        /// <inheritdoc />
        public Metadata Metadata
        {
            get
            {
                // TaskHelper.WaitToComplete(Init()); FIXME
                return _metadata;
            }
        }

        private Cluster(IEnumerable<object> contactPoints, Configuration configuration)
        {
            Configuration = configuration;
            _metadata = new Metadata(configuration, GetActiveSessionOrThrow);
            var protocolVersion = _maxProtocolVersion;
            if (Configuration.ProtocolOptions.MaxProtocolVersionValue != null &&
                Configuration.ProtocolOptions.MaxProtocolVersionValue.Value.IsSupported(configuration))
            {
                protocolVersion = Configuration.ProtocolOptions.MaxProtocolVersionValue.Value;
            }
            _contactPoints = configuration.ParseContactPoints(contactPoints);
        }

        private Session GetActiveSessionOrThrow()
        {
            // Check for a first active session in the cluster and try to increase the reference count on it.
            // If successful, return the session with guarantee that it won't be disposed while in use.
            // The caller is responsible for releasing the session and lowering the reference count.
            // If TryIncreaseReferenceCount fails, it means the session is disposed, so we skip it.
            foreach (var session in _connectedSessions.OfType<Session>().Where(s => s.TryIncreaseReferenceCount()))
            {
                return session;
            }

            // If no active sessions are found, throw an exception.
            throw new InvalidOperationException(
                "No active sessions available in cluster. Create a session (for example, by calling Cluster.Connect()) before accessing metadata.");
        }

        /// <inheritdoc />
        public ICollection<Host> AllHosts()
        {
            return _metadata.AllHosts();
        }

        /// <summary>
        /// Creates a new session on this cluster.
        /// </summary>
        public ISession Connect()
        {
            return Connect(Configuration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Creates a new session on this cluster.
        /// </summary>
        public Task<ISession> ConnectAsync()
        {
            return ConnectAsync(Configuration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public ISession Connect(string keyspace)
        {
            return TaskHelper.WaitToComplete(ConnectAsync(keyspace));
        }

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public async Task<ISession> ConnectAsync(string keyspace)
        {
            // TODO: Handle multiple contact points (join into a comma-separated list
            // and then split on the Rust side).
            string firstContactPoint = _contactPoints.First();

            var session = await Session.CreateAsync(this, firstContactPoint, keyspace).ConfigureAwait(false);

            _connectedSessions.Add(session);
            Logger.Info("Session connected ({0})", session.GetHashCode());
            return session;
        }

        /// <summary>
        /// Creates new session on this cluster, and sets it to default keyspace.
        /// If default keyspace does not exist then it will be created and session will be set to it.
        /// Name of default keyspace can be specified during creation of cluster object with <c>Cluster.Builder().WithDefaultKeyspace("keyspace_name")</c> method.
        /// </summary>
        /// <param name="replication">Replication property for this keyspace. To set it, refer to the <see cref="ReplicationStrategies"/> class methods.
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option.
        /// <p>Default value is <c>SimpleStrategy</c> with <c>'replication_factor' = 2</c></p></param>
        /// <param name="durableWrites">Whether to use the commit log for updates on this keyspace. Default is set to <c>true</c>.</param>
        /// <returns>a new session on this cluster set to default keyspace.</returns>
        public ISession ConnectAndCreateDefaultKeyspaceIfNotExists(Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            var session = Connect(null);
            session.CreateKeyspaceIfNotExists(Configuration.ClientOptions.DefaultKeyspace, replication, durableWrites);
            session.ChangeKeyspace(Configuration.ClientOptions.DefaultKeyspace);
            return session;
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <inheritdoc />
        public Host GetHost(IPEndPoint address)
        {
            return Metadata.GetHost(address);
        }

        /// <inheritdoc />
        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            return Metadata.GetReplicas(partitionKey);
        }

        /// <inheritdoc />
        public ICollection<HostShard> GetReplicas(string keyspace, byte[] partitionKey)
        {
            return Metadata.GetReplicas(keyspace, partitionKey);
        }

        /// <inheritdoc />
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchema(keyspace, table);
        }

        /// <inheritdoc />
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchemaAsync(keyspace, table);
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            ShutdownAsync(timeoutMs).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            var sessions = _connectedSessions.ClearAndGet();
            try
            {
                var tasks = new List<Task>();
                foreach (var s in sessions)
                {
                    tasks.Add(s.ShutdownAsync());
                }

                await Task.WhenAll(tasks).WaitToCompleteAsync(timeoutMs).ConfigureAwait(false);
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count == 1)
                {
                    throw ex.InnerExceptions[0];
                }
                throw;
            }
            _metadata.ShutDown(timeoutMs);

            // Dispose policies
            var speculativeExecutionPolicies = new HashSet<ISpeculativeExecutionPolicy>(new ReferenceEqualityComparer<ISpeculativeExecutionPolicy>());
            // FIXME:
            // foreach (var options in Configuration.RequestOptions.Values)
            // {
            //     speculativeExecutionPolicies.Add(options.SpeculativeExecutionPolicy);
            // }

            foreach (var sep in speculativeExecutionPolicies)
            {
                sep.Dispose();
            }

            Cluster.Logger.Info("Cluster #{0} [{1}] has been shut down.", GetHashCode(), Metadata.ClusterName);
            return;
        }
    }
}