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
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.ExecutionProfiles;
using Cassandra.Metrics;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="ISession" />
    public class Session : ISession
    {
        private readonly BridgedSession bridgedSession;
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly ICluster _cluster;
        private int _disposed;

        public int BinaryProtocolVersion => 4;

        /// <inheritdoc />
        public ICluster Cluster => _cluster;

        /// <summary>
        /// Gets the cluster configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        /// <summary>
        /// Determines if the session is already disposed
        /// </summary>
        public bool IsDisposed => Volatile.Read(ref _disposed) > 0;

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        private string _keyspace;
        public string Keyspace
        {
            get => _keyspace;
            private set => _keyspace = value;
        }

        /// <inheritdoc />
        public UdtMappingDefinitions UserDefinedTypes { get; private set; }

        public string SessionName { get; }

        public Policies Policies => Configuration.Policies;

        private Session(
            ICluster cluster,
            string keyspace,
            RustBridge.ManuallyDestructible mdSession)
        {
            _cluster = cluster;
            Configuration = cluster.Configuration;
            Keyspace = keyspace;
            bridgedSession = new BridgedSession(mdSession);
        }

        static internal async Task<ISession> CreateAsync(
            ICluster cluster,
            string contactPointUris,
            string keyspace)
        {
            Task<RustBridge.ManuallyDestructible> mdSessionTask = BridgedSession.Create(contactPointUris);

            RustBridge.ManuallyDestructible mdSession = await mdSessionTask.ConfigureAwait(false);
            var session = new Session(cluster, keyspace, mdSession);

            // If a keyspace was specified, validate it exists by executing USE statement
            // This should throw InvalidQueryException if keyspace doesn't exist.
            if (!string.IsNullOrEmpty(keyspace))
            {
                try
                {
                    await session.ExecuteAsync(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
                }
                // TO DO: Catch more specific exception from Rust driver when keyspace does not exist.
                catch (Exception)
                {
                    // If validation fails, instantly dispose the session to avoid connection pool errors.
                    try
                    {
                        session.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Session.Logger.Error($"Failed to dispose session during keyspace validation cleanup: {ex}");
                    }
                    throw;
                }
            }

            return session;
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return ExecuteAsync(statement).ToApm(callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return PrepareAsync(cqlQuery).ToApm(callback, state);
        }

        /// <inheritdoc />
        public void ChangeKeyspace(string keyspace)
        {
            if (Keyspace != keyspace)
            {
                // FIXME: Migrate to Rust `Session::use_keyspace()`.

                Execute(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
            }
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false)));
            Session.Logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
        }

        /// <inheritdoc />
        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            try
            {
                CreateKeyspace(keyspaceName, replication, durableWrites);
            }
            catch (AlreadyExistsException)
            {
                Session.Logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void DeleteKeyspace(string keyspaceName)
        {
            Execute(CqlQueryTools.GetDropKeyspaceCql(keyspaceName, false));
        }

        /// <inheritdoc />
        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            try
            {
                DeleteKeyspace(keyspaceName);
            }
            catch (InvalidQueryException)
            {
                Session.Logger.Info(string.Format("Cannot DELETE keyspace:  {0}  because it not exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            ShutdownAsync().GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task ShutdownAsync()
        {
            // Only dispose once
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                return;
            }

            // FIXME: Actually perform shutdown.
            // Remember to dequeue from Cluster's sessions list.

            // First, we shutdown the session in Rust - this acquires a write lock,
            // waits for all ongoing queries to complete, and blocks future queries.
            Task<RustBridge.ManuallyDestructible> shutdownTask = bridgedSession.Shutdown();
            RustBridge.ManuallyDestructible emptyBridgedResult = await shutdownTask.ConfigureAwait(false);
            EmptyRustResource emptyResource = new EmptyRustResource(emptyBridgedResult);

            // Free the empty bridged result returned by shutdown
            emptyResource.Dispose();

            // Then we dispose the bridged session synchronously (calls session_free in Rust).
            bridgedSession.Dispose();
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            // FIXME: Add removed Metrics.
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            var task = (Task<PreparedStatement>)ar;
            // FIXME: Add removed Metrics.
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            var task = ExecuteAsync(statement, executionProfileName);
            // FIXME: Add removed Metrics.
            TaskHelper.WaitToComplete(task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement)
        {
            return Execute(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery)
        {
            return Execute(GetDefaultStatement(cqlQuery));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, string executionProfileName)
        {
            return Execute(GetDefaultStatement(cqlQuery), executionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetConsistencyLevel(consistency));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetPageSize(pageSize));
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return ExecuteAsync(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)
        {
            switch (statement)
            {
                case RegularStatement s:
                    string queryString = s.QueryString;
                    object[] queryValues = s.QueryValues ?? [];

                    // Check if this is a USE statement to track keyspace changes.
                    // TODO: perform whole logic related to USE statements on the Rust side.
                    bool isUseStatement = IsUseKeyspace(queryString, out string newKeyspace);

                    Task<RustBridge.ManuallyDestructible> task;
                    if (queryValues.Length == 0)
                    {
                        // Use session_use_keyspace for USE statements and session_query for other statements.
                        // TODO: perform whole logic related to USE statements on the Rust side.
                        if (isUseStatement)
                        {
                            // For USE statements, call the dedicated use_keyspace method
                            // case_sensitive = true to respect the exact casing provided.
                            task = bridgedSession.UseKeyspace(newKeyspace, true);
                        }
                        else
                        {
                            task = bridgedSession.Query(queryString);
                        }
                    }
                    else
                    {
                        task = bridgedSession.QueryWithValues(
                            queryString,
                            queryValues
                        );
                    }

                    return task.ContinueWith(t =>
                    {
                        RustBridge.ManuallyDestructible mdRowSet = t.Result;
                        var rowSet = new RowSet(mdRowSet);

                        // TODO: Fix this logic once we have proper USE statement handling in the driver. Make sure no race conditions occur when updating the keyspace
                        if (isUseStatement)
                        {
                            _keyspace = newKeyspace;
                        }

                        return rowSet;
                    }, TaskContinuationOptions.ExecuteSynchronously);

                case BoundStatement bs:
                    // Only support bound statements without values for now.

                    // The managed PreparedStatement object (and the BoundStatement that
                    // references it) is rooted here by the local variable `bs`. Because there's an
                    // active reference in this scope, the GC will not collect the managed object
                    // while this method is executing — so the native resource under the pointer
                    // is guaranteed to still exist for the duration of this call.
                    IntPtr queryPrepared = bs.PreparedStatement.bridgedPreparedStatement.DangerousGetHandle();
                    object[] queryValuesBound = bs.QueryValues ?? [];

                    Task<RustBridge.ManuallyDestructible> boundTask;
                    if (queryValuesBound.Length == 0)
                    {
                        boundTask = bridgedSession.QueryBound(queryPrepared);
                    }
                    else
                    {
                        boundTask = bridgedSession.QueryBoundWithValues(
                            queryPrepared,
                            queryValuesBound
                        );
                    }

                    return boundTask.ContinueWith(t =>
                    {
                        RustBridge.ManuallyDestructible mdRowSet = t.Result;
                        return new RowSet(mdRowSet);
                    }, TaskContinuationOptions.ExecuteSynchronously);

                case BatchStatement s:
                    throw new NotImplementedException("Batches are not yet supported");

                default:
                    throw new ArgumentException("Unsupported statement type");
            }
        }

        public IDriverMetrics GetMetrics()
        {
            throw new NotImplementedException("GetMetrics is not yet implemented"); // FIXME: bridge with Rust metrics.
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery)
        {
            return Prepare(cqlQuery, null, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            // TODO: support custom payload in Rust Driver, then implement this.
            return Prepare(cqlQuery, null, customPayload);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace)
        {
            return Prepare(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            var task = PrepareAsync(cqlQuery, keyspace, customPayload);
            // FIXME: Add removed Metrics.
            TaskHelper.WaitToComplete(task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query)
        {
            return PrepareAsync(query, null, null);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            // TODO: support custom payload in Rust Driver, then implement this.
            return PrepareAsync(query, null, customPayload);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace)
        {
            return PrepareAsync(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(
            string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            if (customPayload != null)
            {
                throw new NotSupportedException("Custom payload is not yet supported in Prepare");
            }

            if (keyspace != null)
            {
                throw new NotSupportedException($"Protocol version 4 does not support" +
                                                " setting the keyspace as part of the PREPARE request");
            }

            Task<RustBridge.ManuallyDestructible> task = bridgedSession.Prepare(cqlQuery);

            return task.ContinueWith(t =>
            {
                RustBridge.ManuallyDestructible mdPreparedStatement = t.Result;
                var ps = new PreparedStatement(mdPreparedStatement, cqlQuery);
                return ps;
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            // Deprecated and implemented as no-op.
        }

        public bool WaitForSchemaAgreement(IPEndPoint hostAddress)
        {
            // Deprecated and implemented as no-op.
            return false;
        }

        private IStatement GetDefaultStatement(string cqlQuery)
        {
            return new SimpleStatement(cqlQuery);
        }

        private IRequestOptions GetRequestOptions(string executionProfileName)
        {
            // FIXME: bridge with Rust execution profiles.
            if (!Configuration.RequestOptions.TryGetValue(executionProfileName, out var profile))
            {
                throw new ArgumentException("The provided execution profile name does not exist. It must be added through the Cluster Builder.");
            }

            return profile;
        }

        // TODO: Remove this method once we have proper USE statement handling in the driver.
        // Checks if a query is a USE statement and extracts the keyspace name.
        // Returns true if the query is a USE statement, false otherwise.
        private bool IsUseKeyspace(string query, out string keyspace)
        {
            keyspace = null;

            if (string.IsNullOrWhiteSpace(query))
                return false;

            var trimmed = query.Trim();

            // Check if it starts with USE (case-insensitive)
            if (!trimmed.StartsWith("USE ", StringComparison.OrdinalIgnoreCase))
                return false;

            // Extract the keyspace name
            var parts = trimmed.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
                return false;

            var ksName = parts[1].TrimEnd(';');

            // Remove quotes if present
            if (ksName.StartsWith("\"") && ksName.EndsWith("\""))
            {
                keyspace = ksName.Substring(1, ksName.Length - 2).Replace("\"\"", "\"");
            }
            else
            {
                // Unquoted identifiers are lowercase in CQL.
                keyspace = ksName.ToLower();
            }

            return true;
        }

        /// <summary>
        /// Gets the ClusterState from the Rust session.
        /// </summary>
        internal BridgedClusterState GetClusterState()
        {
            return bridgedSession.GetClusterState();
        }

        internal bool TryIncreaseReferenceCount()
        {
            return bridgedSession.TryIncreaseReferenceCount();
        }

        internal void DecreaseReferenceCount()
        {
            bridgedSession.DecreaseReferenceCount();
        }
    }
}