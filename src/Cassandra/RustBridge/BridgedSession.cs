using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Represents a bridged session resource managed by Rust.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class provides methods to create, manage, and interact with a session
    /// resource that is owned and managed by Rust code. It uses P/Invoke to call
    /// into the Rust library for session operations.
    /// </para>
    /// <para>
    /// It inherits from <see cref="RustResource"/> to ensure that the underlying
    /// native resource is properly released when no longer needed.
    /// </para>
    /// </remarks>
    internal sealed class BridgedSession : RustResource
    {
        private static readonly Logger Logger = new Logger(typeof(BridgedSession));

        internal BridgedSession(ManuallyDestructible mdSession) : base(mdSession)
        {
        }


        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_create(Tcb<ManuallyDestructible> tcb, BridgedSessionConfig config);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_shutdown(Tcb<ManuallyDestructible> tcb, IntPtr session);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement, SimpleStatementExecutionOptions executionOptions);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIMaybeException session_get_cluster_state(IntPtr sessionPtr, out ManuallyDestructible clusterState, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIMaybeException session_check_local_dc_existence(IntPtr sessionPtr, [MarshalAs(UnmanagedType.LPUTF8Str)] string localDc, IntPtr constructorsPtr);

        /// <summary>
        /// Executes a query with values supplied via the populate-callback pattern.
        /// Rust invokes <paramref name="populateValuesCallback"/> synchronously during this call,
        /// passing a pointer to a stack-allocated <c>PreSerializedValues</c>. The callback
        /// populates it by calling <c>psv_add_value</c> / <c>psv_add_null</c> / <c>psv_add_unset</c>.
        /// The <paramref name="populateValuesContext"/> pointer must remain valid for the duration
        /// of the call; it is not used after this function returns.
        /// </summary>
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_with_values(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement, IntPtr populateValuesContext, IntPtr populateValuesCallback, SimpleStatementExecutionOptions executionOptions);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_prepare(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_bound(
            Tcb<ManuallyDestructible> tcb,
            IntPtr session,
            IntPtr preparedStatement,
            PreparedStatementExecutionOptions executionOptions);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_bound_with_values(
            Tcb<ManuallyDestructible> tcb,
            IntPtr session,
            IntPtr preparedStatement,
            IntPtr populateValuesContext, IntPtr populateValuesCallback,
            PreparedStatementExecutionOptions executionOptions);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException session_get_keyspace(IntPtr session, IntPtr writeToStr, IntPtr context, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_await_schema_agreement(Tcb<EmptyAsyncResult> tcb, IntPtr session);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_await_schema_agreement_with_row_set(Tcb<EmptyAsyncResult> tcb, IntPtr session, IntPtr rowSet);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_await_schema_agreement_with_required_node(Tcb<EmptyAsyncResult> tcb, IntPtr session, byte* hostId);

        /// <summary>
        /// Creates a new session connected to the specified Cassandra URI. 
        /// Checks the existence of the configured local datacenter.
        /// </summary>
        /// <param name="uri"></param>
        /// <param name="keyspace"></param>
        /// <param name="clusterConfig">Cluster configuration to be applied to the session.</param>
        static internal async Task<BridgedSession> Create(string uri, string keyspace, Configuration clusterConfig)
        {
            /*
             * TaskCompletionSource is a way to programatically control a Task.
             * We create one here and pass it to Rust code, which will complete it.
             * This is a common pattern to bridge async code between C# and native code.
             */
            TaskCompletionSource<ManuallyDestructible> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            // Invoke the native code, which will complete the TCS when done.
            // We need to pass a pointer to CompleteTask because Rust code cannot directly
            // call C# methods.
            // Even though Rust code statically knows the name of the method, it cannot
            // directly call it because the .NET runtime does not expose the method
            // in a way that Rust can call it.
            // So we pass a pointer to the method and Rust code will call it via that pointer.
            // This is a common pattern to call C# code from native code ("reversed P/Invoke").
            var tcb = Tcb<ManuallyDestructible>.WithTcs(tcs);
            var bridgedSessionConfig = BridgedSessionConfig.BuildFrom(uri, keyspace, clusterConfig);
            session_create(tcb, bridgedSessionConfig);

            var bridgedSession = new BridgedSession(await tcs.Task.ConfigureAwait(false));

            // Validate the configured local datacenter against the connected cluster, mirroring
            // the post-connect check the old driver performed in DCAwareRoundRobinPolicy.
            // Only DC-aware policies set a local DC; null means there is nothing to validate.
            string localDc = bridgedSessionConfig.loadBalancingPolicy.localDC;
            if (localDc != null)
            {
                try
                {
                    unsafe
                    {
                        bridgedSession.RunWithIncrement(handle =>
                            session_check_local_dc_existence(handle, localDc, (IntPtr)Globals.ConstructorsPtr));
                    }
                }
                catch
                {
                    bridgedSession.Dispose();
                    throw;
                }
            }

            return bridgedSession;
        }

        /// <summary>
        /// Shuts down the session.
        /// </summary>
        internal Task<ManuallyDestructible> Shutdown()
        {
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_shutdown(tcb, ptr));
        }

        /// <summary>
        /// Executes a query on the session.
        /// </summary>
        /// <param name="statement">CQL statement to be executed on the session.</param>
        /// <param name="hasConsistencyLevel">Whether a consistency level override was specified.</param>
        /// <param name="consistencyLevel">Consistency level to use for the query.</param>
        /// <param name="isIdempotent">Whether the query is idempotent.</param>
        /// <param name="pageSize">Page size for the query (must be positive).</param>
        internal Task<ManuallyDestructible> Query(
            string statement,
            bool hasConsistencyLevel,
            ushort consistencyLevel,
            bool isIdempotent,
            int pageSize)
        {
            var executionOptions = new SimpleStatementExecutionOptions(
                hasConsistencyLevel, consistencyLevel, isIdempotent, pageSize);
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query(tcb, ptr, statement, executionOptions));
        }

        /// <summary>
        /// Executes a query with serialized values.
        /// </summary>
        /// <param name="statement">CQL statement to be executed on the session.</param>
        /// <param name="queryValues">Values to be serialized on demand and bound to the query.</param>
        /// <param name="serializer">Serializer to use for converting CLR values to CQL bytes.</param>
        /// <param name="hasConsistencyLevel">Whether a consistency level override was specified.</param>
        /// <param name="consistencyLevel">Consistency level to use for the query.</param>
        /// <param name="isIdempotent">Whether the query is idempotent.</param>
        /// <param name="pageSize">Page size for the query (must be positive).</param>
        internal unsafe Task<ManuallyDestructible> QueryWithValues(
            string statement,
            object[] queryValues,
            ISerializer serializer,
            bool hasConsistencyLevel,
            ushort consistencyLevel,
            bool isIdempotent,
            int pageSize)
        {
            var populateCtx = SerializationHandler.CreateContext(queryValues, serializer);
            var ctxIntPtr = (IntPtr)Unsafe.AsPointer(ref populateCtx);
            var executionOptions = new SimpleStatementExecutionOptions(
                hasConsistencyLevel, consistencyLevel, isIdempotent, pageSize);
            var task = RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) =>
                session_query_with_values(
                    tcb, ptr, statement,
                    ctxIntPtr,
                    (IntPtr)SerializationHandler.PopulateValuesPtr,
                    executionOptions));
            GC.KeepAlive(populateCtx);
            return task;
        }

        /// <summary>
        /// Prepares a statement on the session.
        /// </summary>
        /// <param name="preparedStatement">CQL statement to be prepared on the session.</param>
        internal Task<ManuallyDestructible> Prepare(string preparedStatement)
        {
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_prepare(tcb, ptr, preparedStatement));
        }

        /// <summary>
        /// Executes a prepared statement with bound values.
        /// </summary>
        /// <param name="preparedStatement">Pointer to the prepared statement handle.</param>
        /// <param name="hasConsistencyLevel">Whether a consistency level override was specified.</param>
        /// <param name="consistencyLevel">Consistency level to use for the query.</param>
        /// <param name="isIdempotent">Indicates whether the query is idempotent.</param>
        /// <param name="pageSize">Page size for the query (must be positive).</param>
        internal Task<ManuallyDestructible> QueryBound(
            IntPtr preparedStatement,
            bool hasConsistencyLevel,
            ushort consistencyLevel,
            bool isIdempotent,
            int pageSize)
        {
            var executionOptions = new PreparedStatementExecutionOptions(
                hasConsistencyLevel,
                consistencyLevel,
                isIdempotent,
                pageSize);

            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query_bound(
                tcb,
                ptr,
                preparedStatement,
                executionOptions));
        }

        /// <summary>
        /// Executes a prepared statement with bound values.
        /// </summary>
        /// <param name="preparedStatement">Pointer to the prepared statement handle.</param>
        /// <param name="queryValues">Values to be serialized on demand and bound to the prepared statement.</param>
        /// <param name="serializer">Serializer to use for converting CLR values to CQL bytes.</param>
        /// <param name="hasConsistencyLevel">Whether a consistency level override was specified.</param>
        /// <param name="consistencyLevel">Consistency level to use for the query.</param>
        /// <param name="isIdempotent">Indicates whether the query is idempotent.</param>
        /// <param name="pageSize">Page size for the query (must be positive).</param>
        internal unsafe Task<ManuallyDestructible> QueryBoundWithValues(
            IntPtr preparedStatement,
            object[] queryValues,
            ISerializer serializer,
            bool hasConsistencyLevel,
            ushort consistencyLevel,
            bool isIdempotent,
            int pageSize)
        {
            var populateCtx = SerializationHandler.CreateContext(queryValues, serializer);
            var ctxIntPtr = (IntPtr)Unsafe.AsPointer(ref populateCtx);

            var executionOptions = new PreparedStatementExecutionOptions(
                hasConsistencyLevel,
                consistencyLevel,
                isIdempotent,
                pageSize);

            var task = RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) =>
                session_query_bound_with_values(
                    tcb, ptr, preparedStatement,
                    ctxIntPtr,
                    (IntPtr)SerializationHandler.PopulateValuesPtr,
                    executionOptions));
            GC.KeepAlive(populateCtx);
            return task;
        }

        /// <summary>
        /// Waits for schema agreement on the session, requiring agreement from the coordinator
        /// node that served the given <paramref name="rowSet"/>. 
        /// </summary>
        /// <param name="rowSet">The RowSet resource whose coordinator must agree.</param>
        internal Task WaitForSchemaAgreementWithRowSet(BridgedRowSet rowSet)
        {
            // Lifetime: it is sufficient to keep the RowSet referenced only until
            // session_await_schema_agreement_with_row_set returns. The native side only
            // borrows the RowSet synchronously.
            return RunAsyncWithIncrement<EmptyAsyncResult>((tcb, sessionPtr) =>
            {
                try
                {
                    rowSet.RunWithIncrement(rowSetHandle =>
                    {
                        session_await_schema_agreement_with_row_set(tcb, sessionPtr, rowSetHandle);
                        return FFIMaybeException.Ok();
                    });
                }
                catch (Exception ex)
                {
                    // The native call never started (e.g. the RowSet was already disposed, so its
                    // DangerousAddRef threw), which means the Rust-invoked completion callback that
                    // frees the TCB's GCHandle will never run. We need to call it ourselves to finish 
                    // the TCB and avoid a memory leak.
                    Tcb<EmptyAsyncResult>.FailTask(tcb.tcs, FFIMaybeException.FromException(ex));
                }
            });
        }

        /// <summary>
        /// Waits for cluster-wide schema agreement on the session.
        /// </summary>
        internal Task WaitForSchemaAgreement()
        {
            return RunAsyncWithIncrement<EmptyAsyncResult>(
                (tcb, ptr) => session_await_schema_agreement(tcb, ptr));
        }

        /// <summary>
        /// Waits for schema agreement on the session, requiring agreement from the node
        /// identified by <paramref name="hostId"/>.
        /// </summary>
        /// <param name="hostId">The host ID (UUID) of the node that must agree.</param>
        internal Task WaitForSchemaAgreementWithRequiredNode(Guid hostId)
        {
            Span<byte> hostIdBytes = stackalloc byte[16];
            GuidToFFIFormat(hostId, hostIdBytes);

            unsafe
            {
                fixed (byte* hostIdPtr = hostIdBytes)
                {
                    var hostIdAddr = (IntPtr)hostIdPtr; //pointer-typed variables cannot be captured in lambdas, thus this trick
                    return RunAsyncWithIncrement<EmptyAsyncResult>(
                        (tcb, ptr) => session_await_schema_agreement_with_required_node(tcb, ptr, (byte*)hostIdAddr));
                }
            }
        }

        /// <summary>
        /// Gets the cluster state associated with this session.
        /// </summary>
        internal BridgedClusterState GetClusterState()
        {
            ManuallyDestructible mdClusterState = default;
            unsafe
            {
                RunWithIncrement(handle => session_get_cluster_state(handle, out mdClusterState, (IntPtr)Globals.ConstructorsPtr));
            }
            return new BridgedClusterState(mdClusterState);
        }

        /// <summary>
        /// Gets the keyspace of the session. Returns the name of the current keyspace as a string, or null if no keyspace is set.
        /// Note: This method involves marshaling a string from native code, which can be expensive.
        /// Exceptions thrown by the native code will be propagated as FFIMaybeException.
        /// </summary>
        internal string GetKeyspace()
        {
            var stringContainer = new FFIManagedStringWriter.StringContainer();
            unsafe
            {
                RunWithIncrement(handle =>
                    session_get_keyspace(
                        handle,
                        (IntPtr)FFIManagedStringWriter.WriteToStrPtr,
                        (IntPtr)Unsafe.AsPointer(ref stringContainer),
                        (IntPtr)Globals.ConstructorsPtr
                    )
                );
            }
            return stringContainer.Value;
        }

        /// <summary>
        /// TCP socket options passed to Rust.
        /// Any changes to this struct must be mirrored in the corresponding Rust struct.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct BridgedTcpConfig
        {
            internal FFIBool tcpNoDelay;
            internal FFIBool tcpKeepAlive;
            internal int tcpKeepAliveIntervalMillis;
            internal int receiveBufferSize;
            internal FFIBool reuseAddress;
            internal int sendBufferSize;
            internal int soLinger;

            internal static BridgedTcpConfig BuildFrom(SocketOptions socketOptions)
            {
                return new BridgedTcpConfig
                {
                    tcpNoDelay = socketOptions?.TcpNoDelay ?? SocketOptions.DefaultTcpNoDelay,
                    tcpKeepAlive = socketOptions?.KeepAlive ?? SocketOptions.DefaultKeepAlive,
                    tcpKeepAliveIntervalMillis = socketOptions?.KeepAliveIntervalMillis ?? SocketOptions.DefaultKeepAliveIntervalMillis,
                    receiveBufferSize = socketOptions?.ReceiveBufferSize ?? 0,
                    reuseAddress = socketOptions?.ReuseAddress ?? false,
                    sendBufferSize = socketOptions?.SendBufferSize ?? 0,
                    soLinger = socketOptions?.SoLinger ?? -1,
                };
            }
        }
        [StructLayout(LayoutKind.Sequential)]
        internal struct BridgedLoadBalancingPolicy
        {
            internal FFIBool isTokenAware;
            internal FFIBool permitDcFailover;
            [MarshalAs(UnmanagedType.LPUTF8Str)]
            internal string localDC;

            /// <summary>
            /// Extracts the relevant information from the provided ILoadBalancingPolicy and its potential child policies.
            /// <p>The sensible policies that the user can specify are:</p>
            /// <list type="bullet">
            /// <item>RoundRobinPolicy</item>
            /// <item>DCAwareRoundRobinPolicy</item>
            /// <item>TokenAwarePolicy(RoundRobinPolicy)</item>
            /// <item>TokenAwarePolicy(DCAwareRoundRobinPolicy)</item>
            /// <item>DefaultLoadBalancingPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy))</item>
            /// <item>DefaultLoadBalancingPolicy(TokenAwarePolicy(RoundRobinPolicy)) (that policy is constructed when the user does not specify any policy when creating a cluster)</item>
            /// </list>
            /// </summary>
            /// <param name="lbp">The load balancing policy to extract configuration from.</param>
            /// <returns>A <see cref="BridgedLoadBalancingPolicy"/> representing the extracted configuration.</returns>
            /// <exception cref="NotSupportedException">Thrown when the policy type is not supported.</exception>
            internal static BridgedLoadBalancingPolicy BuildFrom(ILoadBalancingPolicy lbp)
            {
                BridgedLoadBalancingPolicy rustLBP = new BridgedLoadBalancingPolicy
                {
                    isTokenAware = false,
                    localDC = null,
                };

                // The loop unwraps layers of TokenAwarePolicy and DefaultLoadBalancingPolicy until it finds DCAwareRoundRobinPolicy or RoundRobinPolicy.
                // The chain is finite and acyclic because every child policy is assigned once at construction and is
                // exposed through a get-only property, so this loop is guaranteed to terminate.
                while (lbp != null)
                {
                    switch (lbp)
                    {
                        case TokenAwarePolicy tokenAware:
                            if (rustLBP.isTokenAware)
                            {
                                Logger.Warning("Found a TokenAwarePolicy that is a child of another TokenAwarePolicy. Such double wrapping is redundant and unnecessary.");
                            }
                            rustLBP.isTokenAware = true;
                            lbp = tokenAware.ChildPolicy;
                            break;

                        case DefaultLoadBalancingPolicy defaultPolicy:
                            lbp = defaultPolicy.ChildPolicy;
                            break;

                        case DCAwareRoundRobinPolicy dcAware:
                            rustLBP.permitDcFailover = dcAware.PermitDcFailover;
                            rustLBP.localDC = dcAware.LocalDc;
                            return rustLBP;

                        case RoundRobinPolicy:
                            return rustLBP;

                        case RetryLoadBalancingPolicy:
                            throw new NotSupportedException(
                                "RetryLoadBalancingPolicy is not supported. " +
                                "The Rust driver handles node reconnection internally.");

                        default:
                            throw new NotSupportedException($"Load balancing policy {lbp.GetType().Name} is not supported.");
                    }
                }

                throw new NotSupportedException("Load balancing policy cannot be null or have a null child policy.");
            }
        }
        /// <summary>
        /// Configuration struct used to pass session creation parameters from C# to Rust.
        /// Any changes to this struct must be mirrored in the corresponding Rust struct.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct BridgedSessionConfig
        {
            [MarshalAs(UnmanagedType.LPUTF8Str)]
            internal string Uri;

            [MarshalAs(UnmanagedType.LPUTF8Str)]
            internal string Keyspace;

            internal int connectTimeoutMillis;

            internal BridgedTcpConfig tcp;

            internal BridgedLoadBalancingPolicy loadBalancingPolicy;

            internal static BridgedSessionConfig BuildFrom(string uri, string keyspace, Configuration clusterConfig)
            {
                return new BridgedSessionConfig
                {
                    Uri = uri,
                    Keyspace = keyspace ?? "",
                    connectTimeoutMillis = clusterConfig.SocketOptions?.ConnectTimeoutMillis ?? SocketOptions.DefaultConnectTimeoutMillis,
                    tcp = BridgedTcpConfig.BuildFrom(clusterConfig.SocketOptions),
                    loadBalancingPolicy = BridgedLoadBalancingPolicy.BuildFrom(clusterConfig.Policies.LoadBalancingPolicy)
                };
            }
        }

        /// <summary>
        /// Execution options passed alongside a prepared statement.
        /// Any changes to this struct must be mirrored in the Rust FFI definition.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        private readonly struct PreparedStatementExecutionOptions
        {
            internal readonly ushort ConsistencyLevel;
            internal readonly FFIBool HasConsistencyLevel;
            internal readonly FFIBool IsIdempotent;
            internal readonly int PageSize;

            internal PreparedStatementExecutionOptions(
                bool hasConsistencyLevel,
                ushort consistencyLevel,
                bool isIdempotent,
                int pageSize)
            {
                HasConsistencyLevel = hasConsistencyLevel;
                ConsistencyLevel = consistencyLevel;
                IsIdempotent = isIdempotent;
                PageSize = pageSize;
            }
        }

        /// <summary>
        /// Execution options passed alongside a simple (unprepared) statement.
        /// Any changes to this struct must be mirrored in the Rust FFI definition.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        private readonly struct SimpleStatementExecutionOptions
        {
            internal readonly ushort ConsistencyLevel;
            internal readonly FFIBool HasConsistencyLevel;
            internal readonly FFIBool IsIdempotent;
            internal readonly int PageSize;

            internal SimpleStatementExecutionOptions(
                bool hasConsistencyLevel,
                ushort consistencyLevel,
                bool isIdempotent,
                int pageSize)
            {
                HasConsistencyLevel = hasConsistencyLevel;
                ConsistencyLevel = consistencyLevel;
                IsIdempotent = isIdempotent;
                PageSize = pageSize;
            }
        }
    }
}
