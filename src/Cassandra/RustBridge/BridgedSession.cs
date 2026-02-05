using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
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
        internal BridgedSession(ManuallyDestructible mdSession) : base(mdSession)
        {
        }


        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_create(Tcb<ManuallyDestructible> tcb, [MarshalAs(UnmanagedType.LPUTF8Str)] string uri);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_shutdown(Tcb<ManuallyDestructible> tcb, IntPtr session);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIException session_get_cluster_state(IntPtr sessionPtr, out ManuallyDestructible clusterState, IntPtr constructorsPtr);

        /// <summary>
        /// Executes a query with already-serialized values.
        /// 
        /// Note: This method transfers ownership of valuesPtr to native code, thus invalidating the SerializedValues instance after use.
        /// Values, once passed to this method, should not be used again in managed code, it's the Rust side's responsibility to handle retries
        /// and to free the memory.
        /// </summary>
        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_with_values(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement, IntPtr valuesPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_prepare(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_bound(Tcb<ManuallyDestructible> tcb, IntPtr session, IntPtr preparedStatement);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_query_bound_with_values(Tcb<ManuallyDestructible> tcb, IntPtr session, IntPtr preparedStatement, IntPtr valuesPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void session_use_keyspace(Tcb<ManuallyDestructible> tcb, IntPtr session, [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspace, FFIBool isCaseSensitive);

        /// <summary>
        /// Creates a new session connected to the specified Cassandra URI.
        /// </summary>
        /// <param name="uri"></param>
        static internal Task<ManuallyDestructible> Create(string uri)
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
            session_create(tcb, uri);

            return tcs.Task;
        }

        /// <summary>
        /// Shuts down the session.
        /// </summary>
        internal Task<ManuallyDestructible> Shutdown()
        {
            TaskCompletionSource<ManuallyDestructible> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var tcb = Tcb<ManuallyDestructible>.WithTcs(tcs);
            session_shutdown(tcb, handle);
            return tcs.Task;
        }

        /// <summary>
        /// Executes a query on the session.
        /// </summary>
        /// <param name="statement">CQL statement to be executed on the session.</param>
        internal Task<ManuallyDestructible> Query(string statement)
        {
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query(tcb, ptr, statement));
        }

        /// <summary>
        /// Executes a query with serialized values.
        /// </summary>
        /// <param name="statement">CQL statement to be executed on the session.</param>
        /// <param name="queryValues">Values to be serialized and bound to the query.</param>
        internal Task<ManuallyDestructible> QueryWithValues(string statement, object[] queryValues)
        {
            IntPtr valuesPtr = SerializationHandler.InitializeSerializedValues(queryValues).TakeNativeHandle();
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query_with_values(tcb, ptr, statement, valuesPtr));
        }

        /// <summary>
        /// Sets the keyspace for the session.
        /// </summary>
        /// <param name="keyspace"></param>
        /// <param name="isCaseSensitive"></param>
        internal Task<ManuallyDestructible> UseKeyspace(string keyspace, bool isCaseSensitive)
        {
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_use_keyspace(tcb, ptr, keyspace, isCaseSensitive));
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
        internal Task<ManuallyDestructible> QueryBound(IntPtr preparedStatement)
        {
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query_bound(tcb, ptr, preparedStatement));
        }
        
        /// <summary>
        /// Executes a prepared statement with bound values.
        /// </summary>
        /// <param name="preparedStatement">Pointer to the prepared statement handle.</param>
        /// <param name="queryValues">Values to be serialized and bound to the prepared statement.</param>
        internal Task<ManuallyDestructible> QueryBoundWithValues(IntPtr preparedStatement, object[] queryValues)
        {
            IntPtr valuesPtr = SerializationHandler.InitializeSerializedValues(queryValues).TakeNativeHandle();
            return RunAsyncWithIncrement<ManuallyDestructible>((tcb, ptr) => session_query_bound_with_values(tcb, ptr, preparedStatement, valuesPtr));
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
    }
}