using System;

using System.Runtime.InteropServices;

namespace Cassandra
{
    /// <summary>
    /// Bridges a Rust-owned prepared statement resource to C# via SafeHandle.
    /// Inherits destructor and handle management from RustResource.
    /// </summary>
    internal sealed class BridgedPreparedStatement : RustResource
    {
        internal BridgedPreparedStatement(ManuallyDestructible mdPrepared) : base(mdPrepared)
        {
        }

        internal bool IsLwt()
        {
            bool isLwt = false;
            RunWithIncrement(handle => prepared_statement_is_lwt(handle, out isLwt));
            return isLwt;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern RustBridge.FfiException prepared_statement_is_lwt(IntPtr prepared_statement, [MarshalAs(UnmanagedType.U1)] out bool isLwt);
    }
}
