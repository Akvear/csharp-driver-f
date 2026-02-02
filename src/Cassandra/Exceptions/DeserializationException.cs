using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    /// <summary>
    /// Exception that indicates an error during deserialization of data.
    /// </summary>
    public class DeserializationException : DriverException
    {
        public DeserializationException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr DeserializationExceptionFromRust(RustBridge.FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new DeserializationException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}