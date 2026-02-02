using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    /// <summary>
    /// Exception that indicates an error during serialization of data.
    /// </summary>
    public class SerializationException : DriverException
    {
        public SerializationException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr SerializationExceptionFromRust(RustBridge.FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new SerializationException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}