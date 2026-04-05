using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

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
        internal static FFIGCHandle DeserializationExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new DeserializationException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}