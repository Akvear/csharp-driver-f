using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

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
        internal static FFIGCHandle SerializationExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new SerializationException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}