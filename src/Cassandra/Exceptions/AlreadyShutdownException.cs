using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    public class AlreadyShutdownException : DriverException
    {
        public AlreadyShutdownException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle AlreadyShutdownExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new AlreadyShutdownException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
