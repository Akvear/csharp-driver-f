using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    public class RustException : DriverException
    {
        public RustException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle RustExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new RustException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
