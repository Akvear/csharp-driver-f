using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    public class RustException : DriverException
    {
        public RustException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr RustExceptionFromRust(RustBridge.FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new RustException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}
