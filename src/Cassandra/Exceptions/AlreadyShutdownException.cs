using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    public class AlreadyShutdownException : DriverException
    {
        public AlreadyShutdownException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr AlreadyShutdownExceptionFromRust(RustBridge.FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new AlreadyShutdownException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}
