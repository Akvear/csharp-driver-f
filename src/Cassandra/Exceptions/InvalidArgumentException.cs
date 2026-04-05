using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Indicates that an argument passed to a method is invalid. This can be useful when during an FFI call a method
    /// is called with invalid arguments, and we want to handle that in a specific way on the CSharp side.
    /// </summary>
    public class InvalidArgumentException : DriverException
    {
        public InvalidArgumentException(string message) : base(message, null)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle InvalidArgumentExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();

            var exception = new InvalidArgumentException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
