using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Thrown when waiting for schema agreement exceeds the configured timeout.
    /// </summary>
    public class SchemaAgreementTimeoutException : SchemaAgreementException
    {
        public SchemaAgreementTimeoutException(string message) : base(message)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle SchemaAgreementTimeoutExceptionFromRust(FFIString message)
        {
            var exception = new SchemaAgreementTimeoutException(message.ToManagedString());

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
