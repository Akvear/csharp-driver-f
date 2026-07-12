using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Thrown when a host required for schema agreement is not present in the connection pool.
    /// </summary>
    public class SchemaAgreementRequiredHostAbsentException : SchemaAgreementException
    {
        public SchemaAgreementRequiredHostAbsentException(string message) : base(message)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle SchemaAgreementRequiredHostAbsentExceptionFromRust(FFIString message)
        {
            var exception = new SchemaAgreementRequiredHostAbsentException(message.ToManagedString());

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
