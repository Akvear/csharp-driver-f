using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Thrown when the schema version query result cannot be converted into a rows result
    /// while waiting for schema agreement.
    /// </summary>
    public class SchemaAgreementRowsResultException : SchemaAgreementException
    {
        public SchemaAgreementRowsResultException(string message) : base(message)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle SchemaAgreementRowsResultExceptionFromRust(FFIString message)
        {
            var exception = new SchemaAgreementRowsResultException(message.ToManagedString());

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
