using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Thrown when a single row of the schema version query response cannot be deserialized
    /// while waiting for schema agreement.
    /// </summary>
    public class SchemaAgreementSingleRowException : SchemaAgreementException
    {
        public SchemaAgreementSingleRowException(string message) : base(message)
        { }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle SchemaAgreementSingleRowExceptionFromRust(FFIString message)
        {
            var exception = new SchemaAgreementSingleRowException(message.ToManagedString());

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
