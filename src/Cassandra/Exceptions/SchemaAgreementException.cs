namespace Cassandra
{
    /// <summary>
    /// Base type for schema-agreement failures. Specific failures are reported as derived
    /// exceptions (e.g. <see cref="SchemaAgreementTimeoutException"/>) so callers can
    /// distinguish them by type, while still being able to catch any schema-agreement
    /// failure via this type. Failures that originate from a lower-level error
    /// (connection pool, prepare, request) are instead surfaced as the corresponding
    /// driver exception for that error.
    /// </summary>
    public abstract class SchemaAgreementException : DriverException
    {
        protected SchemaAgreementException(string message) : base(message, null)
        { }
    }
}
