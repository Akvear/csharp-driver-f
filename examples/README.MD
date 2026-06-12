# C# Driver usage samples

This folder contains examples on how to use some features of the C# Driver for ScyllaDB and [Apache Cassandra][cassandra].

You should also visit the [Documentation][doc-index] and [FAQ][faq].

## Code samples
 - Concurrent execution
   - [Inserts multiple rows in a table limiting the amount of parallel requests](ConcurrentExecutions/ExecuteInLoop/Program.cs)
 - Mapper
   - [Use the mapper with multiple keyspaces and only one session](Mapper/MultipleKeyspacesSingleSession/Program.cs)
 - Simple
   - [Insert and select with a simple statement](Simple/Program.cs)
 - Prepared
   - [Insert and select with a prepared statement](Prepared/Program.cs)
 - Collections
   - [Insert and select list, set, and map values](Collections/Program.cs)
 - UUIDs
   - [Insert and select uuid and timeuuid values](Uuids/Program.cs)
 - Duration
   - [Insert and select a duration value](Duration/Program.cs)
 - Schema metadata
   - [Inspect keyspace and table metadata](SchemaMetadata/Program.cs)
 - User Defined Types
   - [Map, insert and retrieve User-Defined Types](UserDefinedTypes/Program.cs)
 - TLS/SSL (Not working in current release)
   - Server Auth
     - [Loading a root certificate in code](Ssl/SslServerAuthOnly/LoadingCertificateManuallyExample.cs)
     - [Using a trusted root certificate or a certificate from the certificate store](Ssl/SslServerAuthOnly/WindowsCertificateStoreExample.cs)
   - Client and Server Auth
     - [Loading a root certificate and client certificates in code](Ssl/SslTwoWayAuth/LoadingCertificateManuallyExample.cs)
     - [Using certificates from the certificate store](Ssl/SslTwoWayAuth/WindowsCertificateStoreExample.cs)
 - Metrics (Not working in current release)
   - [Graphite and Grafana integration with App.Metrics](Metrics/AppMetricsGraphite/Program.cs)
 - Logging
   - [Trace-based logging example](Logging/TraceBasedLogging/Program.cs)
   - [ILoggerFactory-based logging example](Logging/FactoryBasedLogging/Program.cs)
 - OpenTelemetry (Not working in current release)
   - [Using OpenTelemetry Console Exporter with the driver](OpenTelemetry/Exporter/ConsoleExporter/Program.cs)
   - [Distributed Tracing using a Web API and a Client](OpenTelemetry/DistributedTracing)
 - Column Encryption (Not working in current release)
   - [Example usage of the Column Encryption feature](ColumnEncryption/ColumnEncryptionExample/Program.cs)

Each example is generally structured in a way where the `Cluster` and `Session` are connected at the beginning and shutdown/disposed at the end.
While this is suitable for example console applications, you should reuse the `Session` and `Cluster` instances and only call `cluster.ShutdownAsync()` / `cluster.Dispose()` when exiting your application.

If you have any questions regarding these examples, feel free to post your questions in the [mailing list][mailing-list].

[cassandra]: https://cassandra.apache.org/
[doc-index]: https://csharp-driver.docs.scylladb.com/stable/
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
[faq]: https://csharp-driver.docs.scylladb.com/stable/faq/
