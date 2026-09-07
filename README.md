# ScyllaDB C# RS Driver

![CI workflow](https://github.com/scylladb/csharp-rs-driver/actions/workflows/main.yml/badge.svg?branch=master)

This is a client-side driver for [ScyllaDB](https://www.scylladb.com/) written in C# and Rust.
This driver is an overlay over the [ScyllaDB Rust Driver](https://github.com/scylladb/scylla-rust-driver),
with the interface based on the [ScyllaDB C# Driver](https://github.com/scylladb/csharp-driver) (a fork of the DataStax C# Driver for Apache Cassandra).
Although optimized for ScyllaDB, the driver is also compatible with [Apache Cassandra®](https://cassandra.apache.org/).

The driver targets .NET 8+.

## Getting started

### Installation

The driver is not published to NuGet yet, so it has to be built manually from source. You need:

- the [.NET SDK](https://dotnet.microsoft.com/download) 8 or 9,
- a [Rust toolchain](https://rustup.rs/) supporting edition 2024 (Rust 1.85 or newer).

```bash
git clone https://github.com/scylladb/csharp-rs-driver.git
cd csharp-rs-driver
cp build/scylladb-dev.snk build/scylladb.snk   # development assembly signing key
dotnet build src/Cassandra/Cassandra.csproj
```

### Documentation

The documentation sources live in the [`docs`](docs/source) directory:

- [Getting started](docs/source/topics/getting-started.md)
- [Migration guide](docs/source/migration-guide/index.md) — API differences against the C# Driver
- [Using the driver](docs/source/topics/using) — statements, data types, paging, metadata, routing
- [Observability](docs/source/topics/observability) — logging and tracing

Unimplemented features are tracked in the repository [issues](https://github.com/scylladb/csharp-rs-driver/issues).

### Examples

You can find example usages of the driver in the [examples directory](examples). Each example is a standalone console
application; the [examples README](examples/README.MD) marks the ones that do not work in the current release.

### Basic usage

```csharp
// Configure the builder with your cluster's contact points
var cluster = Cluster.Builder()
                     .AddContactPoints("host1")
                     .Build();

// Connect to the nodes using a keyspace
var session = cluster.Connect("sample_keyspace");

// Execute a query on a connection synchronously
var rs = session.Execute("SELECT * FROM sample_table");

// Iterate through the RowSet
foreach (var row in rs)
{
    var value = row.GetValue<int>("sample_int_column");

    // Do something with the value
}
```

## Features

The driver supports the following:

- Simple and prepared statements, executed synchronously (`Execute`) or asynchronously (`ExecuteAsync`)
- Asynchronous IO, parallel execution, request pipelining
- Automatic paging — configurable page size with transparent multi-page iteration, both sync and async
- Connection pooling, auto node discovery and automatic reconnection
- Configurable load balancing: datacenter preference and datacenter failover
- Full CQL data type coverage: all native types (including `counter`, `decimal`, `varint`, `duration`, `inet`,
  `date`/`time`/`timestamp`, `uuid`/`timeuuid`), collections, tuples, user-defined types and vectors
- Schema metadata: keyspaces with replication strategy, tables, columns, primary keys, UDTs and cluster nodes
- Replica lookup with `GetReplicas`
- Automatic schema agreement checks (`WaitForSchemaAgreement`)
- TCP socket options: `TCP_NODELAY`, keepalive and its interval, send/receive buffer sizes, `SO_REUSEADDR`, zero linger
- Error handling based on the Rust driver, mapped onto the existing C# exception hierarchy
- Driver logging: Rust `tracing` events forwarded to `System.Diagnostics.Trace` and to an `ILoggerFactory`
- CQL binary protocol version 4

## Not yet supported

These parts of the C# Driver API are present but not yet wired to the Rust driver:

- Batch statements
- Authentication
- SSL/TLS, and SNI / secure connection bundles (ScyllaDB Cloud)
- Retry policies, speculative execution and reconnection policy configuration
- Execution profiles
- Driver metrics (App.Metrics) and OpenTelemetry instrumentation
- Column encryption
- Query tracing, client warnings, custom payloads and client-side timestamps
- Materialized view, secondary index, UDF/UDA and virtual table metadata
- Heartbeat and connection pool tuning options

Some APIs were removed or changed rather than postponed; the [migration guide](docs/source/migration-guide/index.md)
lists them together with the reasoning and the recommended replacements.

## Compatibility

- **ScyllaDB** — CI runs integration tests against the latest and previous LTS releases, and the latest and previous
  non-LTS releases
- **.NET** — 8+
- **Platforms** — Linux, Windows and macOS, on x86_64 and arm64

Full integration tests against a real ScyllaDB cluster run on Linux x86_64; the other platforms are covered by unit
tests and Simulacron-based integration tests.

## Building and running the tests

```bash
make check                        # formatting, linting and code analysis (C# and Rust)
make test-unit                    # unit tests
make test-integration-simulacron  # integration tests against Simulacron
make test-integration-scylla      # integration tests against a CCM-managed ScyllaDB cluster
make test-logging                 # logging tests (each case runs in a fresh process)
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the coding standards, the code analyzers and the review process.

## Getting help

- [Slack channel][scylla-slack]
- Issues: [GitHub repository][driver-github-repo].

## Reference documentation

- [CQL binary protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec)
- [Developing applications with ScyllaDB drivers][dev-guide]

## License

ScyllaDB C# RS Driver is licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[dev-guide]: https://docs.scylladb.com/stable/get-started/develop-with-scylladb/index.html
[scylla-forum]: https://forum.scylladb.com/
[scylla-slack]: https://scylladb-users.slack.com
[driver-github-repo]: https://github.com/scylladb/csharp-rs-driver
