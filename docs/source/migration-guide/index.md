# Migration Guide

This guide helps you migrate from the DataStax C# Driver (or ScyllaDB's fork) to the new C# RS Driver, which is an API-compatible rewrite of the C# driver as a wrapper over the Rust driver.

## Overview

The C# RS Driver aims to maintain API compatibility with the existing drivers while providing improved performance and features through the underlying Rust driver. Most of your existing code should work with minimal changes.

## Host API

### Deleted as no longer supported APIs

TODO

### Semantic/Syntactic incompatibilities

TODO

## Metadata API

### Semantic/Syntactic incompatibilities

**Important Behavioral Difference:**
In the DataStax C# driver, `Metadata` is accessible at the `Cluster` level without establishing a session. In C# RS Driver, you must create a `Session` before accessing `Metadata`, as the underlying Rust driver acquires `Metadata` information at the `Session` level.

**Migration Impact:** Ensure you establish a session before querying any metadata properties or methods.

## SocketOptions API

### Added APIs

#### `KeepAliveIntervalMillis` and `SetKeepAliveIntervalMillis`

```csharp
public int KeepAliveIntervalMillis { get; }
public SocketOptions SetKeepAliveIntervalMillis(int keepAliveIntervalMillis);
```

The interval (in milliseconds) between TCP keep-alive probes can now be configured explicitly. This option only takes effect when `KeepAlive` is set to `true`. The underlying Rust driver requires a specific interval to enable TCP keepalive.

**Migration Impact:** No action required unless you want to tune the keep-alive interval. The default value is `2000`ms. It is different from the C# Socket default which is `1000`ms.

### Removed APIs

#### `UseStreamMode` and `SetStreamMode`

```csharp
// REMOVED
public bool UseStreamMode { get; }
public SocketOptions SetStreamMode(bool useStreamMode);
```

Stream mode was a C# specific option that allowed the user to choose between .NET NetworkStream interface or SocketEventArgs interface.
The C# RS Driver now delegates all connection and I/O handling to the Rust driver, which manages its own async I/O model. The concept of "stream mode" is therefore not applicable and has been removed.

**Migration Impact:** Remove any calls to `SetStreamMode`. The Rust driver handles everything automatically and efficiently.

#### `DefunctReadTimeoutThreshold` and `SetDefunctReadTimeoutThreshold`

```csharp
// REMOVED
public int DefunctReadTimeoutThreshold { get; }
public SocketOptions SetDefunctReadTimeoutThreshold(int amountOfTimeouts);
```

The defunct-read-timeout threshold was used by the original driver to decide when to mark a connection as defunct after a number of consecutive read timeouts. Connection lifecycle and health management is now fully owned by the Rust driver, which uses its own internal mechanisms for detecting and replacing unhealthy connections.

**Migration Impact:** Remove any calls to `SetDefunctReadTimeoutThreshold`. Connection health management is handled automatically by the Rust driver.

### Not yet supported API

#### `ReadTimeoutMillis` and `SetReadTimeoutMillis`

```csharp
public int ReadTimeoutMillis { get; }
public SocketOptions SetReadTimeoutMillis(int milliseconds)
```

The underlying Rust driver only supports a global, session-level request timeout configured at session creation time. It does not expose a mechanism for per-attempt timeouts. As a result, the value set through `SetReadTimeoutMillis` is accepted by the API but currently has no effect.

**Migration Impact:** Remove any calls to `SetReadTimeoutMillis`. Once the Rust driver gains support for per-attempt timeouts, this option will usable again.