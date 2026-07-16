using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{

    /// <summary>
    /// Helper that starts a TCP listener on a random port and tracks whether an
    /// incoming connection was received. Used to verify that the Rust driver
    /// actually attempts to connect to the expected host:port.
    /// </summary>
    internal sealed class TcpConnectionProbe : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly TaskCompletionSource<bool> _connected =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Port { get; }
        public Task<bool> Connected => _connected.Task;

        public TcpConnectionProbe() : this(IPAddress.Loopback) { }

        public TcpConnectionProbe(IPAddress address)
        {
            _listener = new TcpListener(address, 0);
            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
            {
                _listener.Server.DualMode = true;
            }
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

            _ = Task.Run(async () =>
            {
                try
                {
                    using var client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _connected.TrySetResult(true);
                }
                catch
                {
                    _connected.TrySetResult(false);
                }
            });
        }

        /// <summary>
        /// Waits up to <paramref name="timeout"/> for an incoming TCP connection.
        /// Returns true if a connection was received.
        /// </summary>
        public async Task<bool> WaitForConnectionAsync(TimeSpan? timeout = null)
        {
            var t = timeout ?? TimeSpan.FromSeconds(10);
            var completed = await Task.WhenAny(_connected.Task, Task.Delay(t)).ConfigureAwait(false);
            return _connected.Task.IsCompleted && _connected.Task.Result;
        }

        public void Dispose()
        {
            _listener.Stop();
        }
    }

    /// <summary>
    /// Integration tests that verify the Rust driver actually attempts TCP connections
    /// to the contact points formatted by <see cref="Configuration.ParseContactPoints"/>.
    /// A simple TCP listener is used — no real cluster or Simulacron needed.
    /// </summary>
    [TestFixture]
    public class ContactPointConnectionTests
    {
        private static async Task ConnectAndIgnoreError(ICluster cluster)
        {
            try
            {
                await cluster.ConnectAsync(null).ConfigureAwait(false);
            }
            catch
            {
                // Expected — the listener is not a CQL server.
            }
        }

        [Test]
        public async Task Connect_WithStringContactPoint_AttemptsConnectionToConfiguredPort()
        {
            using var probe = new TcpConnectionProbe();
            using var cluster = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .WithPort(probe.Port)
                .Build();

            await ConnectAndIgnoreError(cluster);

            Assert.That(await probe.WaitForConnectionAsync(), Is.True,
                "Expected the Rust driver to attempt a TCP connection to the listener");
        }

        [Test]
        public async Task Connect_WithIPEndPointContactPoint_AttemptsConnectionToEndPointPort()
        {
            using var probe = new TcpConnectionProbe();
            using var cluster = Cluster.Builder()
                .AddContactPoint(new IPEndPoint(IPAddress.Loopback, probe.Port))
                .Build();

            await ConnectAndIgnoreError(cluster);

            Assert.That(await probe.WaitForConnectionAsync(), Is.True,
                "Expected the Rust driver to attempt a TCP connection to the IPEndPoint port");
        }

        [Test]
        public async Task Connect_WithIPAddressContactPoint_AttemptsConnectionToConfiguredPort()
        {
            using var probe = new TcpConnectionProbe();
            using var cluster = Cluster.Builder()
                .AddContactPoint(IPAddress.Loopback)
                .WithPort(probe.Port)
                .Build();

            await ConnectAndIgnoreError(cluster);

            Assert.That(await probe.WaitForConnectionAsync(), Is.True,
                "Expected the Rust driver to attempt a TCP connection to the configured port");
        }

        [Test]
        public async Task Connect_WithIPv6Address_AttemptsConnectionToConfiguredPort()
        {
            using var probe = new TcpConnectionProbe(IPAddress.IPv6Loopback);
            using var cluster = Cluster.Builder()
                .AddContactPoint(IPAddress.IPv6Loopback)
                .WithPort(probe.Port)
                .Build();

            await ConnectAndIgnoreError(cluster);

            Assert.That(await probe.WaitForConnectionAsync(), Is.True,
                "Expected the Rust driver to attempt a TCP connection to [::1]:port");
        }

        [Test]
        public async Task Connect_WithHostname_AttemptsConnectionToConfiguredPort()
        {
            // "localhost" may resolve to 127.0.0.1 or ::1. Listen on dual-stack (IPv6Any)
            // which accepts both IPv4 and IPv6 connections.
            using var probe = new TcpConnectionProbe(IPAddress.IPv6Any);
            using var cluster = Cluster.Builder()
                .AddContactPoint("localhost")
                .WithPort(probe.Port)
                .Build();

            await ConnectAndIgnoreError(cluster);

            Assert.That(await probe.WaitForConnectionAsync(), Is.True,
                "Expected the Rust driver to attempt a TCP connection to localhost:port");
        }
    }
}
