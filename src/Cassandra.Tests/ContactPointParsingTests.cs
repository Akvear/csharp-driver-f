using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    [TestFixture]
    public class ContactPointParsingTests
    {
        private Configuration CreateConfiguration()
        {
            var builder = Cluster.Builder().AddContactPoint("127.0.0.1").WithPort(9042);
            return builder.GetConfiguration();
        }

        [Test]
        public void ParseContactPoints_StringHostname_UsesConfiguredPort()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { "myhost" }).ToList();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("myhost:9042", result[0]);
        }

        [Test]
        public void ParseContactPoints_StringIpv4_UsesConfiguredPort()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { "192.168.1.1" }).ToList();
            Assert.AreEqual("192.168.1.1:9042", result[0]);
        }

        [Test]
        public void ParseContactPoints_StringWithColon_Throws()
        {
            var config = CreateConfiguration();
            var ex = NUnit.Framework.Assert.Throws<InvalidOperationException>(() =>
                config.ParseContactPoints(new object[] { "host:9042" }).ToList());
            NUnit.Framework.Assert.That(ex.Message, Does.Contain("should not contain a port"));
        }

        [Test]
        public void ParseContactPoints_IPAddress_UsesConfiguredPort()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { IPAddress.Parse("10.0.0.1") }).ToList();
            Assert.AreEqual("10.0.0.1:9042", result[0]);
        }

        [Test]
        public void ParseContactPoints_IPv6Address_WrapsInBrackets()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { IPAddress.IPv6Loopback }).ToList();
            Assert.AreEqual("[::1]:9042", result[0]);
        }

        [Test]
        public void ParseContactPoints_StringIpv6_UsesConfiguredPortAndWrapsInBrackets()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { "::1" }).ToList();
            Assert.AreEqual("[::1]:9042", result[0]);
        }

        [Test]
        public void ParseContactPoints_IPEndPoint_UsesEndPointPort()
        {
            var config = CreateConfiguration();
            var endpoint = new IPEndPoint(IPAddress.Parse("192.168.0.1"), 7000);
            var result = config.ParseContactPoints(new object[] { endpoint }).ToList();
            Assert.AreEqual("192.168.0.1:7000", result[0]);
        }

        [Test]
        public void ParseContactPoints_IPv6EndPoint_WrapsInBrackets()
        {
            var config = CreateConfiguration();
            var endpoint = new IPEndPoint(IPAddress.Parse("::1"), 7001);
            var result = config.ParseContactPoints(new object[] { endpoint }).ToList();
            Assert.AreEqual("[::1]:7001", result[0]);
        }

        [Test]
        public void ParseContactPoints_UnsupportedType_Throws()
        {
            var config = CreateConfiguration();
            var ex = NUnit.Framework.Assert.Throws<InvalidOperationException>(() =>
                config.ParseContactPoints(new object[] { 12345 }).ToList());
            NUnit.Framework.Assert.That(ex.Message, Does.Contain("should be either string, IPEndPoint, or IPAddress"));
        }

        [Test]
        public void ParseContactPoints_MultipleContactPoints_ReturnsAll()
        {
            var config = CreateConfiguration();
            var contactPoints = new object[]
            {
                "host1",
                IPAddress.Parse("10.0.0.2"),
                new IPEndPoint(IPAddress.Parse("10.0.0.3"), 8000)
            };
            var result = config.ParseContactPoints(contactPoints).ToList();
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual("host1:9042", result[0]);
            Assert.AreEqual("10.0.0.2:9042", result[1]);
            Assert.AreEqual("10.0.0.3:8000", result[2]);
        }

        [Test]
        public void ParseContactPoints_EmptyList_ReturnsEmpty()
        {
            var config = CreateConfiguration();
            var result = config.ParseContactPoints(new object[] { }).ToList();
            Assert.AreEqual(0, result.Count);
        }
    }
}
