//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    // Host ids cross the FFI boundary as 16-byte RFC 4122 / network-order UUIDs (what Rust's
    // Uuid::as_bytes() emits and Uuid::from_slice() expects). .NET's Guid uses a mixed-endian
    // layout, so GuidToFFIFormat / GuidFromFFIFormat must shuffle on both sides and be exact
    // inverses. WaitForSchemaAgreement(IPEndPoint) relies on this round trip, so a mismatch would
    // send Rust a uuid that matches no node.
    public class HostIdFFIFormatTests : BaseUnitTest
    {
        // 550e8400-e29b-41d4-a716-446655440000 in canonical RFC 4122 byte order.
        private static readonly byte[] CanonicalBytes =
        {
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00
        };
        private static readonly Guid Uuid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");

        [Test]
        public void GuidToFFIFormat_ProducesCanonicalRfc4122Bytes()
        {
            Span<byte> buffer = stackalloc byte[16];
            RustBridge.GuidToFFIFormat(Uuid, buffer);
            NUnit.Framework.Legacy.CollectionAssert.AreEqual(CanonicalBytes, buffer.ToArray());
        }

        [Test]
        public void GuidFromFFIFormat_ParsesCanonicalRfc4122Bytes()
        {
            Assert.AreEqual(Uuid, RustBridge.GuidFromFFIFormat(CanonicalBytes));
        }

        [Test]
        public void GuidFFIFormat_SerializeThenDeserialize_RoundTrips()
        {
            Span<byte> buffer = stackalloc byte[16];
            RustBridge.GuidToFFIFormat(Uuid, buffer);
            var roundTripped = RustBridge.GuidFromFFIFormat(buffer);
            Assert.AreEqual(Uuid, roundTripped);
        }

        [Test]
        public void GuidFFIFormat_DeserializeThenSerialize_RoundTrips()
        {
            var guid = RustBridge.GuidFromFFIFormat(CanonicalBytes);
            Span<byte> buffer = stackalloc byte[16];
            RustBridge.GuidToFFIFormat(guid, buffer);
            NUnit.Framework.Legacy.CollectionAssert.AreEqual(CanonicalBytes, buffer.ToArray());
        }
    }
}
