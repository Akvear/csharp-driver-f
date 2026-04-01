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
using System.Net;

namespace Cassandra.Serialization.Primitive
{
    internal class IpAddressSerializer : TypeSerializer<IPAddress>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Inet; }
        }

        public override IPAddress Deserialize(ushort protocolVersion, ReadOnlySpan<byte> buffer, IColumnInfo typeInfo)
        {
            if (buffer.Length == 4 || buffer.Length == 16)
            {
                return new IPAddress(buffer);
            }
            throw new DriverInternalError("Invalid length of Inet address: " + buffer.Length);
        }

        public override byte[] Serialize(ushort protocolVersion, IPAddress value)
        {
            return value.GetAddressBytes();
        }
    }
}
