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

namespace Cassandra.Serialization.Primitive
{
    internal class GuidSerializer : TypeSerializer<Guid>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Uuid; }
        }

        public override Guid Deserialize(ushort protocolVersion, ReadOnlySpan<byte> buffer, IColumnInfo typeInfo)
        {
            Span<byte> shuffled = stackalloc byte[16];
            GuidShuffle(buffer, shuffled);
            return new Guid((ReadOnlySpan<byte>)shuffled);
        }

        public override byte[] Serialize(ushort protocolVersion, Guid value)
        {
            var result = new byte[16];
            GuidShuffle((ReadOnlySpan<byte>)value.ToByteArray(), result);
            return result;
        }
    }
}
