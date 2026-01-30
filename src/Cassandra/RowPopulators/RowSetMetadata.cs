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
using System.Collections.Generic;
using Cassandra.Serialization;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    [Flags]
    internal enum RowSetMetadataFlags
    {
        GlobalTablesSpec = 0x0001,
        HasMorePages = 0x0002,
        NoMetadata = 0x0004,
        MetadataChanged = 0x0008
    }

    /// <summary>
    /// Specifies a Cassandra data type of a field
    /// </summary>
    public enum ColumnTypeCode
    {
        Custom = 0x0000,
        Ascii = 0x0001,
        Bigint = 0x0002,
        Blob = 0x0003,
        Boolean = 0x0004,
        Counter = 0x0005,
        Decimal = 0x0006,
        Double = 0x0007,
        Float = 0x0008,
        Int = 0x0009,
        Text = 0x000A,
        Timestamp = 0x000B,
        Uuid = 0x000C,
        Varchar = 0x000D,
        Varint = 0x000E,
        Timeuuid = 0x000F,
        Inet = 0x0010,
        Date = 0x0011,
        Time = 0x0012,
        SmallInt = 0x0013,
        TinyInt = 0x0014,
        Duration = 0x0015,
        List = 0x0020,
        Map = 0x0021,
        Set = 0x0022,
        Vector = 0x0023,
        /// <summary>
        /// User defined type
        /// </summary>
        Udt = 0x0030,
        /// <summary>
        /// Tuple of n subtypes
        /// </summary>
        Tuple = 0x0031
    }

    /// <summary>
    /// Specifies the type information associated with collections, maps, udts and other Cassandra types
    /// </summary>
    public interface IColumnInfo
    {
    }

    public class CustomColumnInfo : IColumnInfo
    {
        public string CustomTypeName { get; set; }

        public CustomColumnInfo()
        {

        }

        public CustomColumnInfo(string name)
        {
            CustomTypeName = name;
        }

        public override int GetHashCode()
        {
            return (CustomTypeName ?? "").GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var other = obj as CustomColumnInfo;
            if (other == null)
            {
                return false;
            }
            return CustomTypeName == other.CustomTypeName;
        }
    }

    public class ListColumnInfo : IColumnInfo, ICollectionColumnInfo
    {
        public ColumnTypeCode ValueTypeCode { get; set; }
        public IColumnInfo ValueTypeInfo { get; set; }

        ColumnDesc ICollectionColumnInfo.GetChildType()
        {
            return new ColumnDesc
            {
                TypeCode = ValueTypeCode,
                TypeInfo = ValueTypeInfo
            };
        }
    }

    public class SetColumnInfo : IColumnInfo, ICollectionColumnInfo
    {
        public ColumnTypeCode KeyTypeCode { get; set; }
        public IColumnInfo KeyTypeInfo { get; set; }

        ColumnDesc ICollectionColumnInfo.GetChildType()
        {
            return new ColumnDesc
            {
                TypeCode = KeyTypeCode,
                TypeInfo = KeyTypeInfo
            };
        }
    }

    public class MapColumnInfo : IColumnInfo
    {
        public ColumnTypeCode KeyTypeCode { get; set; }
        public IColumnInfo KeyTypeInfo { get; set; }
        public ColumnTypeCode ValueTypeCode { get; set; }
        public IColumnInfo ValueTypeInfo { get; set; }
    }

    public class VectorColumnInfo : IColumnInfo, ICollectionColumnInfo
    {
        public ColumnTypeCode ValueTypeCode { get; set; }
        public IColumnInfo ValueTypeInfo { get; set; }
        public int? Dimensions { get; set; }
        ColumnDesc ICollectionColumnInfo.GetChildType()
        {
            return new ColumnDesc
            {
                TypeCode = ValueTypeCode,
                TypeInfo = ValueTypeInfo
            };
        }
    }

    internal interface ICollectionColumnInfo
    {
        ColumnDesc GetChildType();
    }

    /// <summary>
    /// Represents the type information associated with a User Defined Type
    /// </summary>
    public class UdtColumnInfo : IColumnInfo
    {
        /// <summary>
        /// Fully qualified type name: keyspace.typeName
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the list of the inner fields contained in the UDT definition
        /// </summary>
        public List<ColumnDesc> Fields { get; private set; }

        public UdtColumnInfo(string name)
        {
            Name = name;
            Fields = new List<ColumnDesc>();
        }

        public override int GetHashCode()
        {
            return ("UDT>" + Name).GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (!(obj is UdtColumnInfo))
            {
                return false;
            }
            return GetHashCode() == obj.GetHashCode();
        }
    }

    /// <summary>
    /// Represents the information associated with a tuple column.
    /// </summary>
    public class TupleColumnInfo : IColumnInfo
    {
        /// <summary>
        /// Gets the list of the inner fields contained in the UDT definition
        /// </summary>
        public List<ColumnDesc> Elements { get; set; }

        public TupleColumnInfo()
        {
            Elements = new List<ColumnDesc>();
        }

        internal TupleColumnInfo(IEnumerable<ColumnDesc> elements)
        {
            Elements = new List<ColumnDesc>(elements);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 19;
                foreach (var elem in Elements)
                {
                    hash = hash * 31 +
                        (elem.TypeCode.GetHashCode() ^ (elem.TypeInfo != null ? elem.TypeInfo.GetHashCode() : 0));
                }
                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            if (!(obj is TupleColumnInfo))
            {
                return false;
            }
            return GetHashCode() == obj.GetHashCode();
        }
    }

    /// <summary>
    /// Represents the information for a given data type
    /// </summary>
    public class ColumnDesc
    {
        public string Keyspace { get; set; }
        public string Name { get; set; }
        public string Table { get; set; }
        public ColumnTypeCode TypeCode { get; set; }
        public IColumnInfo TypeInfo { get; set; }
        public bool IsStatic { get; set; }
        internal bool IsReversed { get; set; }
        internal bool IsFrozen { get; set; }
    }

    /// <summary>
    /// Represents the information of columns and other state values associated with a RowSet
    /// </summary>
    public class RowSetMetadata
    {
        /// <summary>
        /// Gets or sets the index of the columns within the row
        /// </summary>
        public Dictionary<string, int> ColumnIndexes { get; protected set; }

        internal byte[] PagingState { get; private set; }

        /// <summary>
        /// Gets the new_metadata_id.
        /// </summary>
        internal byte[] NewResultMetadataId { get; }

        /// <summary>
        /// Returns the keyspace as defined in the metadata response by global tables spec or the first column.
        /// </summary>
        internal string Keyspace { get; private set; }
        internal string Table { get; private set; }

        public CqlColumn[] Columns { get; internal set; }

        /// <summary>
        /// Gets or sets the column index of the partition keys.
        /// It returns null when partition keys were not parsed.
        /// </summary>
        internal int[] PartitionKeys { get; private set; }

        internal int Flags { get; private set; }

        /// <summary>
        /// Whether the new_metadata_id was set.
        /// </summary>
        internal bool HasNewResultMetadataId() => NewResultMetadataId != null;

        // for testing
        internal RowSetMetadata()
        {
        }
    }
}
