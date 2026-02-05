using System;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Bridges a Rust-owned RowSet resource to C# via SafeHandle.
    /// Inherits destructor and handle management from RustResource.
    /// </summary>
    internal sealed class BridgedRowSet : RustResource
    {
        internal BridgedRowSet(ManuallyDestructible mdRowSet) : base(mdRowSet)
        {
        }

        // Internal API

        /// <summary>
        /// Gets the next row and deserializes its values into the provided values array.
        /// </summary>
        /// <param name="values">An array to hold the deserialized values of the row.</param>
        /// <param name="Columns">The columns metadata for the row.</param>
        /// <param name="serializer">The serializer to use for deserialization.</param>
        /// <returns>True if a row was retrieved; false if there are no more rows.</returns>
        internal bool NextRow(ref object[] values, CqlColumn[] Columns, ref IGenericSerializer serializer)
        {
            var valuesHandle = GCHandle.Alloc(values);
            IntPtr valuesPtr = GCHandle.ToIntPtr(valuesHandle);

            var columnsHandle = GCHandle.Alloc(Columns);
            IntPtr columnsPtr = GCHandle.ToIntPtr(columnsHandle);

            var serializerHandle = GCHandle.Alloc(serializer);
            IntPtr serializerPtr = GCHandle.ToIntPtr(serializerHandle);
            FFIBool hasRow = false;

            try
            {
                unsafe
                {
                    RunWithIncrement(handle => row_set_next_row(handle, (IntPtr)deserializeValue, columnsPtr, valuesPtr, serializerPtr, out hasRow, (IntPtr)Globals.ConstructorsPtr));
                }
            }
            finally
            {
                valuesHandle.Free();
                columnsHandle.Free();
                serializerHandle.Free();
            }
            return hasRow;
        }

        /// <summary>
        /// Extracts the columns metadata from the Rust RowSet.
        /// </summary>
        /// <returns>An array of CqlColumn representing the columns metadata.</returns>
        internal CqlColumn[] ExtractColumnsFromRust()
        {
            // Query Rust for the number of columns
            var count = GetColumnsCount();
            if (count <= 0)
            {
                return [];
            }

            var columns = new CqlColumn[count];
            for (nuint i = 0; i < count; i++)
            {
                columns[i] = new CqlColumn();
            }

            unsafe
            {
                void* columnsPtr = Unsafe.AsPointer(ref columns);
                FillColumnsMetadata((IntPtr)columnsPtr, (IntPtr)setColumnMetaPtr, (IntPtr)Globals.ConstructorsPtr);
            }

            // This was recommended by ChatGPT in the general case to ensure the raw pointer is still valid.
            // I believe it's not needed in this particular case, because `columns` are returned from this function,
            // so they must live at least until `return`.
            // GC.KeepAlive(columns);

            return columns;
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, FFIException> setColumnMetaPtr = &SetColumnMeta;

        /// <summary>
        /// This shall be called by Rust code for each column.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIException SetColumnMeta(
            IntPtr columnsPtr,
            nuint columnIndex,
            FFIString name,
            FFIString keyspace,
            FFIString table,
            byte typeCode,
            IntPtr typeInfoPtr,
            byte isFrozen
        )
        {
            unsafe
            {
                // Safety:
                // 1. pointer validity:
                //   - columnsPtr is a valid pointer to an array of CqlColumn.
                //   - the referenced CqlColumn[] array lives **on the stack of the caller** (ExtractColumnsFromRust),
                //     so it cannot be GC-collected during this call.
                //   - the CqlColumn[] materialised here is transient, i.e., not stored beyond this call.
                // 2. array length:
                //   - the referenced CqlColumn[] array has length equal to the number of columns in the RowSet.
                //   - columnIndex is within bounds of the columns array.
                int index = (int)columnIndex;

                CqlColumn[] columns = Unsafe.Read<CqlColumn[]>((void*)columnsPtr);
                {
                    if (index < 0 || index >= columns.Length)
                    {
                        // I am not sure whether this warrant panicking or returning an error.
                        return FFIException.FromException(
                            new IndexOutOfRangeException($"Column index {index} is out of range (0..{columns.Length - 1})")
                        );
                    }

                    var col = columns[index];
                    col.Name = name.ToManagedString();
                    col.Keyspace = keyspace.ToManagedString();
                    col.Table = table.ToManagedString();
                    col.TypeCode = (ColumnTypeCode)typeCode;
                    col.Index = index;
                    col.Type = MapTypeFromCode(col.TypeCode);
                    col.IsFrozen = isFrozen != 0;

                    // If a non-null type-info handle was provided by Rust, build the corresponding IColumnInfo
                    if (typeInfoPtr != IntPtr.Zero)
                    {
                        col.TypeInfo = BuildTypeInfoFromHandle(typeInfoPtr, col.TypeCode);
                    }
                }
                return FFIException.Ok();
            }
        }

        // Private methods and P/Invoke

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException row_set_next_row(IntPtr rowSetPtr, IntPtr deserializeValue, IntPtr columnsPtr, IntPtr valuesPtr, IntPtr serializerPtr, out FFIBool hasRow, IntPtr constructorsPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException row_set_get_columns_count(IntPtr rowSetPtr, out nuint count);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException row_set_fill_columns_metadata(IntPtr rowSetPtr, IntPtr columnsPtr, IntPtr metadataSetter, IntPtr constructorsPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern byte row_set_type_info_get_code(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_list_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_set_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_udt_name(IntPtr typeInfoHandle, out FFIString name);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_udt_field_count(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_udt_field(IntPtr typeInfoHandle, nuint index, out FFIString fieldName, out IntPtr fieldTypeHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_map_children(IntPtr typeInfoHandle, out IntPtr keyHandle, out IntPtr valueHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_tuple_field_count(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_tuple_field(IntPtr typeInfoHandle, nuint index, out IntPtr fieldHandle);

        private void FillColumnsMetadata(IntPtr columnsPtr, IntPtr metadataSetter, IntPtr constructorsPtr)
        {
            RunWithIncrement(handle => row_set_fill_columns_metadata(handle, columnsPtr, metadataSetter, constructorsPtr));
        }

        private nuint GetColumnsCount()
        {
            nuint count = 0;
            RunWithIncrement(handle => row_set_get_columns_count(handle, out count));
            return count;
        }

        // This function is called from UnmanagedCallersOnly context - it must not throw exceptions.
        private static IColumnInfo BuildTypeInfoFromHandle(IntPtr handle, ColumnTypeCode code)
        {
            if (handle == IntPtr.Zero) return null;
            try
            {
                switch (code)
                {
                    case ColumnTypeCode.List:
                        // For List: ask Rust for the child handle and build recursively
                        unsafe
                        {
                            row_set_type_info_get_list_child(handle, out IntPtr child);
                            var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                            var childInfo = BuildTypeInfoFromHandle(child, childCode);
                            var listInfo = new ListColumnInfo { ValueTypeCode = childCode, ValueTypeInfo = childInfo };
                            return listInfo;
                        }
                    case ColumnTypeCode.Map:
                        // For Map: ask Rust for key/value handles
                        unsafe
                        {
                            row_set_type_info_get_map_children(handle, out IntPtr keyHandle, out IntPtr valueHandle);
                            var keyCode = (ColumnTypeCode)row_set_type_info_get_code(keyHandle);
                            var valueCode = (ColumnTypeCode)row_set_type_info_get_code(valueHandle);
                            var keyInfo = BuildTypeInfoFromHandle(keyHandle, keyCode);
                            var valueInfo = BuildTypeInfoFromHandle(valueHandle, valueCode);
                            var mapInfo = new MapColumnInfo { KeyTypeCode = keyCode, KeyTypeInfo = keyInfo, ValueTypeCode = valueCode, ValueTypeInfo = valueInfo };
                            return mapInfo;
                        }
                    case ColumnTypeCode.Tuple:
                        // For Tuple: get amount of fields and then each field
                        unsafe
                        {
                            nuint count = row_set_type_info_get_tuple_field_count(handle);
                            var tupleInfo = new TupleColumnInfo();
                            for (nuint i = 0; i < count; i++)
                            {
                                row_set_type_info_get_tuple_field(handle, i, out IntPtr fieldHandle);
                                var fCode = (ColumnTypeCode)row_set_type_info_get_code(fieldHandle);
                                var fInfo = BuildTypeInfoFromHandle(fieldHandle, fCode);
                                var desc = new ColumnDesc { TypeCode = fCode, TypeInfo = fInfo };
                                tupleInfo.Elements.Add(desc);
                            }
                            return tupleInfo;
                        }
                    case ColumnTypeCode.Udt:
                        // For UDT: get name+keyspace and then the fields
                        unsafe
                        {
                            row_set_type_info_get_udt_name(handle, out FFIString udtName);
                            var name = udtName.ToManagedString();
                            var udtInfo = new UdtColumnInfo(name ?? "");
                            nuint fcount = row_set_type_info_get_udt_field_count(handle);
                            for (nuint i = 0; i < fcount; i++)
                            {
                                row_set_type_info_get_udt_field(handle, i, out FFIString fieldName, out IntPtr fieldTypeHandle);
                                {
                                    var fname = fieldName.ToManagedString();
                                    var fcode = (ColumnTypeCode)row_set_type_info_get_code(fieldTypeHandle);
                                    var fInfo = BuildTypeInfoFromHandle(fieldTypeHandle, fcode);
                                    var desc = new ColumnDesc { Name = fname, TypeCode = fcode, TypeInfo = fInfo };
                                    udtInfo.Fields.Add(desc);
                                }
                            }
                            return udtInfo;
                        }
                    case ColumnTypeCode.Set:
                        // For Set: ask Rust for the single element child
                        unsafe
                        {
                            row_set_type_info_get_set_child(handle, out IntPtr child);
                            {
                                var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                                var childInfo = BuildTypeInfoFromHandle(child, childCode);
                                var setInfo = new SetColumnInfo { KeyTypeCode = childCode, KeyTypeInfo = childInfo };
                                return setInfo;
                            }
                        }
                    default:
                        return null;
                }
            }
            catch (Exception e)
            {
                // Realistically nothing throws exceptions here so there shouldn't be any exceptions to catch.
                Environment.FailFast($"Unexpected exception in BuildTypeInfoFromHandle: {e}");
                return null;
            }
        }

        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, IntPtr, FFIByteSlice, FFIException> deserializeValue = &DeserializeValue;

        /// <summary>
        /// This shall be called by Rust code for each column in a row.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static FFIException DeserializeValue(
            IntPtr columnsPtr,
            IntPtr valuesPtr,
            nuint valueIndex,
            IntPtr serializerPtr,
            FFIByteSlice FFIframeSlice
        )
        {
            try
            {
                var valuesHandle = GCHandle.FromIntPtr(valuesPtr);
                var columnsHandle = GCHandle.FromIntPtr(columnsPtr);
                var serializerHandle = GCHandle.FromIntPtr(serializerPtr);

                if (valuesHandle.Target is object[] values && columnsHandle.Target is CqlColumn[] columns && serializerHandle.Target is IGenericSerializer serializer)
                {
                    CqlColumn column = columns[valueIndex];

                    // TODO: reuse the frameSlice buffer.
                    var frameSlice = FFIframeSlice.ToSpan().ToArray();
                    int length = frameSlice.Length;
                    values[valueIndex] = serializer.Deserialize(ProtocolVersion.V4, frameSlice, 0, length, column.TypeCode, column.TypeInfo);
                }
                else
                {
                    throw new InvalidOperationException("GCHandle referenced type mismatch.");
                }
                return FFIException.Ok();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] DeserializeValue threw exception: {ex}");
                return FFIException.FromException(ex);
            }
        }

        private static Type MapTypeFromCode(ColumnTypeCode code)
        {
            return code switch
            {
                ColumnTypeCode.Ascii => typeof(string),
                ColumnTypeCode.Bigint => typeof(long),
                ColumnTypeCode.Blob => typeof(byte[]),
                ColumnTypeCode.Boolean => typeof(bool),
                ColumnTypeCode.Counter => typeof(long),
                ColumnTypeCode.Decimal => typeof(decimal),
                ColumnTypeCode.Double => typeof(double),
                ColumnTypeCode.Float => typeof(float),
                ColumnTypeCode.Int => typeof(int),
                ColumnTypeCode.Text => typeof(string),
                ColumnTypeCode.Timestamp => typeof(DateTime),
                ColumnTypeCode.Uuid => typeof(Guid),
                ColumnTypeCode.Varchar => typeof(string),
                ColumnTypeCode.Varint => typeof(System.Numerics.BigInteger),
                ColumnTypeCode.Timeuuid => typeof(Guid),
                ColumnTypeCode.Inet => typeof(System.Net.IPAddress),
                ColumnTypeCode.Date => typeof(DateOnly),
                ColumnTypeCode.Time => typeof(TimeOnly),
                ColumnTypeCode.SmallInt => typeof(short),
                ColumnTypeCode.TinyInt => typeof(sbyte),
                ColumnTypeCode.Duration => typeof(TimeSpan),
                ColumnTypeCode.List => typeof(object),
                ColumnTypeCode.Map => typeof(object),
                ColumnTypeCode.Set => typeof(object),
                ColumnTypeCode.Udt => typeof(object),
                ColumnTypeCode.Tuple => typeof(object),
                _ => typeof(object)
            };
        }
    }
}
