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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Cassandra.Serialization;
using System.Linq;
using System.Management;

// ReSharper disable DoNotCallOverridableMethodsInConstructor
// ReSharper disable CheckNamespace

namespace Cassandra
{
    /// <summary>
    /// Represents the result of a query returned by the server.
    /// <para>
    /// The retrieval of the rows of a <see cref="RowSet"/> is generally paged (a first page
    /// of result is fetched and the next one is only fetched once all the results
    /// of the first page have been consumed). The size of the pages can be configured
    /// either globally through <see cref="QueryOptions.SetPageSize(int)"/> or per-statement
    /// with <see cref="IStatement.SetPageSize(int)"/>. Though new pages are automatically
    /// and transparently fetched when needed, it is possible to force the retrieval
    /// of the next page early through <see cref="FetchMoreResults"/> and  <see cref="FetchMoreResultsAsync"/>.
    /// </para>
    /// <para>
    /// The RowSet dequeues <see cref="Row"/> items while iterated. After a full enumeration of this instance, following
    /// enumerations will be empty, as all rows have been dequeued.
    /// </para>
    /// </summary>
    /// <remarks>Parallel enumerations are supported and thread-safe.</remarks>
    public class RowSet : SafeHandle, IEnumerable<Row>, IDisposable
    {
        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle()
        {
            row_set_free(handle);
            return true;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_free(IntPtr rowSetPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        // bool does not work well with C FFI, use int instead (0 = false, non-0 = true).
        unsafe private static extern int row_set_next_row(IntPtr rowSetPtr, IntPtr deserializeValue, IntPtr columnsPtr, IntPtr valuesPtr, IntPtr serializerPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_get_columns_count(IntPtr rowSetPtr);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_fill_columns_metadata(IntPtr rowSetPtr, IntPtr columnsPtr, IntPtr metadataSetter);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern byte row_set_type_info_get_code(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_list_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_set_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_udt_name(IntPtr typeInfoHandle, out IntPtr namePtr, out nuint nameLen, out IntPtr keyspacePtr, out nuint keyspaceLen);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_udt_field_count(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_udt_field(IntPtr typeInfoHandle, nuint index, out IntPtr fieldNamePtr, out nuint fieldNameLen, out IntPtr fieldTypeHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_map_children(IntPtr typeInfoHandle, out IntPtr keyHandle, out IntPtr valueHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_tuple_field_count(IntPtr typeInfoHandle);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern int row_set_type_info_get_tuple_field(IntPtr typeInfoHandle, nuint index, out IntPtr fieldHandle);

        private bool _exhausted = false;

        /// <summary>
        /// Determines if when dequeuing, it will automatically fetch the following result pages.
        /// </summary>
        protected internal bool AutoPage
        {
            get => throw new NotImplementedException("AutoPage getter is not yet implemented"); // FIXME: bridge with Rust paging.
            set => throw new NotImplementedException("AutoPage setter is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// Gets or set the internal row list. It contains the rows of the latest query page.
        /// </summary>
        protected virtual ConcurrentQueue<Row> RowQueue
        {
            get => throw new NotImplementedException("RowQueue getter is not yet implemented"); // FIXME: bridge with Rust paging.
            set => throw new NotImplementedException("RowQueue setter is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// Gets the execution info of the query
        /// </summary>
        public virtual ExecutionInfo Info { get; set; }

        /// <summary>
        /// Gets or sets the columns in the RowSet
        /// </summary>
        public virtual CqlColumn[] Columns { get; set; }

        /// <summary>
        /// Gets or sets the paging state of the query for the RowSet.
        /// When set it states that there are more pages.
        /// </summary>
        public virtual byte[] PagingState
        {
            get => throw new NotImplementedException("PagingState getter is not yet implemented"); // FIXME: bridge with Rust paging state.
            protected internal set => throw new NotImplementedException("PagingState getter is not yet implemented"); // FIXME: bridge with Rust paging state.;
        }

        /// <summary>
        /// Returns whether this ResultSet has more results.
        /// It has side-effects, if the internal queue has been consumed it will page for more results.
        /// </summary>
        /// <seealso cref="IsFullyFetched"/>
        public virtual bool IsExhausted()
        {
            return _exhausted;
        }

        /// <summary>
        /// Whether all results from this result set has been fetched from the database.
        /// </summary>
        public virtual bool IsFullyFetched => PagingState == null || !AutoPage;

        /// <summary>
        /// Creates a new instance of RowSet.
        /// </summary>
        public RowSet(IntPtr rowSetPtr) : base(IntPtr.Zero, true)
        {
            handle = rowSetPtr;
            Columns = ExtractColumnsFromRust(rowSetPtr);
            Info = new ExecutionInfo();
        }

        /// <summary>
        /// Creates a new instance of RowSet.
        /// </summary>
        public RowSet() : base(IntPtr.Zero, true)
        {
            Info = new ExecutionInfo();
            Columns = new CqlColumn[0];
            AutoPage = true;
        }

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
                            if (row_set_type_info_get_list_child(handle, out IntPtr child) != 0)
                            {
                                var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                                var childInfo = BuildTypeInfoFromHandle(child, childCode);
                                var listInfo = new ListColumnInfo { ValueTypeCode = childCode, ValueTypeInfo = childInfo };
                                return listInfo;
                            }
                        }
                        return null;
                    case ColumnTypeCode.Map:
                        // For Map: ask Rust for key/value handles
                        unsafe
                        {
                            if (row_set_type_info_get_map_children(handle, out IntPtr keyHandle, out IntPtr valueHandle) != 0)
                            {
                                var keyCode = (ColumnTypeCode)row_set_type_info_get_code(keyHandle);
                                var valueCode = (ColumnTypeCode)row_set_type_info_get_code(valueHandle);
                                var keyInfo = BuildTypeInfoFromHandle(keyHandle, keyCode);
                                var valueInfo = BuildTypeInfoFromHandle(valueHandle, valueCode);
                                var mapInfo = new MapColumnInfo { KeyTypeCode = keyCode, KeyTypeInfo = keyInfo, ValueTypeCode = valueCode, ValueTypeInfo = valueInfo };
                                return mapInfo;
                            }
                        }
                        return null;
                    case ColumnTypeCode.Tuple:
                        // For Tuple: get amount of fields and then each field
                        unsafe
                        {
                            nuint count = row_set_type_info_get_tuple_field_count(handle);
                            var tupleInfo = new TupleColumnInfo();
                            for (nuint i = 0; i < count; i++)
                            {
                                if (row_set_type_info_get_tuple_field(handle, i, out IntPtr fieldHandle) != 0)
                                {
                                    var fCode = (ColumnTypeCode)row_set_type_info_get_code(fieldHandle);
                                    var fInfo = BuildTypeInfoFromHandle(fieldHandle, fCode);
                                    var desc = new ColumnDesc { TypeCode = fCode, TypeInfo = fInfo };
                                    tupleInfo.Elements.Add(desc);
                                }
                            }
                            return tupleInfo;
                        }
                    case ColumnTypeCode.Udt:
                        // For UDT: get name+keyspace and then the fields
                        unsafe
                        {
                            if (row_set_type_info_get_udt_name(handle, out IntPtr udtNamePtr, out nuint udtNameLen, out IntPtr udtKsPtr, out nuint udtKsLen) != 0)
                            {
                                var name = (udtNamePtr == IntPtr.Zero || udtNameLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(udtNamePtr, (int)udtNameLen);
                                var ks = (udtKsPtr == IntPtr.Zero || udtKsLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(udtKsPtr, (int)udtKsLen);
                                var udtInfo = new UdtColumnInfo(name ?? "");
                                nuint fcount = row_set_type_info_get_udt_field_count(handle);
                                for (nuint i = 0; i < fcount; i++)
                                {
                                    if (row_set_type_info_get_udt_field(handle, i, out IntPtr fieldNamePtr, out nuint fieldNameLen, out IntPtr fieldTypeHandle) != 0)
                                    {
                                        var fname = (fieldNamePtr == IntPtr.Zero || fieldNameLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(fieldNamePtr, (int)fieldNameLen);
                                        var fcode = (ColumnTypeCode)row_set_type_info_get_code(fieldTypeHandle);
                                        var fInfo = BuildTypeInfoFromHandle(fieldTypeHandle, fcode);
                                        var desc = new ColumnDesc { Name = fname, TypeCode = fcode, TypeInfo = fInfo };
                                        udtInfo.Fields.Add(desc);
                                    }
                                }
                                return udtInfo;
                            }
                        }
                        return null;
                    case ColumnTypeCode.Set:
                        // For Set: ask Rust for the single element child
                        unsafe
                        {
                            if (row_set_type_info_get_set_child(handle, out IntPtr child) != 0)
                            {
                                var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                                var childInfo = BuildTypeInfoFromHandle(child, childCode);
                                var setInfo = new SetColumnInfo { KeyTypeCode = childCode, KeyTypeInfo = childInfo };
                                return setInfo;
                            }
                        }
                        return null;
                    default:
                        return null;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"[FFI] BuildTypeInfoFromHandle threw: {e}");
                return null;
            }
        }

        private static CqlColumn[] ExtractColumnsFromRust(IntPtr rowSetPtr)
        {
            // Query Rust for the number of columns
            var count = row_set_get_columns_count(rowSetPtr);
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
                row_set_fill_columns_metadata(rowSetPtr, (IntPtr)columnsPtr, (IntPtr)setColumnMetaPtr);
            }

            // This was recommended by ChatGPT in the general case to ensure the raw pointer is still valid.
            // I believe it's not needed in this particular case, because `columns` are returned from this function,
            // so they must live at least until `return`.
            // GC.KeepAlive(columns);

            return columns;
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, IntPtr, nuint, IntPtr, nuint, IntPtr, nuint, byte, IntPtr, byte, void> setColumnMetaPtr = &SetColumnMeta;

        /// <summary>
        /// This shall be called by Rust code for each column.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static void SetColumnMeta(
            IntPtr columnsPtr,
            nuint columnIndex,
            IntPtr namePtr,
            nuint nameLen,
            IntPtr keyspacePtr,
            nuint keyspaceLen,
            IntPtr tablePtr,
            nuint tableLen,
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
                    if (index < 0 || index >= columns.Length) return;

                    var col = columns[index];
                    col.Name = (namePtr == IntPtr.Zero || nameLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(namePtr, (int)nameLen);
                    col.Keyspace = (keyspacePtr == IntPtr.Zero || keyspaceLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(keyspacePtr, (int)keyspaceLen);
                    col.Table = (tablePtr == IntPtr.Zero || tableLen == 0) ? string.Empty : Marshal.PtrToStringUTF8(tablePtr, (int)tableLen);
                    col.TypeCode = (ColumnTypeCode)typeCode;
                    col.Index = index;
                    col.Type = MapTypeFromCode(col.TypeCode);
                    col.IsFrozen = isFrozen != 0;

                    // If a non-null type-info handle was provided by Rust, build the corresponding IColumnInfo
                    if (typeInfoPtr != IntPtr.Zero)
                    {
                        try
                        {
                            col.TypeInfo = BuildTypeInfoFromHandle(typeInfoPtr, col.TypeCode);
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"[FFI] BuildTypeInfoFromHandle threw: {ex}");
                        }
                    }

                }
            }
        }
#nullable enable
        private Row? DeserializeRow()
#nullable disable
        {
            object[] values = new object[Columns.Length];
            var valuesHandle = GCHandle.Alloc(values);
            IntPtr valuesPtr = GCHandle.ToIntPtr(valuesHandle);

            var columnsHandle = GCHandle.Alloc(Columns);
            IntPtr columnsPtr = GCHandle.ToIntPtr(columnsHandle);

            // TODO: reuse the serializer instance. Perhaps a static instance? Then no need to pass it around by pointer.
            IGenericSerializer serializer = (IGenericSerializer)new GenericSerializer();
            var serializerHandle = GCHandle.Alloc(serializer);
            IntPtr serializerPtr = GCHandle.ToIntPtr(serializerHandle);

            try
            {
                unsafe
                {
                    bool has_row = row_set_next_row(handle, (IntPtr)deserializeValue, columnsPtr, valuesPtr, serializerPtr) != 0;
                    if (!has_row)
                    {
                        _exhausted = true;
                        return null;
                    }
                }
            }
            finally
            {
                valuesHandle.Free();
                columnsHandle.Free();
                serializerHandle.Free();
            }

            var columnIndexes = new Dictionary<string, int>(StringComparer.Ordinal);
            for (int i = 0; i < Columns.Length; ++i)
            {
                var name = Columns[i].Name;
                if (string.IsNullOrEmpty(name))
                    continue;
                if (!columnIndexes.ContainsKey(name))
                    columnIndexes[name] = i;
            }

            return new Row(values, Columns, columnIndexes);
        }

        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, IntPtr, IntPtr, nuint, void> deserializeValue = &DeserializeValue;

        /// <summary>
        /// This shall be called by Rust code for each column in a row.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static void DeserializeValue(
            IntPtr columnsPtr,
            IntPtr valuesPtr,
            nuint valueIndex,
            IntPtr serializerPtr,
            IntPtr frameSlicePtr,
            nuint length
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
                    // TODO: handle deserialize exceptions.

                    // TODO: reuse the frameSlice buffer.
                    var frameSlice = new byte[length];
                    Marshal.Copy(frameSlicePtr, frameSlice, 0, (int)length);
                    values[valueIndex] = serializer.Deserialize(ProtocolVersion.V4, frameSlice, 0, (int)length, column.TypeCode, column.TypeInfo);
                }
                else
                {
                    throw new InvalidOperationException("GCHandle referenced type mismatch.");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] DeserializeValue threw exception: {ex}");
            }
        }


        /// <summary>
        /// Forces the fetching the next page of results for this <see cref="RowSet"/>.
        /// </summary>
        public void FetchMoreResults()
        {
            throw new NotImplementedException("FetchMoreResults is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// Asynchronously retrieves the next page of results for this <see cref="RowSet"/>.
        /// <para>
        /// The Task will be completed once the internal queue is filled with the new <see cref="Row"/>
        /// instances.
        /// </para>
        /// </summary>
        public Task FetchMoreResultsAsync()
        {
            throw new NotImplementedException("FetchMoreResultsAsync is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// The number of rows available in this row set that can be retrieved without blocking to fetch.
        /// </summary>
        public int GetAvailableWithoutFetching()
        {
            throw new NotImplementedException("GetAvailableWithoutFetching is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// For backward compatibility: It is possible to iterate using the RowSet as it is enumerable.
        /// <para>Obsolete: Note that it will be removed in future versions</para>
        /// </summary>
        public IEnumerable<Row> GetRows()
        {
            //legacy: Keep the GetRows method for Compatibility.
            return this;
        }

        /// <inheritdoc />
        public virtual IEnumerator<Row> GetEnumerator()
        {
            while (!IsExhausted())
            {
                Row row = DeserializeRow();
                if (row == null)
                    yield break;

                yield return row;
            }

            yield break;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets the next results and add the rows to the current <see cref="RowSet"/> queue.
        /// </summary>
        protected virtual void PageNext()
        {
            throw new NotImplementedException("PageNext is not yet implemented"); // FIXME: bridge with Rust paging.
        }

        /// <summary>
        /// Adds a row to the inner row list for tests purposes
        /// </summary>
        internal virtual void AddRow(Row row)
        {
            if (RowQueue == null)
            {
                throw new InvalidOperationException("Can not append a Row to a RowSet instance created for VOID results");
            }
            RowQueue.Enqueue(row);
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