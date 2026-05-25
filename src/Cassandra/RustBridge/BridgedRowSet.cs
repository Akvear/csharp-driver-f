using System;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Result of a synchronous attempt to read the next row.
    /// Must match the Rust <c>SyncNextRowResult</c> enum layout.
    /// </summary>
    internal enum SyncNextRowResult : byte
    {
        /// <summary>A row was successfully read and deserialized.</summary>
        GotRow = 0,
        /// <summary>No more rows available (the result set is exhausted).</summary>
        Exhausted = 1,
        /// <summary>Could not complete synchronously; fall back to the async path.</summary>
        NeedAsync = 2,
    }

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
        internal Task<bool> NextRow(object[] values, CqlColumn[] Columns, IGenericSerializer serializer)
        {
            // Fast path: synchronous, zero-alloc.
            // Attempts to read the next row without spawning a tokio task.
            // This succeeds when the pager lock is uncontended and the next row
            // is already buffered in the current page (the common case).
            unsafe
            {
                SyncNextRowResult syncResult = default;
                IntPtr columnsPtr = (IntPtr)Unsafe.AsPointer(ref Columns);
                IntPtr valuesPtr = (IntPtr)Unsafe.AsPointer(ref values);
                IntPtr serializerPtr = (IntPtr)Unsafe.AsPointer(ref serializer);

                RunWithIncrement(handle =>
                    row_set_try_next_row_sync(
                        handle,
                        (IntPtr)deserializeValueDirect,
                        columnsPtr,
                        valuesPtr,
                        serializerPtr,
                        (IntPtr)Globals.ConstructorsPtr,
                        out syncResult
                    )
                );

                // Prevent the GC from collecting the managed objects before
                // the synchronous native call has finished using their pointers.
                GC.KeepAlive(Columns);
                GC.KeepAlive(values);
                GC.KeepAlive(serializer);

                switch (syncResult)
                {
                    case SyncNextRowResult.GotRow:
                        return Task.FromResult(true);
                    case SyncNextRowResult.Exhausted:
                        return Task.FromResult(false);
                    case SyncNextRowResult.NeedAsync:
                        // Fall through to the async path below.
                        break;
                }
            }

            return NextRowAsync(values, Columns, serializer);
        }

        /// <summary>
        /// Async slow path for NextRow: spawns a tokio task to fetch the next
        /// row when it is not immediately available (e.g. page boundary).
        /// Split out so the fast path avoids async state machine allocation.
        /// </summary>
        private async Task<bool> NextRowAsync(object[] values, CqlColumn[] Columns, IGenericSerializer serializer)
        {
            // Slow path: the row is not immediately available (e.g. waiting for the
            // next page to be fetched from the server).
            // Allocate GCHandles (owned by Rust) and a TaskCompletionSource,
            // and spawn a tokio task to complete the operation asynchronously.
            var columnsHandle = new FFIGCHandle(GCHandle.Alloc(Columns));
            var valuesHandle = new FFIGCHandle(GCHandle.Alloc(values));
            var serializerHandle = new FFIGCHandle(GCHandle.Alloc(serializer));

            Task<FFIBool> task;
            unsafe
            {
                task = RunAsyncWithIncrement<FFIBool>((tcb, row_set) => row_set_next_row_async(tcb, row_set, (IntPtr)deserializeValue, columnsHandle, valuesHandle, serializerHandle, (IntPtr)Globals.ConstructorsPtr));
            }
            return await task.ConfigureAwait(false);
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
                FillColumnsMetadata((IntPtr)columnsPtr, (IntPtr)setColumnMetaPtr);
            }

            // This was recommended by ChatGPT in the general case to ensure the raw pointer is still valid.
            // I believe it's not needed in this particular case, because `columns` are returned from this function,
            // so they must live at least until `return`.
            // GC.KeepAlive(columns);

            return columns;
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, FFIMaybeException> setColumnMetaPtr = &SetColumnMeta;

        /// <summary>
        /// This shall be called by Rust code for each column.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIMaybeException SetColumnMeta(
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
                        return FFIMaybeException.FromException(
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
                        col.TypeInfo = BuildTypeInfoFromHandle(typeInfoPtr, col.TypeCode, col.Keyspace);
                    }
                }
                return FFIMaybeException.Ok();
            }
        }

        // Private methods and P/Invoke

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_next_row_async(Tcb<FFIBool> tcb, IntPtr rowSetPtr, IntPtr deserializeValue, FFIGCHandle columnsHandle, FFIGCHandle valuesHandle, FFIGCHandle serializerHandle, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException row_set_try_next_row_sync(IntPtr rowSetPtr, IntPtr deserializeValue, IntPtr columnsPtr, IntPtr valuesPtr, IntPtr serializerPtr, IntPtr constructorsPtr, out SyncNextRowResult result);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException row_set_get_columns_count(IntPtr rowSetPtr, out nuint count);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException row_set_fill_columns_metadata(IntPtr rowSetPtr, IntPtr columnsPtr, IntPtr metadataSetter);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern byte row_set_type_info_get_code(IntPtr typeInfoHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_list_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_set_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_vector_child(IntPtr typeInfoHandle, out IntPtr childHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern ushort row_set_type_info_get_vector_dimensions(IntPtr typeInfoHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_udt_name(IntPtr typeInfoHandle, out FFIString name);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_udt_field_count(IntPtr typeInfoHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_udt_field(IntPtr typeInfoHandle, nuint index, out FFIString fieldName, out IntPtr fieldTypeHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_map_children(IntPtr typeInfoHandle, out IntPtr keyHandle, out IntPtr valueHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern nuint row_set_type_info_get_tuple_field_count(IntPtr typeInfoHandle);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern void row_set_type_info_get_tuple_field(IntPtr typeInfoHandle, nuint index, out IntPtr fieldHandle);

        private void FillColumnsMetadata(IntPtr columnsPtr, IntPtr metadataSetter)
        {
            RunWithIncrement(handle => row_set_fill_columns_metadata(handle, columnsPtr, metadataSetter));
        }

        private nuint GetColumnsCount()
        {
            nuint count = 0;
            RunWithIncrement(handle => row_set_get_columns_count(handle, out count));
            return count;
        }

        // This function is called from UnmanagedCallersOnly context - make sure no exceptions cross the FFI boundary.
        internal static IColumnInfo BuildTypeInfoFromHandle(IntPtr handle, ColumnTypeCode code, string keyspaceHint)
        {
            if (handle == IntPtr.Zero) return null;
            try
            {
                if (keyspaceHint == null)
                {
                    throw new ArgumentNullException(nameof(keyspaceHint), "Keyspace hint cannot be null when building column type info from handle.");
                }
                switch (code)
                {
                    case ColumnTypeCode.List:
                        // For List: ask Rust for the child handle and build recursively
                        unsafe
                        {
                            row_set_type_info_get_list_child(handle, out IntPtr child);
                            var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                            var childInfo = BuildTypeInfoFromHandle(child, childCode, keyspaceHint);
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
                            var keyInfo = BuildTypeInfoFromHandle(keyHandle, keyCode, keyspaceHint);
                            var valueInfo = BuildTypeInfoFromHandle(valueHandle, valueCode, keyspaceHint);
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
                                var fInfo = BuildTypeInfoFromHandle(fieldHandle, fCode, keyspaceHint);
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
                            var name = $"{keyspaceHint}.{udtName.ToManagedString() ?? ""}";
                            var udtInfo = new UdtColumnInfo(name);
                            nuint fcount = row_set_type_info_get_udt_field_count(handle);
                            for (nuint i = 0; i < fcount; i++)
                            {
                                row_set_type_info_get_udt_field(handle, i, out FFIString fieldName, out IntPtr fieldTypeHandle);
                                {
                                    var fname = fieldName.ToManagedString();
                                    var fcode = (ColumnTypeCode)row_set_type_info_get_code(fieldTypeHandle);
                                    var fInfo = BuildTypeInfoFromHandle(fieldTypeHandle, fcode, keyspaceHint);
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
                                var childInfo = BuildTypeInfoFromHandle(child, childCode, keyspaceHint);
                                var setInfo = new SetColumnInfo { KeyTypeCode = childCode, KeyTypeInfo = childInfo };
                                return setInfo;
                            }
                        }
                    case ColumnTypeCode.Vector:
                        // For Vector: ask Rust for the element child and dimensions
                        unsafe
                        {
                            row_set_type_info_get_vector_child(handle, out IntPtr child);
                            var childCode = (ColumnTypeCode)row_set_type_info_get_code(child);
                            var childInfo = BuildTypeInfoFromHandle(child, childCode, keyspaceHint);
                            var dims = (int)row_set_type_info_get_vector_dimensions(handle);
                            var vectorInfo = new VectorColumnInfo { ValueTypeCode = childCode, ValueTypeInfo = childInfo, Dimensions = dims };
                            return vectorInfo;
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

        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, IntPtr, FFISliceRaw, FFIMaybeException> deserializeValue = &DeserializeValue;

        /// <summary>
        /// Callback for the sync path. Same native signature as <see cref="DeserializeValue"/>,
        /// but recovers managed objects from raw stack-slot pointers (via Unsafe.Read)
        /// instead of GCHandles.
        /// </summary>
        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nuint, IntPtr, FFISliceRaw, FFIMaybeException> deserializeValueDirect = &DeserializeValueDirect;

        /// <summary>
        /// This shall be called by Rust code for each column in a row (async path).
        /// Recovers managed objects from GCHandles.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static FFIMaybeException DeserializeValue(
            IntPtr columnsPtr,
            IntPtr valuesPtr,
            nuint valueIndex,
            IntPtr serializerPtr,
            FFISliceRaw FFIframeSlice
        )
        {
            try
            {
                var valuesHandle = GCHandle.FromIntPtr(valuesPtr);
                var columnsHandle = GCHandle.FromIntPtr(columnsPtr);
                var serializerHandle = GCHandle.FromIntPtr(serializerPtr);

                if (valuesHandle.Target is object[] values && columnsHandle.Target is CqlColumn[] columns && serializerHandle.Target is IGenericSerializer serializer)
                {
                    DeserializeValueInner(columns, values, valueIndex, serializer, FFIframeSlice);
                }
                else
                {
                    throw new InvalidOperationException("GCHandle referenced type mismatch.");
                }
                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] DeserializeValue threw exception: {ex}");
                return FFIMaybeException.FromException(ex);
            }
        }

        /// <summary>
        /// This shall be called by Rust code for each column in a row (sync path).
        /// Recovers managed objects from raw stack-slot pointers (via Unsafe.Read)
        /// instead of GCHandles.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static FFIMaybeException DeserializeValueDirect(
            IntPtr columnsPtr,
            IntPtr valuesPtr,
            nuint valueIndex,
            IntPtr serializerPtr,
            FFISliceRaw FFIframeSlice
        )
        {
            try
            {
                unsafe
                {
                    var columns = Unsafe.Read<CqlColumn[]>((void*)columnsPtr);
                    var values = Unsafe.Read<object[]>((void*)valuesPtr);
                    var serializer = Unsafe.Read<IGenericSerializer>((void*)serializerPtr);

                    DeserializeValueInner(columns, values, valueIndex, serializer, FFIframeSlice);
                }
                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] DeserializeValueDirect threw exception: {ex}");
                return FFIMaybeException.FromException(ex);
            }
        }

        /// <summary>
        /// Core deserialization logic shared by deserialization callbacks.
        /// </summary>
        private static void DeserializeValueInner(CqlColumn[] columns, object[] values, nuint valueIndex, IGenericSerializer serializer, FFISliceRaw frameSliceRaw)
        {
            CqlColumn column = columns[valueIndex];
            ReadOnlySpan<byte> frameSlice = frameSliceRaw.As<byte>().ToSpan();
            values[valueIndex] = serializer.Deserialize(ProtocolVersion.V4, frameSlice, column.TypeCode, column.TypeInfo);
        }

        internal static Type MapTypeFromCode(ColumnTypeCode code)
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
                ColumnTypeCode.Vector => typeof(object),
                ColumnTypeCode.Udt => typeof(object),
                ColumnTypeCode.Tuple => typeof(object),
                _ => typeof(object)
            };
        }
    }
}
