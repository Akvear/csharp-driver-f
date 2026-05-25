using System;

using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Bridges a Rust-owned prepared statement resource to C# via SafeHandle.
    /// Inherits destructor and handle management from RustResource.
    /// </summary>
    internal sealed class BridgedPreparedStatement : RustResource
    {
        internal BridgedPreparedStatement(ManuallyDestructible mdPrepared) : base(mdPrepared)
        {
        }

        internal RowSetMetadata ExtractVariablesMetadataFromRust()
        {
            // Query Rust for the number of variable column specs
            var count = GetVariablesColumnSpecsCount();
            if (count == 0)
            {
                return new RowSetMetadata(Array.Empty<CqlColumn>());
            }

            var columns = new CqlColumn[count];
            for (nuint i = 0; i < count; i++)
            {
                columns[i] = new CqlColumn();
            }

            var pkIndexes = new List<int>();
            unsafe
            {
                RunWithIncrement(handle =>
                    prepared_statement_fill_column_specs_metadata(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref columns),
                        (IntPtr)setColumnMetaPtr,
                        (IntPtr)Unsafe.AsPointer(ref pkIndexes),
                        (IntPtr)AddPkIndexPtr
                    )
                );
            }

            var metadata = new RowSetMetadata(columns, pkIndexes.ToArray());
            return metadata;
        }

        internal bool IsLwt()
        {
            FFIBool isLwt = false;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_is_lwt(handle, out isLwt));
            }
            return isLwt;
        }

        internal ConsistencyLevel? GetConsistencyLevel()
        {
            int clInt = -1;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_get_consistency_level(
                    handle,
                    out clInt));
            }

            if (clInt < 0)
            {
                return null;
            }

            return (ConsistencyLevel)clInt;
        }

        internal void SetConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            unsafe
            {
                RunWithIncrement(handle =>
                    prepared_statement_set_consistency_level(
                        handle,
                        (ushort)consistencyLevel,
                        (IntPtr)Globals.ConstructorsPtr)
                    );
            }
        }

        internal bool IsIdempotent()
        {
            FFIBool isIdempotent = false;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_get_is_idempotent(handle, out isIdempotent));
            }
            return isIdempotent;
        }

        internal void SetIsIdempotent(bool isIdempotent)
        {
            FFIBool ffiIsIdempotent = isIdempotent;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_set_is_idempotent(handle, ffiIsIdempotent));
            }
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_get_variables_column_specs_count(IntPtr prepared_statement, out nuint count);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_fill_column_specs_metadata(IntPtr prepared_statement, IntPtr columnsPtr, IntPtr metadataSetter, IntPtr pkIndexesPtr, IntPtr addPkIndex);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_is_lwt(IntPtr prepared_statement, out FFIBool isLwt);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_get_consistency_level(IntPtr prepared_statement, out int consistency_level);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_set_consistency_level(IntPtr prepared_statement, ushort consistency_level, IntPtr constructors);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_get_is_idempotent(IntPtr prepared_statement, out FFIBool isIdempotent);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_set_is_idempotent(IntPtr prepared_statement, FFIBool isIdempotent);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, ushort, FFIMaybeException> AddPkIndexPtr = &AddPkIndex;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddPkIndex(
            IntPtr pkIndexesListPtr,
            ushort pkIndex)
        {
            try
            {
                var pkIndexesList = Unsafe.AsRef<List<int>>((void*)pkIndexesListPtr);
                pkIndexesList.Add(pkIndex);
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private nuint GetVariablesColumnSpecsCount()
        {
            nuint count = 0;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_get_variables_column_specs_count(handle, out count));
            }
            return count;
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, FFIMaybeException> setColumnMetaPtr = &BridgedRowSet.SetColumnMeta;
    }
}
