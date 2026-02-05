using System;

using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

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

        internal bool IsLwt()
        {
            FFIBool isLwt = false;
            RunWithIncrement(handle => prepared_statement_is_lwt(handle, out isLwt));
            return isLwt;
        }

        internal RowSetMetadata ExtractVariablesMetadataFromRust()
        {
            // Query Rust for the number of variable column specs
            var count = GetVariablesColumnSpecsCount();
            if (count <= 0)
            {
                return new RowSetMetadata();
            }

            var columns = new CqlColumn[count];
            for (nuint i = 0; i < count; i++)
            {
                columns[i] = new CqlColumn();
            }

            unsafe
            {
                void* columnsPtr = Unsafe.AsPointer(ref columns);
                FillVariablesMetadata((IntPtr)columnsPtr, (IntPtr)setColumnMetaPtr);
            }
            
            var metadata = new RowSetMetadata
            {
                Columns = columns
            };

            return metadata;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException prepared_statement_is_lwt(IntPtr prepared_statement, out FFIBool isLwt);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException prepared_statement_get_variables_column_specs_count(IntPtr prepared_statement, out nuint count);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIException prepared_statement_fill_column_specs_metadata(IntPtr prepared_statement, IntPtr columnsPtr, IntPtr metadataSetter);

        private nuint GetVariablesColumnSpecsCount()
        {
            nuint count = 0;
            RunWithIncrement(handle => prepared_statement_get_variables_column_specs_count(handle, out count));
            return count;
        }

        private void FillVariablesMetadata(IntPtr columnsPtr, IntPtr metadataSetter)
        {
            RunWithIncrement(handle => prepared_statement_fill_column_specs_metadata(handle, columnsPtr, metadataSetter));
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, FFIException> setColumnMetaPtr = &BridgedRowSet.SetColumnMeta;
    }
}
