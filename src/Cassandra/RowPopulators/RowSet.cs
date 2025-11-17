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

        private static CqlColumn[] ExtractColumnsFromRust(IntPtr rowSetPtr)
        {
            var columns = new CqlColumn[1]; // FIXME: bridge with Rust.
            columns[0] = new CqlColumn
            {
                Name = "host_id",
                Type = typeof(Guid),
                TypeCode = ColumnTypeCode.Uuid,
                Index = 0,
                TypeInfo = null
            };

            return columns;

            // throw new NotImplementedException("ExtractColumnsFromRust is not yet implemented"); // FIXME: bridge with Rust.
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
                    Console.Error.WriteLine($"[FFI] row_set_next_row returned {has_row}");
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

            // FIXME: bridge with Rust to get the column indexes.
            var columnIndexes = new Dictionary<string, int>();
            columnIndexes["host_id"] = 0;

            return new Row(values, Columns, columnIndexes);
        }

        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, nint, IntPtr, IntPtr, nint, void> deserializeValue = &DeserializeValue;

        /// <summary>
        /// This shall be called by Rust code for each column in a row.
        /// </summary>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static void DeserializeValue(
            IntPtr columnsPtr,
            IntPtr valuesPtr,
            nint valueIndex,
            IntPtr serializerPtr,
            IntPtr frameSlicePtr,
            nint length
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

                    Console.Error.WriteLine($"[FFI] DeserializeValue [{valueIndex}] done.");
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
    }
}
