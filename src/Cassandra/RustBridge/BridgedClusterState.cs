using System;
using System.Runtime.InteropServices;
using System.Net;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;
using System.Collections.Generic;

namespace Cassandra
{
    internal sealed class BridgedClusterState : RustResource
    {
        internal BridgedClusterState(ManuallyDestructible mdClusterState) : base(mdClusterState)
        {
        }

        internal bool Equals(BridgedClusterState other)
        {
            if (other is null) return false;
            return handle == other.handle;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException cluster_state_fill_nodes(
            IntPtr clusterState,
            IntPtr contextPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFISliceRaw, FFISliceRaw, ushort, FFIString, FFIString, void> AddHostPtr = &AddHostToList;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddHostToList(
            IntPtr contextPtr,
            FFISliceRaw idBytes,
            FFISliceRaw ipBytes,
            ushort port,
            FFIString datacenter,
            FFIString rack)
        {
            try
            {
                // Safety:
                // contextPtr is a pointer to the stack slot holding the 'context' reference (not to the heap object itself).
                // Unsafe.AsPointer(ref T) returns the address of the managed pointer (the stack local).
                // The stack slot is stable for the duration of this callback since:
                // 1. cluster_state_fill_nodes calls this callback synchronously before returning
                // 2. The stack frame containing 'context' remains alive throughout the FFI call
                // 3. If GC moves the RefreshContext object on the heap, it updates the reference value in the stack slot
                // 4. Unsafe.Read dereferences the pointer to get the current reference value
                // This matches the pattern used in row_set_fill_columns_metadata.
                var context = Unsafe.AsRef<Metadata.RefreshContext>((void*)contextPtr);

                var hostId = new Guid(idBytes.As<byte>().ToSpan());

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6). ipBytes is an FFISlice<byte>
                // and it accesses unmanaged memory that is only valid for the duration of this callback invocation.
                // The IPAddress constructor must be called synchronously here so it can copy the data immediately.
                var ipAddress = new IPAddress(ipBytes.As<byte>().ToSpan());
                var address = new IPEndPoint(ipAddress, port);

                // Try to reuse existing host object if id matches and address is the same
                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    // If the address matches, reuse the instance.
                    if (host.Address.Equals(address))
                    {
                        context.AddHost(host);
                        return;
                    }
                }

                // If either datacenter or rack is null, the method ToManagedString returns null.
                var dcString = datacenter.ToManagedString();
                var rackString = rack.ToManagedString();

                // Create Host instance and add it to the dictionaries.
                host = new Host(address, hostId, dcString, rackString);
                context.AddHost(host);
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddHostCallback", ex);
            }
        }

        internal void FillHostCache(Metadata.RefreshContext context)
        {
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_fill_nodes(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref context),
                        (IntPtr)AddHostPtr
                    )
                );
            }

            GC.KeepAlive(context);
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_keyspace_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr contextPtr,
            IntPtr replicationOptionsPtr,
            StrategyAddRepFactorCallbacks addRepFactorCallbacks,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIMaybeException> SimpleStrategyAddRepFactorPtr = &SimpleStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException SimpleStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions["replication_factor"] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, nuint, FFIMaybeException> NetworkTopologyStrategyAddRepFactorPtr = &NetworkTopologyStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException NetworkTopologyStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString datacenter,
            nuint repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[datacenter.ToManagedString()] = repFactor.ToString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIString, FFIMaybeException> OtherStrategyAddRepFactorPtr = &OtherStrategyAddRepFactor;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException OtherStrategyAddRepFactor(
            IntPtr replicationOptionsPtr,
            FFIString strategyClass,
            FFIString repFactor)
        {
            try
            {
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);
                replicationOptions[strategyClass.ToManagedString()] = repFactor.ToManagedString();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        [StructLayout(LayoutKind.Sequential)]
        private unsafe readonly struct StrategyAddRepFactorCallbacks
        {
            public readonly IntPtr SimpleStrategyCallback;
            public readonly IntPtr NetworkTopologyStrategyCallback;
            public readonly IntPtr OtherStrategyCallback;

            public StrategyAddRepFactorCallbacks()
            {
                SimpleStrategyCallback = (IntPtr)SimpleStrategyAddRepFactorPtr;
                NetworkTopologyStrategyCallback = (IntPtr)NetworkTopologyStrategyAddRepFactorPtr;
                OtherStrategyCallback = (IntPtr)OtherStrategyAddRepFactorPtr;
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIBool, FFIString, IntPtr, FFIMaybeException> FillKeyspaceMetadataPtr = &FillKeyspaceMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException FillKeyspaceMetadata(
            IntPtr contextPtr,
            FFIBool durableWrites,
            FFIString strategyClass,
            IntPtr replicationOptionsPtr)
        {
            try
            {
                var keyspaceMeta = Unsafe.AsRef<KeyspaceMetadata>((void*)contextPtr);
                var replicationOptions = Unsafe.AsRef<Dictionary<string, string>>((void*)replicationOptionsPtr);

                var replication = new Dictionary<string, int>();
                foreach (var option in replicationOptions)
                {
                    if (int.TryParse(option.Value, out var parsedValue))
                    {
                        replication[option.Key] = parsedValue;
                    }
                    else
                    {
                        throw new InvalidOperationException($"Failed to parse replication option value '{option.Value}' for key '{option.Key}' into an integer.");
                    }
                }

                keyspaceMeta.FillKeyspaceMetadata(durableWrites, strategyClass.ToManagedString(), replication);
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal KeyspaceMetadata GetKeyspaceMetadata(BridgedClusterState clusterState, string keyspaceName)
        {
            var ksmd = new KeyspaceMetadata(clusterState, keyspaceName);
            var replicationOptions = new Dictionary<string, string>();
            var addRepFactorCallbacks = new StrategyAddRepFactorCallbacks();
            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_keyspace_metadata(
                            handle,
                            keyspaceName,
                            (IntPtr)Unsafe.AsPointer(ref ksmd),
                            (IntPtr)Unsafe.AsPointer(ref replicationOptions),
                            addRepFactorCallbacks,
                            (IntPtr)FillKeyspaceMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing keyspace.
                throw new InvalidOperationException($"Error retrieving metadata for keyspace '{keyspaceName}'.", ex);
            }

            GC.KeepAlive(replicationOptions);
            GC.KeepAlive(addRepFactorCallbacks);

            return ksmd;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_keyspace_names(
            IntPtr clusterState,
            IntPtr keyspaceNameListPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddKeyspaceNamePtr = &AddKeyspaceName;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddKeyspaceName(
            IntPtr keyspaceNameListPtr,
            FFIString keyspaceName)
        {
            try
            {
                var keyspaceNameList = Unsafe.AsRef<List<string>>((void*)keyspaceNameListPtr);
                keyspaceNameList.Add(keyspaceName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal List<string> GetKeyspaceNames()
        {
            List<string> keyspaceNames = new List<string>();

            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_get_keyspace_names(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref keyspaceNames),
                        (IntPtr)AddKeyspaceNamePtr
                    )
                );
            }

            return keyspaceNames;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_table_names(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            IntPtr tableNameListPtr,
            IntPtr callback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddTableNamesPtr = &AddTableName;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddTableName(
            IntPtr tableNameListPtr,
            FFIString tableName)
        {
            try
            {
                var tableNameList = Unsafe.AsRef<List<string>>((void*)tableNameListPtr);
                tableNameList.Add(tableName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        internal List<string> GetTableNames(string keyspaceName)
        {
            List<string> tableNames = new List<string>();

            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_table_names(
                            handle,
                            keyspaceName,
                            (IntPtr)Unsafe.AsPointer(ref tableNames),
                            (IntPtr)AddTableNamesPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing keyspace.
                throw new InvalidOperationException($"Error retrieving metadata for keyspace '{keyspaceName}'.", ex);
            }

            return tableNames;
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, byte, IntPtr, FFIBool, FFIBool, FFIMaybeException> ConstructTableColumnPtr = &ConstructTableColumn;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException ConstructTableColumn(
            IntPtr tableColumnsListPtr,
            FFIString columnName,
            byte typeCode,
            IntPtr typeInfoPtr,
            FFIBool isStatic,
            FFIBool isFrozen)
        {
            try
            {
                var tableColumnsList = Unsafe.AsRef<List<TableColumn>>((void*)tableColumnsListPtr);

                var column = new TableColumn
                {
                    // From `CqlColumn`
                    Index = -1, // FIXME
                    Type = BridgedRowSet.MapTypeFromCode((ColumnTypeCode)typeCode),
                    // From `ColumnDesc`
                    Name = columnName.ToManagedString(),
                    TypeCode = (ColumnTypeCode)typeCode,
                    TypeInfo = typeInfoPtr != IntPtr.Zero ? BridgedRowSet.BuildTypeInfoFromHandle(typeInfoPtr, (ColumnTypeCode)typeCode) : null,
                    IsStatic = isStatic,
                    IsFrozen = isFrozen
                };
                tableColumnsList.Add(column);

                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIString, FFIMaybeException> AddPrimaryKeyPtr = &AddPrimaryKey;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddPrimaryKey(
            IntPtr keysListPtr,
            FFIString keyName)
        {
            try
            {
                var keysList = Unsafe.AsRef<List<string>>((void*)keysListPtr);
                keysList.Add(keyName.ToManagedString());
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException cluster_state_get_table_metadata(
            IntPtr clusterState,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspaceName,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName,
            IntPtr tableColumnsListPtr,
            IntPtr constructTableColumnCallback,
            IntPtr partitionKeysListPtr,
            IntPtr clusteringKeysListPtr,
            IntPtr AddPrimaryKeyCallback,
            IntPtr tableContextPtr,
            IntPtr constructTableMetadataCallback,
            IntPtr constructorsPtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, IntPtr, FFIMaybeException> FillTableMetadataPtr = &FillTableMetadata;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException FillTableMetadata(
            IntPtr tableContextPtr,
            IntPtr tableColumnsListPtr,
            IntPtr partitionKeys,
            IntPtr clusteringKeys)
        {
            try
            {
                var tableMetadata = Unsafe.AsRef<TableMetadata>((void*)tableContextPtr);
                var tableColumnsList = Unsafe.AsRef<List<TableColumn>>((void*)tableColumnsListPtr);

                var tableColumnsDictionary = new Dictionary<string, TableColumn>();
                foreach (var tc in tableColumnsList)
                {
                    tc.Keyspace = tableMetadata.KeyspaceName;
                    tc.Table = tableMetadata.Name;
                    tableColumnsDictionary[tc.Name] = tc;
                }

                var partitionKeysList = Unsafe.AsRef<List<string>>((void*)partitionKeys);
                TableColumn[] partitionKeysColumns = new TableColumn[partitionKeysList.Count];
                for (int i = 0; i < partitionKeysList.Count; i++)
                {
                    var pkName = partitionKeysList[i];
                    if (!tableColumnsDictionary.TryGetValue(pkName, out var column))
                    {
                        throw new InvalidOperationException($"Partition key column '{pkName}' not found in columns list for table '{tableMetadata.Name}'.");
                    }
                    partitionKeysColumns[i] = column;
                }

                // FIXME: we currently don't have access to clustering key order info, so we default to Ascending.
                var clusteringKeysList = Unsafe.AsRef<List<string>>((void*)clusteringKeys);
                var clusteringKeysColumns = new Tuple<TableColumn, DataCollectionMetadata.SortOrder>[clusteringKeysList.Count];
                for (int i = 0; i < clusteringKeysList.Count; i++)
                {
                    var ckName = clusteringKeysList[i];
                    if (!tableColumnsDictionary.TryGetValue(ckName, out var column))
                    {
                        throw new InvalidOperationException($"Clustering key column '{ckName}' not found in columns list for table '{tableMetadata.Name}'.");
                    }
                    clusteringKeysColumns[i] = new Tuple<TableColumn, DataCollectionMetadata.SortOrder>(column, DataCollectionMetadata.SortOrder.Ascending);
                }

                // TODO: bridge table options.
                tableMetadata.SetValues(tableColumnsDictionary, partitionKeysColumns, clusteringKeysColumns, null);

                return FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }
        }

        internal TableMetadata GetTableMetadata(string keyspaceName, string tableName)
        {
            var tmd = new TableMetadata(keyspaceName, tableName);
            var tableColumnsList = new List<TableColumn>();
            var partitionKeys = new List<string>();
            var clusteringKeys = new List<string>();

            try
            {
                unsafe
                {
                    RunWithIncrement(handle =>
                        cluster_state_get_table_metadata(
                            handle,
                            keyspaceName,
                            tableName,
                            (IntPtr)Unsafe.AsPointer(ref tableColumnsList),
                            (IntPtr)ConstructTableColumnPtr,
                            (IntPtr)Unsafe.AsPointer(ref partitionKeys),
                            (IntPtr)Unsafe.AsPointer(ref clusteringKeys),
                            (IntPtr)AddPrimaryKeyPtr,
                            (IntPtr)Unsafe.AsPointer(ref tmd),
                            (IntPtr)FillTableMetadataPtr,
                            (IntPtr)Globals.ConstructorsPtr
                        )
                    );
                }
            }
            catch (InvalidArgumentException)
            {
                // If the keyspace or table was not found return null.
                return null;
            }
            catch (Exception ex)
            {
                // For other exceptions, rethrow as they indicate a failure in metadata retrieval rather than missing table.
                throw new InvalidOperationException($"Error retrieving metadata for table '{tableName}' in keyspace '{keyspaceName}'.", ex);
            }

            GC.KeepAlive(tableColumnsList);
            GC.KeepAlive(partitionKeys);
            GC.KeepAlive(clusteringKeys);

            return tmd;
        }
    }
}
