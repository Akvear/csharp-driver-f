use crate::error_conversion::FFIMaybeException;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, CSharpStr, FFI, FFIBool, FFIPtr, FFISlice, FFIStr, FromArc,
    RefFFI, ffi_callback_for_each,
};
use crate::row_set::column_type_to_code;
use crate::task::ExceptionConstructors;
use scylla::cluster::ClusterState;
use scylla::cluster::metadata::ColumnType;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Opaque type representing the C# RefreshContext.
#[derive(Clone, Copy)]
enum RefreshContext {}

/// Transparent wrapper around a pointer to the C# RefreshContext.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct RefreshContextPtr(FFIPtr<'static, RefreshContext>);

/// Callback type for constructing C# Host objects.
/// The callback receives raw pointers to node metadata and is responsible for:
/// 1. Constructing a C# Host object from the provided data
/// 2. Adding the Host to the C# RefreshContext referenced by refresh_context_ptr
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation
/// - String pointers (datacenter_ptr, rack_ptr) are only valid for the duration of the callback
/// - The callback must not store these pointers or access them after returning
/// - The callback must not throw exceptions across the FFI boundary
type ConstructCSharpHost = unsafe extern "C" fn(
    refresh_context_ptr: RefreshContextPtr,
    id_bytes: FFISlice<'_, u8>,
    ip_bytes: FFISlice<'_, u8>,
    port: u16,
    datacenter: FFIStr<'_>,
    rack: FFIStr<'_>,
);

/// Populates a C# RefreshContext with node information from the cluster state.
/// For each node in the cluster state, this function:
/// 1. Converts the node's metadata (IP, port, datacenter, rack, host ID) to FFI types
/// 2. Invokes the callback with pointers to this temporary data
/// 3. The callback must synchronously copy all data and add the Host to the RefreshContext
///
/// # Safety
/// - `refresh_context_ptr` must point to a valid C# RefreshContext that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) and byte arrays (IP, host ID) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_fill_nodes(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    refresh_context_ptr: RefreshContextPtr,
    callback: ConstructCSharpHost,
) -> FFIMaybeException {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    for node in cluster_state.get_nodes_info() {
        // UUID as bytes
        let uuid_bytes = FFISlice::new(node.host_id.as_bytes());

        // The octets() returns an owned stack array. We store it in outer-scope
        // variables so we can take a slice that outlives the match expression.
        let ip_bytes_storage_v4: [u8; 4];
        let ip_bytes_storage_v6: [u8; 16];

        // Serialize IP address to bytes
        let port = node.address.port();
        let ip_bytes_slice: &[u8] = match node.address.ip() {
            std::net::IpAddr::V4(ipv4) => {
                ip_bytes_storage_v4 = ipv4.octets();
                let bytes = &ip_bytes_storage_v4[..];
                tracing::trace!("[FFI] Node IPv4: {:?}, port: {}", bytes, port);
                bytes
            }
            std::net::IpAddr::V6(ipv6) => {
                ip_bytes_storage_v6 = ipv6.octets();
                let bytes = &ip_bytes_storage_v6[..];
                tracing::trace!("[FFI] Node IPv6: {:?}, port: {}", bytes, port);
                bytes
            }
        };

        let ip_bytes = FFISlice::new(ip_bytes_slice);

        // Get datacenter (Option<String>) - pass null when missing
        let dc_str = match node.datacenter.as_deref() {
            Some(s) => FFIStr::new(s),
            None => FFIStr::null(),
        };

        // Get rack (Option<String>) - pass null when missing
        let rack_str = match node.rack.as_deref() {
            Some(s) => FFIStr::new(s),
            None => FFIStr::null(),
        };

        // Invoke the callback to construct and add the Host to the C# list object.
        // All pointers passed to the callback are only valid during this invocation.
        // The callback must copy all data immediately.
        unsafe {
            callback(
                refresh_context_ptr,
                uuid_bytes,
                ip_bytes,
                port,
                dc_str,
                rack_str,
            );
        }
    }

    FFIMaybeException::ok()
}

/// Opaque type representing the C# KeyspaceNameList.
enum KeyspaceNameList {}

/// Transparent wrapper around a pointer to the C# KeyspaceNameList.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct KeyspaceNameListPtr(FFIPtr<'static, KeyspaceNameList>);

/// Callback type for adding a single keyspace name to a C# KeyspaceNameList.
/// The callback receives a keyspace name and is responsible for
/// adding it to the C# KeyspaceNameList referenced by keyspace_name_list_ptr.
///
/// # Safety
/// - The string pointer (keyspace_name) is only valid for the duration of the callback
/// - The callback must not store this pointer or access it after returning
type AddKeyspaceName = unsafe extern "C" fn(
    keyspace_name_list_ptr: KeyspaceNameListPtr,
    keyspace_name: FFIStr<'_>,
) -> FFIMaybeException;

/// Populates a C# List with keyspace names from the cluster state. For each keyspace in the cluster state,
/// this function invokes the callback with a single keyspace name. The callback must synchronously copy
/// the string data and add it to the KeyspaceNameList.
///
/// # Safety
/// - `keyspace_name_list_ptr` must point to a valid C# List that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data of keyspace names (e.g., via Marshal.PtrToStringUTF8) immediately.
/// - The callback must return a valid FFIMaybeException.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_keyspace_names(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_name_list_ptr: KeyspaceNameListPtr,
    add_keyspace_name_callback: AddKeyspaceName,
) -> FFIMaybeException {
    tracing::trace!("[FFI] cluster_state_get_keyspace_names called");

    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    unsafe {
        let ffi_exception = ffi_callback_for_each(
            keyspace_name_list_ptr,
            add_keyspace_name_callback,
            cluster_state
                .keyspaces_iter()
                .map(|(ks_name, _)| FFIStr::new(ks_name)),
        );
        if ffi_exception.has_exception() {
            return ffi_exception;
        }
    }

    FFIMaybeException::ok()
}

/// Opaque type representing the C# KeyspaceContext.
enum KeyspaceContext {}

/// Transparent wrapper around a pointer to the C# KeyspaceContext.
#[repr(transparent)]
pub struct KeyspaceContextPtr(FFIPtr<'static, KeyspaceContext>);

// Opaque type representing replication options for a keyspace - Dictionary<string, string> in C#.
enum ReplicationOptions {}

/// Transparent wrapper around a pointer to the C# ReplicationOptions.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ReplicationOptionsPtr(FFIPtr<'static, ReplicationOptions>);

// Callbacks for adding replication factors to the replication options in C# KeyspaceMetadata construction.
type SimpleStrategyAddRepFactor = unsafe extern "C" fn(
    replication_options_ptr: ReplicationOptionsPtr,
    rep_factor: usize,
) -> FFIMaybeException;

type NetworkTopologyStrategyAddRepFactor = unsafe extern "C" fn(
    replication_options_ptr: ReplicationOptionsPtr,
    datacenter: FFIStr<'_>,
    rep_factor: usize,
) -> FFIMaybeException;

type OtherStrategyAddRepFactor = unsafe extern "C" fn(
    replication_options_ptr: ReplicationOptionsPtr,
    key: FFIStr<'_>,
    value: FFIStr<'_>,
) -> FFIMaybeException;

// Replication factor callbacks grouped into a single struct to simplify passing them to the keyspace metadata construction function.
#[repr(C)]
pub struct StrategyAddRepFactor {
    simple_strategy: SimpleStrategyAddRepFactor,
    network_topology_strategy: NetworkTopologyStrategyAddRepFactor,
    other_strategy: OtherStrategyAddRepFactor,
}

/// Callback type for constructing C# KeyspaceMetadata objects.
/// The callback receives raw pointers to keyspace metadata and is responsible for:
/// 1. Constructing a C# KeyspaceMetadata object from the provided data.
/// 2. Setting the KeyspaceMetadata on the C# KeyspaceContext referenced by keyspace_context_ptr.
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation.
/// - String pointers (strategy_class, key-value pairs) are only valid for the duration of the callback.
/// - The callback must not store these pointers or access them after returning.
/// - The callback must return a valid FFIMaybeException.
type ConstructCSharpKeyspaceMetadata = unsafe extern "C" fn(
    keyspace_context_ptr: KeyspaceContextPtr,
    durable_writes: bool,
    strategy_class: FFIStr<'_>,
    replication_options_ptr: ReplicationOptionsPtr,
) -> FFIMaybeException;

/// Populates a C# KeyspaceContext with keyspace metadata from the cluster state.
/// For the specified keyspace in the cluster state, this function:
/// 1. Converts the keyspace's metadata (durable_writes, strategy class) to FFI types.
/// 2. Constructs the replication options by invoking the appropriate callbacks for the keyspace's replication strategy.
/// 3. Invokes the callback with pointers to this temporary data.
///
/// # Safety
/// - `keyspace_context_ptr` and `replication_options_ptr` must point to valid C# KeyspaceContext and ReplicationOptions that remain allocated during this call.
/// - All string pointers passed to the callback are temporary and only valid during that invocation.
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) immediately.
/// - The callback must return a valid FFIMaybeException.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_keyspace_metadata(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_name: CSharpStr<'_>,
    keyspace_context_ptr: KeyspaceContextPtr,
    replication_options_ptr: ReplicationOptionsPtr,
    add_rep_factor_callback: StrategyAddRepFactor,
    construct_keyspace_callback: ConstructCSharpKeyspaceMetadata,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    tracing::trace!("[FFI] cluster_state_get_keyspace_metadata");

    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    let keyspace_name = keyspace_name
        .as_cstr()
        .expect("valid C string for keyspace_name")
        .to_str()
        .expect("valid UTF-8 keyspace name");

    let Some(keyspace) = cluster_state.get_keyspace(keyspace_name) else {
        // If the keyspace is not found, return invalid argument exception to indicate the caller provided an invalid keyspace name.
        let ex = constructors
            .invalid_argument_exception_constructor
            .construct_from_rust("Keyspace not found in cluster metadata");
        return FFIMaybeException::from_exception(ex);
    };

    tracing::trace!("[FFI] Found keyspace: '{}'", keyspace_name);

    let durable_writes = keyspace.durable_writes;

    #[deny(clippy::wildcard_enum_match_arm)]
    let strategy_class = match &keyspace.strategy {
        scylla::cluster::metadata::Strategy::SimpleStrategy { replication_factor } => {
            unsafe {
                let res = (add_rep_factor_callback.simple_strategy)(
                    replication_options_ptr,
                    *replication_factor,
                );
                if res.has_exception() {
                    return res;
                }
            }
            FFIStr::new("SimpleStrategy")
        }

        scylla::cluster::metadata::Strategy::NetworkTopologyStrategy {
            datacenter_repfactors,
        } => {
            tracing::trace!(
                "[FFI] NetworkTopologyStrategy with datacenter rep_factors: {:?}",
                datacenter_repfactors
            );
            for (dc, rf) in datacenter_repfactors {
                unsafe {
                    let res = (add_rep_factor_callback.network_topology_strategy)(
                        replication_options_ptr,
                        FFIStr::new(dc),
                        *rf,
                    );
                    if res.has_exception() {
                        return res;
                    }
                }
            }
            FFIStr::new("NetworkTopologyStrategy")
        }

        scylla::cluster::metadata::Strategy::LocalStrategy => FFIStr::new("LocalStrategy"),

        scylla::cluster::metadata::Strategy::Other { name, data } => {
            tracing::trace!("[FFI] Other strategy '{}' with options: {:?}", name, data);
            for (k, v) in data {
                unsafe {
                    let res = (add_rep_factor_callback.other_strategy)(
                        replication_options_ptr,
                        FFIStr::new(k),
                        FFIStr::new(v),
                    );
                    if res.has_exception() {
                        return res;
                    }
                }
            }
            FFIStr::new(name.as_str())
        }

        // The match is exhaustive, but we add a wildcard arm to satisfy the compiler since the Strategy enum is non-exhaustive.
        // If we ever encounter a new strategy variant that we don't know how to handle, we need to update this code to support it.
        _ => unreachable!("All strategy variants should be covered"),
    };

    // Invoke the construct_keyspace_callback to construct and fill the C# KeyspaceMetadata object.
    // All pointers passed to the callback are only valid during this invocation.
    // The callback must copy all data immediately.
    unsafe {
        construct_keyspace_callback(
            keyspace_context_ptr,
            durable_writes,
            strategy_class,
            replication_options_ptr,
        )
    }
}

/// Opaque type representing the C# TableNameList.
enum TableNameList {}

/// Transparent wrapper around a pointer to the C# TableNameList.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct TableNameListPtr(FFIPtr<'static, TableNameList>);

/// Callback type for adding a single table name to a C# TableNameList.
/// The callback receives a table name and is responsible for
/// adding it to the C# TableNameList referenced by table_name_list_ptr.
///
/// # Safety
/// - The string pointer (table_name) is only valid for the duration of the callback.
/// - The callback must not store this pointer or access it after returning.
type AddTableName = unsafe extern "C" fn(
    table_name_list_ptr: TableNameListPtr,
    table_name: FFIStr<'_>,
) -> FFIMaybeException;

/// Populates a C# List with table names from the cluster state. For each table in the cluster state,
/// this function invokes the callback with a single table name. The callback must synchronously copy
/// the string data and add it to the TableNameList.
///
/// # Safety
/// - `table_name_list_ptr` must point to a valid C# List that remains allocated during this call.
/// - All string pointers passed to the callback are temporary and only valid during that invocation.
/// - The callback must copy string data of table names (e.g., via Marshal.PtrToStringUTF8) immediately.
/// - The callback must return a valid FFIMaybeException.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_table_names(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_name: CSharpStr<'_>,
    table_name_list_ptr: TableNameListPtr,
    callback: AddTableName,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    let keyspace_name = keyspace_name
        .as_cstr()
        .expect("valid C string for keyspace_name")
        .to_str()
        .expect("valid UTF-8 keyspace name");

    let Some(keyspace) = cluster_state.get_keyspace(keyspace_name) else {
        // If the keyspace is not found, return invalid argument exception to indicate the caller provided an invalid keyspace name.
        let ex = constructors
            .invalid_argument_exception_constructor
            .construct_from_rust("Keyspace not found in cluster metadata");
        return FFIMaybeException::from_exception(ex);
    };

    unsafe {
        ffi_callback_for_each(
            table_name_list_ptr,
            callback,
            keyspace.tables.keys().map(|k| FFIStr::new(k.as_str())),
        )
    }
}

enum TableColumns {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct TableColumnsPtr(FFIPtr<'static, TableColumns>);

type ConstructCSharpTableColumn = unsafe extern "C" fn(
    columns_ptr: TableColumnsPtr,
    column_name: FFIStr<'_>,
    type_code: u8,
    type_info: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_static: FFIBool,
    is_frozen: FFIBool,
) -> FFIMaybeException;

/// Opaque type representing the C# primary keys (PartitionKey or ClusteringKey).
enum PrimaryKey {}

/// Transparent wrapper around a pointer to the C# primary keys (PartitionKey or ClusteringKey).
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct PrimaryKeysPtr(FFIPtr<'static, PrimaryKey>);

/// Callback type for adding a single primary key column name to a C# PartitionKeys / ClusteringKeys.
/// The callback receives a primary key name and is responsible for adding it to
/// the C# PartitionKeys / ClusteringKeys referenced by primary_keys_ptr.
type AddPrimaryKey = unsafe extern "C" fn(
    primary_keys_ptr: PrimaryKeysPtr,
    primary_key: FFIStr<'_>,
) -> FFIMaybeException;

/// Opaque type representing the C# TableContext.
#[derive(Clone, Copy)]
enum TableContext {}

/// Transparent wrapper around a pointer to the C# TableContext.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct TableContextPtr(FFIPtr<'static, TableContext>);

type ConstructCSharpTableMetadata = unsafe extern "C" fn(
    table_context_ptr: TableContextPtr,
    table_columns_ptr: TableColumnsPtr,
    partition_keys: PrimaryKeysPtr,
    clustering_keys: PrimaryKeysPtr,
) -> FFIMaybeException;

/// Retrieves metadata for a single table and exposes it to C# via callbacks.
///
/// For the specified `(keyspace_name, table_name)` this function:
/// 1. Iterates over all columns of the table and invokes `construct_table_column`,
///    using `table_columns_ptr` to let C# accumulate column metadata.
/// 2. Iterates over the partition key columns and invokes `add_primary_key_callback`
///    for each, using `partition_keys_ptr`.
/// 3. Iterates over the clustering key columns and invokes `add_primary_key_callback`
///    for each, using `clustering_keys_ptr`.
/// 4. Finally invokes `construct_table_metadata` with `table_context_ptr`,
///    `table_columns_ptr`, `partition_keys_ptr`, and `clustering_keys_ptr` so C# can
///    construct the final table metadata object.
///
/// # Safety
/// - `cluster_state_ptr` must point to a valid `ClusterState` that remains alive for
///   the duration of this call.
/// - All FFI pointers (`table_columns_ptr`, `partition_keys_ptr`, `clustering_keys_ptr`,
///   and `table_context_ptr`) must reference valid C# objects for the duration of this call.
/// - Any string or buffer pointers passed to the callbacks are temporary and only valid
///   for the duration of the respective callback invocation; the callbacks must
///   synchronously copy any data they need (e.g., via `Marshal.PtrToStringUTF8`).
/// - The callbacks must not throw managed exceptions across the FFI boundary; use
///   `Environment.FailFast` or equivalent error handling from managed code if needed.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_table_metadata(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace_name: CSharpStr<'_>,
    table_name: CSharpStr<'_>,
    table_columns_ptr: TableColumnsPtr,
    construct_table_column: ConstructCSharpTableColumn,
    partition_keys_ptr: PrimaryKeysPtr,
    clustering_keys_ptr: PrimaryKeysPtr,
    add_primary_key_callback: AddPrimaryKey,
    table_context_ptr: TableContextPtr,
    construct_table_metadata: ConstructCSharpTableMetadata,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    tracing::trace!("[FFI] cluster_state_get_table_metadata called");

    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    let keyspace_name = keyspace_name
        .as_cstr()
        .expect("valid C string for keyspace_name")
        .to_str()
        .expect("valid UTF-8 keyspace name");

    let Some(keyspace) = cluster_state.get_keyspace(keyspace_name) else {
        // If the keyspace is not found, return invalid argument exception to indicate the caller provided an invalid keyspace name.
        let ex = constructors
            .invalid_argument_exception_constructor
            .construct_from_rust("Keyspace not found in cluster metadata");
        return FFIMaybeException::from_exception(ex);
    };

    let table_name = table_name
        .as_cstr()
        .expect("valid C string for table_name")
        .to_str()
        .expect("valid UTF-8 table name");

    let Some(table) = keyspace.tables.get(table_name) else {
        // If the table is not found, return invalid argument exception to indicate the caller provided an invalid table name.
        let ex = constructors
            .invalid_argument_exception_constructor
            .construct_from_rust("Table not found in keyspace metadata");
        return FFIMaybeException::from_exception(ex);
    };

    for (column_name, column) in table.columns.iter() {
        tracing::trace!(
            "[FFI] Passing definition of column '{}' in table '{}.{}'",
            column_name,
            keyspace_name,
            table_name
        );

        let type_code = column_type_to_code(&column.typ);
        let type_info_handle: BridgedBorrowedSharedPtr<ColumnType> = if type_code >= 0x20 {
            RefFFI::as_ptr(&column.typ)
        } else {
            RefFFI::null()
        };

        let is_static = matches!(column.kind, scylla::cluster::metadata::ColumnKind::Static);
        let is_frozen = match &column.typ {
            ColumnType::Collection { frozen, .. } | ColumnType::UserDefinedType { frozen, .. } => {
                *frozen
            }
            _ => false,
        };

        unsafe {
            let ffi_exception = construct_table_column(
                table_columns_ptr,
                FFIStr::new(column_name),
                type_code,
                type_info_handle,
                is_static.into(),
                is_frozen.into(),
            );
            if ffi_exception.has_exception() {
                return ffi_exception;
            }
        }
    }

    unsafe {
        // Add partition keys to the C# PartitionKeys list via the callback
        tracing::trace!(
            "[FFI] Adding partition keys for table '{}.{}'",
            keyspace_name,
            table_name
        );
        let ffi_exception = ffi_callback_for_each(
            partition_keys_ptr,
            add_primary_key_callback,
            table
                .partition_key
                .iter()
                .map(|pk| FFIStr::new(pk.as_str())),
        );
        if ffi_exception.has_exception() {
            return ffi_exception;
        }

        // Add clustering keys to the C# ClusteringKeys list via the callback
        tracing::trace!(
            "[FFI] Adding clustering keys for table '{}.{}'",
            keyspace_name,
            table_name
        );
        let ffi_exception = ffi_callback_for_each(
            clustering_keys_ptr,
            add_primary_key_callback,
            table
                .clustering_key
                .iter()
                .map(|ck| FFIStr::new(ck.as_str())),
        );
        if ffi_exception.has_exception() {
            return ffi_exception;
        }

        // Finally, construct the C# TableMetadata object by invoking the callback with pointers to the table metadata.
        tracing::trace!(
            "[FFI] Constructing C# TableMetadata for table '{}.{}'",
            keyspace_name,
            table_name
        );
        let ffi_exception = construct_table_metadata(
            table_context_ptr,
            table_columns_ptr,
            partition_keys_ptr,
            clustering_keys_ptr,
        );
        if ffi_exception.has_exception() {
            return ffi_exception;
        }
    }

    FFIMaybeException::ok()
}
