use crate::FfiPtr;
use crate::error_conversion::FfiException;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, FFI, FFIByteSlice, FFIStr, FromArc};
use scylla::cluster::ClusterState;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Opaque type representing the C# RefreshContext.
#[derive(Clone, Copy)]
enum RefreshContext {}

/// Transparent wrapper around a pointer to the C# RefreshContext.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct RefreshContextPtr(FfiPtr<'static, RefreshContext>);

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
    id_bytes: FFIByteSlice<'_>,
    ip_bytes: FFIByteSlice<'_>,
    port: u16,
    datacenter: FFIStr<'_>,
    rack: FFIStr<'_>,
);

/// Populates a C# RefreshContext with node information from the cluster state.
/// For each node in the cluster state, this function:
/// 1. Serializes the node's metadata (IP, port, datacenter, rack, host ID) to raw bytes
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
) -> FfiException {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    for node in cluster_state.get_nodes_info() {
        // UUID as bytes
        let uuid_bytes = FFIByteSlice::new(node.host_id.as_bytes());

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

        let ip_bytes = FFIByteSlice::new(ip_bytes_slice);

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

    FfiException::ok()
}
