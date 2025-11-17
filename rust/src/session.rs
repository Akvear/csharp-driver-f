use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::NewSessionError;

use crate::CSharpStr;
use crate::ffi::{ArcFFI, BridgedOwnedSharedPtr, FFI, FromArc};
use crate::task::{BridgedFuture, Tcb};

impl FFI for BridgedSession {
    type Origin = FromArc;
}

#[derive(Debug)]
pub struct BridgedSession {
    inner: Session,
}

#[unsafe(no_mangle)]
pub extern "C" fn session_create(tcb: Tcb, uri: CSharpStr<'_>) {
    // Convert the raw C string to a Rust string
    let uri = uri.as_cstr().unwrap().to_str().unwrap();
    let uri = uri.to_owned();

    BridgedFuture::spawn::<_, _, NewSessionError>(tcb, async move {
        tracing::debug!("[FFI] Create Session... {}", uri);
        let session = SessionBuilder::new().known_node(&uri).build().await?;
        tracing::info!("[FFI] Session created! URI: {}", uri);
        tracing::trace!(
            "[FFI] Contacted node's address: {}",
            session.get_cluster_state().get_nodes_info()[0].address
        );
        Ok(BridgedSession { inner: session })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_free(session_ptr: BridgedOwnedSharedPtr<BridgedSession>) {
    ArcFFI::free(session_ptr);
    tracing::debug!("[FFI] Session freed");
}
