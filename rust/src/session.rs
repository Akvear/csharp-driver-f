use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{NewSessionError, PagerExecutionError, PrepareError};

use crate::CSharpStr;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
use crate::FfiPtr;
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
use crate::task::{BridgedFuture, Tcb};
use std::ffi::CString;

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

#[unsafe(no_mangle)]
pub extern "C" fn session_prepare(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for preparation: \"{}\"",
        statement
    );

    BridgedFuture::spawn::<_, _, PrepareError>(tcb, async move {
        tracing::debug!("[FFI] Preparing statement \"{}\"", statement);
        let ps = bridged_session.inner.prepare(statement).await?;
        tracing::trace!("[FFI] Statement prepared");

        Ok(BridgedPreparedStatement { inner: ps })
    })
}

type AlreadyExistsConstructor = unsafe extern "C" fn(
    keyspace_ptr: *const u8,
    table_ptr: *const u8,
    exception_ptr: ExceptionPtr,
);

#[derive(Clone, Copy)]
enum Exception {}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ExceptionPtr(FfiPtr<'static, Exception>);

// SAFETY:
// `ExceptionPtr` wraps an opaque FFI pointer to a buffer owned/pinned by C#.
// We only copy and pass it across threads; we never dereference it in Rust.
// Raw pointers are `Send` by convention when not dereferenced. Marking this as
// `Send` is sound for our usage because Rust side treats it as an opaque sink
// to write an IntPtr via the managed constructor callback.
unsafe impl Send for ExceptionPtr {}

#[unsafe(no_mangle)]
pub extern "C" fn session_query(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    out_exception: ExceptionPtr,
    exception_ctr_ptr: AlreadyExistsConstructor,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );

    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);
        let result = bridged_session.inner.query_iter(statement, ()).await;
        if let Err(err) = &result {
            // Build and write the managed exception handle via the provided constructor.
            println!("[FFI] Query execution failed: {:?}", err);
            unsafe {
                // Keyspace is null for some reason?
                let keyspace = bridged_session.inner.get_keyspace();
                let keyspace_c = keyspace
                    .as_ref()
                    .map(|ks| CString::new(ks.as_str()).unwrap());
                let keyspace_ptr = keyspace_c
                    .as_ref()
                    .map_or(std::ptr::null(), |c| c.as_ptr() as *const u8);

                // No table information available here; pass null.
                let table_ptr = std::ptr::null();

                exception_ctr_ptr(keyspace_ptr, table_ptr, out_exception);
                println!("[FFI] Exception constructed");
                return Err(err.clone());
            }
        }

        let query_pager = result.unwrap();
        tracing::trace!("[FFI] Statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(query_pager),
        })
    })
}
