use std::convert::Infallible;
use std::sync::Arc;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::ClusterState;
use scylla::errors::{NewSessionError, PagerExecutionError, PrepareError};
use scylla_cql::serialize::row::SerializedValues;
use tokio::sync::RwLock;

use crate::CSharpStr;
use crate::error_conversion::{FfiException, MaybeShutdownError};
use crate::ffi::{
    ArcFFI, BoxFFI, BridgedBorrowedSharedPtr, BridgedOwnedExclusivePtr, BridgedOwnedSharedPtr, FFI,
    FromArc,
};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
use crate::task::{BridgedFuture, ExceptionConstructors, ManuallyDestructible, Tcb};

/// Internal representation of a session bridged to C#.
/// It contains optional connected session state to allow for shutdown.
/// If None, the session has been shut down and cannot be used for queries.
#[derive(Debug)]
pub(crate) struct BridgedSessionInner {
    session: Option<Session>,
}

/// BridgedSession is a thread-safe, asynchronously accessible session wrapper.
/// It uses RwLock to allow multiple concurrent read accesses (queries)
/// while ensuring exclusive access for write operations (shutdown).
pub type BridgedSession = tokio::sync::RwLock<BridgedSessionInner>;
impl FFI for BridgedSession {
    type Origin = FromArc;
}

/// BridgedFuture currently needs to return some result that implements ArcFFI.
/// For operations that don't need to return any data we use EmptyBridgedResult.
/// The user must call empty_bridged_result_free after using such functions.
#[derive(Debug)]
pub struct EmptyBridgedResult;
impl FFI for EmptyBridgedResult {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn empty_bridged_result_free(ptr: BridgedOwnedSharedPtr<EmptyBridgedResult>) {
    ArcFFI::free(ptr);
    tracing::trace!("[FFI] EmptyBridgedResult freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn session_create(tcb: Tcb<ManuallyDestructible>, uri: CSharpStr<'_>) {
    // Convert the raw C string to a Rust string
    let uri = uri.as_cstr().unwrap().to_str().unwrap();
    let uri = uri.to_owned();

    BridgedFuture::spawn::<_, _, NewSessionError, _>(tcb, async move {
        tracing::debug!("[FFI] Create Session... {}", uri);
        let session = SessionBuilder::new().known_node(&uri).build().await?;
        tracing::info!("[FFI] Session created! URI: {}", uri);
        tracing::trace!(
            "[FFI] Contacted node's address: {}",
            session.get_cluster_state().get_nodes_info()[0].address
        );
        Ok(Arc::new(RwLock::new(BridgedSessionInner {
            session: Some(session),
        })))
    })
}

/// Shuts down the session by acquiring a write lock and clearing the connected state.
/// This blocks all future queries. Once shutdown, the session cannot be used for queries anymore.
#[unsafe(no_mangle)]
pub extern "C" fn session_shutdown(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
) {
    // Session pointer being null or invalid implies a serious error on the C# side.
    // We unwrap here to catch such issues early and panic.
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling session shutdown");

    BridgedFuture::spawn::<_, _, Infallible, _>(tcb, async move {
        tracing::debug!("[FFI] Shutting down session");

        // Acquire write lock - this will pause the asynchronous execution until all read locks (queries)
        // are released and then clear the connected state - no more queries can proceed after this.
        let mut session_guard = session_arc.write().await;

        if session_guard.session.is_none() {
            panic!("Session is already shut down");
        }

        session_guard.session = None;
        tracing::info!("[FFI] Session shutdown complete");

        // Return None, providing BridgedSession just to satisfy the type constraints.
        // This is temporary and will be replaced with a proper non-allocating empty result type.
        Ok(None::<Arc<BridgedSession>>)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .query_iter(statement, ())
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Statement executed");

        Ok(Arc::new(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        }))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Take ownership of the pre-serialized values box so we can move it into the async task.
    // Important: the order of operations here matters. We need to ensure we take ownership of the box first. In case any further operations panic,
    // we don't want to leak the pointer.
    // Note: this transfers ownership, so the C# side must not free it!
    let values_box = BoxFFI::from_ptr(values_ptr).expect("non-null PreSerializedValues pointer");

    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    //TODO: use safe error propagation mechanism

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!(
            "[FFI] Preparing and executing statement with pre-serialized values \"{}\"",
            statement
        );

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // First, prepare the statement. Map PrepareError into PagerExecutionError::PrepareError
        // and then into MaybeShutdownError::Inner so the error type matches.
        let prepared = session
            .prepare(statement)
            .await
            .map_err(|e| MaybeShutdownError::Inner(PagerExecutionError::PrepareError(e)))?;

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = values_box.into_serialized_values();

        // Now execute using the internal execute_iter_preserialized helper.
        // Map to appropriate error type.
        let query_pager = session
            .execute_iter_preserialized(prepared, serialized_values)
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed with pre-serialized values");

        Ok(Arc::new(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        }))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_prepare(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for preparation: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PrepareError>, _>(tcb, async move {
        tracing::debug!("[FFI] Preparing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the prepare operation,
        // preventing shutdown until this future completes
        // Map underlying `PrepareError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let ps = session
            .prepare(statement)
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Statement prepared");

        Ok(Arc::new(BridgedPreparedStatement { inner: ps }))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
) {
    let bridged_prepared = ArcFFI::cloned_from_ptr(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `MaybeShutdownError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .execute_iter(bridged_prepared.inner.clone(), ())
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed");

        Ok(Arc::new(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        }))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound_with_values(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Take ownership of the pre-serialized values box so we can move it into the async task.
    // Important: the order of operations here matters. We need to ensure we take ownership of the box first. In case any further operations panic,
    // we don't want to leak the pointer.
    // Note: this transfers ownership, so the C# side must not free it!
    let values_box = BoxFFI::from_ptr(values_ptr).expect("non-null PreSerializedValues pointer");

    let bridged_prepared = ArcFFI::cloned_from_ptr(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = values_box.into_serialized_values();

        let query_pager = session
            .execute_iter_preserialized(bridged_prepared.inner.clone(), serialized_values)
            .await
            .map_err(MaybeShutdownError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed");

        Ok(Arc::new(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        }))
    });
}

// TO DO: Handle setting keyspace in session_query
#[unsafe(no_mangle)]
pub extern "C" fn session_use_keyspace(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    keyspace: CSharpStr<'_>,
    case_sensitive: bool,
) {
    let keyspace = keyspace.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling use_keyspace: \"{}\" (case_sensitive: {})",
        keyspace,
        case_sensitive
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    // TODO: replace PagerExecutionError with UseKeyspaceError.
    BridgedFuture::spawn::<_, _, MaybeShutdownError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing use_keyspace \"{}\"", keyspace);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(MaybeShutdownError::AlreadyShutdown);
        };

        // TO DO: Fix error handling here to create a new C# exception type for
        // UseKeyspaceError when use_keyspace isn't called anymore as part of Execute.
        // Use Session::use_keyspace() to update the Rust session's internal keyspace state.
        session
            .use_keyspace(&keyspace, case_sensitive)
            .await
            .map_err(|e| {
                // Error type conversion: UseKeyspaceError -> PagerExecutionError
                // We need this because BridgedFuture expects PagerExecutionError to match RowSet return.
                match e {
                    scylla::errors::UseKeyspaceError::RequestError(req_err) => {
                        // Common case: request failure (e.g., keyspace doesn't exist)
                        let req_error: scylla::errors::RequestError = req_err.into();
                        MaybeShutdownError::Inner(PagerExecutionError::NextPageError(
                            req_error.into(),
                        ))
                    }
                    scylla::errors::UseKeyspaceError::BadKeyspaceName(_)
                    | scylla::errors::UseKeyspaceError::KeyspaceNameMismatch { .. }
                    | scylla::errors::UseKeyspaceError::RequestTimeout(..)
                    | _ => {
                        // Catch-all for BadKeyspaceName, KeyspaceNameMismatch, RequestTimeout
                        // and any future UseKeyspaceError variants (marked #[non_exhaustive])
                        let req_attempt_err =
                            scylla::errors::RequestAttemptError::UnexpectedResponse(
                                scylla::errors::CqlResponseKind::Error,
                            );
                        let req_error: scylla::errors::RequestError = req_attempt_err.into();
                        MaybeShutdownError::Inner(PagerExecutionError::NextPageError(
                            req_error.into(),
                        ))
                    }
                }
            })?;

        tracing::trace!("[FFI] use_keyspace executed successfully");
        Ok(Arc::new(RowSet::empty()))
    })
}

/// Sets `out_cluster_state` to the current cluster state as a ManuallyDestructible resource.
/// This function provides access to the cluster topology information from the session.
/// The returned ClusterState is a snapshot at the time of the call.
///
/// # Safety
/// - The session pointer must be valid and not freed
#[unsafe(no_mangle)]
pub extern "C" fn session_get_cluster_state(
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    out_cluster_state: *mut ManuallyDestructible,
    constructors: &'static ExceptionConstructors,
) -> FfiException {
    let session_arc =
        ArcFFI::as_ref(session_ptr).expect("valid and non-null BridgedSession pointer");

    // Try to acquire a read lock synchronously.
    let Ok(session_guard) = session_arc.try_read() else {
        // Session is currently shutting down.
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FfiException::from_exception(ex);
    };

    // Check if session is connected or if it has been shut down.
    let Some(session) = session_guard.session.as_ref() else {
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FfiException::from_exception(ex);
    };

    // Get the cluster state from the session and convert it into an ArcFFI-wrapped pointer.
    let cluster_state = session.get_cluster_state();
    let md = ManuallyDestructible::from_destructible::<ClusterState>(cluster_state);
    unsafe {
        *out_cluster_state = md;
    }
    FfiException::ok()
}
