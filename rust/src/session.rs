use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{NewSessionError, PagerExecutionError, PrepareError};
use scylla_cql::serialize::row::SerializedValues;

use crate::CSharpStr;
use crate::ffi::{
    ArcFFI, BoxFFI, BridgedBorrowedSharedPtr, BridgedOwnedExclusivePtr, BridgedOwnedSharedPtr, FFI,
    FromArc,
};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
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

#[unsafe(no_mangle)]
pub extern "C" fn session_query(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    //TODO: use safe error propagation mechanism

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );
    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);
        let query_pager = bridged_session.inner.query_iter(statement, ()).await?;
        tracing::trace!("[FFI] Statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values(
    tcb: Tcb,
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
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    //TODO: use safe error propagation mechanism

    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!(
            "[FFI] Preparing and executing statement with pre-serialized values \"{}\"",
            statement
        );

        // First, prepare the statement.
        let prepared = bridged_session.inner.prepare(statement).await?;

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = values_box.into_serialized_values();

        // Now execute using the internal execute_iter_preserialized helper.
        let query_pager = bridged_session
            .inner
            .execute_iter_preserialized(prepared, serialized_values)
            .await?;

        tracing::trace!("[FFI] Prepared statement executed with pre-serialized values");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
) {
    let bridged_prepared = ArcFFI::cloned_from_ptr(prepared_statement_ptr).unwrap();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let query_pager = bridged_session
            .inner
            .execute_iter(bridged_prepared.inner.clone(), ())
            .await?;
        tracing::trace!("[FFI] Prepared statement executed");

        Ok(RowSet {
            pager: std::sync::Mutex::new(Some(query_pager)),
        })
    })
}

// TO DO: Handle setting keyspace in session_query
#[unsafe(no_mangle)]
pub extern "C" fn session_use_keyspace(
    tcb: Tcb,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    keyspace: CSharpStr<'_>,
    case_sensitive: i32,
) {
    let keyspace = keyspace.as_cstr().unwrap().to_str().unwrap().to_owned();
    let bridged_session = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    let case_sensitive = case_sensitive != 0;

    tracing::trace!(
        "[FFI] Scheduling use_keyspace: \"{}\" (case_sensitive: {})",
        keyspace,
        case_sensitive
    );
    BridgedFuture::spawn::<_, _, PagerExecutionError>(tcb, async move {
        tracing::debug!("[FFI] Executing use_keyspace \"{}\"", keyspace);

        // TO DO: Fix error handling here to create a new C# exception type for
        // UseKeyspaceError when use_keyspace isn't called anymore as part of Execute.
        // Use Session::use_keyspace() to update the Rust session's internal keyspace state.
        bridged_session
            .inner
            .use_keyspace(&keyspace, case_sensitive)
            .await
            .map_err(|e| {
                // Error type conversion: UseKeyspaceError -> PagerExecutionError
                // We need this because BridgedFuture expects PagerExecutionError to match RowSet return.
                match e {
                    scylla::errors::UseKeyspaceError::RequestError(req_err) => {
                        // Common case: request failure (e.g., keyspace doesn't exist)
                        let req_error: scylla::errors::RequestError = req_err.into();
                        PagerExecutionError::NextPageError(req_error.into())
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
                        PagerExecutionError::NextPageError(req_error.into())
                    }
                }
            })?;

        tracing::trace!("[FFI] use_keyspace executed successfully");
        Ok(RowSet::empty())
    })
}
