use scylla::statement::prepared::PreparedStatement;

use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, FFI, FromArc};

#[derive(Debug)]
pub struct BridgedPreparedStatement {
    pub(crate) inner: PreparedStatement,
}

impl FFI for BridgedPreparedStatement {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_is_lwt(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
) -> i32 {
    ArcFFI::as_ref(prepared_statement_ptr)
        .unwrap()
        .inner
        .is_confirmed_lwt() as _
}
