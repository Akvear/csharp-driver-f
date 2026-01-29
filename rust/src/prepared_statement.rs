use scylla::statement::prepared::PreparedStatement;

use crate::{
    error_conversion::FfiException,
    ffi::{ArcFFI, BridgedBorrowedSharedPtr, FFI, FromArc},
};

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
    is_lwt: *mut bool,
) -> FfiException {
    unsafe {
        *is_lwt = ArcFFI::as_ref(prepared_statement_ptr)
            .unwrap()
            .inner
            .is_confirmed_lwt();
    }
    FfiException::ok()
}

/// Gets the number of variable column specifications in the prepared statement.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_get_variables_column_specs_count(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    out_num_fields: *mut usize,
) -> FfiException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr).unwrap();

    unsafe {
        *out_num_fields = prepared_statement.inner.get_variable_col_specs().len();
    }

    FfiException::ok()
}
