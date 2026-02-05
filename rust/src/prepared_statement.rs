use crate::FfiPtr;
use crate::error_conversion::FfiException;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, FFI, FFIStr, FromArc, RefFFI};
use crate::row_set::column_type_to_code;
use scylla::frame::response::result::ColumnType;
use scylla::statement::prepared::PreparedStatement;

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

#[derive(Clone, Copy)]
enum Columns {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ColumnsPtr(FfiPtr<'static, Columns>);

// Function pointer type for setting column specs metadata in C#.
type SetPreparedStatementVariablesMetadata = unsafe extern "C" fn(
    columns_ptr: ColumnsPtr,
    value_index: usize,
    name: FFIStr<'_>,
    keyspace: FFIStr<'_>,
    table: FFIStr<'_>,
    type_code: u8,
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_frozen: u8,
) -> FfiException;

/// Calls back into C# for each column to provide column specs metadata.
/// `metadata_setter` is a function pointer supplied by C# - it will be called synchronously for each column.
/// SAFETY: This function assumes that `columns_ptr` is a valid pointer
/// to a C# CQLColumn array of length equal to the number of columns,
/// and that `set_metadata` is a valid function pointer that can be called safely.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_fill_column_specs_metadata(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    columns_ptr: ColumnsPtr,
    set_prepared_statement_variables_metadata: SetPreparedStatementVariablesMetadata,
) -> FfiException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr).unwrap();

    // Iterate column specs and call the metadata setter
    for (i, spec) in prepared_statement
        .inner
        .get_variable_col_specs()
        .iter()
        .enumerate()
    {
        let name = FFIStr::new(spec.name());
        let keyspace = FFIStr::new(spec.table_spec().ks_name());
        let table = FFIStr::new(spec.table_spec().table_name());

        let type_code = column_type_to_code(spec.typ());

        let type_info_handle: BridgedBorrowedSharedPtr<ColumnType> = if type_code >= 0x20 {
            RefFFI::as_ptr(spec.typ())
        } else {
            RefFFI::null()
        };

        let is_frozen = match spec.typ() {
            ColumnType::Collection { frozen, .. } | ColumnType::UserDefinedType { frozen, .. } => {
                *frozen
            }
            _ => false,
        };

        unsafe {
            let ffi_exception = set_prepared_statement_variables_metadata(
                columns_ptr,
                i,
                name,
                keyspace,
                table,
                type_code,
                type_info_handle,
                is_frozen as u8,
            );

            // If there is an exception returned from callback, throw it as soon as possible
            if ffi_exception.has_exception() {
                return ffi_exception;
            }
        }
    }
    FfiException::ok()
}
