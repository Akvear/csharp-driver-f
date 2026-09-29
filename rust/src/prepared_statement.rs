use crate::error_conversion::FFIMaybeException;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, FFI, FFIBool, FFIPtr, FFIStr, FromArc, RefFFI,
    ffi_callback_for_each,
};
use crate::row_set::column_type_to_code;
use crate::task::ExceptionConstructors;
use scylla::frame::response::result::ColumnType;
use scylla::statement::prepared::PreparedStatement;
use std::sync::RwLock;

#[derive(Debug)]
pub struct BridgedPreparedStatement {
    pub(crate) inner: RwLock<PreparedStatement>,
}

impl FFI for BridgedPreparedStatement {
    type Origin = FromArc;
}

/// Gets the number of variable column specifications in the prepared statement.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_get_variables_column_specs_count(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    out_num_fields: *mut usize,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let guard = prepared_statement
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics");

    unsafe {
        *out_num_fields = guard.get_variable_col_specs().len();
    }

    FFIMaybeException::ok()
}

#[derive(Clone, Copy)]
enum Columns {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ColumnsPtr<'a>(FFIPtr<'a, Columns>);

// Function pointer type for setting column specs metadata in C#.
type SetPreparedStatementVariablesMetadata = unsafe extern "C" fn(
    columns_ptr: ColumnsPtr<'_>,
    value_index: usize,
    name: FFIStr<'_>,
    keyspace: FFIStr<'_>,
    table: FFIStr<'_>,
    type_code: u8,
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_frozen: u8,
) -> FFIMaybeException;

enum PartitionKeyIndexesList {}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct PartitionKeyIndexesListPtr<'a>(FFIPtr<'a, PartitionKeyIndexesList>);

type AddPartitionKeyIndex = unsafe extern "C" fn(
    pk_indexes_list_ptr: PartitionKeyIndexesListPtr<'_>,
    index: u16,
) -> FFIMaybeException;

/// Calls back into C# for each column to provide column specs metadata.
/// `metadata_setter` is a function pointer supplied by C# - it will be called synchronously for each column.
/// SAFETY: This function assumes that `columns_ptr` is a valid pointer
/// to a C# CQLColumn array of length equal to the number of columns,
/// and that `set_metadata` is a valid function pointer that can be called safely.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_fill_column_specs_metadata(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    columns_ptr: ColumnsPtr<'_>,
    set_prepared_statement_variables_metadata: SetPreparedStatementVariablesMetadata,
    pk_indexes_list_ptr: PartitionKeyIndexesListPtr<'_>,
    add_pk_index: AddPartitionKeyIndex,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let guard = prepared_statement
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics");

    // Iterate column specs and call the metadata setter
    for (i, spec) in guard.get_variable_col_specs().iter().enumerate() {
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

    unsafe {
        ffi_callback_for_each(
            pk_indexes_list_ptr,
            add_pk_index,
            guard
                .get_variable_pk_indexes()
                .iter()
                .map(|pk_indexes| pk_indexes.index),
        )
    }
    .into()
}

#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_is_lwt(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    is_lwt: &mut FFIBool,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let guard = prepared_statement
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics");

    let is_lwt_value = guard.is_confirmed_lwt();

    *is_lwt = is_lwt_value.into();

    FFIMaybeException::ok()
}

/// Gets consistency level of the prepared statement.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_get_consistency_level(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    consistency_level: Option<&mut i32>,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let guard = prepared_statement
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics");

    let maybe_consistency = guard.get_consistency();

    if let (Some(cl), Some(consistency_level)) = (maybe_consistency, consistency_level) {
        *consistency_level = cl as i32;
    }
    FFIMaybeException::ok()
}

/// Sets consistency level of the prepared statement.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_set_consistency_level(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    consistency_level: u16,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let mut guard = prepared_statement
        .inner
        .write()
        .expect("poisoning impossible due to process-aborting panics");

    let Ok(cl) = consistency_level.try_into() else {
        let ex = constructors
            .invalid_argument_exception_constructor
            .construct_from_rust(
                format!(
                    "Invalid consistency level value {0} passed from C#.",
                    consistency_level
                )
                .as_str(),
            );
        return FFIMaybeException::from_exception(ex);
    };

    guard.set_consistency(cl);

    FFIMaybeException::ok()
}

/// Gets whether the prepared statement is idempotent.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_get_is_idempotent(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    is_idempotent: &mut FFIBool,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let guard = prepared_statement
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics");
    let is_idempotent_value = guard.get_is_idempotent();

    *is_idempotent = is_idempotent_value.into();

    FFIMaybeException::ok()
}

/// Sets whether the prepared statement is idempotent.
#[unsafe(no_mangle)]
pub extern "C" fn prepared_statement_set_is_idempotent(
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    is_idempotent: FFIBool,
) -> FFIMaybeException {
    let prepared_statement = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");

    let mut guard = prepared_statement
        .inner
        .write()
        .expect("poisoning impossible due to process-aborting panics");

    guard.set_is_idempotent(is_idempotent.into());

    FFIMaybeException::ok()
}
