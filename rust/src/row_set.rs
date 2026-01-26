use scylla::client::pager::QueryPager;
use scylla::cluster::metadata::CollectionType;
use scylla::frame::response::result::{ColumnType, NativeType};

use crate::FfiPtr;
use crate::error_conversion::FfiException;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, FFI, FFIByteSlice, FFIStr, FromArc, FromRef, RefFFI,
};
use crate::task::BridgedFuture;
use crate::task::ExceptionConstructors;

// TO DO: Don't use mock RowSet - remove Option<> from the pager field
#[derive(Debug)]
pub(crate) struct RowSet {
    // FIXME: consider if this Mutex is necessary. Perhaps BoxFFI is a better fit?
    //
    // Rust explanation:
    // This Mutex is here because QueryPager's next_column_iterator takes &mut self,
    // and we need interior mutability to call it from row_set_next_row.
    // C# explanation:
    // This Mutex is here because we need to mutate the pager when fetching the next row,
    // and it's possible that C# code will call row_set_next_row concurrently,
    // because RowSet claims it supports parallel enumeration, and does not enforce any locking
    // on its own.
    pub(crate) pager: std::sync::Mutex<Option<QueryPager>>,
}

impl RowSet {
    // Creates an empty RowSet with no pager (zero rows, zero columns).
    pub(crate) fn empty() -> Self {
        RowSet {
            pager: std::sync::Mutex::new(None),
        }
    }
}

impl FFI for RowSet {
    type Origin = FromArc;
}

impl FFI for ColumnType<'_> {
    type Origin = FromRef;
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_get_columns_count(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
    out_num_fields: *mut usize,
) -> FfiException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.lock().unwrap();
    unsafe {
        *out_num_fields = match pager.as_ref() {
            Some(pager) => pager.column_specs().len(),
            None => 0,
        };
    }
    FfiException::ok()
}

// Function pointer type for setting column metadata in C#.
type SetMetadata = unsafe extern "C" fn(
    columns_ptr: ColumnsPtr,
    value_index: usize,
    name: FFIStr<'_>,
    keyspace: FFIStr<'_>,
    table: FFIStr<'_>,
    type_code: u8,
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_frozen: u8,
) -> FfiException;

/// Calls back into C# for each column to provide metadata.
/// `metadata_setter` is a function pointer supplied by C# - it will be called synchronously for each column.
/// SAFETY: This function assumes that `columns_ptr` is a valid pointer
/// to a C# CQLColumn array of length equal to the number of columns,
/// and that `set_metadata` is a valid function pointer that can be called safely.
#[unsafe(no_mangle)]
pub extern "C" fn row_set_fill_columns_metadata(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
    columns_ptr: ColumnsPtr,
    set_metadata: SetMetadata,
    constructors: &ExceptionConstructors,
) -> FfiException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_ref() else {
        // Return a RustException built via constructors as a quick workaround.
        let ex = constructors
            .rust_exception_constructor
            .construct_from_rust("RowSet has no pager to get metadata from");
        return FfiException::from_exception(ex);
    };

    // Iterate column specs and call the metadata setter
    for (i, spec) in pager.column_specs().iter().enumerate() {
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
            let ffi_exception = set_metadata(
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

#[derive(Clone, Copy)]
enum Columns {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ColumnsPtr(FfiPtr<'static, Columns>);

#[derive(Clone, Copy)]
enum Values {}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ValuesPtr(FfiPtr<'static, Values>);

#[derive(Clone, Copy)]
enum Serializer {}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct SerializerPtr(FfiPtr<'static, Serializer>);

type DeserializeValue = unsafe extern "C" fn(
    columns_ptr: ColumnsPtr,
    values_ptr: ValuesPtr,
    value_index: usize,
    serializer_ptr: SerializerPtr,
    frame_slice: FFIByteSlice<'_>,
) -> FfiException;

#[unsafe(no_mangle)]
pub extern "C" fn row_set_next_row<'row_set>(
    row_set_ptr: BridgedBorrowedSharedPtr<'row_set, RowSet>,
    deserialize_value: DeserializeValue,
    columns_ptr: ColumnsPtr,
    values_ptr: ValuesPtr,
    serializer_ptr: SerializerPtr,
    out_has_row: *mut bool,
    constructors: &ExceptionConstructors,
) -> FfiException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let mut pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_mut() else {
        unsafe {
            *out_has_row = false;
        }
        return FfiException::ok(); // Empty RowSet has no rows
    };
    let num_columns = pager.column_specs().len();

    let deserialize_fut = async {
        // Returns Ok(true) when a row was read and deserialized,
        // Ok(false) when there are no more rows,
        // Err(FfiException) when an error occurs and should be propagated to C#.
        // TODO: consider how to handle possibility of the metadata to change between pages.
        // While unlikely, it's not impossible.
        // For now, we just assume it won't happen and ignore `_new_page_began`.
        // The problem is that C# assumes the same metadata for the whole RowSet,
        // and they are passed through `ColumnsPtr`. Currently, if the metadata changes,
        // C# code will attempt to deserialize columns with wrong types, likely leading to exceptions.
        let Some(next) = pager.next_column_iterator().await else {
            tracing::trace!("[FFI] No more rows available!");
            return Ok(false);
        };

        let (mut column_iterator, _new_page_began) = match next {
            // Successfully obtained the next row's column iterator
            Ok(values) => values,
            // Error while fetching the column value
            Err(err) => return Err(FfiException::from_error(err, constructors)),
        };

        for value_index in 0..num_columns {
            let Some(column_res) = column_iterator.next() else {
                // Error: fewer columns than expected
                // TODO: Implement error type for too few columns - server provided less columns than claimed in the metadata
                let ex = constructors
                    .rust_exception_constructor
                    .construct_from_rust(format!(
                        "Row contains fewer columns ({} of {}) than metadata claims",
                        value_index, num_columns
                    ));
                return Err(FfiException::from_exception(ex));
            };

            let raw_column = match column_res {
                Ok(rc) => rc,
                Err(err) => return Err(FfiException::from_error(err, constructors)),
            };

            let Some(frame_slice) = raw_column.slice else {
                // The value is null, so we skip deserialization.
                // We can do that because `object[] values` in C# is initialized with nulls.
                continue;
            };

            unsafe {
                let ffi_exception = deserialize_value(
                    columns_ptr,
                    values_ptr,
                    value_index,
                    serializer_ptr,
                    FFIByteSlice::new(frame_slice.as_slice()),
                );
                if ffi_exception.has_exception() {
                    return Err(ffi_exception);
                }
            }
        }

        Ok(true)
    };

    // This is inherently inefficient, but necessary due to blocking C# API upon page boundaries.
    // TODO: implement async C# API (IAsyncEnumerable) to avoid this.
    let (has_row, result) = match BridgedFuture::block_on(deserialize_fut) {
        Ok(has_row) => (has_row, FfiException::ok()),
        Err(exception) => (false, exception),
    };
    unsafe {
        *out_has_row = has_row;
    }

    result
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_code(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> u8 {
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_code");
    };
    column_type_to_code(type_info)
}

// Specific child accessors

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_list_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_child_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_list_child");
    }

    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_list_child");
    };
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::List(inner),
            ..
        } => {
            let child = inner.as_ref();
            unsafe {
                out_child_handle.write(RefFFI::as_ptr(child));
            }
        }
        _ => panic!("row_set_type_info_get_list_child called on non-List ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_set_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_child_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_set_child");
    }

    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_set_child");
    };
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Set(inner),
            ..
        } => {
            let child = inner.as_ref();
            unsafe {
                out_child_handle.write(RefFFI::as_ptr(child));
            }
        }
        _ => panic!("row_set_type_info_get_set_child called on non-Set ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_map_children<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_key_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_value_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_key_handle.is_null() || out_value_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_map_children");
    }

    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_map_children");
    };
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Map(key, value),
            ..
        } => {
            let key_child = key.as_ref();
            let value_child = value.as_ref();
            let k_ptr = RefFFI::as_ptr(key_child);
            let v_ptr = RefFFI::as_ptr(value_child);
            unsafe {
                *out_key_handle = k_ptr;
                *out_value_handle = v_ptr;
            }
        }
        _ => panic!("row_set_type_info_get_map_children called on non-Map ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_tuple_field_count(
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
) -> usize {
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_tuple_field_count");
    };
    match type_info {
        ColumnType::Tuple(fields) => fields.len(),
        _ => panic!("row_set_type_info_get_tuple_field_count called on non-Tuple ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_tuple_field<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    index: usize,
    out_field_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_field_handle.is_null() {
        // Not sure whether to check out parameters
        panic!("Null pointer passed to row_set_type_info_get_tuple_field");
    }

    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_tuple_field");
    };
    match type_info {
        ColumnType::Tuple(fields) => {
            let Some(field) = fields.get(index) else {
                panic!("Index out of bounds in row_set_type_info_get_tuple_field");
            };
            let ptr = RefFFI::as_ptr(field);
            unsafe {
                *out_field_handle = ptr;
            }
        }
        _ => panic!("row_set_type_info_get_tuple_field called on non-Tuple ColumnType"),
    }
}

// --- UDT accessors ---

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_name(
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    out_name: *mut FFIStr<'_>,
) {
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_udt_name");
    };
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            let name = definition.name.as_ref();
            unsafe {
                out_name.write(FFIStr::new(name));
            }
        }
        _ => panic!("row_set_type_info_get_udt_name called on non-UDT ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field_count(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> usize {
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_udt_field_count");
    };
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => definition.field_types.len(),
        _ => panic!("row_set_type_info_get_udt_field_count called on non-UDT ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    index: usize,
    out_field_name: *mut FFIStr<'typ>,
    out_field_type_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_field_type_handle.is_null() || out_field_name.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_udt_field");
    }
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_udt_field");
    };
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            let Some((field_name, field_type)) = definition.field_types.get(index) else {
                panic!("Index out of bounds in row_set_type_info_get_udt_field");
            };
            unsafe {
                out_field_name.write(FFIStr::new(field_name.as_ref()));
            }
            let child = field_type;
            let ptr = RefFFI::as_ptr(child);
            unsafe {
                *out_field_type_handle = ptr;
            }
        }
        _ => panic!("row_set_type_info_get_udt_field called on non-UDT ColumnType"),
    }
}

fn column_type_to_code(typ: &ColumnType) -> u8 {
    match typ {
        ColumnType::Native(nt) => match nt {
            NativeType::Ascii => 0x01,
            NativeType::BigInt => 0x02,
            NativeType::Blob => 0x03,
            NativeType::Boolean => 0x04,
            NativeType::Counter => 0x05,
            NativeType::Decimal => 0x06,
            NativeType::Double => 0x07,
            NativeType::Float => 0x08,
            NativeType::Int => 0x09,
            NativeType::Text => 0x0A,
            NativeType::Timestamp => 0x0B,
            NativeType::Uuid => 0x0C,
            NativeType::Varint => 0x0E,
            NativeType::Timeuuid => 0x0F,
            NativeType::Inet => 0x10,
            NativeType::Date => 0x11,
            NativeType::Time => 0x12,
            NativeType::SmallInt => 0x13,
            NativeType::TinyInt => 0x14,
            NativeType::Duration => 0x15,
            _ => 0x00,
        },
        ColumnType::Collection { typ, .. } => match typ {
            CollectionType::List { .. } => 0x20,
            CollectionType::Map { .. } => 0x21,
            CollectionType::Set { .. } => 0x22,
            _ => 0x00,
        },
        ColumnType::Vector { .. } => 0x20, // FIXME: handle Vector as custom type
        ColumnType::UserDefinedType { .. } => 0x30,
        ColumnType::Tuple(_) => 0x31,
        _ => 0x00,
    }
}
