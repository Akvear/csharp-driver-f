use core::panic;

use scylla::client::pager::QueryPager;
use scylla::cluster::metadata::CollectionType;
use scylla::frame::response::result::{ColumnType, NativeType};

use crate::FfiPtr;
use crate::error_conversion::FfiExceptionPackage;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FFIByteSlice, FFIStr, FromArc,
    FromRef, RefFFI,
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
pub extern "C" fn row_set_free(row_set_ptr: BridgedOwnedSharedPtr<RowSet>) {
    ArcFFI::free(row_set_ptr);
    tracing::trace!("[FFI] RowSet freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_get_columns_count(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
) -> usize {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.lock().unwrap();
    pager.as_ref().map(|p| p.column_specs().len()).unwrap_or(0)
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
) -> FfiExceptionPackage;

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
) -> FfiExceptionPackage {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_ref() else {
        // Return a RustException built via constructors as a quick workaround.
        let ex = constructors
            .rust_exception_constructor
            .construct_from_rust("RowSet has no pager to get metadata from");
        return FfiExceptionPackage::from_exception(ex);
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
            ColumnType::Collection { frozen, .. } => {
                if *frozen {
                    1
                } else {
                    0
                }
            }
            ColumnType::UserDefinedType { frozen, .. } => {
                if *frozen {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        };

        unsafe {
            let ffi_exception_package = set_metadata(
                columns_ptr,
                i,
                name,
                keyspace,
                table,
                type_code,
                type_info_handle,
                is_frozen,
            );
            // If there is an exception returned from callback, throw it as soon as possible
            if ffi_exception_package.has_exception() {
                return ffi_exception_package;
            }
        }
    }
    FfiExceptionPackage::ok()
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
);

#[unsafe(no_mangle)]
pub extern "C" fn row_set_next_row<'row_set>(
    row_set_ptr: BridgedBorrowedSharedPtr<'row_set, RowSet>,
    deserialize_value: DeserializeValue,
    columns_ptr: ColumnsPtr,
    values_ptr: ValuesPtr,
    serializer_ptr: SerializerPtr,
    out_num_columns: *mut i32,
    constructors: &ExceptionConstructors,
) -> FfiExceptionPackage {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let mut pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_mut() else {
        unsafe {
            *out_num_columns = 0;
        }
        return FfiExceptionPackage::ok(); // Empty RowSet has no rows
    };
    let num_columns = pager.column_specs().len();

    let deserialize_fut = async {
        // Returns Ok(true) when a row was read and deserialized,
        // Ok(false) when there are no more rows,
        // Err(FfiExceptionPackage) when an error occurs and should be propagated to C#.
        // TODO: consider how to handle possibility of the metadata to change between pages.
        // While unlikely, it's not impossible.
        // For now, we just assume it won't happen and ignore `_new_page_began`.
        // The problem is that C# assumes the same metadata for the whole RowSet,
        // and they are passed through `ColumnsPtr`. Currently, if the metadata changes,
        // C# code will attempt to deserialize columns with wrong types, likely leading to exceptions.
        match pager.next_column_iterator().await {
            Some(Ok((mut column_iterator, _new_page_began))) => {
                // For each column in the row, we call `deserialize_value()`.
                for value_index in 0..num_columns {
                    match column_iterator.next() {
                        Some(Ok(raw_column)) => {
                            if let Some(frame_slice) = raw_column.slice {
                                unsafe {
                                    // TODO: Check for exceptions from C# side
                                    deserialize_value(
                                        columns_ptr,
                                        values_ptr,
                                        value_index,
                                        serializer_ptr,
                                        FFIByteSlice::new(frame_slice.as_slice()),
                                    );
                                }
                            } else {
                                // The value is null, so we skip deserialization.
                                // We can do that because `object[] values` in C# is initialized with nulls.
                                continue;
                            }
                        }
                        Some(Err(err)) => {
                            // Error while fetching the column value
                            return Err(FfiExceptionPackage::from_error(err, constructors));
                        }
                        None => {
                            // TODO: Implement error type for too few columns - server provided less columns than claimed in the metadata
                            let ex = constructors.rust_exception_constructor.construct_from_rust(
                                format!(
                                    "Row contains fewer columns ({} of {}) than metadata claims",
                                    value_index, num_columns
                                ),
                            );
                            return Err(FfiExceptionPackage::from_exception(ex));
                        }
                    }
                }
                Ok(true)
            }
            Some(Err(err)) => Err(FfiExceptionPackage::from_error(err, constructors)),
            None => {
                tracing::trace!("[FFI] No more rows available!");
                Ok(false)
            }
        }
    };

    // This is inherently inefficient, but necessary due to blocking C# API upon page boundaries.
    // TODO: implement async C# API (IAsyncEnumerable) to avoid this.
    match BridgedFuture::block_on(deserialize_fut) {
        Ok(has_row) => {
            unsafe {
                *out_num_columns = has_row as i32;
            }
            FfiExceptionPackage::ok()
        }
        Err(exception_package) => {
            unsafe {
                *out_num_columns = 0;
            }
            exception_package
        }
    }
}

// There are a lot of panics below - not sure if it's supposed to be like that.
// Some of these checks are probably redundant (looking at passing a null pointer).

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_code(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> u8 {
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_code");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    column_type_to_code(type_info)
}

// Specific child accessors

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_list_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_list_child");
    }
    if out_child_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_list_child");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_set_child");
    }
    if out_child_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_set_child");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_map_children");
    }
    if out_key_handle.is_null() || out_value_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_map_children");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_tuple_field_count");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    if type_info_handle.is_null() || out_field_handle.is_null() {
        // Not sure whether to check out parameters
        panic!("Null pointer passed to row_set_type_info_get_tuple_field");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    out_keyspace: *mut FFIStr<'_>,
) {
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_udt_name");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            let name = definition.name.as_ref();
            let ks = definition.keyspace.as_ref();
            unsafe {
                out_name.write(FFIStr::new(name));
                out_keyspace.write(FFIStr::new(ks));
            }
        }
        _ => panic!("row_set_type_info_get_udt_name called on non-UDT ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field_count(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> usize {
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_udt_field_count");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
    if type_info_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_udt_field");
    }
    if out_field_type_handle.is_null() || out_field_name.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_udt_field");
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
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
