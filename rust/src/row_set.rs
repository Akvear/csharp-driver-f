use scylla::client::pager::QueryPager;
use scylla::cluster::metadata::CollectionType;
use scylla::deserialize::FrameSlice;
use scylla::frame::response::result::{ColumnType, NativeType};

use crate::error_conversion::{ErrorToException as _, FFIException, FFIMaybeException};
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, FFI, FFIGCHandle, FFINonNullPtr, FFISlice, FFIStr, FromArc,
    FromRef, GCHandlePtr, RefFFI,
};
use crate::task::{BridgedFuture, ExceptionConstructors, Tcb};

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
    pub(crate) pager: tokio::sync::Mutex<QueryPager>,
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
) -> FFIMaybeException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.blocking_lock();
    unsafe {
        *out_num_fields = pager.column_specs().len();
    }
    FFIMaybeException::ok()
}

// Function pointer type for setting column metadata in C#.
type SetMetadata = unsafe extern "C" fn(
    columns_ptr: FFINonNullPtr<'_, Columns>,
    value_index: usize,
    name: FFIStr<'_>,
    keyspace: FFIStr<'_>,
    table: FFIStr<'_>,
    type_code: u8,
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_frozen: u8,
) -> FFIMaybeException;

/// Calls back into C# for each column to provide metadata.
/// `metadata_setter` is a function pointer supplied by C# - it will be called synchronously for each column.
/// SAFETY: This function assumes that `columns_ptr` is a valid pointer
/// to a C# CQLColumn array of length equal to the number of columns,
/// and that `set_metadata` is a valid function pointer that can be called safely.
#[unsafe(no_mangle)]
pub extern "C" fn row_set_fill_columns_metadata(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
    columns_ptr: FFINonNullPtr<'_, Columns>,
    set_metadata: SetMetadata,
) -> FFIMaybeException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.blocking_lock();

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
    FFIMaybeException::ok()
}

/// Opaque C# representation of column metadata array.
pub enum Columns {}
/// Opaque C# representation of deserialized column values array.
pub enum Values {}
/// Opaque C# representation of (de)serializer.
pub enum Serializer {}

type DeserializeValue = unsafe extern "C" fn(
    columns_ptr: GCHandlePtr<'_, Columns>,
    values_ptr: GCHandlePtr<'_, Values>,
    value_index: usize,
    serializer_ptr: GCHandlePtr<'_, Serializer>,
    frame_slice: FFISlice<'_, u8>,
) -> FFIMaybeException;

/// Deserializes all columns of the next row from `next_column_iterator()` result,
/// calling back into C# via `call_deserialize` for each non-null column value.
///
/// Returns `Ok(true)` if a row was deserialized, `Ok(false)` if no more rows,
/// or `Err` with an FFI exception on failure.
fn deserialize_next_row(
    next: Option<
        Result<
            (scylla::deserialize::row::ColumnIterator<'_, '_>, bool),
            scylla::errors::NextRowError,
        >,
    >,
    num_columns: usize,
    mut deser_csharp_value: impl FnMut(usize, FrameSlice<'_>) -> FFIMaybeException,
    constructors: &'static ExceptionConstructors,
) -> Result<bool, FFIException> {
    // TODO: consider how to handle possibility of the metadata to change between pages.
    // While unlikely, it's not impossible.
    // For now, we just assume it won't happen and ignore `_new_page_began`.
    // The problem is that C# assumes the same metadata for the whole RowSet,
    // and they are passed through `FFINonNullPtr<'_, Columns>`. Currently, if the metadata changes,
    // C# code will attempt to deserialize columns with wrong types, likely leading to exceptions.
    let Some(next) = next else {
        tracing::trace!("[FFI] No more rows available!");
        return Ok(false);
    };

    let (mut column_iterator, _new_page_began) = match next {
        // Successfully obtained the next row's column iterator
        Ok(values) => values,
        // Error while fetching the column value
        Err(err) => return Err(err.to_exception(constructors)),
    };

    for value_index in 0..num_columns {
        let Some(column_res) = column_iterator.next() else {
            // Error: fewer columns than expected
            // TODO: Implement error type for too few columns - server provided less columns than claimed in the metadata
            let ex = constructors
                .rust_exception_constructor
                .construct_from_rust(format_args!(
                    "Row contains fewer columns ({} of {}) than metadata claims",
                    value_index, num_columns
                ));
            return Err(ex);
        };

        let raw_column = match column_res {
            Ok(rc) => rc,
            Err(err) => return Err(err.to_exception(constructors)),
        };

        let Some(frame_slice) = raw_column.slice else {
            // The value is null, so we skip deserialization.
            // We can do that because `object[] values` in C# is initialized with nulls.
            continue;
        };

        let ffi_exception = deser_csharp_value(value_index, frame_slice);
        if let Some(e) = ffi_exception.try_into_ffi_exception() {
            return Err(e);
        }
    }

    Ok(true)
}

// Replaced by async version in row_set_next_row_async, but this will be
// still used in some way in next commits. Stay tuned!
#[unsafe(no_mangle)]
pub extern "C" fn row_set_next_row<'row_set>(
    row_set_ptr: BridgedBorrowedSharedPtr<'row_set, RowSet>,
    deserialize_value: DeserializeValue,
    columns_handle: FFIGCHandle<Columns>,
    values_handle: FFIGCHandle<Values>,
    serializer_handle: FFIGCHandle<Serializer>,
    out_has_row: *mut bool,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let deserialize_fut = async {
        let mut pager = row_set.pager.lock().await;
        let num_columns = pager.column_specs().len();
        let next = pager.next_column_iterator().await;

        deserialize_next_row(
            next,
            num_columns,
            |value_index, frame_slice| unsafe {
                deserialize_value(
                    columns_handle.borrow(),
                    values_handle.borrow(),
                    value_index,
                    serializer_handle.borrow(),
                    FFISlice::new(frame_slice.as_slice()),
                )
            },
            constructors,
        )
    };

    // This is inherently inefficient, but necessary due to blocking C# API upon page boundaries.
    // TODO: implement async C# API (IAsyncEnumerable) to avoid this.
    let (has_row, result) = match BridgedFuture::block_on(deserialize_fut) {
        Ok(has_row) => (has_row, FFIMaybeException::ok()),
        Err(exception) => (false, FFIMaybeException::from_exception(exception)),
    };
    unsafe {
        *out_has_row = has_row;
    }

    result
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_next_row_async<'row_set>(
    tcb: Tcb<bool>,
    row_set_ptr: BridgedBorrowedSharedPtr<'row_set, RowSet>,
    deserialize_value: DeserializeValue,
    columns_handle: FFIGCHandle<Columns>,
    values_handle: FFIGCHandle<Values>,
    serializer_handle: FFIGCHandle<Serializer>,
    constructors: &'static ExceptionConstructors,
) {
    let row_set = ArcFFI::cloned_from_ptr(row_set_ptr).unwrap();
    BridgedFuture::spawn(tcb, async move {
        let mut pager = row_set.pager.lock().await;
        let num_columns = pager.column_specs().len();

        let next = pager.next_column_iterator().await;

        deserialize_next_row(
            next,
            num_columns,
            |value_index, frame_slice| unsafe {
                deserialize_value(
                    columns_handle.borrow(),
                    values_handle.borrow(),
                    value_index,
                    serializer_handle.borrow(),
                    FFISlice::new(frame_slice.as_slice()),
                )
            },
            constructors,
        )
    });
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
pub extern "C" fn row_set_type_info_get_udt_name<'a>(
    type_info_handle: BridgedBorrowedSharedPtr<'a, ColumnType<'a>>,
    out_name: *mut FFIStr<'a>,
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

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_vector_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) {
    if out_child_handle.is_null() {
        panic!("Null pointer passed to row_set_type_info_get_vector_child");
    }

    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_vector_child");
    };
    match type_info {
        ColumnType::Vector { typ, .. } => {
            let child = typ.as_ref();
            unsafe {
                out_child_handle.write(RefFFI::as_ptr(child));
            }
        }
        _ => panic!("row_set_type_info_get_vector_child called on non-Vector ColumnType"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_vector_dimensions(
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
) -> u16 {
    let Some(type_info) = RefFFI::as_ref(type_info_handle) else {
        panic!("Null pointer passed to row_set_type_info_get_vector_dimensions");
    };
    match type_info {
        ColumnType::Vector { dimensions, .. } => *dimensions,
        _ => panic!("row_set_type_info_get_vector_dimensions called on non-Vector ColumnType"),
    }
}

pub(crate) fn column_type_to_code(typ: &ColumnType) -> u8 {
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
        ColumnType::Vector { .. } => 0x23,
        ColumnType::UserDefinedType { .. } => 0x30,
        ColumnType::Tuple(_) => 0x31,
        _ => 0x00,
    }
}
