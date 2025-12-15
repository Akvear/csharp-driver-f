use scylla::client::pager::QueryPager;
use scylla::cluster::metadata::CollectionType;
use scylla::errors::DeserializationError;
use scylla::frame::response::result::{ColumnType, NativeType};

use crate::FfiPtr;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc, FromRef, RefFFI,
};
use crate::task::BridgedFuture;

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
    name_ptr: *const u8,
    name_len: usize,
    keyspace_ptr: *const u8,
    keyspace_len: usize,
    table_ptr: *const u8,
    table_len: usize,
    type_code: u8,
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    is_frozen: u8,
);

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
) {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_ref() else {
        return;
    };

    // Iterate column specs and call the metadata setter
    for (i, spec) in pager.column_specs().iter().enumerate() {
        fn str_to_ptr_and_len(s: &str) -> (*const u8, usize) {
            if s.is_empty() {
                (std::ptr::null(), 0)
            } else {
                (s.as_ptr(), s.len())
            }
        }

        let name = spec.name();
        let (name_ptr, name_len) = str_to_ptr_and_len(name);

        let ks = spec.table_spec().ks_name();
        let (keyspace_ptr, keyspace_len) = str_to_ptr_and_len(ks);

        let table = spec.table_spec().table_name();
        let (table_ptr, table_len) = str_to_ptr_and_len(table);

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
            set_metadata(
                columns_ptr,
                i,
                name_ptr,
                name_len,
                keyspace_ptr,
                keyspace_len,
                table_ptr,
                table_len,
                type_code,
                type_info_handle,
                is_frozen,
            );
        }
    }
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
    frame_slice_ptr: *const u8,
    length: usize,
);

#[unsafe(no_mangle)]
pub extern "C" fn row_set_next_row<'row_set>(
    row_set_ptr: BridgedBorrowedSharedPtr<'row_set, RowSet>,
    deserialize_value: DeserializeValue,
    columns_ptr: ColumnsPtr,
    values_ptr: ValuesPtr,
    serializer_ptr: SerializerPtr,
) -> i32 {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let mut pager_guard = row_set.pager.lock().unwrap();
    let Some(pager) = pager_guard.as_mut() else {
        return 0; // Empty RowSet has no rows
    };
    let num_columns = pager.column_specs().len();

    let deserialize_fut = async {
        // TODO: consider how to handle possibility of the metadata to change between pages.
        // While unlikely, it's not impossible.
        // For now, we just assume it won't happen and ignore `_new_page_began`.
        // The problem is that C# assumes the same metadata for the whole RowSet,
        // and they are passed through `ColumnsPtr`. Currently, if the metadata changes,
        // C# code will attempt to deserialize columns with wrong types, likely leading to exceptions.
        if let Some(Ok((mut column_iterator, _new_page_began))) = pager.next_column_iterator().await
        {
            // For each column in the row, we call `deserialize_value()`.
            for value_index in 0..num_columns {
                let raw_column = column_iterator.next().unwrap_or_else(|| {
                    // FIXME: handle error properly, passing it to C#.
                    #[expect(unreachable_code)]
                    Err(DeserializationError::new(todo!(
                        "Implement error type for too few columns - server provided less columns than claimed in the metadata"
                    )))
                }).unwrap(); // FIXME: handle error properly, passing it to C#.

                if let Some(frame_slice) = raw_column.slice {
                    unsafe {
                        deserialize_value(
                            columns_ptr,
                            values_ptr,
                            value_index,
                            serializer_ptr,
                            frame_slice.as_slice().as_ptr(),
                            frame_slice.as_slice().len(),
                        );
                    }
                } else {
                    // The value is null, so we skip deserialization.
                    // We can do that because `object[] values` in C# is initialized with nulls.
                    continue;
                }
            }
            true
        } else {
            tracing::trace!("[FFI] No more rows available!");
            false
        }
    };

    // This is inherently inefficient, but necessary due to blocking C# API upon page boundaries.
    // TODO: implement async C# API (IAsyncEnumerable) to avoid this.
    BridgedFuture::block_on(deserialize_fut) as i32
}

// TODO: Below change all unwrap() to unwrap_or_else() with proper error handling

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_code(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> u8 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    column_type_to_code(type_info)
}

// Specific child accessors

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_list_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::List(inner),
            ..
        } => {
            if out_child_handle.is_null() {
                return 0;
            }
            let child = inner.as_ref();
            unsafe {
                out_child_handle.write(RefFFI::as_ptr(child));
            }
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_set_child<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_child_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Set(inner),
            ..
        } => {
            if out_child_handle.is_null() {
                return 0;
            }
            let child = inner.as_ref();
            unsafe {
                out_child_handle.write(RefFFI::as_ptr(child));
            }
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_map_children<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_key_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    out_value_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Map(key, value),
            ..
        } => {
            if out_key_handle.is_null() || out_value_handle.is_null() {
                return 0;
            }
            let key_child = key.as_ref();
            let value_child = value.as_ref();
            let k_ptr = RefFFI::as_ptr(key_child);
            let v_ptr = RefFFI::as_ptr(value_child);
            unsafe {
                *out_key_handle = k_ptr;
                *out_value_handle = v_ptr;
            }
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_tuple_field_count(
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
) -> usize {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Tuple(fields) => fields.len(),
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_tuple_field<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    index: usize,
    out_field_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Tuple(fields) => {
            if out_field_handle.is_null() {
                return 0;
            }
            let Some(field) = fields.get(index) else {
                return 0;
            };
            let ptr = RefFFI::as_ptr(field);
            unsafe {
                *out_field_handle = ptr;
            }
            1
        }
        _ => 0,
    }
}

// --- UDT accessors ---

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_name(
    type_info_handle: BridgedBorrowedSharedPtr<'_, ColumnType<'_>>,
    out_name_ptr: *mut *const u8,
    out_name_len: *mut usize,
    out_keyspace_ptr: *mut *const u8,
    out_keyspace_len: *mut usize,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            let name = definition.name.as_ref();
            let ks = definition.keyspace.as_ref();
            unsafe { *out_name_ptr = name.as_ptr() };
            unsafe { *out_name_len = name.len() };
            unsafe { *out_keyspace_ptr = ks.as_ptr() };
            unsafe { *out_keyspace_len = ks.len() };
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field_count(
    type_info_handle: BridgedBorrowedSharedPtr<ColumnType<'_>>,
) -> usize {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => definition.field_types.len(),
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field<'typ>(
    type_info_handle: BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
    index: usize,
    out_field_name_ptr: *mut *const u8,
    out_field_name_len: *mut usize,
    out_field_type_handle: *mut BridgedBorrowedSharedPtr<'typ, ColumnType<'typ>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = RefFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            if out_field_type_handle.is_null() {
                return 0;
            }
            let Some((field_name, field_type)) = definition.field_types.get(index) else {
                return 0;
            };
            unsafe { *out_field_name_ptr = field_name.as_ptr() };
            unsafe { *out_field_name_len = field_name.len() };
            let child = field_type;
            let ptr = RefFFI::as_ptr(child);
            unsafe {
                *out_field_type_handle = ptr;
            }
            1
        }
        _ => 0,
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
