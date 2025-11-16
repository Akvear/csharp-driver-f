use scylla::client::pager::QueryPager;
use scylla::cluster::metadata::CollectionType;
use scylla::errors::DeserializationError;
use scylla::frame::response::result::{ColumnType, NativeType};

use crate::FfiPtr;
use crate::ffi::{
    ArcFFI, BoxFFI, BridgedBorrowedSharedPtr, BridgedOwnedExclusivePtr, BridgedOwnedSharedPtr,
    BridgedPtr, FFI, FromArc, FromBox,
};
use crate::task::BridgedFuture;

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
    pub(crate) pager: std::sync::Mutex<QueryPager>,
}

impl FFI for RowSet {
    type Origin = FromArc;
}

// Not sure about this lifetime
impl FFI for ColumnType<'_> {
    type Origin = FromBox;
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_free(row_set_ptr: BridgedOwnedSharedPtr<RowSet>) {
    ArcFFI::free(row_set_ptr);
    println!("RowSet freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_get_columns_count(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
) -> usize {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.lock().unwrap();
    pager.column_specs().len()
}

type SetMetadata = unsafe extern "C" fn(
    columns_ptr: ColumnsPtr,
    value_index: usize,
    name_ptr: *const u8,
    name_len: usize,
    keyspace_ptr: *const u8,
    keyspace_len: usize,
    table_ptr: *const u8,
    table_len: usize,
    type_code: usize,
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType>,
);

/// Calls back into C# for each column to provide metadata.
/// `metadata_setter` is a function pointer supplied by C# - it will be called synchronously for each column.
#[unsafe(no_mangle)]
pub extern "C" fn row_set_fill_columns_metadata(
    row_set_ptr: BridgedBorrowedSharedPtr<'_, RowSet>,
    columns_ptr: ColumnsPtr,
    set_metadata: SetMetadata,
) -> i32 {
    let row_set = ArcFFI::as_ref(row_set_ptr).unwrap();
    let pager = row_set.pager.lock().unwrap();

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

        let type_code = column_type_to_code(spec.typ()) as usize;

        // Another big issue to address: creating a new Box for each ColumnType using clone()
        // Allocating a lot of memory to pass the values to C#
        // I wasn't able to figure out how to do it differently
        // Open to suggestions
        let mut type_info_handle: BridgedOwnedExclusivePtr<ColumnType> = BridgedPtr::null();
        if type_code >= 0x00020 {
            let boxed = Box::new(spec.typ().clone());
            type_info_handle = BoxFFI::into_ptr(boxed)
        }

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
            );
        }
    }

    1
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
    let mut pager = row_set.pager.lock().unwrap();
    let num_columns = pager.column_specs().len();

    let deserialize_fut = async {
        if let Some(Ok(mut column_iterator)) = pager.next_column_iterator().await {
            // For each column in the row, we call `deserialize_value()`.
            for value_index in 0..num_columns {
                let raw_column = column_iterator.next().unwrap_or_else(|| {
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
            println!("No more rows available!");
            false
        }
    };

    BridgedFuture::block_on(deserialize_fut) as i32
}

// TODO: Below change all unwrap() to unwrap_or_else() with proper error handling

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_free(type_info_handle: BridgedOwnedExclusivePtr<ColumnType>) {
    if type_info_handle.is_null() {
        return;
    }
    BoxFFI::free(type_info_handle);
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_code(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType>,
) -> usize {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    column_type_to_code(type_info) as usize
}

// Specific child accessors

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_list_child(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    out_child_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::List(inner),
            ..
        } => {
            if out_child_handle.is_null() {
                return 0;
            }
            let child = (*inner).as_ref().clone();
            let boxed = Box::new(child);
            let child_ptr = BoxFFI::into_ptr(boxed);
            unsafe {
                *out_child_handle = child_ptr;
            }
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_set_child(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    out_child_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Set(inner),
            ..
        } => {
            if out_child_handle.is_null() {
                return 0;
            }
            let child = (*inner).as_ref().clone();
            let boxed = Box::new(child);
            let child_ptr = BoxFFI::into_ptr(boxed);
            unsafe {
                *out_child_handle = child_ptr;
            }
            1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_map_children(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    out_key_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
    out_value_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Collection {
            typ: CollectionType::Map(key, value),
            ..
        } => {
            if out_key_handle.is_null() || out_value_handle.is_null() {
                return 0;
            }
            let key_child = (*key).as_ref().clone();
            let value_child = (*value).as_ref().clone();
            let boxed_k = Box::new(key_child);
            let boxed_v = Box::new(value_child);
            let k_ptr = BoxFFI::into_ptr(boxed_k);
            let v_ptr = BoxFFI::into_ptr(boxed_v);
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
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> usize {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Tuple(fields) => fields.len(),
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_tuple_field(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    index: usize,
    out_field_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::Tuple(fields) => {
            if index >= fields.len() {
                return 0;
            }
            if out_field_handle.is_null() {
                return 0;
            }
            let field = fields[index].clone();
            let boxed = Box::new(field);
            let ptr = BoxFFI::into_ptr(boxed);
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
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    out_name_ptr: *mut *const u8,
    out_name_len: *mut usize,
    out_keyspace_ptr: *mut *const u8,
    out_keyspace_len: *mut usize,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
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
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> usize {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => definition.field_types.len(),
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn row_set_type_info_get_udt_field(
    type_info_handle: BridgedOwnedExclusivePtr<ColumnType<'static>>,
    index: usize,
    out_field_name_ptr: *mut *const u8,
    out_field_name_len: *mut usize,
    out_field_type_handle: *mut BridgedOwnedExclusivePtr<ColumnType<'static>>,
) -> i32 {
    if type_info_handle.is_null() {
        return 0;
    }

    let type_info = BoxFFI::as_ref(type_info_handle).unwrap();
    match type_info {
        ColumnType::UserDefinedType { definition, .. } => {
            if index >= definition.field_types.len() || out_field_type_handle.is_null() {
                return 0;
            }
            let (ref field_name, ref field_type) = definition.field_types[index];
            unsafe { *out_field_name_ptr = field_name.as_ptr() };
            unsafe { *out_field_name_len = field_name.len() };

            let child = field_type.clone();
            let boxed = Box::new(child);
            let ptr = BoxFFI::into_ptr(boxed);
            unsafe {
                *out_field_type_handle = ptr;
            }
            1
        }
        _ => 0,
    }
}

fn column_type_to_code(typ: &ColumnType) -> u16 {
    match typ {
        ColumnType::Native(nt) => match nt {
            NativeType::Ascii => 0x0001,
            NativeType::BigInt => 0x0002,
            NativeType::Blob => 0x0003,
            NativeType::Boolean => 0x0004,
            NativeType::Counter => 0x0005,
            NativeType::Decimal => 0x0006,
            NativeType::Double => 0x0007,
            NativeType::Float => 0x0008,
            NativeType::Int => 0x0009,
            NativeType::Text => 0x000A,
            NativeType::Timestamp => 0x000B,
            NativeType::Uuid => 0x000C,
            NativeType::Varint => 0x000E,
            NativeType::Timeuuid => 0x000F,
            NativeType::Inet => 0x0010,
            NativeType::Date => 0x0011,
            NativeType::Time => 0x0012,
            NativeType::SmallInt => 0x0013,
            NativeType::TinyInt => 0x0014,
            NativeType::Duration => 0x0015,
            _ => 0x0000,
        },
        ColumnType::Collection { typ, .. } => match typ {
            CollectionType::List { .. } => 0x0020,
            CollectionType::Map { .. } => 0x0021,
            CollectionType::Set { .. } => 0x0022,
            _ => 0x0000,
        },
        ColumnType::Vector { .. } => 0x0020,
        ColumnType::UserDefinedType { .. } => 0x0030,
        ColumnType::Tuple(_) => 0x0031,
        _ => 0x0000,
    }
}
