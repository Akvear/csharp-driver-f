use scylla::client::pager::QueryPager;
use scylla::errors::DeserializationError;

use crate::FfiPtr;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
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

#[unsafe(no_mangle)]
pub extern "C" fn row_set_free(row_set_ptr: BridgedOwnedSharedPtr<RowSet>) {
    ArcFFI::free(row_set_ptr);
    tracing::trace!("[FFI] RowSet freed");
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
