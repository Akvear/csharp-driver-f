use super::csharp_memory::{CsharpSerializedValue, CsharpValuePtr};
use super::pre_serialized_values::PreSerializedValues;
use crate::FfiError;
use crate::ffi::{BoxFFI, BridgedBorrowedExclusivePtr, BridgedOwnedExclusivePtr};
use std::ffi::CString;

// TODO: consider moving to pre_serialized_values/pre_serialized_values.rs

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_new() -> BridgedOwnedExclusivePtr<PreSerializedValues> {
    BoxFFI::into_ptr(Box::new(PreSerializedValues::new()))
}

/// Adds a pre-serialized value from a C#-owned buffer to the builder.
///
/// # Safety
/// `value_ptr` and the data it points to must remain valid for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pre_serialized_values_add_value(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    value_ptr: CsharpValuePtr,
    value_len: usize,
) -> FfiError {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        return FfiError::new(
            1,
            CString::new("invalid PreSerializedValues pointer in pre_serialized_values_add_value")
                .unwrap(),
        );
    };
    let value = CsharpSerializedValue::new(value_ptr, value_len);
    match unsafe { values.add_value(value) } {
        Ok(()) => FfiError::ok(),
        Err(e) => {
            let msg = format!("failed to add value: {e}");
            FfiError::new(
                1,
                CString::new(msg)
                    .unwrap_or_else(|_| CString::new("invalid utf8 in error").unwrap()),
            )
        }
    }
}

/// Adds a null cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        return FfiError::new(
            1,
            CString::new("invalid PreSerializedValues pointer in pre_serialized_values_add_null")
                .unwrap(),
        );
    };
    match values.add_null() {
        Ok(()) => FfiError::ok(),
        Err(e) => {
            let msg = format!("failed to add null: {e}");
            FfiError::new(
                1,
                CString::new(msg)
                    .unwrap_or_else(|_| CString::new("invalid utf8 in error").unwrap()),
            )
        }
    }
}

/// Adds an unset cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        return FfiError::new(
            1,
            CString::new("invalid PreSerializedValues pointer in pre_serialized_values_add_unset")
                .unwrap(),
        );
    };
    match values.add_unset() {
        Ok(()) => FfiError::ok(),
        Err(e) => {
            let msg = format!("failed to add unset: {e}");
            FfiError::new(
                1,
                CString::new(msg)
                    .unwrap_or_else(|_| CString::new("invalid utf8 in error").unwrap()),
            )
        }
    }
}

/// Frees the PreSerializedValues if it was not consumed by a query.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_free(
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Simply drop the Box<PreSerializedValues>
    let _ = BoxFFI::from_ptr(values_ptr);
}
