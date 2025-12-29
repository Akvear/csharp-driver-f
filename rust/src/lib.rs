mod error_conversion;
pub mod ffi;
mod logging;
mod pre_serialized_values;
mod prepared_statement;
mod row_set;
mod session;
mod task;

use std::ffi::{CStr, CString, c_char};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiPtr<'a, T: Sized> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T> Debug for FfiPtr<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ptr = self
            .ptr
            .map(|nn| nn.as_ptr())
            .unwrap_or(std::ptr::null::<T>() as *mut T);
        write!(f, "FfiPtr({:p})", ptr)
    }
}

type CSharpStr<'a> = FfiPtr<'a, c_char>;
impl<'a> CSharpStr<'a> {
    fn as_cstr(&self) -> Option<&CStr> {
        self.ptr.map(|ptr| unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}

/// Simple error struct that can be returned over FFI.
/// TODO: replace with a more general mechanism.
#[repr(C)]
pub struct FfiError {
    /// Numeric error code; 0 means success.
    code: i32,
    /// Pointer to a NUL-terminated UTF-8 message allocated by Rust.
    /// The receiver is responsible for eventually freeing it with
    /// `ffi_error_free_message`.
    message: *mut c_char,
}

impl FfiError {
    fn ok() -> Self {
        FfiError {
            code: 0,
            message: std::ptr::null_mut(),
        }
    }

    fn new(code: i32, msg: CString) -> Self {
        let ptr = msg.into_raw();
        FfiError { code, message: ptr }
    }
}

/// Frees an error message previously returned via `FfiError::new`.
///
/// # Safety
/// The `msg` pointer must have been produced by `CString::into_raw` and not
/// been freed already. Passing any other pointer value is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ffi_error_free_message(msg: *mut c_char) {
    if msg.is_null() {
        return;
    }
    unsafe {
        // SAFETY: `msg` must have been allocated from a `CString::into_raw`.
        let _ = CString::from_raw(msg);
    }
}
