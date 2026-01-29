mod error_conversion;
pub mod ffi;
mod logging;
mod metadata;
mod pre_serialized_values;
mod prepared_statement;
mod row_set;
mod session;
mod task;

use std::ffi::{CStr, c_char};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiPtr<'a, T: Sized> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a ()>,
}

// Compile-time assertion that `FfiPtr` is pointer-sized.
// Ensures ABI compatibility with C# (opaque GCHandle/IntPtr across FFI).
const _: [(); std::mem::size_of::<FfiPtr<'_, ()>>()] = [(); std::mem::size_of::<*const ()>()];

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
