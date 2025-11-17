pub mod ffi;
mod logging;
mod row_set;
mod session;
mod task;

use std::ffi::{CStr, c_char};
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiPtr<'a, T: Sized> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a ()>,
}

type CSharpStr<'a> = FfiPtr<'a, c_char>;
impl<'a> CSharpStr<'a> {
    fn as_cstr(&self) -> Option<&CStr> {
        self.ptr.map(|ptr| unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}
