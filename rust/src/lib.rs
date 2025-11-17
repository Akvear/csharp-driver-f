pub mod ffi;
mod logging;
mod task;

use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiPtr<'a, T: Sized> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a ()>,
}
