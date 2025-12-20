use crate::FfiPtr;

/// Marker type for C# values.
#[derive(Clone, Copy)]
pub enum CsharpValue {}

/// Opaque pointer to a C# value buffer.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CsharpValuePtr {
    inner: FfiPtr<'static, CsharpValue>,
}

impl CsharpValuePtr {
    #[inline]
    pub fn as_raw(&self) -> *const u8 {
        self.inner
            .ptr
            .map(|p| p.as_ptr() as *const u8)
            .unwrap_or(std::ptr::null())
    }
}

/// A pre-serialized value from C#, holding the buffer pointer and length.
/// The C# side is responsible for keeping the memory valid.
pub struct CsharpSerializedValue {
    pub ptr: CsharpValuePtr,
    pub len: usize,
}

impl CsharpSerializedValue {
    /// Construct a new `CsharpSerializedValue`.
    pub(crate) fn new(ptr: CsharpValuePtr, len: usize) -> Self {
        Self { ptr, len }
    }

    /// Returns the pre-serialized bytes from the C# buffer.
    ///
    /// # Safety
    /// The caller must ensure that the underlying C# buffer remains valid and pinned
    /// for the entire lifetime of the returned slice.
    ///
    /// Note that the lifetime of the returned slice is tied to `&self`, but this
    /// is just a convenience for the borrow checker. The actual validity of the
    /// memory is determined by the C# side (GC pinning), which Rust cannot track.
    /// Therefore, this function is unsafe: it is possible for `&self` to be valid
    /// while the underlying C# memory has been freed or moved if the C# side
    /// unpinned it prematurely.
    pub(crate) unsafe fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_raw(), self.len) }
    }
}
