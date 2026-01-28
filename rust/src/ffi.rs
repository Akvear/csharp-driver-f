use std::ffi::c_void;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

mod sealed {
    // This is a sealed trait - its whole purpose is to be unnameable.
    // This means we need to disable the check.
    #[expect(unnameable_types)]
    pub trait Sealed {}
}

/// A trait representing ownership (i.e. Rust mutability) of the pointer.
///
/// Pointer can either be [`Exclusive`] or [`Shared`].
///
/// ## Shared pointers
/// Shared pointers can only be converted to **immutable** Rust referential types.
/// There is no way to obtain a mutable reference from such pointer.
///
/// In some cases, we need to be able to mutate the data behind a shared pointer.
///
/// ## Exclusive pointers
/// Exclusive pointers can be converted to both immutable and mutable Rust referential types.
pub trait Ownership: sealed::Sealed {}

/// Represents shared (immutable) pointer.
pub struct Shared;
impl sealed::Sealed for Shared {}
impl Ownership for Shared {}

/// Represents exclusive (mutable) pointer.
pub struct Exclusive;
impl sealed::Sealed for Exclusive {}
impl Ownership for Exclusive {}

/// Represents additional properties of the pointer.
pub trait Properties: sealed::Sealed {
    type Ownership: Ownership;
}

impl<O: Ownership> Properties for O {
    type Ownership = O;
}

/// Represents a valid non-dangling pointer.
///
/// ## Safety and validity guarantees
/// Apart from trivial constructors such as [`BridgedPtr::null()`] and [`BridgedPtr::null_mut()`], there
/// is only one way to construct a [`BridgedPtr`] instance - from raw pointer via [`BridgedPtr::from_raw()`].
/// This constructor is `unsafe`. It is user's responsibility to ensure that the raw pointer
/// provided to the constructor is **valid**. In other words, the pointer comes from some valid
/// allocation, or from some valid reference.
///
/// ## Generic lifetime and aliasing guarantees
/// We distinguish two types of pointers: shared ([`Shared`]) and exclusive ([`Exclusive`]).
/// Shared pointers can be converted to immutable (&) references, while exclusive pointers
/// can be converted to either immutable (&) or mutable (&mut) reference. User needs to pick
/// the correct mutability property of the pointer during construction. This is yet another
/// reason why [`BridgedPtr::from_raw`] is `unsafe`.
///
/// Pointer is parameterized by the lifetime. Thanks to that, we can tell whether the pointer
/// **owns** or **borrows** the pointee. Once again, user is responsible for "picking"
/// the correct lifetime when creating the pointer. For example, when raw pointer
/// comes from [`Box::into_raw()`], user could create a [`BridgedPtr<'static, T, (Exclusive,)>`].
/// `'static` lifetime represents that user is the exclusive **owner** of the pointee, and
/// is responsible for freeing the memory (e.g. via [`Box::from_raw()`]).
/// On the other hand, when pointer is created from some immutable reference `&'a T`,
/// the correct choice of BridgedPtr would be [`BridgedPtr<'a, T, (Shared,)>`]. It means that
/// holder of the created pointer **borrows** the pointee (with some lifetime `'a`
/// inherited from the immutable borrow `&'a T`).
///
/// Both [`BridgedPtr::into_ref()`] and [`BridgedPtr::into_mut_ref()`] consume the pointer.
/// At first glance, it seems impossible to obtain multiple immutable reference from one pointer.
/// This is why pointer reborrowing mechanism is introduced. There are two methods: [`BridgedPtr::borrow()`]
/// and [`BridgedPtr::borrow_mut()`]. Both of them cooperate with borrow checker and enforce
/// aliasing XOR mutability principle at compile time.
///
/// ## Safe conversions to referential types
/// Thanks to the above guarantees, conversions to referential types are **safe**.
/// See methods [`BridgedPtr::into_ref()`] and [`BridgedPtr::into_mut_ref()`].
///
/// ## Memory layout
/// We use repr(transparent), so the struct has the same layout as underlying [`Option<NonNull<T>>`].
/// Thanks to <https://doc.rust-lang.org/std/option/#representation optimization>,
/// we are guaranteed, that for `T: Sized`, our struct has the same layout
/// and function call ABI as simply [`NonNull<T>`].
#[repr(transparent)]
pub struct BridgedPtr<'a, T: Sized, P: Properties> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a P>,
}

// Compile-time assertion that `BridgedPtr` is pointer-sized.
// Ensures ABI compatibility with C# (opaque GCHandle/IntPtr across FFI).
const _: [(); std::mem::size_of::<BridgedPtr<'_, (), Shared>>()] =
    [(); std::mem::size_of::<*const ()>()];

/// Casts the pointer to a pointer to `c_void`.
/// This is useful to accomodate for non-generics APIs,
/// i.e., C FFI functions that accept `*mut c_void` parameters.
impl<'a, T: Sized, P: Properties> BridgedPtr<'a, T, P> {
    pub(crate) fn cast_to_void(self) -> BridgedPtr<'a, c_void, P> {
        BridgedPtr {
            ptr: self.ptr.map(|p| p.cast()),
            _phantom: PhantomData,
        }
    }

    /// Casts the pointer to a pointer to `S`.
    /// Used to cast from `c_void` back to concrete type.
    pub(crate) unsafe fn cast<S>(self) -> BridgedPtr<'a, S, P> {
        BridgedPtr {
            ptr: self.ptr.map(|p| p.cast()),
            _phantom: PhantomData,
        }
    }
}

/// Owned shared pointer.
/// Can be used for pointers with shared ownership - e.g. pointers coming from [`Arc`] allocation.
pub type BridgedOwnedSharedPtr<T> = BridgedPtr<'static, T, Shared>;

/// Borrowed shared pointer.
/// Can be used for pointers created from some immutable reference.
pub type BridgedBorrowedSharedPtr<'a, T> = BridgedPtr<'a, T, Shared>;

/// Owned exclusive pointer.
/// Can be used for pointers with exclusive ownership - e.g. pointers coming from [`Box`] allocation.
pub type BridgedOwnedExclusivePtr<T> = BridgedPtr<'static, T, Exclusive>;

/// Borrowed exclusive pointer.
/// This can be for example obtained from mutable reborrow of some [`BridgedOwnedExclusivePtr`].
pub type BridgedBorrowedExclusivePtr<'a, T> = BridgedPtr<'a, T, Exclusive>;

/// Pointer constructors.
impl<T: Sized, P: Properties> BridgedPtr<'_, T, P> {
    pub fn null() -> Self {
        BridgedPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_none()
    }

    /// Constructs [`Bridged`] from raw pointer.
    ///
    /// ## Safety
    /// User needs to ensure that the pointer is **valid**.
    /// User is also responsible for picking correct ownership property and lifetime
    /// of the created pointer.
    unsafe fn from_raw(raw: *const T) -> Self {
        BridgedPtr {
            ptr: NonNull::new(raw as *mut T),
            _phantom: PhantomData,
        }
    }
}

/// Conversion to raw pointer.
impl<T: Sized, P: Properties> BridgedPtr<'_, T, P> {
    pub(crate) fn to_raw(&self) -> Option<*mut T> {
        self.ptr.map(|ptr| ptr.as_ptr())
    }
}

/// Constructors for to exclusive pointers.
impl<T: Sized> BridgedPtr<'_, T, Exclusive> {
    pub(crate) fn null_mut() -> Self {
        BridgedPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: Sized, P: Properties> BridgedPtr<'a, T, P> {
    /// Converts a pointer to an optional valid reference.
    /// The reference inherits the lifetime of the pointer.
    fn into_ref(self) -> Option<&'a T> {
        // SAFETY: Thanks to the validity and aliasing ^ mutability guarantees,
        // we can safely convert the pointer to valid immutable reference with
        // correct lifetime.
        unsafe { self.ptr.map(|p| p.as_ref()) }
    }
}

impl<'a, T: Sized> BridgedPtr<'a, T, Exclusive> {
    /// Converts a pointer to an optional valid mutable reference.
    /// The reference inherits the lifetime of the pointer.
    fn into_mut_ref(self) -> Option<&'a mut T> {
        // SAFETY: Thanks to the validity and aliasing ^ mutability guarantees,
        // we can safely convert the pointer to valid mutable (and exclusive) reference with
        // correct lifetime.
        unsafe { self.ptr.map(|mut p| p.as_mut()) }
    }
}

impl<T: Sized, P: Properties> BridgedPtr<'_, T, P> {
    /// Immutably reborrows the pointer.
    /// Resulting pointer inherits the lifetime from the immutable borrow
    /// of original pointer.
    #[allow(clippy::needless_lifetimes)]
    pub fn borrow<'a>(&'a self) -> BridgedPtr<'a, T, Shared> {
        BridgedPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

impl<T: Sized> BridgedPtr<'_, T, Exclusive> {
    /// Mutably reborrows the pointer.
    /// Resulting pointer inherits the lifetime from the mutable borrow
    /// of original pointer. Since the method accepts a mutable reference
    /// to the original pointer, we enforce aliasing ^ mutability principle at compile time.
    #[allow(clippy::needless_lifetimes)]
    pub fn borrow_mut<'a>(&'a mut self) -> BridgedPtr<'a, T, Exclusive> {
        BridgedPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/*
 * FFI traits and implementations - various ownership kinds
 */

mod origin_sealed {
    // This is a sealed trait - its whole purpose is to be unnameable.
    // This means we need to disable the check.
    #[expect(unnameable_types)]
    pub trait FromBoxSealed {}

    // This is a sealed trait - its whole purpose is to be unnameable.
    // This means we need to disable the check.
    #[expect(unnameable_types)]
    pub trait FromArcSealed {}

    // This is a sealed trait - its whole purpose is to be unnameable.
    // This means we need to disable the check.
    #[expect(unnameable_types)]
    pub trait FromRefSealed {}
}

/// Defines a pointer manipulation API for non-shared heap-allocated data.
///
/// Implement this trait for types that are allocated by the driver via [`Box::new`],
/// and then returned to the user as a pointer. The user is responsible for freeing
/// the memory associated with the pointer using corresponding driver's API function.
pub trait BoxFFI: Sized + origin_sealed::FromBoxSealed {
    /// Consumes the Box and returns a pointer with exclusive ownership.
    /// The pointer needs to be freed. See [`BoxFFI::free()`].
    fn into_ptr(self: Box<Self>) -> BridgedPtr<'static, Self, Exclusive> {
        #[allow(clippy::disallowed_methods)]
        let ptr = Box::into_raw(self);

        // SAFETY:
        // 1. validity guarantee - pointer is obviously valid. It comes from box allocation.
        // 2. pointer's lifetime - we choose 'static lifetime. It is ok, because holder of the
        //    pointer becomes the owner of pointee. He is responsible for freeing the memory
        //    via BoxFFI::free() - which accepts 'static pointer. User is not able to obtain
        //    another pointer with 'static lifetime pointing to the same memory.
        // 3. ownership - user becomes an exclusive owner of the pointee. Thus, it's ok
        //    for the pointer to be `Exclusive`.
        unsafe { BridgedPtr::from_raw(ptr) }
    }

    /// Consumes the pointer with exclusive ownership back to the Box.
    fn from_ptr(ptr: BridgedPtr<'static, Self, Exclusive>) -> Option<Box<Self>> {
        // SAFETY:
        // The only way to obtain an owned pointer (with 'static lifetime) is BoxFFI::into_ptr().
        // It creates a pointer based on Box allocation. It is thus safe to convert the pointer
        // back to owned `Box`.
        unsafe {
            ptr.to_raw().map(|p| {
                #[allow(clippy::disallowed_methods)]
                Box::from_raw(p)
            })
        }
    }

    /// Creates a reference from an exclusive pointer.
    /// Reference inherits the lifetime of the pointer's borrow.
    #[allow(clippy::needless_lifetimes)]
    fn as_ref<'a, O: Ownership>(ptr: BridgedPtr<'a, Self, O>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    /// Creates a mutable from an exlusive pointer.
    /// Reference inherits the lifetime of the pointer's mutable borrow.
    #[allow(clippy::needless_lifetimes)]
    fn as_mut_ref<'a>(ptr: BridgedPtr<'a, Self, Exclusive>) -> Option<&'a mut Self> {
        ptr.into_mut_ref()
    }

    /// Frees the pointee.
    fn free(ptr: BridgedPtr<'static, Self, Exclusive>) {
        std::mem::drop(BoxFFI::from_ptr(ptr));
    }

    // Currently used only in tests.
    #[allow(dead_code)]
    fn null<'a>() -> BridgedPtr<'a, Self, Shared> {
        BridgedPtr::null()
    }

    fn null_mut<'a>() -> BridgedPtr<'a, Self, Exclusive> {
        BridgedPtr::null_mut()
    }
}

/// Defines a pointer manipulation API for shared heap-allocated data.
///
/// Implement this trait for types that require a shared ownership of data.
/// The data should be allocated via [`Arc::new`], and then returned to the user as a pointer.
/// The user is responsible for freeing the memory associated
/// with the pointer using corresponding driver's API function.
pub trait ArcFFI: Sized + origin_sealed::FromArcSealed {
    /// Creates a pointer from a valid reference to Arc-allocated data.
    /// Holder of the pointer borrows the pointee.
    #[allow(clippy::needless_lifetimes)]
    fn as_ptr<'a>(self: &'a Arc<Self>) -> BridgedPtr<'a, Self, Shared> {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::as_ptr(self);

        // SAFETY:
        // 1. validity guarantee - pointer is valid, since it's obtained from Arc allocation.
        // 2. pointer's lifetime - pointer inherits the lifetime of provided Arc's borrow.
        //    What's important is that the returned pointer borrows the data, and is not the
        //    shared owner. Thus, user cannot call ArcFFI::free() on such pointer.
        // 3. ownership - we always create a `Shared` pointer.
        unsafe { BridgedPtr::from_raw(ptr) }
    }

    /// Creates a pointer from a valid Arc allocation.
    fn into_ptr(self: Arc<Self>) -> BridgedPtr<'static, Self, Shared> {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::into_raw(self);

        // SAFETY:
        // 1. validity guarantee - pointer is valid, since it's obtained from Arc allocation
        // 2. pointer's lifetime - returned pointer has a 'static lifetime. It is a shared
        //    owner of the pointee. User has to decrement the RC of the pointer (and potentially free the memory)
        //    via ArcFFI::free().
        // 3. ownership - we always create a `Shared` pointer.
        unsafe { BridgedPtr::from_raw(ptr) }
    }

    /// Converts shared owned pointer back to owned Arc.
    fn from_ptr(ptr: BridgedPtr<'static, Self, Shared>) -> Option<Arc<Self>> {
        // SAFETY:
        // The only way to obtain a pointer with shared ownership ('static lifetime) is
        // ArcFFI::into_ptr(). It converts an owned Arc into the pointer. It is thus safe
        // to convert such pointer back to owned Arc.
        unsafe {
            ptr.to_raw().map(|p| {
                #[allow(clippy::disallowed_methods)]
                Arc::from_raw(p)
            })
        }
    }

    /// Increases the reference count of the pointer, and returns an owned Arc.
    fn cloned_from_ptr(ptr: BridgedPtr<'_, Self, Shared>) -> Option<Arc<Self>> {
        // SAFETY:
        // All pointers created via ArcFFI API are originated from Arc allocation.
        // It is thus safe to increase the reference count of the pointer, and convert
        // it to Arc. Because of the borrow-checker, it is not possible for the user
        // to provide a pointer that points to already deallocated memory.
        unsafe {
            ptr.to_raw().map(|p| {
                #[allow(clippy::disallowed_methods)]
                Arc::increment_strong_count(p);
                #[allow(clippy::disallowed_methods)]
                Arc::from_raw(p)
            })
        }
    }

    /// Converts a shared borrowed pointer to reference.
    /// The reference inherits the lifetime of pointer's borrow.
    #[allow(clippy::needless_lifetimes)]
    fn as_ref<'a>(ptr: BridgedPtr<'a, Self, Shared>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    /// Decreases the reference count (and potentially frees) of the owned pointer.
    fn free(ptr: BridgedPtr<'static, Self, Shared>) {
        std::mem::drop(ArcFFI::from_ptr(ptr));
    }

    fn null<'a>() -> BridgedPtr<'a, Self, Shared> {
        BridgedPtr::null()
    }

    fn is_null(ptr: &BridgedPtr<'_, Self, Shared>) -> bool {
        ptr.is_null()
    }
}

/// Defines a pointer manipulation API for data owned by some other object.
///
/// Implement this trait for the types that do not need to be freed (directly) by the user.
/// The lifetime of the data is bound to some other object owning it.
pub trait RefFFI: Sized + origin_sealed::FromRefSealed {
    /// Creates a borrowed pointer from a valid reference.
    #[allow(clippy::needless_lifetimes)]
    fn as_ptr<'a>(&'a self) -> BridgedPtr<'a, Self, Shared> {
        // SAFETY:
        // 1. validity guarantee - pointer is valid, since it's obtained a valid reference.
        // 2. pointer's lifetime - pointer inherits the lifetime of provided reference's borrow.
        // 3. ownerhsip - we always create a `Shared` pointer.
        unsafe { BridgedPtr::from_raw(self) }
    }

    /// Creates a borrowed pointer from a weak reference.
    ///
    /// ## SAFETY
    /// User needs to ensure that the pointee is not freed when pointer is being
    /// dereferenced.
    ///
    /// ## Why this method is unsafe? - Example
    /// ```
    /// # use csharp_wrapper::ffi::{BridgedBorrowedSharedPtr, FFI, FromRef, RefFFI};
    /// # use std::sync::{Arc, Weak};
    ///
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromRef;
    /// }
    ///
    /// let arc = Arc::new(Foo);
    /// let weak = Arc::downgrade(&arc);
    /// let ptr: BridgedBorrowedSharedPtr<Foo> = unsafe { RefFFI::weak_as_ptr(&weak) };
    /// std::mem::drop(arc);
    ///
    /// // The ptr is now dangling. The user can "safely" dereference it using RefFFI API.
    ///
    /// ```
    #[allow(clippy::needless_lifetimes)]
    unsafe fn weak_as_ptr<'a>(w: &'a Weak<Self>) -> BridgedPtr<'a, Self, Shared> {
        match w.upgrade() {
            Some(a) => {
                #[allow(clippy::disallowed_methods)]
                let ptr = Arc::as_ptr(&a);
                unsafe { BridgedPtr::from_raw(ptr) }
            }
            None => BridgedPtr::null(),
        }
    }

    /// Converts a borrowed pointer to reference.
    /// The reference inherits the lifetime of pointer's borrow.
    #[allow(clippy::needless_lifetimes)]
    fn as_ref<'a>(ptr: BridgedPtr<'a, Self, Shared>) -> Option<&'a Self> {
        ptr.into_ref()
    }

    fn null<'a>() -> BridgedPtr<'a, Self, Shared> {
        BridgedPtr::null()
    }

    fn is_null(ptr: &BridgedPtr<'_, Self, Shared>) -> bool {
        ptr.is_null()
    }
}

/// This trait should be implemented for types that are passed between
/// C and Rust API. We currently distinguish 3 kinds of implementors,
/// wrt. the origin of the pointer. The implementor should pick one of the 3 ownership
/// kinds as the associated type:
/// - [`FromBox`]
/// - [`FromArc`]
/// - [`FromRef`]
#[allow(clippy::upper_case_acronyms)]
pub trait FFI {
    type Origin;
}

/// Represents types with an exclusive ownership.
///
/// Use this associated type for implementors that require:
/// - owned exclusive pointer manipulation via [`BoxFFI`]
/// - exclusive ownership of the corresponding object
/// - potential mutability of the corresponding object
/// - manual memory freeing
///
/// C API user should be responsible for freeing associated memory manually
/// via corresponding API call.
pub struct FromBox;
impl<T> origin_sealed::FromBoxSealed for T where T: FFI<Origin = FromBox> {}
impl<T> BoxFFI for T where T: FFI<Origin = FromBox> {}

/// Represents types with a shared ownership.
///
/// Use this associated type for implementors that require:
/// - pointer with shared ownership manipulation via [`ArcFFI`]
/// - shared ownership of the corresponding object
/// - manual memory freeing
///
/// C API user should be responsible for freeing (decreasing reference count of)
/// associated memory manually via corresponding API call.
pub struct FromArc;
impl<T> origin_sealed::FromArcSealed for T where T: FFI<Origin = FromArc> {}
impl<T> ArcFFI for T where T: FFI<Origin = FromArc> {}

/// Represents borrowed types.
///
/// Use this associated type for implementors that do not require any assumptions
/// about the pointer type (apart from validity).
/// The implementation will enable [`BridgedBorrowedPtr`] manipulation via [`RefFFI`]
///
/// C API user is not responsible for freeing associated memory manually. The memory
/// should be freed automatically, when the owner is being dropped.
pub struct FromRef;
impl<T> origin_sealed::FromRefSealed for T where T: FFI<Origin = FromRef> {}
impl<T> RefFFI for T where T: FFI<Origin = FromRef> {}

mod tests {
    /// ```compile_fail,E0499
    /// # use csharp_wrapper::ffi::{BridgedOwnedExclusivePtr, BridgedBorrowedExclusivePtr, FFI, BoxFFI, FromBox};
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromBox;
    /// }
    ///
    /// let mut ptr: BridgedOwnedExclusivePtr<Foo> = BoxFFI::into_ptr(Box::new(Foo));
    /// let borrowed_mut_ptr1: BridgedBorrowedExclusivePtr<Foo> = ptr.borrow_mut();
    /// let borrowed_mut_ptr2: BridgedBorrowedExclusivePtr<Foo> = ptr.borrow_mut();
    /// let mutref1 = BoxFFI::as_mut_ref(borrowed_mut_ptr2);
    /// let mutref2 = BoxFFI::as_mut_ref(borrowed_mut_ptr1);
    /// ```
    fn _test_box_ffi_cannot_have_two_mutable_references() {}

    /// ```compile_fail,E0502
    /// # use csharp_wrapper::ffi::{BridgedOwnedExclusivePtr, BridgedBorrowedSharedPtr, BridgedBorrowedExclusivePtr, FFI, BoxFFI, FromBox};
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromBox;
    /// }
    ///
    /// let mut ptr: BridgedOwnedExclusivePtr<Fo> = BoxFFI::into_ptr(Box::new(Foo));
    /// let borrowed_mut_ptr: BridgedBorrowedExclusivePtr<Foo> = ptr.borrow_mut();
    /// let borrowed_ptr: BridgedBorrowedSharedPtr<Foo> = ptr.borrow();
    /// let immref = BoxFFI::as_ref(borrowed_ptr);
    /// let mutref = BoxFFI::as_mut_ref(borrowed_mut_ptr);
    /// ```
    fn _test_box_ffi_cannot_have_mutable_and_immutable_references_at_the_same_time() {}

    /// ```compile_fail,E0505
    /// # use csharp_wrapper::ffi::{BridgedOwnedExclusivePtr, BridgedBorrowedSharedPtr, FFI, BoxFFI, FromBox};
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromBox;
    /// }
    ///
    /// let ptr: BridgedOwnedExclusivePtr<Foo> = BoxFFI::into_ptr(Box::new(Foo));
    /// let borrowed_ptr: BridgedBorrowedSharedPtr<Foo> = ptr.borrow();
    /// BoxFFI::free(ptr);
    /// let immref = BoxFFI::as_ref(borrowed_ptr);
    /// ```
    fn _test_box_ffi_cannot_free_while_having_borrowed_pointer() {}

    /// ```compile_fail,E0505
    /// # use csharp_wrapper::ffi::{BridgedOwnedSharedPtr, BridgedBorrowedSharedPtr, FFI, ArcFFI, FromArc};
    /// # use std::sync::Arc;
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromArc;
    /// }
    ///
    /// let ptr: BridgedOwnedSharedPtr<Foo> = ArcFFI::into_ptr(Arc::new(Foo));
    /// let borrowed_ptr: BridgedBorrowedSharedPtr<Foo> = ptr.borrow();
    /// ArcFFI::free(ptr);
    /// let immref = ArcFFI::cloned_from_ptr(borrowed_ptr);
    /// ```
    fn _test_arc_ffi_cannot_clone_after_free() {}

    /// ```compile_fail,E0505
    /// # use csharp_wrapper::ffi::{BridgedBorrowedSharedPtr, FFI, ArcFFI, FromArc};
    /// # use std::sync::Arc;
    /// struct Foo;
    /// impl FFI for Foo {
    ///     type Origin = FromArc;
    /// }
    ///
    /// let arc = Arc::new(Foo);
    /// let borrowed_ptr: BridgedBorrowedSharedPtr<Foo> = ArcFFI::as_ptr(&arc);
    /// std::mem::drop(arc);
    /// let immref = ArcFFI::cloned_from_ptr(borrowed_ptr);
    /// ```
    fn _test_arc_ffi_cannot_dereference_borrowed_after_drop() {}
}

/*
 * Compound FFI types with length - byte slices and strings.
 */

/// Represents a byte slice passed over FFI from Rust to C#.
/// SAFETY: `ptr` must be a valid pointer to a byte array of length `len`.
#[repr(C)]
pub struct FFIByteSlice<'a> {
    ptr: BridgedBorrowedSharedPtr<'a, u8>,
    len: usize,
}

impl<'a> FFIByteSlice<'a> {
    pub(crate) fn new(slice: impl AsRef<[u8]>) -> Self {
        let s = slice.as_ref();
        let ptr = unsafe {
            // SAFETY: slice.as_ptr() returns a valid reference to a byte slice.
            // Lifetime is inherited from the slice reference, so it's safe to create
            // a borrowed pointer with the same lifetime.
            BridgedBorrowedSharedPtr::from_raw(s.as_ptr())
        };
        FFIByteSlice { ptr, len: s.len() }
    }
}

/// Represents a string passed over FFI from Rust to C#.
/// SAFETY: `slice` must be a valid pointer a UTF-8 encoded string with correctly set length.
#[repr(transparent)]
pub struct FFIStr<'a> {
    slice: FFIByteSlice<'a>,
}

impl<'a> FFIStr<'a> {
    pub(crate) fn new(s: impl AsRef<str>) -> Self {
        Self {
            slice: FFIByteSlice::new(s.as_ref().as_bytes()),
        }
    }

    pub(crate) fn null() -> Self {
        Self {
            slice: FFIByteSlice {
                ptr: BridgedBorrowedSharedPtr::null(),
                len: 0,
            },
        }
    }
}
