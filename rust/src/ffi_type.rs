//! Machine-readable description of the layout Rust chose for the types that cross the FFI boundary.
//!
//! ## Why this exists
//!
//! Every non-opaque `#[repr(C)]` type in this crate has a hand-written C# twin. The only thing
//! keeping the two in sync is a doc comment saying "mirror all changes in the exact same order",
//! and a desync is silent memory corruption at run time rather than a compile error.
//!
//! So each such type derives [`FFIType`], which reports **where the compiler actually placed
//! things** via `size_of`/`align_of`/`offset_of!`. The `abi` module exports that description, and
//! the managed test suite (`FfiAbiTests`) compares it against what `Marshal`/`Unsafe` report for
//! the C# mirror.
//!
//! ## Why *leaves*, and not fields
//!
//! A type is described as a flat list of primitive [`AbiLeaf`]s: the recursion descends through
//! nested `#[repr(C)]` structs and `#[repr(transparent)]` newtypes until it reaches something
//! primitive.
//!
//! ## Why leaves carry field names
//!
//! Most of these structs are nothing but pointers - `Tcb`, `FFIGCHandle`, the 23-slot
//! `ExceptionConstructors` table - so offsets and sizes alone cannot tell their fields apart.
//! Swapping `complete_task` and `fail_task` on one side only keeps every offset intact, and Rust
//! would then report success through the failure callback. So each leaf also carries the dotted
//! path of field names leading to it (`tcp.tcp_nodelay`), and the managed side compares those in
//! order, ignoring case and underscores so that each language keeps its own naming convention.
//!
//! A struct in which only one field has any leaves - `FFIStr { slice }`, `BridgedPtr { ptr,
//! _phantom }`, a transparent newtype - is a wrapper, and adds no segment of its own (see
//! [`describe_fields`]). That is what lets `FFIStr` line up with a C# mirror declaring `ptr` and
//! `len` directly.
//!
//! ## What the leaf kind is for, and why it has only two values
//!
//! Offsets and sizes catch almost everything, but they cannot tell `f32` from `i32`: both are four
//! bytes in the same place. That difference is not cosmetic. Calling conventions route integers and
//! floats through *different register files*, so a `#[repr(C)]` struct of two `f32`s is passed in
//! `xmm0` on x86-64 where two `i32`s are passed in `rdi` (`v0` versus `x0` on AArch64). Mirror one
//! as the other and Rust writes a register the managed side never reads - garbage, and no crash to
//! point at it. This is live for us because these structs cross as arguments *passed by value*.
//!
//! So [`AbiKind`] answers exactly one question: does this field travel in a general-purpose
//! register or a floating-point one? [`AbiKind::Integer`] really means "general-purpose register
//! class", which is why integers, `bool`s, enum discriminants, pointers and function pointers all
//! share it.

use std::marker::PhantomData;
use std::ptr::NonNull;

/// Which register class a primitive leaf belongs to - the one ABI property that an offset and a
/// size cannot express. See the module docs for why the distinction stops here.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiKind {
    /// General-purpose register class: integers, `bool`, enum discriminants, pointers and function
    /// pointers.
    Integer = 0,
    /// Floating-point register class: `f32` / `f64`.
    Float = 1,
}

/// One primitive field, at the offset the compiler placed it, relative to the outermost type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiLeaf {
    /// Dotted path of the field names leading to this leaf, relative to the outermost type. Empty
    /// until [`describe_fields`] names it after the field it belongs to.
    pub name: String,
    pub offset: usize,
    pub size: usize,
    pub kind: AbiKind,
}

impl AbiLeaf {
    /// A leaf that does not know yet which field it belongs to - what every primitive reports.
    pub fn unnamed(offset: usize, size: usize, kind: AbiKind) -> Self {
        Self {
            name: String::new(),
            offset,
            size,
            kind,
        }
    }
}

/// One variant of a fieldless integer-repr enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbiVariant {
    pub name: &'static str,
    pub value: i64,
}

/// The full description of one type: its own size and alignment, plus its flattened leaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiTypeLayout {
    pub size: usize,
    pub align: usize,
    pub leaves: Vec<AbiLeaf>,
    /// Non-empty only for enums.
    pub variants: Vec<AbiVariant>,
}

/// A type whose layout is defined and can be described to the managed side.
///
/// Derive it with `#[derive(FFIType)]` rather than implementing it by hand - the derive reads the
/// real field list, so it cannot fall out of step with the struct. The only manual impls are the
/// primitive ones below.
pub trait FFIType: Sized {
    /// Appends this type's primitive leaves to `out`, with offsets relative to `base`.
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>);

    /// Appends this type's enum variants to `out`. Only enums have any.
    fn describe_variants(_out: &mut Vec<AbiVariant>) {}
}

/// The name a type is registered under in `crate::abi` - and so the name its C# mirror claims with
/// `[FfiLayout("...")]`.
///
/// `#[derive(FFIType)]` implements this as well, with the type's own name minus its generic
/// parameters (`Tcb<R>` is `"Tcb"`), unless `#[ffi_type(name = "...")]` says otherwise. The
/// primitive impls below have no name: they are only ever described as leaves of something else,
/// never registered on their own.
pub trait FFITypeName: FFIType {
    const NAME: &'static str;
}

/// Marker for single-machine-word types that have a null niche, so that `Option<Self>` is still one
/// word and can be described the same way.
///
/// This is what lets `Option<GCHandlePtr<..>>` and `Option<extern "C" fn(..)>` be described without
/// a bespoke impl each. The reported size always comes from `size_of::<Option<Self>>()`, so if the
/// niche optimisation ever failed to apply the size would change and the managed comparison would
/// catch it rather than silently reporting the wrong thing.
pub trait WordLike: Sized {}

/// Describes `T` as a single opaque machine word.
pub fn describe_as_word<T>(base: usize, out: &mut Vec<AbiLeaf>) {
    out.push(AbiLeaf::unnamed(base, size_of::<T>(), AbiKind::Integer));
}

/// `T`'s leaves at `base`, collected for [`describe_fields`] rather than appended.
pub fn leaves_of<T: FFIType>(base: usize) -> Vec<AbiLeaf> {
    let mut leaves = Vec::new();
    T::describe_leaves(base, &mut leaves);
    leaves
}

/// `T` as a single machine word at `base`, collected for [`describe_fields`].
pub fn word_leaves<T>(base: usize) -> Vec<AbiLeaf> {
    let mut leaves = Vec::new();
    describe_as_word::<T>(base, &mut leaves);
    leaves
}

/// Appends a struct's leaves to `out`, given each field's name and leaves in declaration order.
///
/// Every leaf is prefixed with the name of the field it came from - unless only one field has any
/// leaves at all, in which case the struct is a wrapper and passes its field's leaves through
/// unchanged. `ManagedLayout` applies the identical rule to the C# mirror; see the module docs for
/// why.
pub fn describe_fields(fields: Vec<(&'static str, Vec<AbiLeaf>)>, out: &mut Vec<AbiLeaf>) {
    let is_wrapper = fields
        .iter()
        .filter(|(_, leaves)| !leaves.is_empty())
        .count()
        == 1;
    for (field, leaves) in fields {
        for mut leaf in leaves {
            if !is_wrapper {
                leaf.name = if leaf.name.is_empty() {
                    field.to_owned()
                } else {
                    format!("{field}.{}", leaf.name)
                };
            }
            out.push(leaf);
        }
    }
}

/// Produces the complete description of `T`.
pub fn layout_of<T: FFIType>() -> AbiTypeLayout {
    let mut leaves = Vec::new();
    T::describe_leaves(0, &mut leaves);
    let mut variants = Vec::new();
    T::describe_variants(&mut variants);

    AbiTypeLayout {
        size: size_of::<T>(),
        align: align_of::<T>(),
        leaves,
        variants,
    }
}

macro_rules! impl_scalar {
    ($($ty:ty => $kind:expr),* $(,)?) => {
        $(
            impl FFIType for $ty {
                fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
                    out.push(AbiLeaf::unnamed(base, size_of::<$ty>(), $kind));
                }
            }
        )*
    };
}

impl_scalar! {
    u8 => AbiKind::Integer,
    u16 => AbiKind::Integer,
    u32 => AbiKind::Integer,
    u64 => AbiKind::Integer,
    usize => AbiKind::Integer,
    i8 => AbiKind::Integer,
    i16 => AbiKind::Integer,
    i32 => AbiKind::Integer,
    i64 => AbiKind::Integer,
    isize => AbiKind::Integer,
    // Rust guarantees `bool` is one byte, matching C#'s `byte`.
    bool => AbiKind::Integer,
    f32 => AbiKind::Float,
    f64 => AbiKind::Float,
}

/// `PhantomData` occupies no space, so it contributes no leaf. This is what allows the
/// lifetime-carrying pointer wrappers in [`crate::ffi`] to derive `FFIType`.
impl<T: ?Sized> FFIType for PhantomData<T> {
    fn describe_leaves(_base: usize, _out: &mut Vec<AbiLeaf>) {}
}

impl<T> FFIType for *const T {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

impl<T> FFIType for *mut T {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

impl<T> FFIType for NonNull<T> {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

impl<T> FFIType for &T {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

impl<T> FFIType for &mut T {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

// Raw pointers deliberately get no `WordLike` impl: they have no null niche, so `Option<*const T>`
// is two words and must not be described as one.
impl<T> WordLike for NonNull<T> {}
impl<T> WordLike for &T {}
impl<T> WordLike for &mut T {}

/// `Option` of a niche-carrying word is still a single word.
impl<T: WordLike> FFIType for Option<T> {
    fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
        describe_as_word::<Self>(base, out)
    }
}

macro_rules! impl_fn_ptr {
    ($($arg:ident),*) => {
        impl<Ret, $($arg),*> FFIType for extern "C" fn($($arg),*) -> Ret {
            fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
                describe_as_word::<Self>(base, out)
            }
        }
        impl<Ret, $($arg),*> WordLike for extern "C" fn($($arg),*) -> Ret {}

        impl<Ret, $($arg),*> FFIType for unsafe extern "C" fn($($arg),*) -> Ret {
            fn describe_leaves(base: usize, out: &mut Vec<AbiLeaf>) {
                describe_as_word::<Self>(base, out)
            }
        }
        impl<Ret, $($arg),*> WordLike for unsafe extern "C" fn($($arg),*) -> Ret {}
    };
}

// `extern "C" fn(A)` is sugar for `extern "C" fn(A) -> ()`, so one impl per arity covers both the
// returning and the non-returning spellings.
impl_fn_ptr!();
impl_fn_ptr!(A0);
impl_fn_ptr!(A0, A1);
impl_fn_ptr!(A0, A1, A2);
impl_fn_ptr!(A0, A1, A2, A3);
impl_fn_ptr!(A0, A1, A2, A3, A4);
impl_fn_ptr!(A0, A1, A2, A3, A4, A5);
impl_fn_ptr!(A0, A1, A2, A3, A4, A5, A6);
impl_fn_ptr!(A0, A1, A2, A3, A4, A5, A6, A7);

#[cfg(test)]
mod tests {
    use super::*;
    use ffi_type_derive::FFIType;

    #[repr(C)]
    #[derive(FFIType)]
    struct Padded {
        a: u8,
        b: u32,
        c: u8,
    }

    #[repr(transparent)]
    #[derive(FFIType)]
    struct Wrapper {
        inner: Padded,
    }

    #[repr(C)]
    #[derive(FFIType)]
    struct Nested {
        head: u8,
        body: Padded,
        tail: *const u8,
    }

    /// Two fields, but only one with any leaves: still a wrapper.
    #[repr(C)]
    #[derive(FFIType)]
    struct Tagged {
        ptr: *const u8,
        _phantom: PhantomData<u8>,
    }

    #[repr(C)]
    #[derive(FFIType)]
    struct TwoTagged {
        first: Tagged,
        second: Tagged,
    }

    /// Wraps a higher-ranked function pointer, so it has no `FFIType` impl of its own.
    #[repr(transparent)]
    struct Callback(#[allow(dead_code)] extern "C" fn(&u8));

    #[repr(C)]
    #[derive(FFIType)]
    #[ffi_type(all_words)]
    struct Callbacks {
        on_success: Callback,
        on_failure: Callback,
    }

    #[repr(C)]
    #[derive(FFIType)]
    #[ffi_type(all_words, name = "RenamedCallbacks")]
    struct OriginalCallbacks {
        on_success: Callback,
    }

    #[repr(C)]
    #[derive(FFIType)]
    #[ffi_type(name = "Renamed")]
    struct Original {
        a: u8,
    }

    #[repr(C)]
    #[derive(FFIType)]
    struct Generic<'a, T> {
        ptr: *const T,
        _phantom: PhantomData<&'a T>,
    }

    #[repr(u8)]
    #[derive(FFIType)]
    enum Discriminants {
        Zero = 0,
        Five = 5,
        Six,
    }

    fn integer(name: &str, offset: usize, size: usize) -> AbiLeaf {
        AbiLeaf {
            name: name.to_owned(),
            offset,
            size,
            kind: AbiKind::Integer,
        }
    }

    fn names(layout: &AbiTypeLayout) -> Vec<&str> {
        layout
            .leaves
            .iter()
            .map(|leaf| leaf.name.as_str())
            .collect()
    }

    #[test]
    fn padding_is_reflected_in_leaf_offsets() {
        let layout = layout_of::<Padded>();
        assert_eq!(layout.size, 12);
        assert_eq!(layout.align, 4);
        assert_eq!(
            layout.leaves,
            vec![integer("a", 0, 1), integer("b", 4, 4), integer("c", 8, 1)]
        );
    }

    #[test]
    fn transparent_newtype_is_indistinguishable_from_its_inner_type() {
        // This is what lets `FFIStr` compare equal to C#'s flat `FFIString`.
        assert_eq!(layout_of::<Wrapper>().leaves, layout_of::<Padded>().leaves);
    }

    #[test]
    fn nested_structs_flatten_with_absolute_offsets_and_dotted_names() {
        let layout = layout_of::<Nested>();
        let offsets: Vec<usize> = layout.leaves.iter().map(|leaf| leaf.offset).collect();
        assert_eq!(offsets, vec![0, 4, 8, 12, 16]);
        assert_eq!(
            names(&layout),
            vec!["head", "body.a", "body.b", "body.c", "tail"]
        );
    }

    #[test]
    fn a_field_without_leaves_does_not_stop_a_struct_being_a_wrapper() {
        // `BridgedPtr { ptr, _phantom }` relies on this to describe itself as a bare pointer.
        assert_eq!(names(&layout_of::<Tagged>()), vec![""]);
        assert_eq!(names(&layout_of::<TwoTagged>()), vec!["first", "second"]);
    }

    #[test]
    fn all_words_describes_every_field_as_a_named_word() {
        let word = size_of::<*const ()>();
        assert_eq!(
            layout_of::<Callbacks>().leaves,
            vec![
                integer("on_success", 0, word),
                integer("on_failure", word, word)
            ]
        );
    }

    #[test]
    fn alignment_equals_the_widest_leaf() {
        // The managed side derives alignment as `max(leaf.size)` because `Marshal` cannot report
        // it. That model must hold for every registered type, so assert it here on a type with
        // mixed field widths.
        for layout in [layout_of::<Padded>(), layout_of::<Nested>()] {
            let widest = layout.leaves.iter().map(|leaf| leaf.size).max().unwrap();
            assert_eq!(layout.align, widest);
        }
    }

    #[test]
    fn enum_variants_carry_explicit_and_implicit_discriminants() {
        let layout = layout_of::<Discriminants>();
        assert_eq!(layout.size, 1);
        assert_eq!(layout.leaves, vec![integer("", 0, 1)]);
        assert_eq!(
            layout.variants,
            vec![
                AbiVariant {
                    name: "Zero",
                    value: 0
                },
                AbiVariant {
                    name: "Five",
                    value: 5
                },
                // Implicit discriminant: the `as` cast in the derive gets this right without the
                // macro having to compute it.
                AbiVariant {
                    name: "Six",
                    value: 6
                },
            ]
        );
    }

    #[test]
    fn option_of_a_niche_word_stays_one_word() {
        assert_eq!(
            leaves_of::<Option<NonNull<u8>>>(0),
            vec![integer("", 0, size_of::<*const u8>())]
        );
    }

    #[test]
    fn floats_are_distinguished_from_integers() {
        assert_eq!(leaves_of::<f64>(0)[0].kind, AbiKind::Float);
    }

    #[test]
    fn a_derived_type_is_named_after_itself_without_generic_parameters() {
        assert_eq!(Padded::NAME, "Padded");
        assert_eq!(Discriminants::NAME, "Discriminants");
        assert_eq!(<Generic<'static, u8> as FFITypeName>::NAME, "Generic");
    }

    #[test]
    fn the_name_option_overrides_the_type_name() {
        assert_eq!(Original::NAME, "Renamed");
    }

    #[test]
    fn options_combine_in_one_attribute() {
        assert_eq!(OriginalCallbacks::NAME, "RenamedCallbacks");
        // `all_words` in the same attribute still applied.
        assert_eq!(
            layout_of::<OriginalCallbacks>().leaves,
            vec![integer("", 0, size_of::<*const ()>())]
        );
    }
}
