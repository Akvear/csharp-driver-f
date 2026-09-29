//! The registry of types whose layout is verified against the managed side, and the exports that
//! hand that registry to the C# test suite.
//!
//! See [`crate::ffi_type`] for how a type's layout is described and why it is described as a flat
//! list of primitive leaves. This module only decides *which* types are checked and how the
//! description travels across the boundary.
//!
//! ## The registry
//!
//! [`REGISTRY`] is the single list of checked types. Registering a type costs one line naming the
//! type - no field names, no sizes, no offsets, and no name either: that comes from the type's
//! [`FFITypeName`], which `#[derive(FFIType)]` sets to the type's own name unless
//! `#[ffi_type(name = "...")]` overrides it. The name exists only so the managed side can say
//! "this C# struct mirrors that Rust type"; it is spelled with an `[FfiLayout("...")]` attribute on
//! the C# struct. Where the two languages disagree on a type's name (C# calls
//! `BoundStatementExecutionOptions` "PreparedStatementExecutionOptions") the Rust spelling is
//! canonical.
//!
//! A generic type is listed with concrete arguments - `Tcb<FFIBool>`, `FFIGCHandle<AbiProbe>` -
//! because describing it needs one real instantiation. Its name leaves them off, so `Tcb<FFIBool>`
//! is registered as `"Tcb"`.
//!
//! Two things are deliberately left out.
//!
//! **Function signatures.** Neither the parameter lists of the exported functions nor the callback
//! typedefs that cross the boundary as individual `extern "C"` arguments (`OnReplicaPair`,
//! `ConstructCSharpHost`, and the rest) are described here, and a `#[ffi_type(word)]` field is
//! reported as one named machine word and nothing more. Signatures are taken on trust - and a wrong
//! one is not guaranteed to fail loudly: a missing trailing argument just makes Rust read whatever
//! the next register or stack slot holds. (`FfiEntryPointTests` checks only that every P/Invoke
//! names an export that exists.) What *is* verified is the layout of the structs carrying
//! function pointers: `ExceptionConstructors` gaining or reordering a slot on one side only, or
//! `StrategyAddRepFactor`'s three callbacks being declared in a different order, would corrupt
//! memory or call the wrong function rather than crash.
//!
//! **Types registered on neither side.** Registration is one line, but omitting it in both places
//! is invisible. A one-sided omission *is* caught - the managed test asserts the pairing is
//! complete in both directions.
//!
//! ## The transport
//!
//! Pull-based and flat: C# asks how many types there are, then asks for each type, then for each of
//! its leaves. No callbacks, no context pointers, and every out-parameter is a blittable
//! `#[repr(C)]` struct, so the managed side needs no `unsafe` code to read it.
//!
//! This module is compiled only under `cfg(test)` or the `integration_testing` feature; it is
//! absent from a shippable build. [`crate::ffi_type`] itself is always compiled, so a plain
//! `cargo build` still type-checks every `#[derive(FFIType)]`.

use std::sync::OnceLock;

use crate::error_conversion::{FFIException, FFIMaybeException};
use crate::ffi::{FFIBool, FFIGCHandle, FFIMaybeGCHandle, FFISlice, FFIStr};
use crate::ffi_type::{AbiTypeLayout, FFITypeName, layout_of};
use crate::metadata::{CSharpHostData, ReplicaPair, StrategyAddRepFactor};
use crate::row_set::SyncNextRowResult;
use crate::session::{BoundStatementExecutionOptions, SimpleStatementExecutionOptions};
use crate::session_config::{BridgedLoadBalancingPolicy, BridgedSessionConfig, BridgedTcpConfig};
use crate::task::{EmptyAsyncResult, ExceptionConstructors, ManuallyDestructible, Tcb};

/// Stand-in for the managed payload type of the generic handle types. Their layout does not depend
/// on it - they are a pointer plus a function pointer regardless - so an uninhabited type is the
/// most honest choice: it cannot accidentally contribute anything.
enum AbiProbe {}

/// One checked type: a name for pairing with the managed side, and a way to describe it.
struct RegisteredType {
    /// `<T as FFITypeName>::NAME` for the registered `T`.
    name: &'static str,
    /// `layout_of::<T>` for the registered `T`, which erases the (possibly generic) type.
    layout: fn() -> AbiTypeLayout,
}

macro_rules! registry {
    ($($ty:ty),* $(,)?) => {
        const REGISTRY: &[RegisteredType] = &[
            $(
                RegisteredType {
                    name: <$ty as FFITypeName>::NAME,
                    layout: layout_of::<$ty>,
                }
            ),*
        ];
    };
}

registry! {
    // Core FFI vocabulary types.
    FFIGCHandle<AbiProbe>,
    FFIMaybeGCHandle<AbiProbe>,
    FFISlice<'static, u8>,
    FFIStr<'static>,
    FFIBool,

    // Exception plumbing.
    FFIException,
    FFIMaybeException,
    ExceptionConstructors,

    // Async plumbing.
    ManuallyDestructible,
    EmptyAsyncResult,
    Tcb<FFIBool>,

    // Statement execution options.
    BoundStatementExecutionOptions,
    SimpleStatementExecutionOptions,

    // Session configuration. These are the padding-sensitive ones: mixed 1-byte and 4-byte fields,
    // and two levels of nesting.
    BridgedTcpConfig,
    BridgedLoadBalancingPolicy<'static>,
    BridgedSessionConfig<'static>,

    // Cluster metadata.
    CSharpHostData<'static>,
    ReplicaPair<'static>,
    StrategyAddRepFactor,

    // Enums.
    SyncNextRowResult,

    // The transport structs below describe themselves, so that the mechanism doing the checking is
    // not itself exempt from it.
    AbiTypeInfo,
    AbiLeafInfo,
    AbiVariantInfo,
}

/// Description of one registered type. Counts come first so the caller knows how many times to call
/// [`ffi_abi_leaf_info`] / [`ffi_abi_variant_info`].
#[repr(C)]
#[derive(ffi_type_derive::FFIType)]
pub struct AbiTypeInfo {
    name: FFIStr<'static>,
    size: usize,
    align: usize,
    leaf_count: usize,
    variant_count: usize,
}

/// Description of one primitive leaf of a registered type.
#[repr(C)]
#[derive(ffi_type_derive::FFIType)]
pub struct AbiLeafInfo {
    /// Dotted field path, as described in [`crate::ffi_type`].
    name: FFIStr<'static>,
    offset: usize,
    size: usize,
    /// An [`AbiKind`] discriminant.
    kind: u8,
}

/// Description of one variant of a registered enum.
#[repr(C)]
#[derive(ffi_type_derive::FFIType)]
pub struct AbiVariantInfo {
    name: FFIStr<'static>,
    value: i64,
}

/// The described registry, computed once. Descriptions involve allocation, and the managed side
/// makes one call per leaf, so this is memoised rather than recomputed per call.
fn manifest() -> &'static [(&'static str, AbiTypeLayout)] {
    static MANIFEST: OnceLock<Vec<(&'static str, AbiTypeLayout)>> = OnceLock::new();

    MANIFEST.get_or_init(|| {
        REGISTRY
            .iter()
            .map(|registered| (registered.name, (registered.layout)()))
            .collect()
    })
}

/// How many types are registered.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_abi_type_count() -> usize {
    manifest().len()
}

/// Describes the registered type at `index`. Returns false if the index is out of range.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_abi_type_info(index: usize, out: &mut AbiTypeInfo) -> FFIBool {
    let Some((name, layout)) = manifest().get(index) else {
        return FFIBool::from(false);
    };

    *out = AbiTypeInfo {
        name: FFIStr::new(name),
        size: layout.size,
        align: layout.align,
        leaf_count: layout.leaves.len(),
        variant_count: layout.variants.len(),
    };
    FFIBool::from(true)
}

/// Describes one leaf of one registered type. Returns false if either index is out of range.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_abi_leaf_info(
    type_index: usize,
    leaf_index: usize,
    out: &mut AbiLeafInfo,
) -> FFIBool {
    let Some(leaf) = manifest()
        .get(type_index)
        .and_then(|(_, layout)| layout.leaves.get(leaf_index))
    else {
        return FFIBool::from(false);
    };

    *out = AbiLeafInfo {
        name: FFIStr::new(&leaf.name),
        offset: leaf.offset,
        size: leaf.size,
        kind: leaf.kind as u8,
    };
    FFIBool::from(true)
}

/// Describes one variant of one registered enum. Returns false if either index is out of range.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_abi_variant_info(
    type_index: usize,
    variant_index: usize,
    out: &mut AbiVariantInfo,
) -> FFIBool {
    let Some(variant) = manifest()
        .get(type_index)
        .and_then(|(_, layout)| layout.variants.get(variant_index))
    else {
        return FFIBool::from(false);
    };

    *out = AbiVariantInfo {
        name: FFIStr::new(variant.name),
        value: variant.value,
    };
    FFIBool::from(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_names_are_unique() {
        // A duplicate name would make the managed side compare one C# struct against the wrong
        // Rust type, or silently drop an entry. Caught here rather than in the export path, which
        // must not panic: the crate is built with `panic = "abort"`.
        let mut names: Vec<&str> = REGISTRY.iter().map(|entry| entry.name).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(count, names.len(), "duplicate name in the ABI registry");
    }

    #[test]
    fn every_registered_type_has_at_least_one_leaf() {
        // A type described by zero leaves would compare equal to anything of the same size, which
        // would be a silently useless entry.
        for (name, layout) in manifest() {
            assert!(
                !layout.leaves.is_empty(),
                "{name} was described by no leaves"
            );
        }
    }

    #[test]
    fn leaves_are_ordered_and_fit_inside_their_type() {
        for (name, layout) in manifest() {
            let mut previous_end = 0;
            for (index, leaf) in layout.leaves.iter().enumerate() {
                assert!(
                    leaf.offset >= previous_end,
                    "{name} leaf {index} at {} overlaps the previous leaf ending at {previous_end}",
                    leaf.offset
                );
                assert!(
                    leaf.offset + leaf.size <= layout.size,
                    "{name} leaf {index} runs past the end of the type ({} > {})",
                    leaf.offset + leaf.size,
                    layout.size
                );
                previous_end = leaf.offset + leaf.size;
            }
        }
    }

    #[test]
    fn alignment_equals_the_widest_leaf() {
        // The managed side cannot ask `Marshal` for alignment, so it derives it as `max(leaf.size)`
        // instead of guessing. That model holds for `#[repr(C)]` aggregates of self-aligned
        // primitives - which is every registered type today. If this ever fails, a type gained an
        // explicit `repr(align(N))` or an over-aligned field, and the managed side needs teaching
        // about it; it must not be "fixed" by relaxing the managed check.
        for (name, layout) in manifest() {
            let widest = layout
                .leaves
                .iter()
                .map(|leaf| leaf.size)
                .max()
                .unwrap_or(1);
            assert_eq!(
                layout.align, widest,
                "{name} has alignment {} but its widest leaf is {widest} bytes",
                layout.align
            );
        }
    }

    #[test]
    fn leaf_names_tell_every_leaf_apart() {
        // The managed side relies on names to catch two same-width fields swapped on one side.
        // That only works if no two leaves of a type share a name - including the empty name a
        // lone leaf gets, which must therefore never appear next to another leaf.
        for (name, layout) in manifest() {
            if layout.leaves.len() == 1 {
                continue;
            }
            let mut leaf_names: Vec<&str> = layout
                .leaves
                .iter()
                .map(|leaf| leaf.name.as_str())
                .collect();
            assert!(
                !leaf_names.contains(&""),
                "{name} has an unnamed leaf next to others: {leaf_names:?}"
            );
            leaf_names.sort_unstable();
            let count = leaf_names.len();
            leaf_names.dedup();
            assert_eq!(count, leaf_names.len(), "{name} has duplicate leaf names");
        }
    }

    #[test]
    fn out_of_range_indices_are_reported_rather_than_panicking() {
        let count = ffi_abi_type_count();
        assert!(count > 0);

        let mut type_info = AbiTypeInfo {
            name: FFIStr::null(),
            size: 0,
            align: 0,
            leaf_count: 0,
            variant_count: 0,
        };
        assert!(!bool::from(ffi_abi_type_info(count, &mut type_info)));
        assert!(bool::from(ffi_abi_type_info(0, &mut type_info)));

        let mut leaf_info = AbiLeafInfo {
            name: FFIStr::null(),
            offset: 0,
            size: 0,
            kind: 0,
        };
        assert!(!bool::from(ffi_abi_leaf_info(count, 0, &mut leaf_info)));
        assert!(!bool::from(ffi_abi_leaf_info(
            0,
            usize::MAX,
            &mut leaf_info
        )));
    }

    #[test]
    fn enum_variants_are_exposed_for_enums_only() {
        let variants: Vec<_> = manifest()
            .iter()
            .filter(|(_, layout)| !layout.variants.is_empty())
            .map(|(name, _)| *name)
            .collect();
        assert_eq!(variants, vec!["SyncNextRowResult"]);

        let layout = layout_of::<SyncNextRowResult>();
        let named: Vec<(&str, i64)> = layout
            .variants
            .iter()
            .map(|variant| (variant.name, variant.value))
            .collect();
        assert_eq!(
            named,
            vec![("GotRow", 0), ("Exhausted", 1), ("NeedAsync", 2)]
        );
    }
}
