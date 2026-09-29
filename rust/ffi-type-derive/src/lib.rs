//! Derive macro for the `FFIType` trait of the `csharp_wrapper` crate.
//!
//! `FFIType` describes where the Rust compiler actually placed the primitive "leaves" of a type
//! that crosses the FFI boundary, so that the managed side can be checked against it. See the
//! `csharp_wrapper::ffi_type` module for the trait itself and the rationale.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Attribute, Data, DeriveInput, Error, Fields, Index, LitStr, Result, parse_macro_input};

/// Derives `FFIType`, describing the type as a flat list of primitive leaves.
///
/// Supported shapes:
/// - `#[repr(C)]` / `#[repr(transparent)]` structs: each field's leaves are emitted in turn, at
///   the offset the compiler chose for that field (via `offset_of!`) and named after the field
///   (via `describe_fields`, which leaves single-field wrappers unnamed). Nested structs and
///   transparent newtypes flatten automatically, because the recursion bottoms out at the
///   primitive impls.
/// - `#[repr(u8)]` (or another integer repr) fieldless enums: a single integer leaf, plus the
///   variant name/discriminant pairs so that reordered discriminants can be detected.
///
/// A type without an explicit `repr` is rejected: `repr(Rust)` layout is not guaranteed, so
/// describing it would be describing something the compiler is free to change.
///
/// # Field attribute
///
/// `#[ffi_type(word)]` describes a field as a single machine word (using its `size_of`) instead of
/// recursing into it. It exists for higher-ranked function pointer fields such as
/// `unsafe extern "C" fn(GCHandlePtr<'_, T>)`: a `for<'a>` type cannot be covered by a normal
/// `impl`, and "one machine word" is in any case all that can meaningfully be said about a function
/// pointer - argument lists are not part of what this machinery verifies.
///
/// # Type attributes
///
/// `#[ffi_type(all_words)]` on a struct is `#[ffi_type(word)]` on every one of its fields. It is
/// for tables of callbacks such as `ExceptionConstructors`, whose 23 fields are newtypes over
/// higher-ranked function pointers.
///
/// `#[ffi_type(name = "...")]` overrides the name the type is registered under. The derive also
/// implements `FFITypeName`, and by default that name is the type's own, without its generic
/// parameters (`Tcb<R>` is `"Tcb"`) - which is what the C# mirror claims with `[FfiLayout]`.
///
/// Options may be combined in one attribute: `#[ffi_type(all_words, name = "...")]`.
#[proc_macro_derive(FFIType, attributes(ffi_type))]
pub fn derive_ffi_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Which `repr` the type was annotated with. Only the layout-relevant distinction matters here.
enum Repr {
    /// `#[repr(C)]` or `#[repr(transparent)]` - a struct with a defined field layout.
    Struct,
    /// `#[repr(u8)]` and friends - an enum with a defined discriminant width.
    Integer,
}

fn expand(input: DeriveInput) -> Result<TokenStream2> {
    let repr = parse_repr(&input)?;
    let options = parse_options(&input.attrs, &["all_words", "name"])?;
    let all_words = options.all_words;
    let name = &input.ident;
    let registered_name = match options.name {
        Some(name) => name,
        None => LitStr::new(&name.unraw().to_string(), name.span()),
    };
    // Deliberately *not* adding a `T: FFIType` bound for each type parameter, which is what a
    // stock derive would do. A type parameter here is almost always only ever pointed *to*
    // (`FFISlice<'a, T>` holds a `T` pointer, not a `T`), so it contributes no leaf of its own and
    // requiring it to be an `FFIType` would make the impl unusable.
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let body = match (&input.data, repr) {
        (Data::Struct(data), Repr::Struct) => struct_body(&data.fields, all_words)?,
        (Data::Enum(_), _) if all_words => {
            return Err(Error::new_spanned(
                &input.ident,
                "`#[ffi_type(all_words)]` only applies to structs",
            ));
        }
        (Data::Enum(data), Repr::Integer) => enum_body(name, data)?,
        (Data::Struct(_), Repr::Integer) => {
            return Err(Error::new_spanned(
                &input.ident,
                "a struct needs `#[repr(C)]` or `#[repr(transparent)]`, not an integer repr",
            ));
        }
        (Data::Enum(_), Repr::Struct) => {
            return Err(Error::new_spanned(
                &input.ident,
                "an enum crossing the FFI boundary needs an integer repr, e.g. `#[repr(u8)]`",
            ));
        }
        (Data::Union(_), _) => {
            return Err(Error::new_spanned(
                &input.ident,
                "`FFIType` cannot be derived for unions: overlapping fields have no single layout",
            ));
        }
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics crate::ffi_type::FFIType for #name #ty_generics #where_clause {
            #body
        }

        #[automatically_derived]
        impl #impl_generics crate::ffi_type::FFITypeName for #name #ty_generics #where_clause {
            const NAME: &'static str = #registered_name;
        }
    })
}

/// Emits `describe_leaves` for a struct: collect each field's leaves at the offset the compiler
/// chose, then let `describe_fields` name them.
fn struct_body(fields: &Fields, all_words: bool) -> Result<TokenStream2> {
    let mut describe = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        let ty = &field.ty;
        // Named fields are addressed by identifier, tuple fields by index; `offset_of!` accepts
        // both spellings.
        let (member, field_name) = match &field.ident {
            Some(ident) => (quote!(#ident), ident.unraw().to_string()),
            None => {
                let index = Index::from(index);
                (quote!(#index), index.index.to_string())
            }
        };
        let offset = quote!(base + ::std::mem::offset_of!(Self, #member));

        let is_word = parse_options(&field.attrs, &["word"])?.word;
        let leaves = if all_words || is_word {
            // `word_leaves` is an unbounded free function, which is exactly why this works for
            // higher-ranked function pointer types that no `impl` can cover.
            quote!(crate::ffi_type::word_leaves::<#ty>(#offset))
        } else {
            quote!(crate::ffi_type::leaves_of::<#ty>(#offset))
        };
        describe.push(quote!((#field_name, #leaves)));
    }

    // A fieldless struct would leave both parameters unused, and the crate builds with
    // `-D warnings`.
    if describe.is_empty() {
        return Ok(quote! {
            fn describe_leaves(
                _base: usize,
                _out: &mut ::std::vec::Vec<crate::ffi_type::AbiLeaf>,
            ) {
            }
        });
    }

    Ok(quote! {
        fn describe_leaves(base: usize, out: &mut ::std::vec::Vec<crate::ffi_type::AbiLeaf>) {
            crate::ffi_type::describe_fields(::std::vec![#(#describe),*], out);
        }
    })
}

/// What the `#[ffi_type(...)]` attributes on one item say.
#[derive(Default)]
struct Options {
    /// `word`: describe this field as one machine word.
    word: bool,
    /// `all_words`: describe every field of this struct as one machine word.
    all_words: bool,
    /// `name = "..."`: register this type under a name other than its own.
    name: Option<LitStr>,
}

/// Parses every `#[ffi_type(...)]` attribute in `attrs`, rejecting any option not in `allowed` -
/// which is how a type-level option on a field, or the other way round, is caught.
fn parse_options(attrs: &[Attribute], allowed: &[&str]) -> Result<Options> {
    let mut options = Options::default();
    for attr in attrs {
        if !attr.path().is_ident("ffi_type") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            if !allowed.contains(&key.as_str()) {
                let expected: Vec<String> = allowed.iter().map(|key| format!("`{key}`")).collect();
                return Err(meta.error(format!(
                    "unsupported `ffi_type` option here; expected {}",
                    expected.join(" or ")
                )));
            }
            match key.as_str() {
                "word" => options.word = true,
                "all_words" => options.all_words = true,
                _ => {
                    let name: LitStr = meta.value()?.parse()?;
                    if name.value().is_empty() {
                        return Err(Error::new_spanned(name, "the name must not be empty"));
                    }
                    options.name = Some(name);
                }
            }
            Ok(())
        })?;
    }
    Ok(options)
}

/// Emits `describe_leaves` + `describe_variants` for a fieldless integer-repr enum.
fn enum_body(name: &syn::Ident, data: &syn::DataEnum) -> Result<TokenStream2> {
    let mut variants = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                &variant.ident,
                "`FFIType` only supports fieldless enum variants: a variant carrying data has no \
                 single discriminant to compare against the managed side",
            ));
        }
        let ident = &variant.ident;
        // Reading the discriminant with an `as` cast rather than parsing the literal out of the
        // AST, so that implicit discriminants (and any future `= EXPR` forms) are handled by the
        // compiler instead of by this macro.
        variants.push(quote! {
            out.push(crate::ffi_type::AbiVariant {
                name: ::std::stringify!(#ident),
                value: (#name::#ident) as i64,
            });
        });
    }

    if variants.is_empty() {
        return Err(Error::new_spanned(
            name,
            "`FFIType` cannot be derived for an uninhabited enum: it never crosses the boundary \
             as a value",
        ));
    }

    Ok(quote! {
        fn describe_leaves(base: usize, out: &mut ::std::vec::Vec<crate::ffi_type::AbiLeaf>) {
            out.push(crate::ffi_type::AbiLeaf::unnamed(
                base,
                ::std::mem::size_of::<Self>(),
                crate::ffi_type::AbiKind::Integer,
            ));
        }

        fn describe_variants(out: &mut ::std::vec::Vec<crate::ffi_type::AbiVariant>) {
            #(#variants)*
        }
    })
}

/// Finds the layout-defining `repr` on the type, rejecting types that do not have one.
fn parse_repr(input: &DeriveInput) -> Result<Repr> {
    const INTEGER_REPRS: &[&str] = &[
        "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize",
    ];

    let mut repr = None;
    for token in repr_tokens(&input.attrs) {
        if token == "C" || token == "transparent" {
            repr = Some(Repr::Struct);
        } else if INTEGER_REPRS.contains(&token.as_str()) {
            repr = Some(Repr::Integer);
        }
        // Anything else (`packed`, `align(N)`, ...) modifies the layout but does not define which
        // of the two shapes above we are looking at, so it is ignored here.
    }

    repr.ok_or_else(|| {
        Error::new_spanned(
            &input.ident,
            "`FFIType` requires an explicit layout: add `#[repr(C)]` or `#[repr(transparent)]` \
             (or an integer repr for enums). Without one the field layout is not guaranteed, so \
             there is nothing meaningful to compare against the managed side.",
        )
    })
}

/// Collects the individual words inside every `#[repr(...)]` attribute, e.g. `["C", "packed"]`.
fn repr_tokens(attrs: &[Attribute]) -> Vec<String> {
    let mut tokens = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        let Ok(list) = attr.meta.require_list() else {
            continue;
        };
        // `repr` contents are simple enough (`C`, `transparent`, `u8`, `packed`, `align(16)`) that
        // splitting the token stream's text is sufficient and avoids depending on the exact shape
        // of syn's `Meta` for every possible spelling.
        for part in list.tokens.to_string().split(',') {
            let part = part.trim();
            if !part.is_empty() {
                tokens.push(part.to_owned());
            }
        }
    }
    tokens
}
