use crate::error_conversion::{FFIException, FFIMaybeException};
use crate::ffi::{BridgedBorrowedExclusivePtr, FFI, FFIPtr, FFISlice, FromBox};
use crate::task::ExceptionConstructors;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::serialize::SerializationError;
use scylla_cql::serialize::row::SerializedValues;
use scylla_cql::serialize::value::SerializeValue;
use scylla_cql::serialize::writers::CellWriter;

/// A single pre-serialized cell: either a C#-backed value, or a
/// logical null/unset marker.
enum PreSerializedCell<'a> {
    Value(FFISlice<'a, u8>),
    Null,
    Unset,
}

/// Thin wrapper describing a pre-serialized C# value by its pointer and length.
/// The C# side is responsible for ensuring the pointer stays valid during serialization.
impl SerializeValue for PreSerializedCell<'_> {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<scylla_cql::serialize::writers::WrittenCellProof<'b>, SerializationError> {
        match self {
            PreSerializedCell::Value(val) => writer
                .set_value(val.as_slice())
                .map_err(SerializationError::new),
            PreSerializedCell::Null => Ok(writer.set_null()),
            PreSerializedCell::Unset => Ok(writer.set_unset()),
        }
    }
}

/// Holds the final serialized values that can be used with queries.
/// Wraps scylla_cql::SerializedValues.
pub(crate) struct PreSerializedValues {
    serialized_values: SerializedValues,
}

impl PreSerializedValues {
    pub(crate) fn new() -> Self {
        Self {
            serialized_values: SerializedValues::new(),
        }
    }

    /// Consume and return the inner SerializedValues.
    pub(crate) fn into_serialized_values(self) -> SerializedValues {
        self.serialized_values
    }

    /// Add a value that was pre-serialized by C#.
    ///
    /// The C# buffer pointed to by `value` must remain valid and pinned for the
    /// duration of this call. The data is copied into the internal buffer immediately.
    pub(crate) fn add_value(&mut self, value: FFISlice<'_, u8>) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Value(value);
        self.serialized_values.add_value(&cell, dummy_column_type())
    }

    pub(crate) fn add_null(&mut self) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Null;
        self.serialized_values.add_value(&cell, dummy_column_type())
    }

    pub(crate) fn add_unset(&mut self) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Unset;
        self.serialized_values.add_value(&cell, dummy_column_type())
    }

    /// Builds `PreSerializedValues` on the stack by asking C# to populate it.
    ///
    /// Rust creates the PSV, then calls the C# `populate` callback, passing a raw
    /// pointer to the stack-allocated PSV. The C# side iterates its values and calls
    /// back into Rust via [`psv_add_value`] / [`psv_add_null`] / [`psv_add_unset`]
    /// for each value. When the callback returns, the PSV is fully built.
    pub(crate) fn from_populate_callback(
        context: PopulateValuesContext<'_>,
        populate: PopulateValues,
    ) -> Result<Self, FFIException> {
        let mut psv = PreSerializedValues::new();

        // SAFETY: The callback must only use the pointer to
        // call the exported `psv_add_*` functions and must not store it.
        let result = unsafe { populate(context, &mut psv as *mut _) };
        match result.try_into_ffi_exception() {
            None => Ok(psv),
            Some(exception) => Err(exception),
        }
    }
}

impl FFI for PreSerializedValues {
    type Origin = FromBox;
}

// Single dummy ColumnType value.
static DUMMY_COLUMN_TYPE: ColumnType<'static> = ColumnType::Native(NativeType::Blob);

fn dummy_column_type() -> &'static ColumnType<'static> {
    &DUMMY_COLUMN_TYPE
}

///
/// # Safety
/// - `psv` must be a valid pointer to a `PreSerializedValues` (obtained from the
///   populate callback's `psv` argument).
/// - `value` must point to pinned memory that remains valid for this call
///   (Rust copies immediately).
/// - `constructors` must point to a valid `ExceptionConstructors`.
#[unsafe(no_mangle)]
pub extern "C" fn psv_add_value(
    psv: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    value: FFISlice<'_, u8>,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let psv = psv
        .into_mut_ref()
        .expect("valid and non-null PreSerializedValues pointer");
    match psv.add_value(value) {
        Ok(()) => FFIMaybeException::ok(),
        Err(e) => FFIMaybeException::from_error(e, constructors),
    }
}

/// Add a NULL cell to the builder.
///
/// # Safety
/// - `psv` must be a valid pointer to a `PreSerializedValues`.
/// - `constructors` must point to a valid `ExceptionConstructors`.
#[unsafe(no_mangle)]
pub extern "C" fn psv_add_null(
    psv: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let psv = psv
        .into_mut_ref()
        .expect("valid and non-null PreSerializedValues pointer");
    match psv.add_null() {
        Ok(()) => FFIMaybeException::ok(),
        Err(e) => FFIMaybeException::from_error(e, constructors),
    }
}

/// Add an UNSET cell to the builder.
///
/// # Safety
/// - `psv` must be a valid pointer to a `PreSerializedValues`.
/// - `constructors` must point to a valid `ExceptionConstructors`.
#[unsafe(no_mangle)]
pub extern "C" fn psv_add_unset(
    psv: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let psv = psv
        .into_mut_ref()
        .expect("valid and non-null PreSerializedValues pointer");
    match psv.add_unset() {
        Ok(()) => FFIMaybeException::ok(),
        Err(e) => FFIMaybeException::from_error(e, constructors),
    }
}

/// Opaque type for the C# populate-values callback context.
enum CSharpPopulateState {}

/// Opaque context pointer passed from C# to Rust and handed back to the
/// populate callback so C# can locate its managed state.
#[repr(transparent)]
pub(crate) struct PopulateValuesContext<'a>(FFIPtr<'a, CSharpPopulateState>);

/// Callback type: Rust hands C# a mutable PSV pointer, C# populates it
/// by calling `psv_add_value` / `psv_add_null` / `psv_add_unset`, then
/// returns `FFIMaybeException::ok()` on success or an exception on error.
pub(crate) type PopulateValues = unsafe extern "C" fn(
    context: PopulateValuesContext<'_>,
    psv: *mut PreSerializedValues,
) -> FFIMaybeException;
