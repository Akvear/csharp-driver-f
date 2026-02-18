use crate::error_conversion::FFIException;
use crate::ffi::{
    BoxFFI, BridgedBorrowedExclusivePtr, BridgedOwnedExclusivePtr, FFI, FFIByteSlice, FromBox,
};
use crate::task::ExceptionConstructors;
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::serialize::SerializationError;
use scylla_cql::serialize::row::SerializedValues;
use scylla_cql::serialize::value::SerializeValue;
use scylla_cql::serialize::writers::CellWriter;

/// A single pre-serialized cell: either a C#-backed value, or a
/// logical null/unset marker.
pub enum PreSerializedCell<'a> {
    Value(FFIByteSlice<'a>),
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
pub struct PreSerializedValues {
    serialized_values: SerializedValues,
}

impl PreSerializedValues {
    pub fn new() -> Self {
        Self {
            serialized_values: SerializedValues::new(),
        }
    }

    /// Consume and return the inner SerializedValues.
    pub fn into_serialized_values(self) -> SerializedValues {
        self.serialized_values
    }

    /// Add a value that was pre-serialized by C#.
    ///
    /// Safety:
    /// - The C# buffer pointed to by `value` must remain valid and pinned for the duration
    ///   of this call. The data is copied into the internal buffer immediately.
    pub(super) fn add_value(&mut self, value: FFIByteSlice<'_>) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Value(value);
        self.serialized_values.add_value(&cell, dummy_column_type())
    }

    pub(super) fn add_null(&mut self) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Null;
        self.serialized_values.add_value(&cell, dummy_column_type())
    }

    pub(super) fn add_unset(&mut self) -> Result<(), SerializationError> {
        let cell = PreSerializedCell::Unset;
        self.serialized_values.add_value(&cell, dummy_column_type())
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

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_new() -> BridgedOwnedExclusivePtr<PreSerializedValues> {
    BoxFFI::into_ptr(Box::new(PreSerializedValues::new()))
}

/// Adds a pre-serialized value from a C#-owned buffer to the builder.
///
/// # Safety
/// `value_ptr` and the data it points to must remain valid for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pre_serialized_values_add_value(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    value: FFIByteSlice<'_>,
    constructors: &'static ExceptionConstructors,
) -> FFIException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_value");
    };
    match values.add_value(value) {
        Ok(()) => FFIException::ok(),
        Err(e) => FFIException::from_error(e, constructors),
    }
}

/// Adds a null cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FFIException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_null");
    };
    match values.add_null() {
        Ok(()) => FFIException::ok(),
        Err(e) => FFIException::from_error(e, constructors),
    }
}

/// Adds an unset cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FFIException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_unset");
    };
    match values.add_unset() {
        Ok(()) => FFIException::ok(),
        Err(e) => FFIException::from_error(e, constructors),
    }
}

/// Frees the PreSerializedValues if it was not consumed by a query.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_free(
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Simply drop the Box<PreSerializedValues>
    let _ = BoxFFI::from_ptr(values_ptr);
}
