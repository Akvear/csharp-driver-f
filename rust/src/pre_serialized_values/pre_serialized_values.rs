use super::csharp_memory::CsharpSerializedValue;
use crate::ffi::{FFI, FromBox};
use scylla_cql::frame::response::result::{ColumnType, NativeType};
use scylla_cql::serialize::SerializationError;
use scylla_cql::serialize::row::SerializedValues;
use scylla_cql::serialize::value::SerializeValue;
use scylla_cql::serialize::writers::CellWriter;

/// A single pre-serialized cell: either a C#-backed value, or a
/// logical null/unset marker.
pub enum PreSerializedCell {
    Value(CsharpSerializedValue),
    Null,
    Unset,
}

/// Thin wrapper describing a pre-serialized C# value by its pointer and length.
/// The C# side is responsible for ensuring the pointer stays valid during serialization.
impl SerializeValue for PreSerializedCell {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<scylla_cql::serialize::writers::WrittenCellProof<'b>, SerializationError> {
        match self {
            PreSerializedCell::Value(val) => {
                let slice = unsafe { val.as_slice() };
                writer.set_value(slice).map_err(SerializationError::new)
            }
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

    /// Add a pre-serialized value described by a `CsharpSerializedValue`.
    ///
    /// Safety:
    /// - The C# buffer pointed to by `value.ptr` must remain valid and pinned for the duration
    ///   of this call. The data is copied into the internal buffer immediately.
    /// - If `value.len > 0`, then `value.ptr` must be non-null and point to at least `len`
    ///   bytes of initialized memory.
    pub(super) unsafe fn add_value(
        &mut self,
        value: CsharpSerializedValue,
    ) -> Result<(), SerializationError> {
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
