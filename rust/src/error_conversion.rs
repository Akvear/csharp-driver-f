use crate::ffi::{FFIByteSlice, FFIStr};
use scylla::errors::{
    BadKeyspaceName, ConnectionError, ConnectionPoolError, DbError, DeserializationError,
    MetadataError, NewSessionError, NextPageError, NextRowError, PagerExecutionError, PrepareError,
    RequestAttemptError, RequestError, SerializationError, TypeCheckError, UseKeyspaceError,
};
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::Arc;
use thiserror::Error;

use crate::task::ExceptionConstructors;

// Opaque type representing a C# Exception.
#[derive(Clone, Copy)]
enum Exception {}

/// A pointer to a C# Exception.
/// This is used across the FFI boundary to represent exceptions created on the C# side.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ExceptionPtr(NonNull<Exception>);

/// Wrapper struct for returning exceptions over FFI.
///
/// The pointer inside this package references a GCHandle allocated on the C# side.
/// Rust must treat this as an opaque handle and must not attempt to free it.
/// At the managed boundary, C# must either throw (which frees) or explicitly free the handle.
/// All changes to this struct must be mirrored in C# code in the exact same order.
#[repr(transparent)]
pub struct FFIException {
    pub exception: Option<ExceptionPtr>,
}

// Compile-time assertion that `FFIException` is pointer-sized.
// Ensures ABI compatibility with C# (opaque GCHandle/IntPtr across FFI).
const _: [(); size_of::<FFIException>()] = [(); size_of::<*const ()>()];

impl FFIException {
    pub(crate) fn ok() -> Self {
        Self { exception: None }
    }

    pub(crate) fn from_exception(exception: ExceptionPtr) -> Self {
        Self {
            exception: Some(exception),
        }
    }

    pub(crate) fn from_error<E>(error: E, constructors: &ExceptionConstructors) -> Self
    where
        E: ErrorToException,
    {
        let exception_ptr = error.to_exception(constructors);
        Self::from_exception(exception_ptr)
    }

    pub(crate) fn has_exception(&self) -> bool {
        self.exception.is_some()
    }
}

#[repr(transparent)]
pub struct RustExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl RustExceptionConstructor {
    /// Creates a generic C# exception for unexpected Rust errors.
    ///
    /// Prefixes the message with "Rust exception:" and forwards it
    /// across the FFI boundary to construct the managed exception.
    pub(crate) fn construct_from_rust(&self, err: impl Display) -> ExceptionPtr {
        let message = format!("Rust exception: {}", err);
        let ffi_message = FFIStr::new(&message);
        unsafe { (self.0)(ffi_message) }
    }
}

// FFI constructor for certain specific C# exception types that we want to map to directly from Rust errors.
// These constructors allow us to preserve the specific exception type and message on the C# side for
// well-known error conditions, rather than converting them all to a generic RustException.

/// FFI constructor for C# `FunctionFailureException`.
#[repr(transparent)]
pub struct FunctionFailureExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl FunctionFailureExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `InvalidConfigurationInQueryException`.
#[repr(transparent)]
pub struct InvalidConfigurationInQueryExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr) -> ExceptionPtr,
);

impl InvalidConfigurationInQueryExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `NoHostAvailableException`.
#[repr(transparent)]
pub struct NoHostAvailableExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr) -> ExceptionPtr,
);

impl NoHostAvailableExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `OperationTimedOutException`.
#[repr(transparent)]
pub struct OperationTimedOutExceptionConstructor(
    unsafe extern "C" fn(timeout_ms: i32) -> ExceptionPtr,
);

impl OperationTimedOutExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, timeout_ms: i32) -> ExceptionPtr {
        unsafe { (self.0)(timeout_ms) }
    }
}

/// FFI constructor for C# `PreparedQueryNotFoundException`.
#[repr(transparent)]
pub struct PreparedQueryNotFoundExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>, unknown_id: FFIByteSlice<'_>) -> ExceptionPtr,
);

impl PreparedQueryNotFoundExceptionConstructor {
    /// Builds a `PreparedQueryNotFoundException` with message and statement id.
    ///
    /// `unknown_id` is the raw statement id bytes associated with the error.
    pub(crate) fn construct_from_rust(&self, message: &str, unknown_id: &[u8]) -> ExceptionPtr {
        let message = FFIStr::new(message);
        let unknown_id = FFIByteSlice::new(unknown_id);
        unsafe { (self.0)(message, unknown_id) }
    }
}

// TODO: Use this constructor for a specific error type.
/// FFI constructor for C# `RequestInvalidException` (currently unused).
#[repr(transparent)]
pub struct RequestInvalidExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl RequestInvalidExceptionConstructor {
    #[expect(dead_code)] // Currently unused
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `AlreadyShutdownException`.
#[repr(transparent)]
pub struct AlreadyShutdownExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl AlreadyShutdownExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `SyntaxErrorException`.
#[repr(transparent)]
pub struct SyntaxErrorExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl SyntaxErrorExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

// TODO: Use this constructor for a specific error type.
/// FFI constructor for C# `TraceRetrievalException` (currently unused).
#[repr(transparent)]
pub struct TraceRetrievalExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl TraceRetrievalExceptionConstructor {
    #[expect(dead_code)] // Currently unused
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `TruncateException`.
#[repr(transparent)]
pub struct TruncateExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl TruncateExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `UnauthorizedException`.
#[repr(transparent)]
pub struct UnauthorizedExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl UnauthorizedExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `AlreadyExistsException`.
#[repr(transparent)]
pub struct AlreadyExistsConstructor(
    unsafe extern "C" fn(keyspace: FFIStr<'_>, table: FFIStr<'_>) -> ExceptionPtr,
);

impl AlreadyExistsConstructor {
    /// Builds an `AlreadyExistsException` from keyspace and table names.
    pub(crate) fn construct_from_rust(&self, keyspace: &str, table: &str) -> ExceptionPtr {
        let ks = FFIStr::new(keyspace);
        let tb = FFIStr::new(table);
        unsafe { (self.0)(ks, tb) }
    }
}

/// FFI constructor for C# `InvalidQueryException`.
#[repr(transparent)]
pub struct InvalidQueryConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl InvalidQueryConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `InvalidTypeException`.
pub struct InvalidTypeExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl InvalidTypeExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `SerializationException`.
pub struct SerializationExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl SerializationExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `DeserializationException`.
pub struct DeserializationExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl DeserializationExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

// Special errors for C# wrapper.

/// Wrapper enum to represent errors that may occur normally or indicate that the session has been
/// shut down. It allows to return a clear error condition while satisfying the return type requirements.
#[derive(Error, Debug, Clone)]
pub(crate) enum MaybeShutdownError<E> {
    #[error("Error: {0}")]
    Inner(E),

    #[error("Session has been shut down and can no longer execute operations")]
    AlreadyShutdown,
}

/// Trait for converting Rust error types into pointers to C# exceptions using constructors from the TCB.
///
/// # Purpose
/// This trait should be implemented for any Rust error type that needs to be communicated to C# code
/// via the FFI boundary. It provides a method to convert the error into an opaque pointer to a C# Exception,
/// using the provided set of exception constructors.
///
/// # When to implement
/// Implement this trait for error types that may be returned from Rust code and need to be represented
/// as exceptions in C#.
///
/// # Safety
/// The returned [`ExceptionPtr`] is an opaque handle pointer to a C# Exception object. Implementors must ensure
/// that any pointers passed to the constructors are valid for the duration of the call.
/// The handle must be freed on the C# side when no longer needed.
pub trait ErrorToException {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr;
}

// This allows returning Infallible as an error type in functions that cannot fail.
impl ErrorToException for std::convert::Infallible {
    fn to_exception(&self, _ctors: &ExceptionConstructors) -> ExceptionPtr {
        match *self {}
    }
}

// This allows returning ExceptionPtr directly as an error type in functions that already produce C# exceptions.
impl ErrorToException for ExceptionPtr {
    fn to_exception(&self, _ctors: &ExceptionConstructors) -> ExceptionPtr {
        *self
    }
}

// Specific mapping for std::io::Error.
// For now it only maps specific connection-related error kinds (ConnectionRefused, TimedOut, NotConnected) to a C# NoHostAvailableException.
// This mapping doesn't deny wildcard matches to ensure all other IO errors are still converted to Rust exceptions without needing to list them all.
impl ErrorToException for Arc<std::io::Error> {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self.kind() {
            std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::NotConnected => ctors
                .no_host_available_exception_constructor
                .construct_from_rust(self.to_string().as_str()),

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for PagerExecutionError.
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for PagerExecutionError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            PagerExecutionError::PrepareError(e) => e.to_exception(ctors),

            PagerExecutionError::SerializationError(e) => e.to_exception(ctors),

            PagerExecutionError::NextPageError(e) => e.to_exception(ctors),

            PagerExecutionError::UseKeyspaceError(e) => e.to_exception(ctors),

            PagerExecutionError::SchemaAgreementError(_) => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for NextPageError.
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for NextPageError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            NextPageError::RequestFailure(e) => e.to_exception(ctors),

            NextPageError::TypeCheckError(e) => e.to_exception(ctors),

            NextPageError::PartitionKeyError(_) | NextPageError::ResultMetadataParseError(_) => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for RequestError.
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for RequestError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            RequestError::ConnectionPoolError(e) => e.to_exception(ctors),

            RequestError::RequestTimeout(duration) => ctors
                .operation_timed_out_exception_constructor
                .construct_from_rust(duration.as_millis().clamp(0, i32::MAX as u128) as i32),

            RequestError::LastAttemptError(e) => e.to_exception(ctors),

            RequestError::EmptyPlan => ctors.rust_exception_constructor.construct_from_rust(self),

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for RequestAttemptError.
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for RequestAttemptError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            RequestAttemptError::SerializationError(e) => e.to_exception(ctors),

            RequestAttemptError::DbError(db_error, message) => {
                (db_error, message.as_str()).to_exception(ctors)
            }

            RequestAttemptError::CqlRequestSerialization(_)
            | RequestAttemptError::UnableToAllocStreamId
            | RequestAttemptError::BrokenConnectionError(_)
            | RequestAttemptError::BodyExtensionsParseError(_)
            | RequestAttemptError::CqlResultParseError(_)
            | RequestAttemptError::CqlErrorParseError(_)
            | RequestAttemptError::UnexpectedResponse(_)
            | RequestAttemptError::RepreparedIdChanged { .. }
            | RequestAttemptError::RepreparedIdMissingInBatch
            | RequestAttemptError::NonfinishedPagingState => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for PrepareError
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for PrepareError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            PrepareError::ConnectionPoolError(e) => e.to_exception(ctors),

            PrepareError::AllAttemptsFailed { first_attempt } => first_attempt.to_exception(ctors),

            PrepareError::PreparedStatementIdsMismatch => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for NewSessionError
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for NewSessionError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            NewSessionError::MetadataError(e) => e.to_exception(ctors),

            NewSessionError::UseKeyspaceError(e) => e.to_exception(ctors),

            NewSessionError::FailedToResolveAnyHostname(_)
            | NewSessionError::EmptyKnownNodesList => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for MetadataError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            MetadataError::ConnectionPoolError(e) => e.to_exception(ctors),

            MetadataError::FetchError(_)
            | MetadataError::Peers(_)
            | MetadataError::Keyspaces(_)
            | MetadataError::Udts(_)
            | MetadataError::Tables(_) => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for UseKeyspaceError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            UseKeyspaceError::BadKeyspaceName(e) => e.to_exception(ctors),

            UseKeyspaceError::RequestError(e) => e.to_exception(ctors),

            UseKeyspaceError::KeyspaceNameMismatch { .. } => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            UseKeyspaceError::RequestTimeout(duration) => ctors
                .operation_timed_out_exception_constructor
                .construct_from_rust(duration.as_millis().clamp(0, i32::MAX as u128) as i32),

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for BadKeyspaceName {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            // Rust considers an empty keyspace name invalid, but the C# wrapper should never allow this to be passed in.
            // (At least for the case of setting a keyspace on Connect().)
            BadKeyspaceName::Empty => ctors
                .invalid_query_constructor
                .construct_from_rust(self.to_string().as_str()),

            BadKeyspaceName::TooLong(..) => ctors
                .invalid_query_constructor
                .construct_from_rust(self.to_string().as_str()),

            BadKeyspaceName::IllegalCharacter(..) => ctors
                .invalid_query_constructor
                .construct_from_rust(self.to_string().as_str()),

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Tuple-based mapping to include the server-provided message alongside DbError
#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for (&DbError, &str) {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        let (db_error, message) = self;
        match db_error {
            DbError::AlreadyExists { keyspace, table } => ctors
                .already_exists_constructor
                .construct_from_rust(keyspace, table),

            DbError::Invalid => ctors.invalid_query_constructor.construct_from_rust(message),

            DbError::SyntaxError => ctors
                .syntax_error_exception_constructor
                .construct_from_rust(message),

            DbError::Unauthorized => ctors
                .unauthorized_exception_constructor
                .construct_from_rust(message),

            DbError::FunctionFailure { .. } => ctors
                .function_failure_exception_constructor
                .construct_from_rust(message),

            DbError::TruncateError => ctors
                .truncate_exception_constructor
                .construct_from_rust(message),

            DbError::Unprepared { statement_id } => ctors
                .prepared_query_not_found_exception_constructor
                .construct_from_rust(message, statement_id),

            DbError::ConfigError => ctors
                .invalid_configuration_in_query_constructor
                .construct_from_rust(message),

            DbError::AuthenticationError
            | DbError::Unavailable { .. }
            | DbError::Overloaded
            | DbError::IsBootstrapping
            | DbError::ReadTimeout { .. }
            | DbError::WriteTimeout { .. }
            | DbError::ReadFailure { .. }
            | DbError::WriteFailure { .. }
            | DbError::ServerError
            | DbError::ProtocolError
            | DbError::RateLimitReached { .. }
            | DbError::Other(_) => ctors
                .rust_exception_constructor
                .construct_from_rust(db_error),

            _ => ctors
                .rust_exception_constructor
                .construct_from_rust(db_error),
        }
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for ConnectionPoolError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            ConnectionPoolError::Broken {
                last_connection_error: ConnectionError::IoError(io_err),
            } => io_err.to_exception(ctors),

            ConnectionPoolError::Broken {
                last_connection_error: ConnectionError::ConnectTimeout,
            } => ctors
                .no_host_available_exception_constructor
                .construct_from_rust(self.to_string().as_str()),

            ConnectionPoolError::Broken { .. }
            | ConnectionPoolError::NodeDisabledByHostFilter
            | ConnectionPoolError::Initializing => {
                ctors.rust_exception_constructor.construct_from_rust(self)
            }

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
impl ErrorToException for NextRowError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            NextRowError::NextPageError(e) => e.to_exception(ctors),

            NextRowError::RowDeserializationError(e) => e.to_exception(ctors),

            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

impl ErrorToException for DeserializationError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors
            .deserialization_exception_constructor
            .construct_from_rust(&self.to_string())
    }
}

impl ErrorToException for SerializationError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors
            .serialization_exception_constructor
            .construct_from_rust(&self.to_string())
    }
}

impl ErrorToException for TypeCheckError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors
            .invalid_type_exception_constructor
            .construct_from_rust(&self.to_string())
    }
}

impl<E> ErrorToException for MaybeShutdownError<E>
where
    E: ErrorToException,
{
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            MaybeShutdownError::Inner(e) => e.to_exception(ctors),
            MaybeShutdownError::AlreadyShutdown => ctors
                .already_shutdown_exception_constructor
                .construct_from_rust(
                    "Session has been shut down and can no longer execute operations",
                ),
        }
    }
}
