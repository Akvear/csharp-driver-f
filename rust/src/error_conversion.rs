use crate::ffi::{FFIByteSlice, FFIStr};
use scylla::errors::{
    ConnectionError, ConnectionPoolError, DbError, DeserializationError, MetadataError,
    NewSessionError, NextPageError, NextRowError, PagerExecutionError, PrepareError,
    RequestAttemptError, RequestError, SerializationError,
};
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::ptr::NonNull;
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
pub struct FfiException {
    pub exception: Option<ExceptionPtr>,
}

// Compile-time assertion that `FfiException` is pointer-sized.
// Ensures ABI compatibility with C# (opaque GCHandle/IntPtr across FFI).
const _: [(); size_of::<FfiException>()] = [(); size_of::<*const ()>()];

impl FfiException {
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
    unsafe extern "C" fn(address: FFIStr<'_>, timeout_ms: i32) -> ExceptionPtr,
);

impl OperationTimedOutExceptionConstructor {
    pub(crate) fn construct_from_rust(&self, address: &str, timeout_ms: i32) -> ExceptionPtr {
        let addr = FFIStr::new(address);
        unsafe { (self.0)(addr, timeout_ms) }
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

pub struct SerializationExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl SerializationExceptionConstructor {
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

// Specific mapping for PagerExecutionError.
impl ErrorToException for PagerExecutionError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(db_error, message)),
            )) => (db_error, message.as_str()).to_exception(ctors),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::RequestTimeout(duration),
            )) => ctors
                .operation_timed_out_exception_constructor
                .construct_from_rust("0.0.0.0:0", duration.as_millis() as i32), // FIXME: address is unknown here; placeholder used

            // TODO: Add more specific mappings for other error types as needed.
            _ => ctors.rust_exception_constructor.construct_from_rust(self),
        }
    }
}

// Specific mapping for PrepareError
impl ErrorToException for PrepareError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors.rust_exception_constructor.construct_from_rust(self) // TODO: convert errors to specific exceptions
    }
}

// Specific mapping for NewSessionError
impl ErrorToException for NewSessionError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            NewSessionError::MetadataError(MetadataError::ConnectionPoolError(
                ConnectionPoolError::Broken {
                    last_connection_error: ConnectionError::IoError(io_err),
                },
            )) => {
                match io_err.kind() {
                    std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::NotConnected => ctors
                        .no_host_available_exception_constructor
                        .construct_from_rust(io_err.to_string().as_str()),
                    _ => ctors.rust_exception_constructor.construct_from_rust(self), // TODO: convert errors to specific exceptions
                }
            }
            _ => ctors.rust_exception_constructor.construct_from_rust(self), // TODO: convert errors to specific exceptions
        }
    }
}

// Tuple-based mapping to include the server-provided message alongside DbError
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

            _ => ctors
                .rust_exception_constructor
                .construct_from_rust(db_error),
        }
    }
}

impl ErrorToException for DeserializationError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors.rust_exception_constructor.construct_from_rust(self) // TODO: convert errors to specific exceptions
    }
}

impl ErrorToException for NextRowError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors.rust_exception_constructor.construct_from_rust(self) // TODO: convert errors to specific exceptions
    }
}

impl ErrorToException for SerializationError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        ctors
            .serialization_exception_constructor
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
