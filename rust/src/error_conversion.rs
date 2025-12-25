use crate::FfiPtr;
use crate::ffi::{FFIByteSlice, FFIStr};
use scylla::errors::{
    ConnectionError, ConnectionPoolError, DbError, MetadataError, NewSessionError, NextPageError,
    PagerExecutionError, PrepareError, RequestAttemptError, RequestError,
};
use std::fmt::{Debug, Display};

use crate::task::ExceptionConstructors;

// Opaque type representing a C# Exception.
#[derive(Clone, Copy)]
enum Exception {}

/// A pointer to a C# Exception.
/// This is used across the FFI boundary to represent exceptions created on the C# side.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ExceptionPtr(FfiPtr<'static, Exception>);

impl<T> Debug for FfiPtr<'static, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ptr = self
            .ptr
            .map(|nn| nn.as_ptr())
            .unwrap_or(std::ptr::null::<T>() as *mut T);
        write!(f, "FfiPtr({:p})", ptr)
    }
}

#[repr(transparent)]
pub struct RustExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl RustExceptionConstructor {
    /// Creates a generic C# exception for unexpected Rust errors.
    ///
    /// Prefixes the message with "Rust exception:" and forwards it
    /// across the FFI boundary to construct the managed exception.
    pub fn construct_from_rust(&self, err: impl Display) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, address: &str, timeout_ms: i32) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str, unknown_id: &[u8]) -> ExceptionPtr {
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
    #[allow(dead_code)] // Currently unused
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

// TODO: Use this constructor for a specific error type.
/// FFI constructor for C# `TraceRejectedException` (currently unused).
#[repr(transparent)]
pub struct TraceRejectedExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

impl TraceRejectedExceptionConstructor {
    #[allow(dead_code)] // Currently unused
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
}

/// FFI constructor for C# `TruncateException`.
#[repr(transparent)]
pub struct TruncateExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl TruncateExceptionConstructor {
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
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
    pub fn construct_from_rust(&self, keyspace: &str, table: &str) -> ExceptionPtr {
        let ks = FFIStr::new(keyspace);
        let tb = FFIStr::new(table);
        unsafe { (self.0)(ks, tb) }
    }
}

/// FFI constructor for C# `InvalidQueryException`.
#[repr(transparent)]
pub struct InvalidQueryConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

impl InvalidQueryConstructor {
    pub fn construct_from_rust(&self, message: &str) -> ExceptionPtr {
        let message = FFIStr::new(message);
        unsafe { (self.0)(message) }
    }
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
/// The returned [`ExceptionPtr`] is an opaque pointer to a C# Exception object. Implementors must ensure
/// that any pointers passed to the constructors are valid for the duration of the call.
pub trait ErrorToException {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr;
}

// Specific mapping for PagerExecutionError.
impl ErrorToException for PagerExecutionError {
    fn to_exception(&self, ctors: &ExceptionConstructors) -> ExceptionPtr {
        match self {
            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::AlreadyExists { keyspace, table },
                    _,
                )),
            )) => ctors
                .already_exists_constructor
                .construct_from_rust(keyspace, table),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::Invalid,
                    message,
                )),
            )) => ctors.invalid_query_constructor.construct_from_rust(message),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::SyntaxError,
                    message,
                )),
            )) => ctors
                .syntax_error_exception_constructor
                .construct_from_rust(message),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::Unauthorized,
                    message,
                )),
            )) => ctors
                .unauthorized_exception_constructor
                .construct_from_rust(message),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::FunctionFailure { .. },
                    message,
                )),
            )) => ctors
                .function_failure_exception_constructor
                .construct_from_rust(message),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::TruncateError,
                    message,
                )),
            )) => ctors
                .truncate_exception_constructor
                .construct_from_rust(message),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::Unprepared { statement_id },
                    message,
                )),
            )) => ctors
                .prepared_query_not_found_exception_constructor
                .construct_from_rust(message, statement_id),

            PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::ConfigError,
                    message,
                )),
            )) => ctors
                .invalid_configuration_in_query_constructor
                .construct_from_rust(message),

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
