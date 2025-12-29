use crate::FfiPtr;
use crate::ffi::{FFIByteSlice, FFIStr};
use std::fmt::Debug;

// Opaque type representing a C# Exception.
#[derive(Clone, Copy)]
enum Exception {}

/// A pointer to a C# Exception.
/// This is used across the FFI boundary to represent exceptions created on the C# side.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ExceptionPtr(FfiPtr<'static, Exception>);

#[repr(transparent)]
pub struct RustExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

/// FFI constructor for C# `FunctionFailureException`.
#[repr(transparent)]
pub struct FunctionFailureExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

/// FFI constructor for C# `InvalidConfigurationInQueryException`.
#[repr(transparent)]
pub struct InvalidConfigurationInQueryExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr) -> ExceptionPtr,
);

/// FFI constructor for C# `NoHostAvailableException`.
#[repr(transparent)]
pub struct NoHostAvailableExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr) -> ExceptionPtr,
);

/// FFI constructor for C# `OperationTimedOutException`.
#[repr(transparent)]
pub struct OperationTimedOutExceptionConstructor(
    unsafe extern "C" fn(address: FFIStr<'_>, timeout_ms: i32) -> ExceptionPtr,
);

/// FFI constructor for C# `PreparedQueryNotFoundException`.
#[repr(transparent)]
pub struct PreparedQueryNotFoundExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>, unknown_id: FFIByteSlice<'_>) -> ExceptionPtr,
);

// TODO: Use this constructor for a specific error type.
/// FFI constructor for C# `RequestInvalidException` (currently unused).
#[repr(transparent)]
pub struct RequestInvalidExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

/// FFI constructor for C# `SyntaxErrorException`.
#[repr(transparent)]
pub struct SyntaxErrorExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

// TODO: Use this constructor for a specific error type.
/// FFI constructor for C# `TraceRetrievalException` (currently unused).
#[repr(transparent)]
pub struct TraceRetrievalExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

/// FFI constructor for C# `TruncateException`.
#[repr(transparent)]
pub struct TruncateExceptionConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);

/// FFI constructor for C# `UnauthorizedException`.
#[repr(transparent)]
pub struct UnauthorizedExceptionConstructor(
    unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr,
);

/// FFI constructor for C# `AlreadyExistsException`.
#[repr(transparent)]
pub struct AlreadyExistsConstructor(
    unsafe extern "C" fn(keyspace: FFIStr<'_>, table: FFIStr<'_>) -> ExceptionPtr,
);

/// FFI constructor for C# `InvalidQueryException`.
#[repr(transparent)]
pub struct InvalidQueryConstructor(unsafe extern "C" fn(message: FFIStr<'_>) -> ExceptionPtr);
