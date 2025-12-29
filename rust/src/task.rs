use futures::FutureExt;
use std::ffi::c_void;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

use crate::FfiPtr;
use crate::error_conversion::{
    AlreadyExistsConstructor, ErrorToException, ExceptionPtr, FunctionFailureExceptionConstructor,
    InvalidConfigurationInQueryExceptionConstructor, InvalidQueryConstructor,
    NoHostAvailableExceptionConstructor, OperationTimedOutExceptionConstructor,
    PreparedQueryNotFoundExceptionConstructor, RequestInvalidExceptionConstructor,
    RustExceptionConstructor, SyntaxErrorExceptionConstructor, TraceRetrievalExceptionConstructor,
    TruncateExceptionConstructor, UnauthorizedExceptionConstructor,
};
use crate::ffi::{ArcFFI, BridgedOwnedSharedPtr};

/// The global Tokio runtime used to execute async tasks.
static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    // Logger must be initialized for the logs to be emitted. As a good
    // heuristic to initialize it early, we do it when creating the global
    // Tokio runtime, which happens lazily on the first use of async tasks.
    // This has a downside that if any logs are emitted before the first
    // async task is spawned, they will be lost.
    // To make this more robust, we could consider initializing the logger at
    // the driver startup, but that would require a new call from C# to Rust,
    // issued somehow early during the C# driver part.
    crate::logging::init_logging();

    Runtime::new().unwrap()
});

/// Opaque type representing a C# TaskCompletionSource<T>.
enum Tcs {}

/// A pointer to a TaskCompletionSource<T> on the C# side.
#[repr(transparent)]
pub struct TcsPtr(FfiPtr<'static, Tcs>);

unsafe impl Send for TcsPtr {}

/// Function pointer type to complete a TaskCompletionSource with a result.
type CompleteTask = unsafe extern "C" fn(tcs: TcsPtr, result: BridgedOwnedSharedPtr<c_void>);

/// Function pointer type to fail a TaskCompletionSource with an exception handle.
type FailTask = unsafe extern "C" fn(tcs: TcsPtr, exception_handle: ExceptionPtr);

/// **Task Control Block** (TCB)
///
/// Contains the necessary information to manually control a Task execution from Rust.
/// This includes a pointer to the Task Completion Source (TCS) on the C# side,
/// as well as function pointers to complete (finish successfully)
/// or fail (set an exception) the task.
#[repr(C)] // <- Ensure FFI-compatible layout
pub struct Tcb {
    tcs: TcsPtr,
    complete_task: CompleteTask,
    fail_task: FailTask,
    // SAFETY: The memory is a leaked unmanaged allocation on the C# side.
    // This guarantees that the pointer remains valid and is not moved or deallocated.
    constructors: &'static ExceptionConstructors,
}

/// Collection of exception constructors passed from C#.
/// This struct holds function pointers to create various exception types.
/// Any changes here must be mirrored on the C# side in the exact same order (alphabetical).
#[repr(C)]
pub struct ExceptionConstructors {
    pub already_exists_constructor: AlreadyExistsConstructor,
    pub function_failure_exception_constructor: FunctionFailureExceptionConstructor,
    pub invalid_configuration_in_query_constructor: InvalidConfigurationInQueryExceptionConstructor,
    pub invalid_query_constructor: InvalidQueryConstructor,
    pub no_host_available_exception_constructor: NoHostAvailableExceptionConstructor,
    pub operation_timed_out_exception_constructor: OperationTimedOutExceptionConstructor,
    pub prepared_query_not_found_exception_constructor: PreparedQueryNotFoundExceptionConstructor,
    pub request_invalid_exception_constructor: RequestInvalidExceptionConstructor,
    pub rust_exception_constructor: RustExceptionConstructor,
    pub syntax_error_exception_constructor: SyntaxErrorExceptionConstructor,
    pub trace_retrieval_exception_constructor: TraceRetrievalExceptionConstructor,
    pub truncate_exception_constructor: TruncateExceptionConstructor,
    pub unauthorized_exception_constructor: UnauthorizedExceptionConstructor,
}

/// A utility struct to bridge Rust tokio futures with C# tasks.
pub(crate) struct BridgedFuture {
    // For now empty - all methods are static.
}

impl BridgedFuture {
    /// Spawns a future onto the global Tokio runtime.
    ///
    /// The future's result is sent back to the C# side using the provided Task Control Block (TCB).
    /// Thus, the result type `T` must implement `ArcFFI` to be safely shared across the FFI boundary.
    // TODO: allow BoxFFI types as well.
    /// If the future panics, the panic is caught and reported as an exception to the C# side.
    /// The future must return a Result, where the Ok variant is sent back to C# on success,
    /// and the Err variant is sent back as an exception message.
    pub(crate) fn spawn<F, T, E>(tcb: Tcb, future: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static + ArcFFI, // Must be shareable across FFI boundary. For now we only support ArcFFI.
        T: Debug,                   // Temporarily, for debug prints.
        E: Debug + Display + ErrorToException, // Error must be printable for logging and exception conversion.
                                               // The ErrorToException trait is used to convert the error to an exception pointer.
    {
        let Tcb {
            tcs,
            complete_task,
            fail_task,
            constructors,
        } = tcb;

        RUNTIME.spawn(async move {
            // Catch panics in the future to prevent unwinding tokio executor thread's stack.
            let result = AssertUnwindSafe(future).catch_unwind().await;

            tracing::trace!(
                "[FFI]: Future completed with result: {} - {:?}",
                std::any::type_name::<T>(),
                result
            );

            match result {
                // On success, complete the task with the result.
                Ok(Ok(res)) => {
                    let arced_res = Arc::new(res);
                    unsafe { complete_task(tcs, ArcFFI::into_ptr(arced_res).cast_to_void()) };
                }

                // On error, fail the task with exception.
                Ok(Err(err)) => {
                    let exception_ptr = err.to_exception(constructors);
                    unsafe { fail_task(tcs, exception_ptr) };
                }
                // On panic, fail the task with the panic message.
                Err(panic) => {
                    // Panic payloads can be of any type, but `panic!()` macro only uses &str or String.
                    let panic_msg = if let Some(s) = panic.downcast_ref::<&str>() {
                        *s
                    } else if let Some(s) = panic.downcast_ref::<String>() {
                        s.as_str()
                    } else {
                        "Weird panic with non-string payload"
                    };
                    let exception_ptr = constructors
                        .rust_exception_constructor
                        .construct_from_rust(panic_msg);
                    unsafe { fail_task(tcs, exception_ptr) };
                }
            }
        });
    }

    /// Blocks the current thread until the provided future completes, returning its output.
    ///
    /// This suits blocking APIs of the C# Driver that need to wait for an async operation to complete.
    /// Although it's inherently inefficient, it's not our choice - the C# Driver's blocking API is what it is.
    /// Use with caution and prefer async APIs whenever possible.
    pub(crate) fn block_on<T>(future: impl Future<Output = T>) -> T {
        RUNTIME.block_on(future)
    }
}

/// An error type that can never be instantiated.
/// Used to represent futures that cannot fail.
pub(crate) enum ImpossibleError {}

impl std::fmt::Debug for ImpossibleError {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::fmt::Display for ImpossibleError {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

#[expect(dead_code)]
/// A result type for futures that cannot fail.
pub(crate) struct UnfallibleFutureResult<T>(Result<T, ImpossibleError>);
