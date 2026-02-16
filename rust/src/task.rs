use futures::FutureExt;
use std::ffi::c_void;
use std::fmt::Debug;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

use crate::FfiPtr;
use crate::error_conversion::{
    AlreadyExistsConstructor, AlreadyShutdownExceptionConstructor,
    DeserializationExceptionConstructor, ErrorToException, ExceptionPtr,
    FunctionFailureExceptionConstructor, InvalidConfigurationInQueryExceptionConstructor,
    InvalidQueryConstructor, NoHostAvailableExceptionConstructor,
    OperationTimedOutExceptionConstructor, PreparedQueryNotFoundExceptionConstructor,
    RequestInvalidExceptionConstructor, RustExceptionConstructor,
    SerializationExceptionConstructor, SyntaxErrorExceptionConstructor,
    TraceRetrievalExceptionConstructor, TruncateExceptionConstructor,
    UnauthorizedExceptionConstructor,
};
use crate::ffi::{ArcFFI, BridgedOwnedSharedPtr};

/// The global Tokio runtime used to execute async tasks.
static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

/// Initialize the Rust logging early.
///
/// This function should be called early during driver initialization to ensure
/// that logging is set up before any operations that might emit logs.
///
/// Calling this function multiple times is safe; subsequent calls will be no-ops.
#[unsafe(no_mangle)]
pub extern "C" fn init_rust_logging() {
    // Force initialization of logging
    crate::logging::init_logging();
}

/// Opaque type representing a C# TaskCompletionSource<T>.
enum Tcs {}

/// A pointer to a TaskCompletionSource<T> on the C# side.
#[repr(transparent)]
pub struct TcsPtr(FfiPtr<'static, Tcs>);

unsafe impl Send for TcsPtr {}

/// A struct representing a manually destructible resource passed across the FFI boundary.
/// It contains a pointer to the resource and a function pointer to its destructor.
/// All changes to this struct's fields must be mirrored in C# code in the exact same order.
#[repr(C)]
pub struct ManuallyDestructible {
    pub ptr: BridgedOwnedSharedPtr<c_void>,
    pub destructor: Option<unsafe extern "C" fn(BridgedOwnedSharedPtr<c_void>)>,
}

impl ManuallyDestructible {
    fn new(
        ptr: BridgedOwnedSharedPtr<c_void>,
        destructor: Option<unsafe extern "C" fn(BridgedOwnedSharedPtr<c_void>)>,
    ) -> Self {
        Self { ptr, destructor }
    }

    fn new_null() -> Self {
        Self {
            ptr: BridgedOwnedSharedPtr::null(),
            destructor: None,
        }
    }

    pub(crate) fn from_destructible<T: Destructible>(value: Arc<T>) -> Self {
        let ptr = ArcFFI::into_ptr(value).cast_to_void();
        let destructor = T::void_destructor();
        ManuallyDestructible::new(ptr, destructor)
    }
}

impl Debug for ManuallyDestructible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw_ptr: *mut c_void = self.ptr.to_raw().unwrap_or(std::ptr::null_mut());

        let destructor_ptr = self.destructor.map(|d| d as *const ());

        f.debug_struct("ManuallyDestructible")
            .field("ptr", &raw_ptr)
            .field("is_null", &self.ptr.is_null())
            .field("destructor", &destructor_ptr)
            .finish()
    }
}

/// Safety: ManuallyDestructible can be sent across threads safely,
/// as it only contains a BridgedOwnedSharedPtr and a function pointer.
unsafe impl Send for ManuallyDestructible {}

/// This trait marks types that can be safely destructed across the FFI boundary.
/// It provides a method to obtain a C ABI function pointer that knows how to free `Self`.
/// This is used with `ManuallyDestructible` to ensure proper resource cleanup.
pub trait Destructible: ArcFFI + Sized + 'static {
    /// Returns an extern "C" function pointer that knows how to free `Self` from a `c_void` pointer.
    fn void_destructor() -> Option<unsafe extern "C" fn(BridgedOwnedSharedPtr<c_void>)> {
        extern "C" fn arc_void_free<T: ArcFFI + 'static>(ptr: BridgedOwnedSharedPtr<c_void>) {
            // SAFETY: The pointer was originally produced via `ArcFFI::into_ptr(Arc<T>)`
            // and then cast to `c_void`. Reinterpret cast back to the concrete type and free.
            let typed_ptr: BridgedOwnedSharedPtr<T> = unsafe { ptr.cast() };
            ArcFFI::free(typed_ptr);
        }

        Some(arc_void_free::<Self>)
    }
}

// Blanket impl: any ArcFFI type is destructible via the generic c_void destructor.
impl<T> Destructible for T where T: ArcFFI + Sized + 'static {}

// Blanket From impl to convert Arc<T> into ManuallyDestructible that stores T.
impl<T: Destructible> From<Arc<T>> for ManuallyDestructible {
    fn from(value: Arc<T>) -> Self {
        Self::from_destructible(value)
    }
}

// TEMPORARY blanket From impl to convert Option<Arc<T>> into ManuallyDestructible that stores T.
// This will be deleted, because we'll no longer need ManuallyDestructible to represent trivial values
// after we make Tcb generic over the result type.
impl<T: Destructible> From<Option<Arc<T>>> for ManuallyDestructible {
    fn from(value: Option<Arc<T>>) -> Self {
        match value {
            Some(v) => Self::from_destructible(v),
            None => ManuallyDestructible::new_null(),
        }
    }
}

/// **Task Control Block** (TCB)
///
/// Contains the necessary information to manually control a Task execution from Rust.
/// This includes a pointer to the Task Completion Source (TCS) on the C# side,
/// as well as function pointers to complete (finish successfully)
/// or fail (set an exception) the task.
#[repr(C)] // <- Ensure FFI-compatible layout
pub struct Tcb<R> {
    tcs: TcsPtr,
    /// Function pointer type to complete a TaskCompletionSource with a result.
    complete_task: unsafe extern "C" fn(tcs: TcsPtr, result: R),
    /// Function pointer type to fail a TaskCompletionSource with an exception handle.
    fail_task: unsafe extern "C" fn(tcs: TcsPtr, exception_handle: ExceptionPtr),
    /// Pointer to the collection of exception constructors.
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
    pub already_shutdown_exception_constructor: AlreadyShutdownExceptionConstructor,
    pub deserialization_exception_constructor: DeserializationExceptionConstructor,
    pub function_failure_exception_constructor: FunctionFailureExceptionConstructor,
    pub invalid_configuration_in_query_constructor: InvalidConfigurationInQueryExceptionConstructor,
    pub invalid_query_constructor: InvalidQueryConstructor,
    pub no_host_available_exception_constructor: NoHostAvailableExceptionConstructor,
    pub operation_timed_out_exception_constructor: OperationTimedOutExceptionConstructor,
    pub prepared_query_not_found_exception_constructor: PreparedQueryNotFoundExceptionConstructor,
    pub request_invalid_exception_constructor: RequestInvalidExceptionConstructor,
    pub rust_exception_constructor: RustExceptionConstructor,
    pub serialization_exception_constructor: SerializationExceptionConstructor,
    pub syntax_error_exception_constructor: SyntaxErrorExceptionConstructor,
    pub trace_retrieval_exception_constructor: TraceRetrievalExceptionConstructor,
    pub truncate_exception_constructor: TruncateExceptionConstructor,
    pub unauthorized_exception_constructor: UnauthorizedExceptionConstructor,
}

impl<R> Tcb<R> {
    /// Completes the task with the provided result, consuming the TCB.
    pub(crate) fn complete_task(self, res: R) {
        unsafe {
            (self.complete_task)(self.tcs, res);
        }
    }

    /// Fails the task with the provided exception, consuming the TCB.
    pub(crate) fn fail_task(self, exception: ExceptionPtr) {
        unsafe {
            (self.fail_task)(self.tcs, exception);
        }
    }
}

/// A utility struct to bridge Rust tokio futures with C# tasks.
pub(crate) struct BridgedFuture {
    // For now empty - all methods are static.
}

impl BridgedFuture {
    /// Spawns a future onto the global Tokio runtime.
    ///
    /// The future's result is sent back to the C# side using the provided Task Control Block (TCB).
    /// If the future panics, the panic is caught and reported as an exception to the C# side.
    /// The future must return a Result, where the Ok variant is sent back to C# on success,
    /// and the Err variant is sent back as an exception.
    pub(crate) fn spawn<F, T, E, R>(tcb: Tcb<R>, future: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, // Result type must be Send to cross threads in tokio runtime.
        T: Debug,          // Temporarily, for debug prints.
        R: From<T> + 'static,
        E: Debug + ErrorToException, // Error must be printable for logging and exception conversion.
                                     // The ErrorToException trait is used to convert the error to an exception pointer.
    {
        RUNTIME.spawn(async move {
            // Catch panics in the future to prevent unwinding tokio executor thread's stack.
            let result = AssertUnwindSafe(future).catch_unwind().await;

            tracing::trace!(
                "[FFI]: Future completed with result: {} - {:?}",
                std::any::type_name::<T>(),
                "<Elided for brevity>"
            );

            match result {
                // On success, complete the task with the result.
                Ok(Ok(res)) => {
                    tcb.complete_task(res.into());
                }

                // On error, fail the task with exception.
                Ok(Err(err)) => {
                    let exception_ptr = err.to_exception(tcb.constructors);
                    tcb.fail_task(exception_ptr);
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
                    let exception_ptr = tcb
                        .constructors
                        .rust_exception_constructor
                        .construct_from_rust(panic_msg);
                    tcb.fail_task(exception_ptr);
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
