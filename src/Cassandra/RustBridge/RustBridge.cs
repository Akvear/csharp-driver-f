using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

/* PInvoke has an overhead of between 10 and 30 x86 instructions per call.
 * In addition to this fixed cost, marshaling creates additional overhead.
 * There is no marshaling cost between blittable types that have the same
 * representation in managed and unmanaged code. For example, there is no cost
 * to translate between int and Int32.
 */

namespace Cassandra
{
    static class RustBridge
    {
        /// <summary>
        /// Represents a UTF-8 string passed over FFI boundary.
        /// Used to pass strings from Rust to C#.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct FFIString
        {
            internal readonly IntPtr ptr;
            internal readonly nuint len;

            internal FFIString(IntPtr ptr, nuint len)
            {
                this.ptr = ptr;
                this.len = len;
            }

            internal string ToManagedString()
            {
                return Marshal.PtrToStringUTF8(ptr, (int)len);
            }
        }

        /// <summary>
        /// Represents a byte slice passed over FFI boundary.
        /// Used to pass byte arrays from Rust to C#.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct FFIByteSlice
        {
            internal readonly IntPtr ptr;
            internal readonly nuint len;

            internal FFIByteSlice(IntPtr ptr, nuint len)
            {
                this.ptr = ptr;
                this.len = len;
            }

            internal Span<byte> ToSpan()
            {
                if (len > int.MaxValue)
                {
                    // Byte slices in Rust can be larger than maximum Span<byte> length.
                    // This should never happen in practice, but we guard against it to avoid UB.
                    Environment.FailFast("FFIByteSlice length exceeds maximum Span<byte> length.");
                    return Span<byte>.Empty;
                }
                unsafe
                {
                    // ToSpan() is called in callbacks so we catch any exceptions here to avoid UB.
                    try
                    {
                        return new Span<byte>((void*)ptr, (int)len);
                    }
                    catch (Exception ex)
                    {
                        Environment.FailFast("Failed to create Span<byte> from FFIByteSlice", ex);
                        return Span<byte>.Empty;
                    }
                }
            }
        }

        internal interface IBridgedTaskResult
        {
            /// <summary>
            /// This must return a pointer to the appropriate [UnmanagedCallersOnly] CompleteTask method for the result type R.
            /// This MUST have the following signature:
            /// unsafe static delegate* unmanaged[Cdecl]&lt;IntPtr tcs, Self this, void&gt;
            /// </summary>
            internal static abstract IntPtr CompleteTaskDelegate { get; }

            /// <summary>
            /// This must return a pointer to the appropriate [UnmanagedCallersOnly] FailTask method for the result type R.
            /// This MUST have the following signature:
            /// unsafe static delegate* unmanaged[Cdecl]&lt;IntPtr tcs, FFIException exception_ptr, void&gt;
            /// </summary>
            internal static abstract IntPtr FailTaskDelegate { get; }
        }

        /// <summary>
        /// Represents a boolean value passed over FFI boundary.
        /// Used to pass bools between Rust and C#, in both directions.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct FFIBool : IBridgedTaskResult
        {
            [MarshalAs(UnmanagedType.U1)]
            private readonly bool value;

            internal FFIBool(bool value)
            {
                this.value = value;
            }

            // Must be public, because `implicit operator` requires it.
            public static implicit operator FFIBool(bool value) => new(value);
            public static implicit operator bool(FFIBool b) => b.value;

            /// <summary>
            /// This shall be called by Rust code when the operation is completed.
            /// </summary>
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, res: bool)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
            internal static void CompleteTask(IntPtr tcsPtr, FFIBool result)
            {
                Tcb<FFIBool>.CompleteTask(tcsPtr, result);
            }

            /// <summary>
            /// This shall be called by Rust code when the operation failed.
            /// </summary>
            //
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, exception_handle: ExceptionPtr)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
            internal static void FailTask(IntPtr tcsPtr, FFIException exceptionPtr)
            {
                Tcb<FFIBool>.FailTask(tcsPtr, exceptionPtr);
            }

            internal unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, FFIBool, void> completeTaskDel = &CompleteTask;
            internal unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, FFIException, void> failTaskDel = &FailTask;

            static IntPtr IBridgedTaskResult.CompleteTaskDelegate
            {
                get
                {
                    unsafe
                    {
                        return (IntPtr)completeTaskDel;
                    }
                }
            }

            static IntPtr IBridgedTaskResult.FailTaskDelegate
            {
                get
                {
                    unsafe
                    {
                        return (IntPtr)failTaskDel;
                    }
                }
            }
        }

        /// <summary>
        /// Struct used to pass a native pointer along with its destructor function pointer.
        /// This is used to transfer ownership of Rust resources to C# code.
        /// All changes to this struct's fields must be mirrored in Rust code in the exact same order.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct ManuallyDestructible : IBridgedTaskResult
        {
            internal readonly IntPtr Ptr;
            internal readonly IntPtr Destructor;

            internal ManuallyDestructible(IntPtr ptr, IntPtr destructor)
            {
                Ptr = ptr;
                Destructor = destructor;
            }

            /// <summary>
            /// This shall be called by Rust code when the operation is completed.
            /// </summary>
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, res: ManuallyDestructible)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
            internal static void CompleteTask(IntPtr tcsPtr, ManuallyDestructible manuallyDestructible)
            {
                Tcb<ManuallyDestructible>.CompleteTask(tcsPtr, manuallyDestructible);
            }

            /// <summary>
            /// This shall be called by Rust code when the operation failed.
            /// </summary>
            //
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, exception_handle: ExceptionPtr)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
            internal static void FailTask(IntPtr tcsPtr, FFIException exceptionPtr)
            {
                Tcb<ManuallyDestructible>.FailTask(tcsPtr, exceptionPtr);
            }


            // This is the only way to get a function pointer to a method decorated
            // with [UnmanagedCallersOnly] that I've found to compile.
            //
            // The delegates are static to ensure 'static lifetime of the function pointers.
            // This is important because the Rust code may call the callbacks
            // long after the P/Invoke call that passed the TCB has returned.
            // If the delegates were not static, they could be collected by the GC
            // and the function pointers would become invalid.
            //
            // `unsafe` is required to get a function pointer to a static method.
            // Note that we can get this pointer because the method is static and
            // decorated with [UnmanagedCallersOnly].
            internal unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, ManuallyDestructible, void> completeTaskDel = &CompleteTask;
            internal unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, FFIException, void> failTaskDel = &FailTask;

            static IntPtr IBridgedTaskResult.CompleteTaskDelegate
            {
                get
                {
                    unsafe
                    {
                        return (IntPtr)completeTaskDel;
                    }
                }
            }

            static IntPtr IBridgedTaskResult.FailTaskDelegate
            {
                get
                {
                    unsafe
                    {
                        return (IntPtr)failTaskDel;
                    }
                }
            }
        }

        /// <summary>
        /// Task Control Block groups entities crucial for controlling Task execution
        /// from Rust code. It's intended to:
        /// - hide some complexity of the interop,
        /// - reduce code duplication,
        /// - squeeze multiple native function parameters into 1.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct Tcb<R> where R : IBridgedTaskResult
        {
            /// <summary>
            ///  Pointer to a GCHandle referencing a TaskCompletionSource&lt;IntPtr&gt;.
            ///  This shall be allocated by the C# code before calling into Rust,
            ///  and freed by the C# callback executed by the Rust code once the operation
            ///  is completed (either successfully or with an error).
            /// </summary>
            internal readonly IntPtr tcs;

            /// <summary>
            ///  Pointer to the C# method to call when the operation is completed successfully.
            /// This shall be set to the function pointer of RustBridge.CompleteTask.
            /// </summary>
            private readonly IntPtr complete_task;

            /// <summary>
            /// Pointer to the C# method to call when the operation fails.
            /// This shall be set to the function pointer of RustBridge.FailTask.
            /// </summary>
            private readonly IntPtr fail_task;

            /// <summary>
            /// Pointer to a static, unmanaged table of exception constructors.
            /// Rust reads constructors from this table to build managed exceptions.
            /// </summary>
            private readonly IntPtr constructors;

            private Tcb(IntPtr tcs, IntPtr completeTask, IntPtr failTask)
            {
                this.tcs = tcs;
                this.complete_task = completeTask;
                this.fail_task = failTask;
                unsafe
                {
                    this.constructors = (IntPtr)Globals.ConstructorsPtr;
                }
            }

            /// <summary>
            /// Creates a TCB for a TaskCompletionSource&lt;R&gt;.
            /// </summary>
            /// <param name="tcs"></param>
            /// <returns></returns>
            internal static Tcb<R> WithTcs(TaskCompletionSource<R> tcs)
            {
                /*
                 * Although GC knows that it must not collect items during a synchronous P/Invoke call,
                 * it doesn't know that the native code will still require the TCS after the P/Invoke
                 * call returns.
                 * And tokio task in Rust will likely still run after the P/Invoke call returns.
                 * So, since we are passing the TCS to asynchronous native code, we need to pin it
                 * so it doesn't get collected by the GC.
                 * We must remember to free the handle later when the TCS is completed (see CompleteTask
                 * method).
                 */
                var handle = GCHandle.Alloc(tcs);

                IntPtr tcsPtr = GCHandle.ToIntPtr(handle);

                // `unsafe` is required to get a function pointer to a static method.
                unsafe
                {
                    IntPtr completeTaskPtr = R.CompleteTaskDelegate;
                    IntPtr failTaskPtr = R.FailTaskDelegate;
                    return new Tcb<R>(tcsPtr, completeTaskPtr, failTaskPtr);
                }
            }

            /// <summary>
            /// This shall be called by Rust code when the operation is completed.
            /// </summary>
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, res: R)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            internal static void CompleteTask(IntPtr tcsPtr, R result)
            {
                try
                {
                    // Recover the GCHandle that was allocated for the TaskCompletionSource.
                    var handle = GCHandle.FromIntPtr(tcsPtr);

                    if (handle.Target is TaskCompletionSource<R> tcs)
                    {
                        // Pass R value back as the result.
                        // The Rust code is responsible for interpreting the pointer's contents
                        // memory is freed when the C# RustResource releases it.
                        tcs.SetResult(result);

                        // Free the handle so the TCS can be collected once no longer used
                        // by the C# code.
                        handle.Free();

                        Console.Error.WriteLine($"[FFI] CompleteTask done.");
                    }
                    else
                    {
                        throw new InvalidOperationException($"GCHandle did not reference a TaskCompletionSource<{typeof(R)}>.");
                    }
                }
                catch (Exception ex)
                {
                    Environment.FailFast($"[FFI] CompleteTask threw exception: {ex}");
                }
            }

            /// <summary>
            /// This shall be called by Rust code when the operation failed.
            /// </summary>
            //
            // Signature in Rust: extern "C" fn(tcs: *mut c_void, exception_handle: ExceptionPtr)
            //
            // This attribute makes the method callable from native code.
            // It also allows taking a function pointer to the method.
            internal static void FailTask(IntPtr tcsPtr, FFIException exceptionPtr)
            {
                try
                {
                    // Recover the GCHandle that was allocated for the TaskCompletionSource.
                    var handle = GCHandle.FromIntPtr(tcsPtr);

                    if (handle.Target is TaskCompletionSource<R> tcsMd)
                    {
                        // Create the exception to pass to the TCS.
                        Exception exception;
                        try
                        {
                            if (exceptionPtr.exception != IntPtr.Zero)
                            {
                                // Recover the exception from the GCHandle passed from Rust.
                                var exHandle = GCHandle.FromIntPtr(exceptionPtr.exception);
                                try
                                {
                                    if (exHandle.Target is Exception ex)
                                    {
                                        exception = ex;
                                    }
                                    else
                                    {
                                        // This should never happen when everything is working correctly.
                                        Environment.FailFast("Failed to recover Exception from GCHandle passed from Rust.");
                                        exception = new RustException("Failed to recover Exception from GCHandle passed from Rust."); // Unreachable, required for compilation
                                    }
                                }
                                finally
                                {
                                    if (exHandle.IsAllocated)
                                    {
                                        exHandle.Free();
                                    }
                                }
                            }
                            else
                            {
                                // Fallback to a generic RustException if no exception was passed.
                                exception = new RustException("Unknown error from Rust");
                            }
                            tcsMd.SetException(exception);
                        }
                        finally
                        {
                            // Free the handle so the TCS can be collected once no longer used
                            // by the C# code.
                            if (handle.IsAllocated)
                            {
                                handle.Free();
                            }
                        }

                        Console.Error.WriteLine($"[FFI] FailTask done.");

                    }
                    else
                    {
                        throw new InvalidOperationException($"GCHandle did not reference a TaskCompletionSource<{typeof(R)}>.");
                    }
                }
                catch (Exception ex)
                {
                    Environment.FailFast($"[FFI] FailTask threw exception: {ex}");
                }
            }
        }

        /// <summary>
        /// Static holder for the exception constructors table.
        /// Allocated once and reused.
        /// Add other global data here as needed.
        /// </summary>
        internal static unsafe class Globals
        {
            // Exception constructors passed to Rust
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, FFIString, IntPtr> AlreadyExistsConstructorPtr = &AlreadyExistsException.AlreadyExistsExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> AlreadyShutdownExceptionConstructorPtr = &AlreadyShutdownException.AlreadyShutdownExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> DeserializationExceptionConstructorPtr = &DeserializationException.DeserializationExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> FunctionFailureExceptionConstructorPtr = &FunctionFailureException.FunctionFailureExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> InvalidConfigurationInQueryExceptionConstructorPtr = &InvalidConfigurationInQueryException.InvalidConfigurationInQueryExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> InvalidQueryConstructorPtr = &InvalidQueryException.InvalidQueryExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> NoHostAvailableExceptionConstructorPtr = &NoHostAvailableException.NoHostAvailableExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, int, IntPtr> OperationTimedOutExceptionConstructorPtr = &OperationTimedOutException.OperationTimedOutExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, FFIByteSlice, IntPtr> PreparedQueryNotFoundExceptionConstructorPtr = &PreparedQueryNotFoundException.PreparedQueryNotFoundExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> RequestInvalidExceptionConstructorPtr = &RequestInvalidException.RequestInvalidExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> RustExceptionConstructorPtr = &RustException.RustExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> SerializationExceptionConstructorPtr = &SerializationException.SerializationExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> SyntaxErrorExceptionConstructorPtr = &SyntaxError.SyntaxErrorFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> TraceRetrievalExceptionConstructorPtr = &TraceRetrievalException.TraceRetrievalExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> TruncateExceptionConstructorPtr = &TruncateException.TruncateExceptionFromRust;
            unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> UnauthorizedExceptionConstructorPtr = &UnauthorizedException.UnauthorizedExceptionFromRust;

            /// <summary>
            /// Table of exception constructors passed to Rust via TCB.
            /// Rust reads constructors from this table to build managed exceptions.
            /// Any changes to this struct must be mirrored in Globals 
            /// and in Rust code in the exact same order (alphabetical).
            /// </summary>
            [StructLayout(LayoutKind.Sequential)]
            internal readonly struct Constructors
            {
                internal readonly IntPtr already_exists_constructor;
                internal readonly IntPtr already_shutdown_exception_constructor;
                internal readonly IntPtr deserialization_exception_constructor;
                internal readonly IntPtr function_failure_exception_constructor;
                internal readonly IntPtr invalid_configuration_in_query_constructor;
                internal readonly IntPtr invalid_query_constructor;
                internal readonly IntPtr no_host_available_exception_constructor;
                internal readonly IntPtr operation_timed_out_exception_constructor;
                internal readonly IntPtr prepared_query_not_found_exception_constructor;
                internal readonly IntPtr request_invalid_exception_constructor;
                internal readonly IntPtr rust_exception_constructor;
                internal readonly IntPtr serialization_exception_constructor;
                internal readonly IntPtr syntax_error_exception_constructor;
                internal readonly IntPtr trace_retrieval_exception_constructor;
                internal readonly IntPtr truncate_exception_constructor;
                internal readonly IntPtr unauthorized_exception_constructor;

                internal Constructors(
                    IntPtr alreadyExistsException,
                    IntPtr alreadyShutdownException,
                    IntPtr deserializationException,
                    IntPtr functionFailureException,
                    IntPtr invalidConfigurationInQueryException,
                    IntPtr invalidQueryException,
                    IntPtr noHostAvailableException,
                    IntPtr operationTimedOutException,
                    IntPtr preparedQueryNotFoundException,
                    IntPtr requestInvalidException,
                    IntPtr rustException,
                    IntPtr serializationException,
                    IntPtr syntaxErrorException,
                    IntPtr traceRetrievalException,
                    IntPtr truncateException,
                    IntPtr unauthorizedException)
                {
                    already_exists_constructor = alreadyExistsException;
                    already_shutdown_exception_constructor = alreadyShutdownException;
                    deserialization_exception_constructor = deserializationException;
                    function_failure_exception_constructor = functionFailureException;
                    invalid_configuration_in_query_constructor = invalidConfigurationInQueryException;
                    invalid_query_constructor = invalidQueryException;
                    no_host_available_exception_constructor = noHostAvailableException;
                    operation_timed_out_exception_constructor = operationTimedOutException;
                    prepared_query_not_found_exception_constructor = preparedQueryNotFoundException;
                    request_invalid_exception_constructor = requestInvalidException;
                    rust_exception_constructor = rustException;
                    serialization_exception_constructor = serializationException;
                    syntax_error_exception_constructor = syntaxErrorException;
                    trace_retrieval_exception_constructor = traceRetrievalException;
                    truncate_exception_constructor = truncateException;
                    unauthorized_exception_constructor = unauthorizedException;
                }
            }
            
            internal static readonly Constructors* ConstructorsPtr;

            /// <summary>
            /// Initializes the Rust driver components.
            /// This must be called early to ensure logging is properly initialized.
            /// </summary>
            [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
            private static extern void init_rust_logging();

            static Globals()
            {
                // Intentionally never freed: this is a single, process-lifetime constructors table
                ConstructorsPtr = (Constructors*)NativeMemory.Alloc((nuint)sizeof(Constructors));
                *ConstructorsPtr = new Constructors(
                    (IntPtr)AlreadyExistsConstructorPtr,
                    (IntPtr)AlreadyShutdownExceptionConstructorPtr,
                    (IntPtr)DeserializationExceptionConstructorPtr,
                    (IntPtr)FunctionFailureExceptionConstructorPtr,
                    (IntPtr)InvalidConfigurationInQueryExceptionConstructorPtr,
                    (IntPtr)InvalidQueryConstructorPtr,
                    (IntPtr)NoHostAvailableExceptionConstructorPtr,
                    (IntPtr)OperationTimedOutExceptionConstructorPtr,
                    (IntPtr)PreparedQueryNotFoundExceptionConstructorPtr,
                    (IntPtr)RequestInvalidExceptionConstructorPtr,
                    (IntPtr)RustExceptionConstructorPtr,
                    (IntPtr)SerializationExceptionConstructorPtr,
                    (IntPtr)SyntaxErrorExceptionConstructorPtr,
                    (IntPtr)TraceRetrievalExceptionConstructorPtr,
                    (IntPtr)TruncateExceptionConstructorPtr,
                    (IntPtr)UnauthorizedExceptionConstructorPtr
                );

                init_rust_logging();
            }
        }

        /// <summary>
        /// Package used to pass exceptions from Rust to C# over FFI boundary.
        /// If the underlying pointer is IntPtr.Zero, no exception occurred.
        /// If it's non-zero, it points to a GCHandle referencing the Exception.
        /// This handle must be freed even when a different exception is thrown.
        /// All changes to this struct's fields must be mirrored in Rust code in the exact same order.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        internal struct FFIException
        {
            // Fields:
            // Pointer to a GCHandle referencing the Exception.
            internal IntPtr exception;

            // Functions:
            // Creates an FfiException from the given Exception.
            internal static FFIException FromException(Exception ex)
            {
                var handle = GCHandle.Alloc(ex);
                IntPtr handlePtr = GCHandle.ToIntPtr(handle);
                return new FFIException
                {
                    exception = handlePtr
                };
            }

            // Creates an FfiException representing no exception.
            internal static FFIException Ok()
            {
                return new FFIException
                {
                    exception = IntPtr.Zero
                };
            }

            internal bool HasException => exception != IntPtr.Zero;
        }

        /// <summary>
        /// Throws the exception contained in the FfiException if any.
        /// This mustn't be used in UnmanagedCallersOnly methods because throwing exceptions
        /// across FFI boundary is UB.
        /// </summary>
        internal static void ThrowIfException(ref FFIException res)
        {
            if (res.exception == IntPtr.Zero)
            {
                return;
            }

            Exception exception;
            var exHandle = GCHandle.FromIntPtr(res.exception);
            try
            {
                if (exHandle.Target is Exception ex)
                {
                    exception = ex;
                }
                else
                {
                    Environment.FailFast("Failed to recover Exception from GCHandle passed from Rust (sync).");
                    return; // Unreachable
                }
            }
            finally
            {
                if (exHandle.IsAllocated)
                {
                    exHandle.Free();
                }
                // Zero out the pointer to avoid double free if caller invokes FreeIfPresent
                res.exception = IntPtr.Zero;
            }
            throw exception;
        }

        /// <summary>
        /// Frees the exception handle contained in the package without throwing.
        /// Safe to call multiple times; subsequent calls become no-ops.
        /// </summary>
        internal static void FreeExceptionHandle(ref FFIException res)
        {
            if (res.exception == IntPtr.Zero)
            {
                return;
            }
            var exHandle = GCHandle.FromIntPtr(res.exception);
            try
            {
                if (exHandle.IsAllocated)
                {
                    exHandle.Free();
                }
            }
            finally
            {
                res.exception = IntPtr.Zero;
            }
        }
    }
}