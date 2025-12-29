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
                try {
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

    [StructLayout(LayoutKind.Sequential)]
    internal struct FfiError
    {
        public int Code;
        public IntPtr Message;
    }

    internal static class FfiErrorHelpers
    {
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void ffi_error_free_message(IntPtr msg);

        /// <summary>
        /// Executes a native operation and throws a managed exception if the returned FfiError
        /// indicates failure. Optionally accepts a human-readable operation name that will be
        /// included in the thrown message for easier debugging.
        /// </summary>
        internal static void ExecuteAndThrowIfFails(Func<FfiError> operation, string operationName = null)
        {
            var err = operation();
            ThrowIfError(err, operationName);
        }

        private static void ThrowIfError(FfiError error, string operationName = null)
        {
            if (error.Code == 0)
            {
                return;
            }

            var message = "Unknown error";
            if (error.Message != IntPtr.Zero)
            {
                var nativeString = Marshal.PtrToStringUTF8(error.Message);
                if (!string.IsNullOrEmpty(nativeString))
                {
                    message = nativeString;
                }
                
                ffi_error_free_message(error.Message);
            }

            if (string.IsNullOrEmpty(operationName))
            {
                throw new InvalidOperationException($"Rust call failed with code {error.Code}: {message}");
            }
            else
            {
                throw new InvalidOperationException($"Operation '{operationName}' failed with code {error.Code}: {message}");
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
    internal readonly struct Tcb
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
                this.constructors = (IntPtr)RustBridgeGlobals.ConstructorsPtr;
            }
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
        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> completeTaskDel = &RustBridge.CompleteTask;
        unsafe readonly static delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> failTaskDel = &RustBridge.FailTask;

        // Exception constructors passed to Rust via TCB
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, FFIString, IntPtr> AlreadyExistsConstructorPtr = &AlreadyExistsException.AlreadyExistsExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> FunctionFailureExceptionConstructorPtr = &FunctionFailureException.FunctionFailureExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> InvalidConfigurationInQueryExceptionConstructorPtr = &InvalidConfigurationInQueryException.InvalidConfigurationInQueryExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> InvalidQueryConstructorPtr = &InvalidQueryException.InvalidQueryExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> NoHostAvailableExceptionConstructorPtr = &NoHostAvailableException.NoHostAvailableExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, int, IntPtr> OperationTimedOutExceptionConstructorPtr = &OperationTimedOutException.OperationTimedOutExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, FFIByteSlice, IntPtr> PreparedQueryNotFoundExceptionConstructorPtr = &PreparedQueryNotFoundException.PreparedQueryNotFoundExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> RequestInvalidExceptionConstructorPtr = &RequestInvalidException.RequestInvalidExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> RustExceptionConstructorPtr = &RustException.RustExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> SyntaxErrorExceptionConstructorPtr = &SyntaxError.SyntaxErrorFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> TraceRetrievalExceptionConstructorPtr = &TraceRetrievalException.TraceRetrievalExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> TruncateExceptionConstructorPtr = &TruncateException.TruncateExceptionFromRust;
        unsafe readonly static delegate* unmanaged[Cdecl]<FFIString, IntPtr> UnauthorizedExceptionConstructorPtr = &UnauthorizedException.UnauthorizedExceptionFromRust;

        /// <summary>
        /// Static holder for the exception constructors table.
        /// Allocated once and reused for all TCBs.
        /// Add other global data here as needed.
        /// </summary>
        public static unsafe class RustBridgeGlobals
        {
            public static readonly Constructors* ConstructorsPtr;

            static RustBridgeGlobals()
            {
                // Intentionally never freed: this is a single, process-lifetime constructors table
                ConstructorsPtr = (Constructors*)NativeMemory.Alloc((nuint)sizeof(Constructors));
                *ConstructorsPtr = new Constructors(
                    (IntPtr)AlreadyExistsConstructorPtr,
                    (IntPtr)FunctionFailureExceptionConstructorPtr,
                    (IntPtr)InvalidConfigurationInQueryExceptionConstructorPtr,
                    (IntPtr)InvalidQueryConstructorPtr,
                    (IntPtr)NoHostAvailableExceptionConstructorPtr,
                    (IntPtr)OperationTimedOutExceptionConstructorPtr,
                    (IntPtr)PreparedQueryNotFoundExceptionConstructorPtr,
                    (IntPtr)RequestInvalidExceptionConstructorPtr,
                    (IntPtr)RustExceptionConstructorPtr,
                    (IntPtr)SyntaxErrorExceptionConstructorPtr,
                    (IntPtr)TraceRetrievalExceptionConstructorPtr,
                    (IntPtr)TruncateExceptionConstructorPtr,
                    (IntPtr)UnauthorizedExceptionConstructorPtr
                );
            }
        }

        internal static Tcb WithTcs(TaskCompletionSource<IntPtr> tcs)
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
                IntPtr completeTaskPtr = (IntPtr)completeTaskDel;
                IntPtr failTaskPtr = (IntPtr)failTaskDel;
                return new Tcb(tcsPtr, completeTaskPtr, failTaskPtr);
            }
        }
    }

    /// <summary>
    /// Table of exception constructors passed to Rust via TCB.
    /// Rust reads constructors from this table to build managed exceptions.
    /// Any changes to this struct must be mirrored in RustBridgeGlobals 
    /// and in Rust code in the exact same order (alphabetical).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal readonly struct Constructors
    {
        internal readonly IntPtr already_exists_constructor;
        internal readonly IntPtr function_failure_exception_constructor;
        internal readonly IntPtr invalid_configuration_in_query_constructor;
        internal readonly IntPtr invalid_query_constructor;
        internal readonly IntPtr no_host_available_exception_constructor;
        internal readonly IntPtr operation_timed_out_exception_constructor;
        internal readonly IntPtr prepared_query_not_found_exception_constructor;
        internal readonly IntPtr request_invalid_exception_constructor;
        internal readonly IntPtr rust_exception_constructor;
        internal readonly IntPtr syntax_error_exception_constructor;
        internal readonly IntPtr trace_retrieval_exception_constructor;
        internal readonly IntPtr truncate_exception_constructor;
        internal readonly IntPtr unauthorized_exception_constructor;

        internal Constructors(
            IntPtr alreadyExistsException,
            IntPtr functionFailureException,
            IntPtr invalidConfigurationInQueryException,
            IntPtr invalidQueryException,
            IntPtr noHostAvailableException,
            IntPtr operationTimedOutException,
            IntPtr preparedQueryNotFoundException,
            IntPtr requestInvalidException,
            IntPtr rustException,
            IntPtr syntaxErrorException,
            IntPtr traceRetrievalException,
            IntPtr truncateException,
            IntPtr unauthorizedException)
        {
            already_exists_constructor = alreadyExistsException;
            function_failure_exception_constructor = functionFailureException;
            invalid_configuration_in_query_constructor = invalidConfigurationInQueryException;
            invalid_query_constructor = invalidQueryException;
            no_host_available_exception_constructor = noHostAvailableException;
            operation_timed_out_exception_constructor = operationTimedOutException;
            prepared_query_not_found_exception_constructor = preparedQueryNotFoundException;
            request_invalid_exception_constructor = requestInvalidException;
            rust_exception_constructor = rustException;
            syntax_error_exception_constructor = syntaxErrorException;
            trace_retrieval_exception_constructor = traceRetrievalException;
            truncate_exception_constructor = truncateException;
            unauthorized_exception_constructor = unauthorizedException;
        }
    }

    static class RustBridge
    {
        /// <summary>
        /// This shall be called by Rust code when the operation is completed.
        /// </summary>
        // Signature in Rust: extern "C" fn(tcs: *mut c_void, res: *mut c_void)
        //
        // This attribute makes the method callable from native code.
        // It also allows taking a function pointer to the method.
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static void CompleteTask(IntPtr tcsPtr, IntPtr resPtr)
        {
            try
            {
                // Recover the GCHandle that was allocated for the TaskCompletionSource.
                var handle = GCHandle.FromIntPtr(tcsPtr);

                if (handle.Target is TaskCompletionSource<IntPtr> tcs)
                {
                    // Simply pass the opaque pointer back as the result.
                    // The Rust code is responsible for interpreting the pointer's contents
                    // and freeing it when no longer needed.
                    tcs.SetResult(resPtr);

                    // Free the handle so the TCS can be collected once no longer used
                    // by the C# code.
                    handle.Free();

                    Console.Error.WriteLine($"[FFI] CompleteTask done.");
                }
                else
                {
                    throw new InvalidOperationException("GCHandle did not reference a TaskCompletionSource<IntPtr>.");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] CompleteTask threw exception: {ex}");
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
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static void FailTask(IntPtr tcsPtr, IntPtr exceptionPtr)
        {
            try
            {
                // Recover the GCHandle that was allocated for the TaskCompletionSource.
                var handle = GCHandle.FromIntPtr(tcsPtr);

                if (handle.Target is TaskCompletionSource<IntPtr> tcs)
                {
                    // Create the exception to pass to the TCS.
                    Exception exception;
                    try
                    {
                        if (exceptionPtr != IntPtr.Zero)
                        {
                            // Recover the exception from the GCHandle passed from Rust.
                            var exHandle = GCHandle.FromIntPtr(exceptionPtr);
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
                        tcs.SetException(exception);
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
                    throw new InvalidOperationException("GCHandle did not reference a TaskCompletionSource<IntPtr>.");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FFI] FailTask threw exception: {ex}");
            }
        }
    }
}