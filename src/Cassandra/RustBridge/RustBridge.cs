using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Threading;
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
    /// Task Control Block groups entities crucial for controlling Task execution
    /// from Rust code. It's intended to:
    /// - hide some complexity of the interop,
    /// - reduce code duplication,
    /// - squeeze 3 native function parameters into 1.
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

        private Tcb(IntPtr tcs, IntPtr completeTask, IntPtr failTask)
        {
            this.tcs = tcs;
            this.complete_task = completeTask;
            this.fail_task = failTask;
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
        // Signature in Rust: extern "C" fn(tcs: *mut c_void, error_msg: *const c_char)
        //
        // This attribute makes the method callable from native code.
        // It also allows taking a function pointer to the method.
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static void FailTask(IntPtr tcsPtr, IntPtr errorMsgPtr)
        {
            try
            {
                // Recover the GCHandle that was allocated for the TaskCompletionSource.
                var handle = GCHandle.FromIntPtr(tcsPtr);

                if (handle.Target is TaskCompletionSource<IntPtr> tcs)
                {
                    // Interpret as ANSI C string (nul-terminated)
                    string errorMsg = Marshal.PtrToStringUTF8(errorMsgPtr)!;
                    tcs.SetException(new RustException(errorMsg));

                    // Free the handle so the TCS can be collected once no longer used
                    // by the C# code.
                    handle.Free();

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