using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>  
    /// Base class for Rust-owned native resources managed through <see cref="SafeHandle"/>.  
    /// </summary>  
    /// <remarks>  
    /// <para>  
    /// This type encapsulates a native pointer returned by Rust together with a corresponding  
    /// destructor function. The destructor is invoked exactly once from <see cref="ReleaseHandle"/>  
    /// when the handle is released by the <see cref="SafeHandle"/> infrastructure.  
    /// </para>  
    /// <para>  
    /// Derive from this class for any Rust resource that needs to be lifetime-managed from C#.  
    /// Implementations must not expose the raw <see cref="SafeHandle.handle"/> to callers except  
    /// where it is safe to do so, and they must respect the ownership model: once wrapped in  
    /// <see cref="RustResource"/>, the native pointer must only be freed by the associated  
    /// destructor.  
    /// </para>  
    /// </remarks> 
    internal abstract class RustResource : SafeHandle
    {
        private readonly IntPtr _destructor;

        internal RustResource(ManuallyDestructible md) : base(IntPtr.Zero, true)
        {
            handle = md.Ptr;
            _destructor = md.Destructor;
        }

        public sealed override bool IsInvalid => handle == IntPtr.Zero;

        /// <summary>  
        /// Releases the underlying native resource by invoking the Rust-provided destructor.  
        /// </summary>  
        /// <remarks>  
        /// This method is called by the <see cref="SafeHandle"/> finalization and disposal  
        /// mechanisms. It must not be called directly by user code. 
        /// </remarks>  
        protected sealed override bool ReleaseHandle()
        {
            if (_destructor != IntPtr.Zero && handle != IntPtr.Zero)
            {
                unsafe
                {
                    var fn = (delegate* unmanaged[Cdecl]<IntPtr, void>)(void*)_destructor;
                    fn(handle);
                    handle = IntPtr.Zero;
                }
            }
            return true;
        }

        /// <summary>
        /// Helper to encapsulate the common pattern for async native calls with incremented ref count:
        /// DangerousAddRef, create TCS, build TCB, invoke native function, finally DangerousRelease.
        /// Using this method ensures that the handle remains valid for the duration of the native call.
        /// Invoke must be a function that takes a Tcb and IntPtr (the handle) and performs the native call.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="invoke"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        internal virtual Task<ManuallyDestructible> RunAsyncWithIncrement<T>(Action<Tcb, IntPtr> invoke)
        {
            bool refAdded = false;
            try
            {
                DangerousAddRef(ref refAdded);

                /*
                * TaskCompletionSource is a way to programatically control a Task.
                * We create one here and pass it to Rust code, which will complete it.
                * This is a common pattern to bridge async code between C# and native code.
                */
                var tcs = new TaskCompletionSource<ManuallyDestructible>(TaskCreationOptions.RunContinuationsAsynchronously);

                // Invoke the native code, which will complete the TCS when done.
                // We need to pass a pointer to CompleteTask because Rust code cannot directly
                // call C# methods.
                // Even though Rust code statically knows the name of the method, it cannot
                // directly call it because the .NET runtime does not expose the method
                // in a way that Rust can call it.
                // So we pass a pointer to the method and Rust code will call it via that pointer.
                // This is a common pattern to call C# code from native code ("reversed P/Invoke").
                var tcb = Tcb.WithTcs(tcs);
                invoke(tcb, handle);
                return (Task<ManuallyDestructible>)(object)tcs.Task;
            }
            finally
            {
                if (refAdded)
                {
                    DangerousRelease();
                }
            }
        }

        /// <summary>
        /// Helper to encapsulate the common pattern for sync native calls with incremented ref count:
        /// DangerousAddRef, invoke native function, finally DangerousRelease.
        /// Using this method ensures that the handle remains valid for the duration of the native call.
        /// Invoke must be a function that takes an IntPtr (the handle) and returns an FfiException.
        /// If the exception is not null, it will be thrown.
        /// </summary>
        /// <param name="invoke"></param>
        internal virtual void RunWithIncrement(Func<IntPtr, RustBridge.FfiException> invoke)
        {
            bool refAdded = false;
            RustBridge.FfiException exception;
            try
            {
                DangerousAddRef(ref refAdded);
                exception = invoke(handle);
                try {
                    RustBridge.ThrowIfException(ref exception);
                }
                finally
                {
                    RustBridge.FreeExceptionHandle(ref exception);
                }
            }
            finally
            {
                if (refAdded)
                {
                    DangerousRelease();
                }
            }
        }

        /// <summary>
        /// Tries to create a RustResource reference to prevent disposal while in use.
        /// Returns true if operation was successful, false otherwise.
        /// Each successful call must be matched with a call to DecreaseReferenceCount().
        /// </summary>
        internal bool TryIncreaseReferenceCount()
        {
            bool refAdded = false;
            try
            {
                DangerousAddRef(ref refAdded);
                return true;
            }
            catch
            {
                if (refAdded)
                {
                    DangerousRelease();
                }
                return false;
            }
        }

        /// <summary>
        /// Decreases the RustResource reference count previously increased by TryIncreaseReferenceCount().
        /// If the reference count reaches zero, the RustResource will be disposed.
        /// </summary>
        internal void DecreaseReferenceCount()
        {
            DangerousRelease();
        }
    }
}