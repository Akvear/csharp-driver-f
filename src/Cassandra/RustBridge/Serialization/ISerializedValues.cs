using System;

namespace Cassandra
{
    internal interface ISerializedValues : IDisposable
    {
        // This interface represents a handle to a native PreSerializedValues container.
        // It implements IDisposable (via SafeHandle) to ensure that native resources are
        // freed if they are not consumed by a query.
        //
        // LIFETIME CONTRACT:
        //  - If the values are NOT used for a query, the instance should be disposed (or left to the GC),
        //    which will trigger the native free function.
        //  - TakeNativeHandle() is a one-shot operation that transfers ownership of the
        //    underlying native handle to the caller.
        //  - AFTER calling TakeNativeHandle(), the caller MUST guarantee that the returned
        //    handle is immediately and unconditionally consumed by a native query
        //    function that ultimately DROPS the underlying PreSerializedValues instance.
        //
        // Example of correct usage:
        //
        //   using (var serializedValues = SerializationHandler.InitializeSerializedValues(queryValues))
        //   {
        //       // ... some logic ...
        //       session_query_with_values(..., serializedValues.TakeNativeHandle());
        //   }
        //
        // Or inline if no logic intervenes:
        //
        //   session_query_with_values(..., SerializationHandler.InitializeSerializedValues(queryValues).TakeNativeHandle());
        
        /// <summary>
        /// Transfers ownership of the underlying native handle to the caller.
        /// The ISerializedValues instance yields ownership and will no longer free the handle upon disposal.
        /// </summary>
        IntPtr TakeNativeHandle();
    }
}
