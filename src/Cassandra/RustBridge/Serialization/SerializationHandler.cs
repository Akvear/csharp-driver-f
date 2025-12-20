using System;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Virtualizes the whole serialization process for values to be passed to Rust FFI calls.
    /// </summary>
    internal static class SerializationHandler
    {
        // The returned ISerializedValues (which is a SafeHandle) manages the native memory lifetime.
        // If the query is not executed, the handle will eventually be released by the GC/Finalizer,
        // preventing leaks. However, for the query to execute, the handle must be passed to the
        // native driver via TakeNativeHandle().
        internal static ISerializedValues InitializeSerializedValues(IEnumerable<object> values)
        {
            ArgumentNullException.ThrowIfNull(values);

            // Create the SerializedValues instance (which allocates the native container)
            // and populate it. If population fails, the instance is disposed, freeing the native memory immediately.
            var serializedValues = new SerializedValues();
            try
            {
                serializedValues.AddMany(values);
                return serializedValues;
            }
            catch
            {
                // Explicitly dispose to release native memory immediately, rather than waiting for the GC.
                serializedValues.Dispose();
                throw;
            }
        }
    }
}
