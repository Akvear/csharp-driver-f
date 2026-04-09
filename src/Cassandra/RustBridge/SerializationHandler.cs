#nullable enable
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Managed context passed to the populate-values callback via an opaque pointer.
    /// Allocated on the caller's stack; its address is taken before the lambda capture
    /// so the struct is never lifted to the heap and the pointer stays valid for the
    /// duration of the synchronous FFI call.
    /// </summary>
    internal struct PopulateValuesContext
    {
        internal readonly IReadOnlyList<object?> Values;
        internal readonly ISerializer Serializer;

        internal PopulateValuesContext(IReadOnlyList<object?> values, ISerializer serializer)
        {
            Values = values;
            Serializer = serializer;
        }
    }

    /// <summary>
    /// Orchestrates the serialization process for values passed to Rust FFI calls
    /// via the builder-callback pattern.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rust creates a <c>PreSerializedValues</c> on its stack and calls the C#
    /// <see cref="PopulateValuesCallback"/>, passing a raw pointer to the PSV.
    /// C# iterates the values and calls back into Rust via the exported
    /// <c>psv_add_value</c> / <c>psv_add_null</c> / <c>psv_add_unset</c> functions,
    /// using <c>fixed</c> to pin each serialized byte[] for the duration of the call
    /// (no <see cref="GCHandle"/> needed).
    /// </para>
    /// <para>
    /// Errors from both C# (serialization failures) and Rust (PSV add failures)
    /// are propagated via <see cref="RustBridge.FFIMaybeException"/>.
    /// </para>
    /// </remarks>
    internal static class SerializationHandler
    {
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException psv_add_value(IntPtr psv, FFISlice<byte> value, IntPtr constructors);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException psv_add_null(IntPtr psv, IntPtr constructors);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIMaybeException psv_add_unset(IntPtr psv, IntPtr constructors);

        /// <summary>
        /// Function pointer to the populate-values callback, suitable for passing to Rust.
        /// </summary>
        internal static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, RustBridge.FFIMaybeException>
            PopulateValuesPtr = &PopulateValuesCallback;

        /// <summary>
        /// Callback invoked by Rust to let C# populate a stack-allocated PSV.
        /// </summary>
        /// <returns><see cref="RustBridge.FFIMaybeException.Ok"/> on success, or an exception on error.</returns>
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe RustBridge.FFIMaybeException PopulateValuesCallback(IntPtr contextPtr, IntPtr psvPtr)
        {
            ref var ctx = ref Unsafe.AsRef<PopulateValuesContext>((void*)contextPtr);

            try
            {
                var constructorsPtr = (IntPtr)Globals.ConstructorsPtr;

                foreach (var value in ctx.Values)
                {
                    RustBridge.FFIMaybeException result;

                    if (value == null)
                    {
                        result = psv_add_null(psvPtr, constructorsPtr);
                    }
                    else if (ReferenceEquals(value, Cassandra.Unset.Value))
                    {
                        result = psv_add_unset(psvPtr, constructorsPtr);
                    }
                    else
                    {
                        byte[] buf = ctx.Serializer.Serialize(value);
                        fixed (byte* ptr = buf)
                        {
                            var slice = new FFISlice<byte>((IntPtr)ptr, (nuint)buf.Length);
                            result = psv_add_value(psvPtr, slice, constructorsPtr);
                        }
                    }

                    if (result.HasException)
                    {
                        return result;
                    }
                }

                return RustBridge.FFIMaybeException.Ok();
            }
            catch (Exception ex)
            {
                return RustBridge.FFIMaybeException.FromException(ex);
            }
        }

        internal static PopulateValuesContext CreateContext(IReadOnlyList<object?> values, ISerializer serializer)
        {
            ArgumentNullException.ThrowIfNull(values);
            ArgumentNullException.ThrowIfNull(serializer);
            return new PopulateValuesContext(values, serializer);
        }
    }
}
