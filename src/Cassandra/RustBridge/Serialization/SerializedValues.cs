using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Cassandra.Serialization;

namespace Cassandra
{
    internal sealed class SerializedValues : SafeHandle, ISerializedValues
    {
        private readonly ISerializer _serializer;

        // This class manages the lifetime of the native PreSerializedValues instance.
        // It inherits from SafeHandle to ensure that the native memory is freed (via pre_serialized_values_free)
        // if the instance is disposed or finalized without having been consumed by a query.
        // Calling TakeNativeHandle() transfers ownership to the caller (and ultimately the native driver),
        // preventing the SafeHandle from freeing the resource.
        internal SerializedValues(ISerializer serializer) : base(IntPtr.Zero, true)
        {
            _serializer = serializer;
            var h = pre_serialized_values_new();
            if (h == IntPtr.Zero)
            {
                throw new InvalidOperationException("pre_serialized_values_new returned null (failed to create PreSerializedValues)");
            }
            SetHandle(h);
        }

        public override bool IsInvalid => handle == IntPtr.Zero;

        /// <summary>
        /// Transfers ownership of the underlying native PreSerializedValues handle to the caller.
        /// This method can only be called once; subsequent calls will throw.
        /// </summary>
        public IntPtr TakeNativeHandle()
        {
            if (IsInvalid)
            {
                throw new InvalidOperationException("The native handle has already been consumed");
            }

            var h = DangerousGetHandle();

            // Detach the handle from this wrapper; the caller now owns it and is responsible
            // for ultimately passing it to a Rust-side query call that consumes/destroys it.
            SetHandleAsInvalid();
            return h;
        }

        protected override bool ReleaseHandle()
        {
            pre_serialized_values_free(handle);
            return true;
        }

        internal void AddMany(IEnumerable<object> values)
        {
            foreach (var v in values)
            {
                Add(v);
            }
        }

        private void Add(object value)
        {
            if (value == null)
            {
                unsafe
                {
                    var res = pre_serialized_values_add_null(handle, (IntPtr)RustBridge.Globals.ConstructorsPtr);
                    try
                    {
                        RustBridge.ThrowIfException(ref res);
                    }
                    finally
                    {
                        RustBridge.FreeExceptionHandle(ref res);
                    }
                }
                return;
            }
            if (ReferenceEquals(value, Unset.Value))
            {
                unsafe
                {
                    var res = pre_serialized_values_add_unset(handle, (IntPtr)RustBridge.Globals.ConstructorsPtr);
                    try
                    {
                        RustBridge.ThrowIfException(ref res);
                    }
                    finally
                    {
                        RustBridge.FreeExceptionHandle(ref res);
                    }
                    return;
                }
            }
            AddValue(_serializer.Serialize(value));
        }

        private void AddValue(byte[] buf)
        {
            unsafe
            {
                fixed (byte* ptr = buf)
                {
                    IntPtr valuePtr = (IntPtr)ptr;
                    UIntPtr valueLen = (UIntPtr)buf.Length;
                    RustBridge.FFIByteSlice valueSlice = new(valuePtr, valueLen);

                    var res = pre_serialized_values_add_value(handle, valueSlice, (IntPtr)RustBridge.Globals.ConstructorsPtr);
                    try
                    {
                        RustBridge.ThrowIfException(ref res);
                    }
                    finally
                    {
                        RustBridge.FreeExceptionHandle(ref res);
                    }

                }
            }
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pre_serialized_values_new();

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pre_serialized_values_free(IntPtr ptr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIException pre_serialized_values_add_unset(IntPtr valuesPtr, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIException pre_serialized_values_add_null(IntPtr valuesPtr, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIException pre_serialized_values_add_value(
            IntPtr valuesPtr,
            RustBridge.FFIByteSlice valueSlice,
            IntPtr constructorsPtr);
    }
}
