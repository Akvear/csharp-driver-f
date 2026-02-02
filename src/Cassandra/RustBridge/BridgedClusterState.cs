using System;
using System.Runtime.InteropServices;
using System.Net;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;

namespace Cassandra
{
    internal sealed class BridgedClusterState : RustResource
    {
        internal BridgedClusterState(ManuallyDestructible mdClusterState) : base(mdClusterState)
        {
        }

        internal bool Equals(BridgedClusterState other)
        {
            if (other is null) return false;
            return handle == other.handle;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FFIException cluster_state_fill_nodes(
            IntPtr clusterState,
            IntPtr contextPtr,
            IntPtr callback);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIByteSlice, FFIByteSlice, ushort, FFIString, FFIString, void> AddHostPtr = &AddHostToList;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe void AddHostToList(
            IntPtr contextPtr,
            FFIByteSlice idBytes,
            FFIByteSlice ipBytes,
            ushort port,
            FFIString datacenter,
            FFIString rack)
        {
            try
            {
                // Safety: 
                // contextPtr is a pointer to the stack slot holding the 'context' reference (not to the heap object itself).
                // Unsafe.AsPointer(ref T) returns the address of the managed pointer (the stack local).
                // The stack slot is stable for the duration of this callback since:
                // 1. cluster_state_fill_nodes calls this callback synchronously before returning
                // 2. The stack frame containing 'context' remains alive throughout the FFI call
                // 3. If GC moves the RefreshContext object on the heap, it updates the reference value in the stack slot
                // 4. Unsafe.Read dereferences the pointer to get the current reference value
                // This matches the pattern used in row_set_fill_columns_metadata.
                var context = Unsafe.AsRef<Metadata.RefreshContext>((void*)contextPtr);

                var hostId = new Guid(idBytes.ToSpan());

                // Construct IPAddress directly from bytes (4 for IPv4, 16 for IPv6). ipBytes is an FFIByteSlice 
                // and it accesses unmanaged memory that is only valid for the duration of this callback invocation. 
                // The IPAddress constructor must be called synchronously here so it can copy the data immediately.
                var ipAddress = new IPAddress(ipBytes.ToSpan());
                var address = new IPEndPoint(ipAddress, port);

                // Try to reuse existing host object if id matches and address is the same
                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    // If the address matches, reuse the instance.
                    if (host.Address.Equals(address))
                    {
                        context.AddHost(host);
                        return;
                    }
                }

                // If either datacenter or rack is null, the method ToManagedString returns null.
                var dcString = datacenter.ToManagedString();
                var rackString = rack.ToManagedString();

                // Create Host instance and add it to the dictionaries.
                host = new Host(address, hostId, dcString, rackString);
                context.AddHost(host);
            }
            catch (Exception ex)
            {
                // Do not throw across FFI boundary - causes undefined behavior.
                // Fail fast to match Rust's panic=abort behavior and make the error obvious.
                Environment.FailFast("Fatal error in AddHostCallback", ex);
            }
        }

        internal void FillHostCache(Metadata.RefreshContext context)
        {
            unsafe
            {
                RunWithIncrement(handle =>
                    cluster_state_fill_nodes(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref context),
                        (IntPtr)AddHostPtr
                    )
                );
            }

            GC.KeepAlive(context);
        }
    }
}