using System;

using System.Runtime.InteropServices;
using System.Threading.Tasks;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Bridge class for an empty Rust where handle and destructor are nullptrs.
    /// This is useful for APIs that need to return a RustResource but have no actual resource to manage.
    /// </summary>
    internal sealed class EmptyRustResource : RustResource
    {
        internal EmptyRustResource(ManuallyDestructible mdEmpty) : base(mdEmpty)
        {
        }

        internal override void RunWithIncrement(Func<IntPtr, RustBridge.FFIException> invoke)
        {
            Environment.FailFast("Attempted to use a null RustResource.");
        }

        internal override Task<ManuallyDestructible> RunAsyncWithIncrement<T>(Action<Tcb, IntPtr> invoke)
        {
            Environment.FailFast("Attempted to use a null RustResource.");
            return null;
        }
    }
}
