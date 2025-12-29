//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Net;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Exception that thrown on a client-side timeout, when the client didn't hear back from the server within <see cref="SocketOptions.ReadTimeoutMillis"/>.
    /// </summary>
    public class OperationTimedOutException : DriverException
    {
        public OperationTimedOutException(IPEndPoint address, int timeout) :
            base($"The host {address} did not reply before timeout {timeout}ms")
        {
        }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr OperationTimedOutExceptionFromRust(FFIString address, int timeout)
        {
            string addressString = address.ToManagedString();
            IPEndPoint addr;
            Exception exception;
            try
            {
                addr = IPEndPoint.Parse(addressString);
                exception = new OperationTimedOutException(addr, timeout);
            }
            catch (FormatException)
            {
                // Invalid address string; return a handle to a generic exception
                exception = new RustException("Failed to parse IPEndPoint from Rust");
            }

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);
            return handlePtr;
        }
    }
}