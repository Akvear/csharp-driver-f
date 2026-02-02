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

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Cassandra
{
    /// <summary>
    ///  Exception thrown if a query trace cannot be retrieved.
    /// </summary>
    public class TraceRetrievalException : DriverException
    {
        public TraceRetrievalException(string message)
            : base(message)
        {
        }

        public TraceRetrievalException(string message, Exception cause)
            : base(message, cause)
        {
        }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static IntPtr TraceRetrievalExceptionFromRust(RustBridge.FFIString message)
        {
            string messageStr = message.ToManagedString();
            var exception = new TraceRetrievalException(messageStr);

            GCHandle handle = GCHandle.Alloc(exception);
            return GCHandle.ToIntPtr(handle);
        }
    }
}
