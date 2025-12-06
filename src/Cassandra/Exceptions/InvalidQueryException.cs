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
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    /// <summary>
    ///  Indicates a syntactically correct but invalid query.
    /// </summary>
    public class InvalidQueryException : QueryValidationException
    {
        public InvalidQueryException(string message)
            : base(message)
        {
        }

        public InvalidQueryException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        public static void InvalidQueryExceptionFromRust(IntPtr messageIntPtr, IntPtr bufferPtr)
        {
            InvalidQueryException exception;
            if (messageIntPtr != IntPtr.Zero)
            {
                string message = Marshal.PtrToStringUTF8(messageIntPtr) ?? string.Empty;
                exception = new InvalidQueryException(message);
            } else {
                exception = new InvalidQueryException("The query is syntactically correct but invalid.");
            }

            GCHandle handle = GCHandle.Alloc(exception);
            IntPtr handlePtr = GCHandle.ToIntPtr(handle);

            if (bufferPtr != IntPtr.Zero)
            {
                Marshal.WriteIntPtr(bufferPtr, handlePtr);
            }
        }
    }
}
