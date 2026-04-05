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
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Exception that is thrown when the driver expected a type and other was provided
    /// </summary>
    public class InvalidTypeException : DriverException
    {
        public object ReceivedType { get; private set; }
        public object[] ExpectedType { get; private set; }
        public String ParamName { get; private set; }

        public InvalidTypeException(String msg)
            : base(msg)
        {
        }

        public InvalidTypeException(String msg, Exception cause)
            : base(msg, cause)
        {
        }

        public InvalidTypeException(String paramName, object receivedType, object[] expectedType)
            : base(string.Format("Received object of type: {0}, expected: {1} {2}. Parameter name that caused exception: {3}",
                                 receivedType, expectedType[0], expectedType.Length > 1 ? "or" + expectedType[1] : "", paramName))
        {
            ReceivedType = receivedType;
            ExpectedType = expectedType;
            ParamName = paramName;
        }

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static FFIGCHandle InvalidTypeExceptionFromRust(FFIString message)
        {
            string msg = message.ToManagedString();
            var exception = new InvalidTypeException(msg);

            GCHandle handle = GCHandle.Alloc(exception);
            return new(handle);
        }
    }
}
