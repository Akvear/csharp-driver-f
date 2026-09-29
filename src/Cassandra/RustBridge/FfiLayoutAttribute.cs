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

namespace Cassandra
{
    /// <summary>
    /// Marks a struct or enum as the managed mirror of a Rust type whose layout is verified against
    /// it. <c>FfiAbiTests</c> discovers every annotated type by reflection and compares it against
    /// the layout the Rust compiler actually chose, so a field added on one side and forgotten on
    /// the other fails a test instead of silently corrupting memory.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pairing deliberately lives on the type rather than in a table inside the test project:
    /// a table is a second place to keep in sync, and the prototype this replaces failed exactly
    /// there. The Rust side lists the type in <c>rust/src/abi.rs</c>, and the test asserts the
    /// pairing is complete in both directions - so annotating a type that Rust does not register,
    /// or registering a type that no managed type claims, is a failure rather than a silent gap.
    /// </para>
    /// <para>
    /// The name is the Rust type's own name without its generic parameters (<c>Tcb&lt;R&gt;</c> is
    /// <c>"Tcb"</c>), unless the Rust type overrides it with <c>#[ffi_type(name = "...")]</c>. The
    /// Rust spelling is canonical where the two languages disagree: C#'s
    /// <c>PreparedStatementExecutionOptions</c> claims <c>"BoundStatementExecutionOptions"</c>, and
    /// <c>FFIString</c> claims <c>"FFIStr"</c>.
    /// </para>
    /// <para>
    /// Field names, on the other hand, must match: they are what tells two same-width fields apart.
    /// They are compared ignoring case and underscores, so each side keeps its own convention
    /// (Rust's <c>tcp_nodelay</c> is C#'s <c>tcpNoDelay</c>), and a struct wrapping a single field
    /// adds no name of its own (C#'s flat <c>FFIString { ptr, len }</c> matches Rust's
    /// <c>FFIStr { slice: FFISlice { ptr, len } }</c>).
    /// </para>
    /// <para>
    /// Multiple types may claim the same Rust name (<c>FFISlice&lt;T&gt;</c> and
    /// <c>FFISliceRaw</c> both mirror <c>FFISlice</c>), and one type may claim several names
    /// (<c>FFIGCHandle</c> mirrors both <c>FFIGCHandle</c> and the transparent newtype over it,
    /// <c>FFIException</c>).
    /// </para>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Struct | AttributeTargets.Enum, AllowMultiple = true)]
    internal sealed class FfiLayoutAttribute : Attribute
    {
        /// <summary>
        /// The name the mirrored Rust type is registered under - its <c>FFITypeName::NAME</c>.
        /// </summary>
        public string RustName { get; }

        public FfiLayoutAttribute(string rustName)
        {
            RustName = rustName;
        }
    }
}
