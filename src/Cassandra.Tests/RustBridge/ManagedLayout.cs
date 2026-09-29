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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Cassandra.Tests
{
    /// <summary>
    /// Which register class a primitive field belongs to. Mirrors Rust's <c>AbiKind</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// An offset and a size cannot tell <c>float</c> from <c>int</c> - both are four bytes in the
    /// same place - yet calling conventions pass them in different register files. A struct passed
    /// by value would then be read out of a register the other side never wrote, silently. That is
    /// the one ABI property the offsets do not already cover, which is the whole reason this exists.
    /// </para>
    /// <para>
    /// <see cref="Integer"/> therefore means "general-purpose register class", and covers pointers
    /// and function pointers as well as integers. Pointers are deliberately not split out: they
    /// share a register class with pointer-sized integers, so it would catch nothing, and C# could
    /// not express it in any case - <c>nuint</c> is an alias for <c>System.UIntPtr</c>, so
    /// reflection reports the same type for a real pointer, for a callback stored as <c>IntPtr</c>,
    /// and for Rust's <c>usize</c> in <c>FFISlice::len</c>.
    /// </para>
    /// </remarks>
    internal enum AbiKind : byte
    {
        Integer = 0,
        Float = 1
    }

    /// <summary>
    /// One primitive field, at the offset it actually sits at within the outermost type.
    /// </summary>
    internal readonly struct AbiLeaf
    {
        internal AbiLeaf(int offset, int size, AbiKind kind, string name)
        {
            Offset = offset;
            Size = size;
            Kind = kind;
            Name = name;
        }

        internal int Offset { get; }

        internal int Size { get; }

        internal AbiKind Kind { get; }

        /// <summary>
        /// Dotted path of the field names leading to this leaf, e.g. <c>tcp.tcpNoDelay</c>. Built by
        /// the same rule on both sides - see <see cref="ManagedLayout"/> - and compared through
        /// <see cref="ManagedLayout.NormalizeName"/>.
        /// </summary>
        internal string Name { get; }

        internal bool SameShapeAs(AbiLeaf other)
        {
            return Offset == other.Offset && Size == other.Size && Kind == other.Kind;
        }

        /// <summary>
        /// This leaf as seen from the struct containing <paramref name="field"/>.
        /// </summary>
        internal AbiLeaf NamedAfter(string field)
        {
            return new AbiLeaf(Offset, Size, Kind, Name.Length == 0 ? field : $"{field}.{Name}");
        }

        public override string ToString()
        {
            var kind = Kind == AbiKind.Float ? " float" : string.Empty;
            var name = Name.Length == 0 ? string.Empty : $" ({Name})";
            return $"offset {Offset}, {Size} byte{(Size == 1 ? string.Empty : "s")}{kind}{name}";
        }
    }

    /// <summary>
    /// The layout of a managed FFI mirror struct, flattened into primitive leaves so it can be
    /// compared against the layout the Rust compiler chose.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Offsets and sizes come from <see cref="Marshal"/>, i.e. the marshalled layout - which is what
    /// P/Invoke actually hands to Rust. That is what lets structs holding a
    /// <c>[MarshalAs(UnmanagedType.LPUTF8Str)] string</c> (<c>BridgedSessionConfig</c>,
    /// <c>BridgedLoadBalancingPolicy</c>) be measured correctly even though they are not blittable:
    /// the string marshals to a single pointer.
    /// </para>
    /// <para>
    /// Every leaf is named after the fields leading to it, by the same rule as Rust's
    /// <c>ffi_type::describe_fields</c>: each struct prefixes its fields' leaves with the field
    /// name, unless only one of its fields has any leaves. Such a struct is a wrapper and adds no
    /// segment of its own, which is what lets <c>FFIString { ptr, len }</c> line up with Rust's
    /// <c>FFIStr { slice: FFISlice { ptr, len } }</c>.
    /// </para>
    /// <para>
    /// Nothing here guesses. An unsupported field type throws with an explanation rather than being
    /// silently skipped or approximated, because a skipped field is a layout check that passes while
    /// verifying nothing.
    /// </para>
    /// </remarks>
    internal sealed class ManagedLayout
    {
        private ManagedLayout(int size, int align, IReadOnlyList<AbiLeaf> leaves)
        {
            Size = size;
            Align = align;
            Leaves = leaves;
        }

        internal int Size { get; }

        /// <summary>
        /// Alignment, derived as the widest leaf.
        /// </summary>
        /// <remarks>
        /// <see cref="Marshal"/> exposes no alignment query, so this is computed rather than
        /// guessed: a <c>repr(C)</c> aggregate of self-aligned primitives aligns to its widest
        /// member. Rust asserts the same identity against its real <c>align_of</c> in
        /// <c>abi::tests::alignment_equals_the_widest_leaf</c>, so if a type ever gains an explicit
        /// over-alignment both sides fail loudly instead of quietly disagreeing.
        /// </remarks>
        internal int Align { get; }

        internal IReadOnlyList<AbiLeaf> Leaves { get; }

        /// <summary>
        /// The layout of an enum: a single integer leaf of the underlying type's width. Enums have no
        /// fields to walk, so they are described directly rather than through
        /// <see cref="Of"/>.
        /// </summary>
        internal static ManagedLayout OfEnum(int underlyingSize)
        {
            var leaves = new[] { new AbiLeaf(0, underlyingSize, AbiKind.Integer, string.Empty) };
            return new ManagedLayout(underlyingSize, underlyingSize, leaves);
        }

        internal static ManagedLayout Of(Type type)
        {
            var leaves = Flatten(type, 0, null);
            var align = leaves.Count == 0 ? 1 : leaves.Max(leaf => leaf.Size);
            return new ManagedLayout(Marshal.SizeOf(type), align, leaves);
        }

        /// <summary>
        /// A field name as it takes part in the comparison: case and underscores are ignored, so that
        /// Rust's <c>tcp_nodelay</c> matches C#'s <c>tcpNoDelay</c> and each language keeps its own
        /// naming convention.
        /// </summary>
        internal static string NormalizeName(string name)
        {
            return name.Replace("_", string.Empty).ToLowerInvariant();
        }

        /// <summary>
        /// Instance fields of <paramref name="type"/>, in ascending offset order.
        /// </summary>
        internal static IReadOnlyList<(FieldInfo Field, int Offset)> OrderedFields(Type type)
        {
            return type
                .GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Select(field => (Field: field, Offset: (int)Marshal.OffsetOf(type, field.Name)))
                .OrderBy(entry => entry.Offset)
                .ToArray();
        }

        /// <summary>
        /// Instance fields of <paramref name="type"/>, in the order reflection reports them.
        /// </summary>
        internal static IReadOnlyList<FieldInfo> DeclaredFields(Type type)
        {
            return type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }

        /// <param name="path">
        /// Full dotted path to <paramref name="type"/>, wrappers included - for error messages only.
        /// </param>
        private static List<AbiLeaf> Flatten(Type type, int baseOffset, string path)
        {
            var fields = new List<(string Name, List<AbiLeaf> Leaves)>();
            foreach (var (field, offset) in OrderedFields(type))
            {
                var fieldPath = path == null ? field.Name : $"{path}.{field.Name}";
                var primitive = Classify(field, fieldPath);
                var fieldLeaves = primitive.HasValue
                    ? new List<AbiLeaf> { new AbiLeaf(baseOffset + offset, primitive.Value.Size, primitive.Value.Kind, string.Empty) }
                    : Flatten(field.FieldType, baseOffset + offset, fieldPath);
                fields.Add((field.Name, fieldLeaves));
            }

            // The wrapper rule; see the class remarks.
            var isWrapper = fields.Count(field => field.Leaves.Count > 0) == 1;
            return fields
                .SelectMany(field => field.Leaves.Select(leaf => isWrapper ? leaf : leaf.NamedAfter(field.Name)))
                .ToList();
        }

        /// <summary>
        /// Classifies a field as a primitive leaf, or returns null to mean "recurse into it".
        /// </summary>
        private static (int Size, AbiKind Kind)? Classify(FieldInfo field, string path)
        {
            var type = field.FieldType;
            if (type.IsEnum)
            {
                type = Enum.GetUnderlyingType(type);
            }

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return (1, AbiKind.Integer);
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return (2, AbiKind.Integer);
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return (4, AbiKind.Integer);
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return (8, AbiKind.Integer);
            }

            if (type == typeof(float))
            {
                return (4, AbiKind.Float);
            }

            if (type == typeof(double))
            {
                return (8, AbiKind.Float);
            }

            if (type == typeof(IntPtr) || type == typeof(UIntPtr) || type.IsPointer)
            {
                return (IntPtr.Size, AbiKind.Integer);
            }

            if (type == typeof(string))
            {
                // Every string marshalling form occupies a single pointer inside a struct, except
                // ByValTStr, which inlines the characters.
                var marshalAs = field.GetCustomAttribute<MarshalAsAttribute>();
                if (marshalAs != null && marshalAs.Value == UnmanagedType.ByValTStr)
                {
                    throw new NotSupportedException(
                        $"{path} is marshalled as ByValTStr, which inlines its characters instead of " +
                        "being a pointer. The Rust side expects a pointer (CSharpStr).");
                }

                return (IntPtr.Size, AbiKind.Integer);
            }

            if (type == typeof(bool))
            {
                throw new NotSupportedException(
                    $"{path} is a `bool`, which marshals to 4 bytes by default and so does not match " +
                    "Rust's 1-byte bool. Declare it as FFIBool.");
            }

            if (type == typeof(char))
            {
                throw new NotSupportedException(
                    $"{path} is a `char`, whose marshalled width depends on the CharSet. Use byte or " +
                    "ushort so the width is unambiguous.");
            }

            if (type.IsValueType && !type.IsPrimitive)
            {
                // A nested struct: recurse so its fields flatten into the same list.
                return null;
            }

            throw new NotSupportedException(
                $"{path} has type {type} which the ABI comparison does not know how to measure. " +
                "Add it to ManagedLayout.Classify if it genuinely crosses the FFI boundary.");
        }
    }
}
