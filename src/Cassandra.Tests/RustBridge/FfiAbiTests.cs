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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace Cassandra.Tests
{
    /// <summary>
    /// Verifies that every C# struct mirroring a Rust FFI type really has the layout Rust chose.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The boundary is maintained by hand on both sides, and the only thing that used to keep the two
    /// in step was a doc comment saying "mirror all changes in the exact same order". A desync there
    /// is silent memory corruption at run time, not a compile error - so these tests exist to turn it
    /// into a specific, named test failure.
    /// </para>
    /// <para>
    /// The comparison is over *flattened primitive leaves*: Rust reports, for each registered type,
    /// the name/offset/size/kind of every primitive after recursing through nested structs and
    /// transparent newtypes, and this fixture computes the same list for the managed mirror. What
    /// cannot change unnoticed is a field's width, position, existence, or name.
    /// </para>
    /// <para>
    /// Names matter because most mirrors are nothing but pointers, and offsets alone cannot tell two
    /// same-width fields apart: swapping <c>complete_task</c> and <c>fail_task</c> in <c>Tcb</c> on
    /// one side only keeps every offset intact. Names are compared ignoring case and underscores,
    /// and a single-field wrapper contributes no name of its own (see <see cref="ManagedLayout"/>),
    /// so <c>FFIString</c>'s two fields still match Rust's nested <c>FFIStr.slice</c>.
    /// </para>
    /// <para>
    /// Registration is one line per type on each side: <c>rust/src/abi.rs</c> lists the Rust type,
    /// and <c>[FfiLayout("...")]</c> marks the managed mirror. The pairing is asserted complete in
    /// both directions, so a one-sided omission fails rather than silently reducing coverage.
    /// </para>
    /// </remarks>
    public class FfiAbiTests : BaseUnitTest
    {
        // The fields of the transport structs below are written by native code through `out`
        // parameters, which the compiler cannot see.
#pragma warning disable CS0649

        /// <summary>Mirror of Rust's <c>AbiTypeInfo</c>.</summary>
        [StructLayout(LayoutKind.Sequential)]
        [FfiLayout("AbiTypeInfo")]
        internal struct AbiTypeInfo
        {
            internal RustBridge.FFIString Name;
            internal nuint Size;
            internal nuint Align;
            internal nuint LeafCount;
            internal nuint VariantCount;
        }

        /// <summary>Mirror of Rust's <c>AbiLeafInfo</c>.</summary>
        [StructLayout(LayoutKind.Sequential)]
        [FfiLayout("AbiLeafInfo")]
        internal struct AbiLeafInfo
        {
            internal RustBridge.FFIString Name;
            internal nuint Offset;
            internal nuint Size;
            internal byte Kind;
        }

        /// <summary>Mirror of Rust's <c>AbiVariantInfo</c>.</summary>
        [StructLayout(LayoutKind.Sequential)]
        [FfiLayout("AbiVariantInfo")]
        internal struct AbiVariantInfo
        {
            internal RustBridge.FFIString Name;
            internal long Value;
        }

        /// <summary>
        /// Non-generic stand-in for <c>Tcb&lt;R&gt;</c>.
        /// </summary>
        /// <remarks>
        /// <c>Marshal.OffsetOf</c> refuses generic types even when closed, so the generic mirrors are
        /// measured through a stand-in whose field names, field types and total size are asserted to
        /// match the real thing (see <see cref="EveryGenericMirror_IsFaithfullyRepresentedByItsStandIn"/>).
        /// That keeps full per-field coverage without the stand-in being able to drift silently.
        /// </remarks>
        [StructLayout(LayoutKind.Sequential)]
        [FfiLayout("Tcb")]
        internal struct TcbStandIn
        {
            internal RustBridge.FFIGCHandle Tcs;
            internal IntPtr CompleteTask;
            internal IntPtr FailTask;
            internal IntPtr Constructors;
        }

#pragma warning restore CS0649

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern nuint ffi_abi_type_count();

        // The Rust functions return `FFIBool`, a `#[repr(transparent)]` newtype over `u8`; declaring
        // the return as `byte` here is the same ABI and avoids marshalling a one-byte struct.
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern byte ffi_abi_type_info(nuint index, out AbiTypeInfo info);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern byte ffi_abi_leaf_info(nuint typeIndex, nuint leafIndex, out AbiLeafInfo info);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern byte ffi_abi_variant_info(nuint typeIndex, nuint variantIndex, out AbiVariantInfo info);

        /// <summary>One registered Rust type, as reported by the native library.</summary>
        private sealed class RustType
        {
            internal string Name { get; set; }

            internal int Size { get; set; }

            internal int Align { get; set; }

            internal List<AbiLeaf> Leaves { get; } = new List<AbiLeaf>();

            internal List<KeyValuePair<string, long>> Variants { get; } = new List<KeyValuePair<string, long>>();
        }

        /// <summary>
        /// The generic managed mirrors, each paired with the non-generic stand-in that is measured on
        /// its behalf. <c>Unsafe.SizeOf</c> is called with an explicit type argument so the closed
        /// generic is resolved by the compiler rather than by reflection.
        /// </summary>
        private static readonly (Type Definition, Type Argument, Type StandIn, int ClosedSize)[] GenericMirrors =
        {
            (typeof(RustBridge.FFISlice<>), typeof(byte), typeof(RustBridge.FFISliceRaw),
                Unsafe.SizeOf<RustBridge.FFISlice<byte>>()),
            (typeof(RustBridge.Tcb<>), typeof(RustBridge.FFIBool), typeof(TcbStandIn),
                Unsafe.SizeOf<RustBridge.Tcb<RustBridge.FFIBool>>()),
        };

        private static List<RustType> _rustTypes;

        [OneTimeSetUp]
        public void LoadRustManifest()
        {
            nuint count;
            try
            {
                count = ffi_abi_type_count();
            }
            catch (Exception ex) when (ex is EntryPointNotFoundException || ex is DllNotFoundException)
            {
                // Deliberately a failure and not Assert.Ignore: a silently skipped layout check is
                // how this rots.
                Assert.Fail(
                    "Could not read the ABI manifest from the native library. The ffi_abi_* exports " +
                    "only exist when Rust is built with `--features integration_testing`. Run " +
                    "`make test-unit` (or `make build-rust-testing` first) instead of building Rust " +
                    $"plainly. Underlying error: {ex.GetType().Name}: {ex.Message}");
                return;
            }

            var types = new List<RustType>();
            for (nuint index = 0; index < count; index++)
            {
                Assert.That(ffi_abi_type_info(index, out var info), Is.Not.EqualTo(0),
                    $"ffi_abi_type_info rejected index {index} of {count}");

                var type = new RustType
                {
                    Name = info.Name.ToManagedString(),
                    Size = checked((int)info.Size),
                    Align = checked((int)info.Align),
                };

                for (nuint leafIndex = 0; leafIndex < info.LeafCount; leafIndex++)
                {
                    Assert.That(ffi_abi_leaf_info(index, leafIndex, out var leaf), Is.Not.EqualTo(0),
                        $"ffi_abi_leaf_info rejected leaf {leafIndex} of {type.Name}");
                    type.Leaves.Add(new AbiLeaf(
                        checked((int)leaf.Offset),
                        checked((int)leaf.Size),
                        (AbiKind)leaf.Kind,
                        leaf.Name.ToManagedString()));
                }

                for (nuint variantIndex = 0; variantIndex < info.VariantCount; variantIndex++)
                {
                    Assert.That(ffi_abi_variant_info(index, variantIndex, out var variant), Is.Not.EqualTo(0),
                        $"ffi_abi_variant_info rejected variant {variantIndex} of {type.Name}");
                    type.Variants.Add(new KeyValuePair<string, long>(
                        variant.Name.ToManagedString(),
                        variant.Value));
                }

                types.Add(type);
            }

            Assert.That(types, Is.Not.Empty, "the Rust ABI registry reported no types at all");
            _rustTypes = types;
        }

        [Test]
        public void EveryRustType_IsClaimedByAManagedMirror()
        {
            var claimed = ManagedMirrors().Select(mirror => mirror.RustName).ToHashSet();
            var unclaimed = _rustTypes.Select(type => type.Name).Where(name => !claimed.Contains(name)).ToArray();

            Assert.That(unclaimed, Is.Empty,
                "these types are registered in rust/src/abi.rs but no managed type carries a matching " +
                $"[FfiLayout(\"...\")]: {string.Join(", ", unclaimed)}");
        }

        [Test]
        public void EveryManagedMirror_IsRegisteredOnTheRustSide()
        {
            var registered = _rustTypes.Select(type => type.Name).ToHashSet();
            var unregistered = ManagedMirrors()
                .Where(mirror => !registered.Contains(mirror.RustName))
                .Select(mirror => $"{mirror.Type.Name} -> \"{mirror.RustName}\"")
                .ToArray();

            Assert.That(unregistered, Is.Empty,
                "these managed types claim to mirror a Rust type that is not in the registry in " +
                $"rust/src/abi.rs: {string.Join(", ", unregistered)}");
        }

        [Test]
        public void NoRustTypeIsRegisteredTwice()
        {
            var duplicates = _rustTypes
                .GroupBy(type => type.Name)
                .Where(group => group.Count() > 1)
                .Select(group => group.Key)
                .ToArray();

            // A duplicate name would silently make one of the two entries unverified.
            Assert.That(duplicates, Is.Empty, $"duplicate names in the Rust registry: {string.Join(", ", duplicates)}");
        }

        [Test]
        public void EveryMirror_HasTheSameSizeAndAlignment()
        {
            foreach (var (rust, managed, type) in Pairs())
            {
                Assert.That(managed.Size, Is.EqualTo(rust.Size),
                    $"{Describe(type, rust)}: managed size {managed.Size}, Rust size {rust.Size}");
                Assert.That(managed.Align, Is.EqualTo(rust.Align),
                    $"{Describe(type, rust)}: managed alignment {managed.Align} (widest leaf), " +
                    $"Rust alignment {rust.Align}");
            }
        }

        [Test]
        public void EveryMirror_HasIdenticalPrimitiveLayout()
        {
            foreach (var (rust, managed, type) in Pairs())
            {
                Assert.That(managed.Leaves.Count, Is.EqualTo(rust.Leaves.Count),
                    $"{Describe(type, rust)}: managed side has {managed.Leaves.Count} primitive " +
                    $"field(s), Rust has {rust.Leaves.Count}. Managed: " +
                    $"[{string.Join("; ", managed.Leaves)}]. Rust: [{string.Join("; ", rust.Leaves)}]");

                for (var index = 0; index < rust.Leaves.Count; index++)
                {
                    var expected = rust.Leaves[index];
                    var actual = managed.Leaves[index];
                    Assert.That(actual.SameShapeAs(expected), Is.True,
                        $"{Describe(type, rust)}: primitive field {index} is at {actual} on the " +
                        $"managed side but at {expected} in Rust");
                }
            }
        }

        [Test]
        public void EveryMirror_NamesItsFieldsLikeRustInTheSameOrder()
        {
            // The layout test above cannot see two same-width fields swapped on one side only - and
            // most mirrors are nothing but pointers, so for them that is the likeliest mistake.
            foreach (var (rust, managed, type) in Pairs())
            {
                var rustNames = rust.Leaves.Select(leaf => ManagedLayout.NormalizeName(leaf.Name)).ToArray();
                var managedNames = managed.Leaves.Select(leaf => ManagedLayout.NormalizeName(leaf.Name)).ToArray();

                Assert.That(managedNames, Is.EqualTo(rustNames),
                    $"{Describe(type, rust)}: fields are named or ordered differently (compared ignoring " +
                    "case and underscores). Managed: " +
                    $"[{string.Join(", ", managed.Leaves.Select(leaf => leaf.Name))}]. Rust: " +
                    $"[{string.Join(", ", rust.Leaves.Select(leaf => leaf.Name))}]");
            }
        }

        [Test]
        public void EveryRegisteredEnum_HasMatchingDiscriminants()
        {
            var compared = 0;
            foreach (var (rust, _, type) in Pairs())
            {
                if (rust.Variants.Count == 0)
                {
                    continue;
                }

                Assert.That(type.IsEnum, Is.True, $"{Describe(type, rust)}: Rust reports variants but the managed type is not an enum");

                // Variant names, unlike field names, do line up between the two languages, and
                // reordering discriminants is a real bug this can catch - so compare by name.
                var managedValues = Enum.GetNames(type)
                    .ToDictionary(name => name, name => Convert.ToInt64(Enum.Parse(type, name)));

                Assert.That(managedValues.Keys.OrderBy(name => name),
                    Is.EqualTo(rust.Variants.Select(variant => variant.Key).OrderBy(name => name)),
                    $"{Describe(type, rust)}: variant names differ");

                foreach (var variant in rust.Variants)
                {
                    Assert.That(managedValues[variant.Key], Is.EqualTo(variant.Value),
                        $"{Describe(type, rust)}: variant {variant.Key} is {managedValues[variant.Key]} " +
                        $"on the managed side but {variant.Value} in Rust");
                }

                compared++;
            }

            Assert.That(compared, Is.GreaterThan(0), "no registered enum was compared");
        }

        [Test]
        public void EveryGenericMirror_IsFaithfullyRepresentedByItsStandIn()
        {
            foreach (var generic in ManagedMirrors().Where(mirror => mirror.Type.IsGenericTypeDefinition))
            {
                var entry = GenericMirrors.FirstOrDefault(candidate => candidate.Definition == generic.Type);
                Assert.That(entry.Definition, Is.Not.Null,
                    $"{generic.Type.Name} is a generic mirror with no stand-in in {nameof(GenericMirrors)}, " +
                    "so its field offsets would go unchecked. Add one.");

                var closed = entry.Definition.MakeGenericType(entry.Argument);
                var closedFieldNames = ManagedLayout.DeclaredFields(closed).Select(field => ManagedLayout.NormalizeName(field.Name)).ToArray();
                var standInFieldNames = ManagedLayout.DeclaredFields(entry.StandIn).Select(field => ManagedLayout.NormalizeName(field.Name)).ToArray();
                var closedFieldTypes = ManagedLayout.DeclaredFields(closed).Select(field => field.FieldType).ToArray();
                var standInFieldTypes = ManagedLayout.DeclaredFields(entry.StandIn).Select(field => field.FieldType).ToArray();

                Assert.That(standInFieldNames, Is.EqualTo(closedFieldNames),
                    $"the stand-in {entry.StandIn.Name} no longer declares the same fields in the same " +
                    $"order as {closed.Name}, so measuring it says nothing about the real type");
                Assert.That(standInFieldTypes, Is.EqualTo(closedFieldTypes),
                    $"the stand-in {entry.StandIn.Name} no longer has the same field types as " +
                    $"{closed.Name}, so measuring it says nothing about the real type");
                Assert.That(Marshal.SizeOf(entry.StandIn), Is.EqualTo(entry.ClosedSize),
                    $"the stand-in {entry.StandIn.Name} is {Marshal.SizeOf(entry.StandIn)} bytes but " +
                    $"{closed.Name} is {entry.ClosedSize}");
            }
        }

        [Test]
        public void ReflectionFieldOrder_MatchesLayoutOrder()
        {
            // The generic stand-in check above compares field names and types in the order reflection
            // reports them, because Marshal.OffsetOf cannot be used on a generic type. This asserts that for
            // every mirror we *can* measure, that order really is ascending-offset order - so the
            // assumption the stand-in check leans on is verified rather than assumed.
            foreach (var mirror in ManagedMirrors().Where(m => Measurable(m.Type)))
            {
                var declared = ManagedLayout.DeclaredFields(mirror.Type).Select(field => field.Name).ToArray();
                var byOffset = ManagedLayout.OrderedFields(mirror.Type).Select(entry => entry.Field.Name).ToArray();
                Assert.That(declared, Is.EqualTo(byOffset),
                    $"{mirror.Type.Name}: reflection reports fields in a different order than their offsets");
            }
        }

        [Test]
        public void EveryRustType_WasActuallyCompared()
        {
            // Guards against the comparison loops silently covering less than the whole registry -
            // the prototype this replaces passed while skipping every generic type.
            var comparedNames = Pairs().Select(pair => pair.Rust.Name).Distinct().ToHashSet();
            var missing = _rustTypes.Select(type => type.Name).Where(name => !comparedNames.Contains(name)).ToArray();

            Assert.That(missing, Is.Empty, $"registered but never compared: {string.Join(", ", missing)}");
            Assert.That(comparedNames.Count, Is.EqualTo(_rustTypes.Count));
        }

        /// <summary>
        /// Every managed type carrying <see cref="FfiLayoutAttribute"/>, paired with the Rust name it
        /// claims. Scans the driver assembly and this test assembly, since the transport mirrors and
        /// the generic stand-ins live here.
        /// </summary>
        private static IEnumerable<(Type Type, string RustName)> ManagedMirrors()
        {
            var assemblies = new[] { typeof(RustBridge).Assembly, typeof(FfiAbiTests).Assembly };
            foreach (var assembly in assemblies)
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var attribute in type.GetCustomAttributes<FfiLayoutAttribute>(false))
                    {
                        yield return (type, attribute.RustName);
                    }
                }
            }
        }

        /// <summary>
        /// Whether a mirror can be measured directly. Generic types cannot - <c>Marshal.OffsetOf</c>
        /// rejects them - and are covered through their stand-in instead.
        /// </summary>
        private static bool Measurable(Type type)
        {
            return !type.IsGenericTypeDefinition && !type.IsEnum;
        }

        /// <summary>
        /// Each measurable managed mirror alongside the Rust type it claims to mirror.
        /// </summary>
        private static IEnumerable<(RustType Rust, ManagedLayout Managed, Type Type)> Pairs()
        {
            var byName = _rustTypes.ToDictionary(type => type.Name);
            foreach (var mirror in ManagedMirrors())
            {
                if (!byName.TryGetValue(mirror.RustName, out var rust))
                {
                    // Reported by EveryManagedMirror_IsRegisteredOnTheRustSide; skipping here keeps
                    // this helper from throwing and masking that clearer failure.
                    continue;
                }

                if (mirror.Type.IsEnum)
                {
                    // An enum's single leaf is its underlying integer; compare it without recursing
                    // through fields, which an enum has none of.
                    var size = Marshal.SizeOf(Enum.GetUnderlyingType(mirror.Type));
                    yield return (rust, ManagedLayout.OfEnum(size), mirror.Type);
                    continue;
                }

                if (mirror.Type.IsGenericTypeDefinition)
                {
                    // Measured through its stand-in, which claims the same Rust name and is verified
                    // to match by EveryGenericMirror_IsFaithfullyRepresentedByItsStandIn.
                    continue;
                }

                yield return (rust, ManagedLayout.Of(mirror.Type), mirror.Type);
            }
        }

        private static string Describe(Type managed, RustType rust)
        {
            return managed.Name == rust.Name ? managed.Name : $"{managed.Name} (Rust {rust.Name})";
        }
    }
}
