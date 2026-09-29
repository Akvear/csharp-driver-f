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
using NUnit.Framework;
using InteropNativeLibrary = System.Runtime.InteropServices.NativeLibrary;

namespace Cassandra.Tests
{
    /// <summary>
    /// Verifies that every P/Invoke into the Rust library names an export that actually exists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <c>[DllImport]</c> is bound lazily, on its first call. A misspelled or since-renamed entry
    /// point therefore compiles, loads, and passes every test that does not happen to call it, and
    /// only throws <see cref="EntryPointNotFoundException"/> once some path does - possibly in
    /// production. This fixture resolves them all up front.
    /// </para>
    /// <para>
    /// Only the export's existence is checked. The native library does not describe its parameter
    /// lists, so a declaration with the right name and the wrong signature still passes; see
    /// <c>rust/src/abi.rs</c>.
    /// </para>
    /// </remarks>
    public class FfiEntryPointTests : BaseUnitTest
    {
        [Test]
        public void EveryPInvoke_ResolvesToAnExportOfTheNativeLibrary()
        {
            var imports = PInvokes().ToArray();
            Assert.That(imports, Is.Not.Empty, "found no P/Invoke declarations at all, so nothing was checked");

            // Resolved the way the runtime resolves a DllImport declared in the driver assembly, so
            // this inspects the same file the driver binds to.
            var library = InteropNativeLibrary.Load(NativeLibrary.CSharpWrapper, typeof(RustBridge).Assembly, null);
            try
            {
                var missing = imports
                    .Where(import => !InteropNativeLibrary.TryGetExport(library, import.EntryPoint, out _))
                    .Select(import => $"{import.Method.DeclaringType.Name}.{import.Method.Name} -> {import.EntryPoint}")
                    .ToArray();

                Assert.That(missing, Is.Empty,
                    "these P/Invoke declarations name an entry point the native library does not export: " +
                    string.Join(", ", missing));
            }
            finally
            {
                InteropNativeLibrary.Free(library);
            }
        }

        /// <summary>
        /// Every method declared as a P/Invoke into the Rust library, in the driver assembly and in
        /// this test assembly (which declares the <c>ffi_abi_*</c> imports).
        /// </summary>
        private static IEnumerable<(MethodInfo Method, string EntryPoint)> PInvokes()
        {
            const BindingFlags flags = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
            var assemblies = new[] { typeof(RustBridge).Assembly, typeof(FfiEntryPointTests).Assembly };
            foreach (var assembly in assemblies)
            {
                foreach (var type in assembly.GetTypes())
                {
                    foreach (var method in type.GetMethods(flags))
                    {
                        var import = method.GetCustomAttribute<DllImportAttribute>();
                        if (import != null && import.Value == NativeLibrary.CSharpWrapper)
                        {
                            yield return (method, import.EntryPoint ?? method.Name);
                        }
                    }
                }
            }
        }
    }
}
