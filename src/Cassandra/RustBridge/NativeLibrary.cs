namespace Cassandra
{
    /// <summary>
    /// Central place for native library names used by DllImport attributes.
    /// This avoids string duplication and makes it easier to change library names
    /// across all P/Invoke declarations.
    /// </summary>
    internal static class NativeLibrary
    {
        /// <summary>
        /// The name of the C# wrapper native library (Rust FFI).
        /// </summary>
        public const string CSharpWrapper = "csharp_wrapper";
    }
}