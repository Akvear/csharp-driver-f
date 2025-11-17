//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Cassandra;

namespace RustWrapper
{
    /// <summary>
    /// TODO: Add description
    /// </summary>
    internal class Program
    {
        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            Console.WriteLine($"Beginning RustWrapper example!");

            var cluster =
                Cluster.Builder()
                    .AddContactPoint("172.42.0.2")
                    .Build();

            using (ISession session = await cluster.ConnectAsync().ConfigureAwait(false))
            {
                // Use session.

                var s = new SimpleStatement("SELECT host_id FROM system.peers");
                RowSet result = await session.ExecuteAsync(s);

                foreach (Row row in result)
                {
                    Console.WriteLine($"Host ID: {row.GetValue<Guid>("host_id")}");
                }

                result.Dispose(); // Dispose() → ReleaseHandle() → Rust frees memory

                var ps = await session.PrepareAsync(s.QueryString);
                var bs = ps.Bind();
                session.Execute(bs);
            } // Dispose() → ReleaseHandle() → Rust frees memory

            // GC.Collect();
            // GC.WaitForPendingFinalizers();
            // GC.Collect();
        }
    }
}