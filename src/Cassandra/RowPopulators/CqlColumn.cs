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
    public class CqlColumn : ColumnDesc
    {
        /// <summary>
        /// Index of the column in the rowset
        /// </summary>
        // FIXME: we don't have access to column index from Rust, so the index is set to -1 by default and is not updated.
        public int Index { get; set; }
        /// <summary>
        /// CLR Type of the column
        /// </summary>
        public Type Type { get; set; }
    }
}