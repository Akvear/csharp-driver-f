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
using System.Collections.ObjectModel;

namespace Cassandra
{
    /// <summary>
    /// Describes a Cassandra table
    /// </summary>
    public class TableMetadata : DataCollectionMetadata
    {
        private static readonly IDictionary<string, IndexMetadata> EmptyIndexes =
            new ReadOnlyDictionary<string, IndexMetadata>(new Dictionary<string, IndexMetadata>());

        /// <summary>
        /// Gets the table indexes by name
        /// </summary>
        private IDictionary<string, IndexMetadata> _indexes;
        // FIXME: we don't have access to index info from Rust, so when user tries to access indexes,
        // we throw an exception instead of returning incorrect information.
        public IDictionary<string, IndexMetadata> Indexes
        {
            get => throw new InvalidOperationException("Indexes access is not supported.");
            protected set => _indexes = value ?? EmptyIndexes;
        }

        /// <summary>
        /// Determines whether the table is a virtual table or not.
        /// </summary>
        public bool IsVirtual { get; protected set; }

        internal string KeyspaceName { get; set; }

        protected TableMetadata()
        {

        }

        internal TableMetadata(string keyspace, string name, IDictionary<string, IndexMetadata> indexes = null, bool isVirtual = false)
        {
            KeyspaceName = keyspace;
            Name = name;
            Indexes = indexes ?? EmptyIndexes;
            IsVirtual = isVirtual;
        }
    }
}
