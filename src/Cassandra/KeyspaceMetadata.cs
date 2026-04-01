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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra
{
    public class KeyspaceMetadata
    {
        private readonly BridgedClusterState _clusterState;

        /// <summary>
        ///  Gets the name of this keyspace.
        /// </summary>
        /// <returns>the name of this CQL keyspace.</returns>
        public string Name { get; }

        /// <summary>
        ///  Gets a value indicating whether durable writes are set on this keyspace.
        /// </summary>
        /// <returns><c>true</c> if durable writes are set on this keyspace
        ///  , <c>false</c> otherwise.</returns>
        public bool DurableWrites { get; private set; }

        /// <summary>
        ///  Gets the Strategy Class of this keyspace.
        /// </summary>
        /// <returns>name of StrategyClass of this keyspace.</returns>
        public string StrategyClass { get; private set; }

        /// <summary>
        ///  Returns the replication options for this keyspace.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the keyspace replication strategy options.</returns>
        public IDictionary<string, int> Replication { get; private set; }

        /// <summary>
        /// Determines whether the keyspace is a virtual keyspace or not.
        /// </summary>
        public bool IsVirtual { get; }

        /// <summary>
        /// Returns the graph engine associated with this keyspace. Returns null if there isn't one.
        /// </summary>
        public string GraphEngine { get; }

        internal KeyspaceMetadata(
            Metadata parent,
            string name,
            bool durableWrites,
            string strategyClass,
            IDictionary<string, string> replicationOptions,
            string graphEngine,
            bool isVirtual = false)
        {
            throw new NotImplementedException("TODO: implement KeyspaceMetadata");
        }

        // Constructor for creating a KeyspaceMetadata with only the name, used for bridging rest of metadata from Rust to C#.
        internal KeyspaceMetadata(
            BridgedClusterState clusterState,
            string name
        )
        {
            Name = name;
            _clusterState = clusterState;
            IsVirtual = false;
        }

        // Internal method to fill the rest of the metadata fields with bridged data from Rust
        // after the KeyspaceMetadata object is created with only the name.
        internal void FillKeyspaceMetadata(
            bool durableWrites,
            string strategyClass,
            IDictionary<string, int> replication
        )
        {
            DurableWrites = durableWrites;
            StrategyClass = strategyClass;
            Replication = replication;
        }

        /// <summary>
        ///  Returns metadata of specified table in this keyspace.
        /// </summary>
        /// <param name="tableName"> the name of table to retrieve </param>
        /// <returns>the metadata for table <c>tableName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public TableMetadata GetTableMetadata(string tableName)
        {
            return _clusterState.GetTableMetadata(Name, tableName);
        }

        /// <summary>
        ///  Returns metadata of specified view in this keyspace.
        /// </summary>
        /// <param name="viewName">the name of view to retrieve </param>
        /// <returns>the metadata for view <c>viewName</c> in this keyspace if it
        ///  exists, <c>null</c> otherwise.</returns>
        public MaterializedViewMetadata GetMaterializedViewMetadata(string viewName)
        {
            throw new NotImplementedException("TODO: implement MaterializedViewMetadata");
        }

        /// <summary>
        /// Removes table metadata forcing refresh the next time the table metadata is retrieved
        /// </summary>
        internal void ClearTableMetadata(string tableName)
        {
            throw new NotImplementedException("TODO: implement TableMetadata");
        }

        /// <summary>
        /// Removes the view metadata forcing refresh the next time the view metadata is retrieved
        /// </summary>
        internal void ClearViewMetadata(string name)
        {
            throw new NotImplementedException("TODO: implement ViewMetadata");
        }

        /// <summary>
        /// Removes function metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearFunction(string name, string[] signature)
        {
            throw new NotImplementedException("TODO: implement FunctionMetadata");
        }

        /// <summary>
        /// Removes aggregate metadata forcing refresh the next time the function metadata is retrieved
        /// </summary>
        internal void ClearAggregate(string name, string[] signature)
        {
            throw new NotImplementedException("TODO: implement AggregateMetadata");
        }

        /// <summary>
        ///  Returns metadata of all tables defined in this keyspace.
        /// </summary>
        /// <returns>an IEnumerable of TableMetadata for the tables defined in this
        ///  keyspace.</returns>
        public IEnumerable<TableMetadata> GetTablesMetadata()
        {
            return GetTablesNames().Select(GetTableMetadata);
        }


        /// <summary>
        ///  Returns names of all tables defined in this keyspace.
        /// </summary>
        /// 
        /// <returns>a collection of all, defined in this
        ///  keyspace tables names.</returns>
        public ICollection<string> GetTablesNames()
        {
            return _clusterState.GetTableNames(Name);
        }

        /// <summary>
        /// <para>
        ///  Deprecated. Please use <see cref="AsCqlQuery"/>.
        /// </para>
        /// <para>
        ///  Returns a CQL query representing this keyspace. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </para>
        /// </summary>
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.</returns>
        public string ExportAsString()
        {
            var sb = new StringBuilder();

            sb.Append(AsCqlQuery()).Append("\n");

            return sb.ToString();
        }

        /// <summary>
        ///  Returns a CQL query representing this keyspace. This method returns a single
        ///  'CREATE KEYSPACE' query with the options corresponding to this name
        ///  definition.
        /// </summary>
        /// <returns>the 'CREATE KEYSPACE' query corresponding to this name.</returns>
        public string AsCqlQuery()
        {
            var sb = new StringBuilder();

            sb.Append("CREATE KEYSPACE ").Append(CqlQueryTools.QuoteIdentifier(Name)).Append(" WITH ");
            sb.Append("REPLICATION = { 'class' : '").Append(StrategyClass).Append("'");

            /* FIXME: reimplement this using Rust wrapper.
            foreach (var rep in [] ReplicationOptions)
            {
                if (rep.Key == "class")
                {
                    continue;
                }
                sb.Append(", '").Append(rep.Key).Append("': '").Append(rep.Value).Append("'");
            } */
            sb.Append(" } AND DURABLE_WRITES = ").Append(DurableWrites);
            sb.Append(";");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the definition of a User defined type
        /// </summary>
        internal UdtColumnInfo GetUdtDefinition(string typeName)
        {
            throw new NotImplementedException("TODO: implement UdtMetadata");
        }

        /// <summary>
        /// Gets the definition of a User defined type asynchronously
        /// </summary>
        internal Task<UdtColumnInfo> GetUdtDefinitionAsync(string typeName)
        {
            throw new NotImplementedException("TODO: implement UdtMetadata");
        }

        /// <summary>
        /// Gets a CQL function by name and signature
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string functionName, string[] signature)
        {
            throw new NotImplementedException("TODO: implement FunctionMetadata");
        }

        /// <summary>
        /// Gets a CQL aggregate by name and signature
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string aggregateName, string[] signature)
        {
            throw new NotImplementedException("TODO: implement AggregateMetadata");
        }
    }
}
