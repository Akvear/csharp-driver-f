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
using System.Net;

namespace Cassandra
{
    /// <summary>
    /// Represents a Cassandra node.
    /// </summary>
    public class Host : IEquatable<Host>
    {
        private static readonly Logger Logger = new Logger(typeof(Host));

        /// <summary>
        /// Determines if the host is UP for the driver
        /// </summary>
        public bool IsUp
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// This property is going to be removed in future versions, use <see cref="IsUp"/> instead.
        /// Used to determines if the host can be considered as UP
        /// </summary>
        public bool IsConsiderablyUp
        {
            get { return IsUp; }
        }

        /// <summary>
        ///  Gets the node address.
        /// </summary>
        public IPEndPoint Address { get; }

        /// <summary>
        /// Gets the node's host id.
        /// </summary>
        public Guid HostId { get; private set; }

        /// <summary>
        ///  Gets the name of the datacenter this host is part of. The returned
        ///  datacenter name is the one as known by Cassandra. Also note that it is
        ///  possible for this information to not be available. In that case this method
        ///  returns <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Datacenter { get; internal set; }

        /// <summary>
        ///  Gets the name of the rack this host is part of. The returned rack name is
        ///  the one as known by Cassandra. Also note that it is possible for this
        ///  information to not be available. In that case this method returns
        ///  <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Rack { get; private set; }

        /// <summary>
        /// The Cassandra version the host is running.
        /// <remarks>
        /// The value returned can be null if the information is unavailable.
        /// </remarks>
        /// </summary>
        public Version CassandraVersion { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Host"/>.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local : Part of the public API
        public Host(IPEndPoint address, IReconnectionPolicy reconnectionPolicy)
        {
            // FIXME
        }

        /// <summary>
        /// Creates a new instance of <see cref="Host"/> from raw metadata.
        /// This constructor is used internally by the FFI bridge when populating
        /// host topology from Rust's ClusterState.
        /// </summary>
        /// <remarks>
        /// This constructor should only be called by the AddHostCallback (AddHostToList) FFI callback in BridgedClusterState.cs.
        /// </remarks>
        internal Host(IPEndPoint address, Guid hostId, string datacenter, string rack)
        {
            Address = address;
            HostId = hostId;
            Datacenter = datacenter;
            Rack = rack;
        }

        /// <summary>
        /// Sets the Host as Down.
        /// Returns false if it was already considered as Down by the driver.
        /// </summary>
        public bool SetDown()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns true if the host was DOWN and it was set as UP.
        /// </summary>
        public bool BringUpIfDown()
        {
            throw new NotImplementedException();
        }

        public void SetAsRemoved()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The hash value of the address of the host
        /// </summary>
        public override int GetHashCode()
        {
            return Address.GetHashCode();
        }

        /// <summary>
        /// Determines if the this instance can be considered equal to the provided host.
        /// </summary>
        public bool Equals(Host other)
        {
            return Equals(Address, other?.Address);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Host)obj);
        }
    }
}