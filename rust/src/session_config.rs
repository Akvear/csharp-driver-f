use std::time::Duration;

use crate::ffi::{CSharpStr, FFIBool};

use scylla::client::SelfIdentity;
use scylla::{
    client::{execution_profile::ExecutionProfile, session_builder::SessionBuilder},
    policies::load_balancing::DefaultPolicy,
};

const DEFAULT_DRIVER_NAME: &str = "ScyllaDB C# RS Driver";
const DEFAULT_DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// TCP socket options passed from C#.
///
/// Any changes to this struct must be mirrored in the corresponding C# struct.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct BridgedTcpConfig {
    /// Whether to enable TCP_NODELAY.
    tcp_nodelay: FFIBool,

    /// Whether to enable TCP keepalive.
    keepalive: FFIBool,

    /// TCP keepalive interval in milliseconds.
    tcp_keepalive_interval_millis: i32,

    /// Receive buffer size in bytes. Values <= 0 mean "use default".
    receive_buffer_size: i32,

    /// Whether to enable SO_REUSEADDR.
    reuse_address: FFIBool,

    /// Send buffer size in bytes. Values <= 0 mean "use default".
    send_buffer_size: i32,

    /// Whether to enable SO_LINGER flag.
    // NOTE: `tcp_set_linger()` in Rust Driver is deprecated, because tokio is incompatible with blocking sockets.
    // Instead, Rust Driver exposes `tcp_zero_linger()` which sets SO_LINGER with a timeout of 0.
    // Therefore, we only expose a boolean here to indicate whether to enable SO_LINGER with a timeout of 0.
    so_linger: FFIBool,
}

impl BridgedTcpConfig {
    /// Apply all TCP socket options in this config to `builder` and return it.
    pub(crate) fn apply_to_builder(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder = builder.tcp_nodelay(self.tcp_nodelay.into());

        if self.keepalive.into() {
            builder = builder.tcp_keepalive_interval(Duration::from_millis(
                self.tcp_keepalive_interval_millis as u64,
            ));
        }

        if self.receive_buffer_size > 0 {
            builder = builder.tcp_recv_buffer_size(self.receive_buffer_size as usize);
        }

        if self.send_buffer_size > 0 {
            builder = builder.tcp_send_buffer_size(self.send_buffer_size as usize);
        }

        builder = builder.tcp_reuse_address(self.reuse_address.into());

        if self.so_linger.into() {
            // FIXME: switch to `tcp_zero_linger()` upon Rust Driver version bump to 1.7.0.
            builder = builder.tcp_linger(Duration::ZERO);
        }

        builder
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct BridgedLoadBalancingPolicy<'a> {
    is_token_aware: bool,
    permit_dc_failover: bool,
    local_dc: CSharpStr<'a>,
}

impl<'a> BridgedLoadBalancingPolicy<'a> {
    /// Returns the configured builder
    pub(crate) fn apply_to_builder(self, builder: SessionBuilder) -> SessionBuilder {
        let local_dc = self
            .local_dc
            .as_cstr()
            .map(|cstr| cstr.to_str().unwrap().to_owned());

        let mut lbpbuilder = DefaultPolicy::builder()
            .token_aware(self.is_token_aware)
            .permit_dc_failover(self.permit_dc_failover);

        if let Some(preferred_dc) = local_dc {
            lbpbuilder = lbpbuilder.prefer_datacenter(preferred_dc);
        }

        let profile_handle = ExecutionProfile::builder()
            .load_balancing_policy(lbpbuilder.build())
            .build()
            .into_handle();

        builder.default_execution_profile_handle(profile_handle)
    }
}
/// Output of [`BridgedSessionConfig::into_session_builder`]: a fully-configured
/// [`SessionBuilder`] together with the URI and keyspace it was built from,
/// borrowed directly from the C#-managed config memory.
///
/// The `&'a str` fields borrow from the source config; a caller that needs them to
/// outlive that borrow (e.g. to capture into a `'static` future) must `.to_owned()`.
pub(crate) struct BridgedSessionConfigResult<'a> {
    pub(crate) uri: &'a str,
    pub(crate) keyspace: &'a str,
    pub(crate) builder: SessionBuilder,
}
/// Configuration for creating a new session passed from C#.
///
/// Any changes to this struct must be mirrored in the corresponding C# struct.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct BridgedSessionConfig<'a> {
    /// Contact point URIs, comma-separated.
    uri: CSharpStr<'a>,

    /// Keyspace to use, or empty string for none.
    keyspace: CSharpStr<'a>,

    /// Connection timeout in milliseconds.
    connect_timeout_millis: i32,

    /// TCP socket options.
    tcp: BridgedTcpConfig,

    load_balancing_policy: BridgedLoadBalancingPolicy<'a>,
}

impl<'a> BridgedSessionConfig<'a> {
    /// Consume this config and produce a [`BridgedSessionConfigResult`] holding a
    /// fully-configured [`SessionBuilder`] alongside the borrowed URI and keyspace.
    ///
    /// This is the single place where all session configuration is applied, so
    /// adding new options only requires changes here and in the struct definition.
    pub(crate) fn into_session_builder(self) -> BridgedSessionConfigResult<'a> {
        let uri = self.uri.as_cstr().unwrap().to_str().unwrap();
        let keyspace = self.keyspace.as_cstr().unwrap().to_str().unwrap();

        let mut builder = SessionBuilder::new().known_nodes(uri.split(',').map(|s| s.trim()));

        // Rust considers an empty string an invalid keyspace name, while C# treats it
        // as "no keyspace". Setting keyspace via Connect() on the C# side is
        // case-sensitive, so we pass case_sensitive = true here.
        if !keyspace.is_empty() {
            builder = builder.use_keyspace(keyspace, true);
        }

        if self.connect_timeout_millis > 0 {
            builder = builder
                .connection_timeout(Duration::from_millis(self.connect_timeout_millis as u64));
        }

        builder = self.tcp.apply_to_builder(builder);
        builder = self.load_balancing_policy.apply_to_builder(builder);

        let identity = SelfIdentity::new()
            .with_custom_driver_name(DEFAULT_DRIVER_NAME)
            .with_custom_driver_version(DEFAULT_DRIVER_VERSION);
        builder = builder.custom_identity(identity);

        BridgedSessionConfigResult {
            uri,
            keyspace,
            builder,
        }
    }
}
