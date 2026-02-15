use std::time::Duration;

use scylla::client::session_builder::SessionBuilder;

/// Socket-related options passed from C# to Rust.
///
/// This struct is intended to be passed by-value over the FFI boundary.
/// It is only primitive fields and `#[repr(C)]`.
///
/// Some C# driver socket options (reuse address, linger, buffer sizes)
/// cannot be set via SessionBuilder.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SocketOptions {
    /// Connection timeout in milliseconds. Values <= 0 mean use default.
    pub connect_timeout_millis: i32,

    /// Whether to enable TCP_NODELAY.
    pub tcp_nodelay: bool,

    /// Whether to enable TCP keepalive.
    pub keepalive: bool,

    /// TCP keepalive interval in milliseconds.
    /// Values <= 0 mean "use default".
    pub tcp_keepalive_interval_millis: i64,
}

impl SocketOptions {
    // Not sure what the default should be.
    const DEFAULT_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(42);

    pub(crate) fn apply_to_session_builder(self, mut builder: SessionBuilder) -> SessionBuilder {
        if self.connect_timeout_millis > 0 {
            builder = builder
                .connection_timeout(Duration::from_millis(self.connect_timeout_millis as u64));
        }

        builder = builder.tcp_nodelay(self.tcp_nodelay);

        if self.keepalive {
            let interval = if self.tcp_keepalive_interval_millis > 0 {
                Duration::from_millis(self.tcp_keepalive_interval_millis as u64)
            } else {
                Self::DEFAULT_TCP_KEEPALIVE_INTERVAL
            };
            builder = builder.tcp_keepalive_interval(interval);
        }

        builder
    }
}
