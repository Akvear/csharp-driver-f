/// Initialize logging for the Rust components.
///
/// Must be called at least once before any logging is performed;
/// otherwise, no log output will be produced.
/// Subsequent calls are no-ops.
pub(crate) fn init_logging() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .without_time()
            .init();
    });
}
