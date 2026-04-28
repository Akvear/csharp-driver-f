use std::convert::TryFrom;
use std::fmt::{Debug, Write};
use std::sync::OnceLock;

use crate::ffi::FFIStr;
use tracing::Level;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

type CSharpLogCallback =
    unsafe extern "C" fn(level: CsharpLogLevel, target: FFIStr<'_>, message: FFIStr<'_>);

#[derive(Copy, Clone)]
struct ProcessLogger {
    callback: CSharpLogCallback,
}

static LOGGER: OnceLock<ProcessLogger> = OnceLock::new();

/// Returns lazily initialized process-global logger configuration.
fn logger() -> &'static ProcessLogger {
    LOGGER
        .get()
        .unwrap_or_else(|| panic!("Logger not initialized"))
}

// Initializes the global logger with the provided C# callback.
fn init_logger(callback: CSharpLogCallback) {
    LOGGER.set(ProcessLogger { callback }).ok();
}

#[repr(u8)]
pub enum CsharpLogLevel {
    Off = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
    Verbose = 4,
}

impl From<Level> for CsharpLogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE | Level::DEBUG => CsharpLogLevel::Verbose,
            Level::INFO => CsharpLogLevel::Info,
            Level::WARN => CsharpLogLevel::Warning,
            Level::ERROR => CsharpLogLevel::Error,
        }
    }
}

impl TryFrom<CsharpLogLevel> for Level {
    type Error = ();

    fn try_from(level: CsharpLogLevel) -> Result<Self, Self::Error> {
        match level {
            CsharpLogLevel::Error => Ok(Level::ERROR),
            CsharpLogLevel::Warning => Ok(Level::WARN),
            CsharpLogLevel::Info => Ok(Level::INFO),
            CsharpLogLevel::Verbose => Ok(Level::TRACE),
            CsharpLogLevel::Off => Err(()),
        }
    }
}

/// Field visitor used to extract primary message and additional key/value
/// fields from tracing events for managed-side formatting.
struct MessageVisitor {
    log_message: String,
}

impl MessageVisitor {
    /// Creates an empty collector for one tracing event.
    fn new() -> Self {
        Self {
            log_message: String::new(),
        }
    }
}

/// Captures event fields that are rendered with Debug formatting.
impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if self.log_message.is_empty() {
            write!(self.log_message, "{field}: {value:?}").unwrap();
        } else {
            write!(self.log_message, ", {field}: {value:?}").unwrap();
        }
    }
}

/// tracing_subscriber layer that forwards events to the C# callback.
struct CSharpForwardingLayer;

impl<S: tracing::Subscriber> Layer<S> for CSharpForwardingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        let event_level = meta.level();

        let target = meta.target();
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let message = visitor.log_message;
        let ffi_level = CsharpLogLevel::from(*event_level);

        let callback = logger().callback;
        unsafe { callback(ffi_level, FFIStr::new(target), FFIStr::new(&message)) };
    }
}

/// Initialize logging for the Rust components with the specified level.
///
/// Must be called at least once before any logging is performed;
/// otherwise, no log output will be produced.
/// Subsequent calls are no-ops.
pub(crate) fn init_logging(callback: CSharpLogCallback, level: CsharpLogLevel) {
    init_logger(callback);
    let level = match Level::try_from(level) {
        Ok(l) => l,
        // If the level is Off or invalid, we don't set up a subscriber,
        // effectively disabling logging.
        Err(()) => return,
    };

    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::default().add_directive(level.to_owned().into()))
            .with(CSharpForwardingLayer),
    )
    .unwrap_or(()) // Ignore if it is set already
}

/// Registers the C# logging callback and initializes the Rust subscriber.
#[unsafe(no_mangle)]
pub extern "C" fn configure_rust_logging(callback: CSharpLogCallback, min_level: CsharpLogLevel) {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        init_logging(callback, min_level);
    });
}
