use std::fmt::{Debug, Write};
use std::sync::OnceLock;

use crate::ffi::FFIStr;
use tracing::Level;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

type CSharpLogCallback = unsafe extern "C" fn(level: CsharpLogLevel, message: FFIStr<'_>);

#[derive(Copy, Clone)]
struct ProcessLogger {
    callback: CSharpLogCallback,
}

static LOGGER: OnceLock<ProcessLogger> = OnceLock::new();

/// Returns lazily initialized process-global logger configuration.
fn logger() -> Option<&'static ProcessLogger> {
    LOGGER.get()
}

// Initializes the global logger with the provided C# callback.
fn init_logger(callback: CSharpLogCallback) {
    LOGGER.set(ProcessLogger { callback }).ok();
}

/// This is equivalent of C# side Diagnostics.CassandraTraceSwitch.Level
/// See LOGGING.md for details.
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
            // We coalesce TRACE and DEBUG into Verbose, since C# doesn't have a separate log level for debug messages.
            Level::TRACE | Level::DEBUG => CsharpLogLevel::Verbose,
            Level::INFO => CsharpLogLevel::Info,
            Level::WARN => CsharpLogLevel::Warning,
            Level::ERROR => CsharpLogLevel::Error,
        }
    }
}

impl From<CsharpLogLevel> for LevelFilter {
    fn from(level: CsharpLogLevel) -> Self {
        match level {
            CsharpLogLevel::Off => LevelFilter::OFF,
            CsharpLogLevel::Error => LevelFilter::ERROR,
            CsharpLogLevel::Warning => LevelFilter::WARN,
            CsharpLogLevel::Info => LevelFilter::INFO,
            CsharpLogLevel::Verbose => LevelFilter::TRACE,
        }
    }
}

/// Formats tracing span and event fields for the C# logger.
struct FormattedFieldsVisitor {
    output: String,
    has_entry: bool,
    field_separator: &'static str,
    message_field_name: Option<&'static str>,
}

impl FormattedFieldsVisitor {
    fn new(field_separator: &'static str, message_field_name: Option<&'static str>) -> Self {
        Self {
            output: String::new(),
            has_entry: false,
            field_separator,
            message_field_name,
        }
    }

    /// Adds one tracing field to the current formatted string.
    fn add_field(&mut self, field: &Field, value: &dyn Debug) {
        let should_omit_name = self
            .message_field_name
            .is_some_and(|message_field_name| field.name() == message_field_name);

        let prefix = if self.has_entry { ", " } else { "" };

        if should_omit_name {
            // If this field is the "message" field, we omit the field name and separator to produce cleaner output.
            write!(self.output, "{prefix}{value:?}").unwrap();
        } else {
            write!(
                self.output,
                "{prefix}{}{}{:?}",
                field.name(),
                self.field_separator,
                value
            )
            .unwrap();
        }

        self.has_entry = true;
    }
}

impl Visit for FormattedFieldsVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.add_field(field, value);
    }
}

/// Stores the formatted fields of a tracing span for later reuse by events in the same span.
struct SpanFields(String);

/// tracing_subscriber layer that forwards events to the C# callback.
struct CSharpForwardingLayer;

impl<S> Layer<S> for CSharpForwardingLayer
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    /// Stores the fields attached to a newly created span.
    ///
    /// We do this once so events can reuse the span context later without
    /// reformatting the fields on every log line.
    fn on_new_span(
        &self,
        attrs: &Attributes<'_>,
        id: &tracing::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut visitor = FormattedFieldsVisitor::new("=", None);
        attrs.record(&mut visitor);
        span.extensions_mut().insert(SpanFields(visitor.output));
    }

    /// Formats a tracing event and forwards it to the C# callback.
    ///
    /// The message is built from the active span chain plus the event fields,
    /// so the C# side sees the same context that Rust tracing would normally
    /// print in its own output.
    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let callback = match logger() {
            Some(logger) => logger.callback,
            None => return, // If the logger is not initialized, we can't forward the log message.
        };

        let meta = event.metadata();
        let event_level = meta.level();

        let mut visitor = FormattedFieldsVisitor::new(": ", Some("message"));
        event.record(&mut visitor);

        let ffi_level = CsharpLogLevel::from(*event_level);

        let mut prefixed_message = String::new();
        if let Some(scope) = ctx.event_scope(event)
            && let Some(span) = scope.from_root().last()
        {
            prefixed_message.push_str(span.name());

            if let Some(fields) = span.extensions().get::<SpanFields>()
                && !fields.0.is_empty()
            {
                prefixed_message.push('{');
                prefixed_message.push_str(&fields.0);
                prefixed_message.push('}');
            }

            prefixed_message.push_str(": ");
        }

        prefixed_message.push('[');
        prefixed_message.push_str(meta.target());
        prefixed_message.push(']');
        prefixed_message.push(' ');

        prefixed_message.push_str(&visitor.output);

        unsafe { callback(ffi_level, FFIStr::new(&prefixed_message)) };
    }
}

/// Registers the C# logging callback and initializes the Rust subscriber.
///
/// Must be called at least once before any logging is performed;
/// otherwise, no log output will be produced.
/// Subsequent calls are no-ops.
#[unsafe(no_mangle)]
pub extern "C" fn configure_rust_logging(callback: CSharpLogCallback, min_level: CsharpLogLevel) {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        init_logger(callback);

        tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(LevelFilter::from(min_level))
                .with(CSharpForwardingLayer),
        )
        .expect("failed to set global default subscriber - it might have already been set");
    });
}

// --- TESTING HELPERS ---

/// Emits one log entry at every supported level.
///
/// Used by tests to verify Rust-to-C# log forwarding.
#[cfg(feature = "integration_testing")]
#[unsafe(no_mangle)]
pub extern "C" fn emit_all_log_levels() {
    tracing::trace!("This is a trace message");
    tracing::debug!("This is a debug message");
    tracing::info!("This is an info message");
    tracing::warn!("This is a warning message");
    tracing::error!("This is an error message");
}
