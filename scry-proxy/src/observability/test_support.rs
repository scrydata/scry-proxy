//! Test-only capturing `tracing::Layer` (Task 9, P4 §4.6).
//!
//! Records span creation/record/close and event fields in-memory so tests can
//! assert on real span names + attribute KEYS produced by driving actual code
//! (not by calling span-builder helpers directly), and — the safety-critical
//! half — that no raw SQL literal ever reaches a span field or log event when
//! `unsafe_debug_logging` is off. This mirrors what `tracing-opentelemetry`'s
//! real `OpenTelemetryLayer` does (span-scoped extension storage via
//! `Registry`'s `LookupSpan`), just recording to memory instead of exporting.
//!
//! It never touches the real OTLP/`fmt` layers `observability::init` installs;
//! it is composed onto its own private `tracing_subscriber::registry()` and
//! installed only for the lifetime of one test via
//! `tracing::subscriber::set_default` — never process-global `init()`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// A single closed span: its name and every field key it declared, mapped to
/// that field's last recorded value (debug-formatted). A field declared
/// `tracing::field::Empty` and never recorded keeps the empty-string
/// placeholder — its KEY is still present, which is all a span-coverage
/// assertion needs.
#[derive(Debug, Clone, Default)]
pub(crate) struct CapturedSpan {
    pub name: String,
    pub fields: HashMap<String, String>,
}

/// Everything captured during a test: closed spans, plus every log event's
/// fields (including `message`), in closure/emission order.
#[derive(Debug, Clone, Default)]
pub(crate) struct Captured {
    pub spans: Vec<CapturedSpan>,
    pub events: Vec<HashMap<String, String>>,
}

impl Captured {
    /// Whether ANY captured span field or log event field contains `needle`
    /// as a substring of its (debug-formatted) value — the single assertion
    /// the log-safety test needs: a distinctive literal must appear NOWHERE.
    pub(crate) fn contains_value(&self, needle: &str) -> bool {
        self.spans.iter().any(|s| s.fields.values().any(|v| v.contains(needle)))
            || self.events.iter().any(|e| e.values().any(|v| v.contains(needle)))
    }

    /// The first closed span with the given name, if any.
    pub(crate) fn span_named(&self, name: &str) -> Option<&CapturedSpan> {
        self.spans.iter().find(|s| s.name == name)
    }
}

struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

impl Visit for FieldVisitor<'_> {
    // `Visit`'s other `record_*` methods (str/i64/u64/bool/f64) all forward to
    // `record_debug` by default, so this one impl captures every field type.
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// Per-span extension storage: the field map accumulated across
/// `on_new_span` + any `on_record` calls, read back at `on_close`.
struct SpanFields(HashMap<String, String>);

/// Test-only `Layer`: captures span lifecycle + event fields into `Captured`.
pub(crate) struct CapturingLayer {
    captured: Arc<Mutex<Captured>>,
}

impl CapturingLayer {
    fn new() -> (Self, Arc<Mutex<Captured>>) {
        let captured = Arc::new(Mutex::new(Captured::default()));
        (Self { captured: Arc::clone(&captured) }, captured)
    }
}

impl<S> Layer<S> for CapturingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // Seed every declared field name (including `Empty` ones) so the KEY
        // is always observable even before a value is ever recorded, then
        // record whatever values were actually passed at creation time.
        let mut fields: HashMap<String, String> = attrs
            .metadata()
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), String::new()))
            .collect();
        let mut visitor = FieldVisitor(&mut fields);
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields(fields));
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(SpanFields(fields)) = extensions.get_mut::<SpanFields>() {
                let mut visitor = FieldVisitor(fields);
                values.record(&mut visitor);
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let name = span.name().to_string();
            let fields =
                span.extensions().get::<SpanFields>().map(|f| f.0.clone()).unwrap_or_default();
            self.captured.lock().unwrap().spans.push(CapturedSpan { name, fields });
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        let mut visitor = FieldVisitor(&mut fields);
        event.record(&mut visitor);
        self.captured.lock().unwrap().events.push(fields);
    }
}

/// Build a `tracing_subscriber::Registry` with the capturing layer installed,
/// plus the shared `Captured` handle to inspect afterward. Install with
/// `tracing::subscriber::set_default` for the duration of a single test
/// (`#[tokio::test]`'s default current-thread runtime keeps everything on one
/// OS thread, so a thread-local default covers the whole async body) —
/// deliberately never the process-global `init()`, so it can never collide
/// with another test's subscriber.
pub(crate) fn capturing_subscriber() -> (impl Subscriber, Arc<Mutex<Captured>>) {
    let (layer, captured) = CapturingLayer::new();
    (tracing_subscriber::registry().with(layer), captured)
}
