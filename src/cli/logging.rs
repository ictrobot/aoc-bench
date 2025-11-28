use std::fmt::{self, Debug, Formatter};
use std::io;
use tracing::field::Field;
use tracing_subscriber::field::{MakeVisitor, Visit, VisitFmt, VisitOutput, VisitWrite};

/// Initialize logging.
///
/// Will panic if called more than once.
pub fn init() {
    let format = std::env::var("LOG_FORMAT").unwrap_or_default();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        );

    if format == "json" {
        subscriber.json().init();
    } else {
        // Only modify fields in the pretty output, keep the full float precision in JSON
        subscriber.map_fmt_fields(CustomFmt).init();
    }
}

#[derive(Debug, Clone)]
struct CustomFmt<V>(V);

impl<T, V> MakeVisitor<T> for CustomFmt<V>
where
    V: MakeVisitor<T>,
{
    type Visitor = CustomFmt<V::Visitor>;

    #[inline]
    fn make_visitor(&self, target: T) -> Self::Visitor {
        CustomFmt(self.0.make_visitor(target))
    }
}

impl<V> Visit for CustomFmt<V>
where
    V: Visit,
{
    #[inline]
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.record_debug(field, &FloatWrapper(value));
    }

    #[inline]
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.record_i64(field, value);
    }

    #[inline]
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.record_u64(field, value);
    }

    #[inline]
    fn record_i128(&mut self, field: &Field, value: i128) {
        self.0.record_i128(field, value);
    }

    #[inline]
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0.record_u128(field, value);
    }

    #[inline]
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.record_bool(field, value);
    }

    #[inline]
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.record_str(field, value);
    }

    #[inline]
    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.0.record_bytes(field, value);
    }

    #[inline]
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0.record_error(field, value);
    }

    #[inline]
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.record_debug(field, value);
    }
}

impl<V, O> VisitOutput<O> for CustomFmt<V>
where
    V: VisitOutput<O>,
{
    #[inline]
    fn finish(self) -> O {
        self.0.finish()
    }
}

impl<V> VisitWrite for CustomFmt<V>
where
    V: VisitWrite,
{
    #[inline]
    fn writer(&mut self) -> &mut dyn io::Write {
        self.0.writer()
    }
}

impl<V> VisitFmt for CustomFmt<V>
where
    V: VisitFmt,
{
    #[inline]
    fn writer(&mut self) -> &mut dyn fmt::Write {
        self.0.writer()
    }
}

// Avoid printing full precision floats in logs
struct FloatWrapper(f64);

impl Debug for FloatWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        #[allow(clippy::cast_precision_loss)]
        const MAX_SAFE_INTEGER: f64 = (1u64 << f64::MANTISSA_DIGITS) as f64;

        if !self.0.is_finite() {
            return write!(f, "{:?}", self.0);
        }

        if self.0.fract() == 0.0 && self.0.abs() <= MAX_SAFE_INTEGER {
            return write!(f, "{:.0}", self.0);
        }

        let abs = self.0.abs();
        if abs >= 1e6 {
            write!(f, "{:.1}", self.0)
        } else if abs >= 1e3 {
            write!(f, "{:.2}", self.0)
        } else if abs >= 1.0 {
            write!(f, "{:.3}", self.0)
        } else if abs >= 0.01 {
            write!(f, "{:.4}", self.0)
        } else {
            write!(f, "{:.4e}", self.0)
        }
    }
}
