//! Prometheus-style metrics facade.
//!
//! A dependency-light, self-contained metrics registry that renders the
//! Prometheus text exposition format. Each process owns a [`Metrics`] registry,
//! instruments the key series at their sources (hit/miss, bytes served, load
//! latency, backend errors, evictions, disk usage, epoch refresh …), and serves
//! [`Metrics::render`] from a `/metrics` endpoint.
//!
//! Three instrument types are supported:
//!
//! - [`Counter`] — monotonically increasing (requests, bytes, errors).
//! - [`Gauge`] — arbitrary up/down value (disk usage, resident bytes, epoch).
//! - [`Histogram`] — bucketed observations (latencies), with `_bucket`, `_sum`,
//!   and `_count` series.
//!
//! Series are keyed by name + a sorted label set, so the same metric can carry
//! `backend`, `worker`, or `form` (whole/paged) labels. All operations are
//! atomic and lock-free on the hot path; a read lock is taken only to look up or
//! create a series.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// A sorted set of label key/value pairs identifying a single series.
pub type Labels = BTreeMap<String, String>;

/// Build a [`Labels`] set from key/value string pairs.
///
/// ```
/// use talon_core::metrics::labels;
/// let l = labels(&[("backend", "s3"), ("form", "whole")]);
/// assert_eq!(l.get("backend").unwrap(), "s3");
/// ```
pub fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The kind of a registered metric, determining how it renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    fn type_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// A monotonically increasing counter series.
#[derive(Clone, Default)]
pub struct Counter {
    v: Arc<AtomicU64>,
}

impl Counter {
    /// Increment by 1.
    pub fn inc(&self) {
        self.add(1);
    }
    /// Increment by `n`.
    pub fn add(&self, n: u64) {
        self.v.fetch_add(n, Ordering::Relaxed);
    }
    /// Current value.
    pub fn get(&self) -> u64 {
        self.v.load(Ordering::Relaxed)
    }
}

/// A gauge that can move up or down, stored as bit-cast `f64`.
#[derive(Clone, Default)]
pub struct Gauge {
    bits: Arc<AtomicU64>,
}

impl Gauge {
    /// Set the gauge to `value`.
    pub fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
    }
    /// Current value.
    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

/// A cumulative histogram with fixed upper-bound buckets.
#[derive(Clone)]
pub struct Histogram {
    bounds: Arc<Vec<f64>>,
    // counts[i] = observations <= bounds[i]; counts[last] = +Inf bucket.
    counts: Arc<Vec<AtomicU64>>,
    sum_bits: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
}

impl Histogram {
    fn new(bounds: Vec<f64>) -> Self {
        let n = bounds.len() + 1; // +Inf bucket
        Self {
            bounds: Arc::new(bounds),
            counts: Arc::new((0..n).map(|_| AtomicU64::new(0)).collect()),
            sum_bits: Arc::new(AtomicU64::new(0)),
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record an observation `v`.
    pub fn observe(&self, v: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|b| v <= *b)
            .unwrap_or(self.bounds.len());
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // Atomically add to the f64 sum via compare-and-swap.
        let mut cur = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + v).to_bits();
            match self.sum_bits.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Total number of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Sum of all observed values.
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
    }
}

enum Series {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
}

struct Family {
    kind: MetricKind,
    help: String,
    // Keyed by the rendered label signature.
    series: BTreeMap<String, (Labels, Series)>,
}

/// A process-wide metrics registry.
#[derive(Clone, Default)]
pub struct Metrics {
    families: Arc<RwLock<BTreeMap<String, Family>>>,
    default_buckets: Arc<Vec<f64>>,
}

fn label_sig(labels: &Labels) -> String {
    let mut s = String::new();
    for (k, v) in labels {
        let _ = write!(s, "{k}={v}\u{1}");
    }
    s
}

impl Metrics {
    /// Create an empty registry with sensible default histogram buckets
    /// (seconds: 1ms … 10s).
    pub fn new() -> Self {
        Self {
            families: Arc::new(RwLock::new(BTreeMap::new())),
            default_buckets: Arc::new(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
        }
    }

    fn ensure_family(&self, name: &str, kind: MetricKind, help: &str) {
        let mut g = self.families.write().unwrap();
        g.entry(name.to_string()).or_insert_with(|| Family {
            kind,
            help: help.to_string(),
            series: BTreeMap::new(),
        });
    }

    /// Get or create a labeled [`Counter`].
    pub fn counter(&self, name: &str, help: &str, labels: Labels) -> Counter {
        self.ensure_family(name, MetricKind::Counter, help);
        let mut g = self.families.write().unwrap();
        let fam = g.get_mut(name).unwrap();
        let sig = label_sig(&labels);
        match fam
            .series
            .entry(sig)
            .or_insert_with(|| (labels, Series::Counter(Counter::default())))
        {
            (_, Series::Counter(c)) => c.clone(),
            _ => panic!("metric {name} already registered with a different type"),
        }
    }

    /// Get or create a labeled [`Gauge`].
    pub fn gauge(&self, name: &str, help: &str, labels: Labels) -> Gauge {
        self.ensure_family(name, MetricKind::Gauge, help);
        let mut g = self.families.write().unwrap();
        let fam = g.get_mut(name).unwrap();
        let sig = label_sig(&labels);
        match fam
            .series
            .entry(sig)
            .or_insert_with(|| (labels, Series::Gauge(Gauge::default())))
        {
            (_, Series::Gauge(x)) => x.clone(),
            _ => panic!("metric {name} already registered with a different type"),
        }
    }

    /// Get or create a labeled [`Histogram`] using the default buckets.
    pub fn histogram(&self, name: &str, help: &str, labels: Labels) -> Histogram {
        let buckets = (*self.default_buckets).clone();
        self.histogram_with(name, help, labels, buckets)
    }

    /// Get or create a labeled [`Histogram`] with explicit bucket bounds.
    pub fn histogram_with(
        &self,
        name: &str,
        help: &str,
        labels: Labels,
        buckets: Vec<f64>,
    ) -> Histogram {
        self.ensure_family(name, MetricKind::Histogram, help);
        let mut g = self.families.write().unwrap();
        let fam = g.get_mut(name).unwrap();
        let sig = label_sig(&labels);
        match fam
            .series
            .entry(sig)
            .or_insert_with(|| (labels, Series::Histogram(Histogram::new(buckets))))
        {
            (_, Series::Histogram(h)) => h.clone(),
            _ => panic!("metric {name} already registered with a different type"),
        }
    }

    /// Render the whole registry in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let g = self.families.read().unwrap();
        let mut out = String::new();
        for (name, fam) in g.iter() {
            let _ = writeln!(out, "# HELP {name} {}", fam.help);
            let _ = writeln!(out, "# TYPE {name} {}", fam.kind.type_str());
            for (_, (labels, series)) in fam.series.iter() {
                match series {
                    Series::Counter(c) => {
                        let _ = writeln!(out, "{name}{} {}", fmt_labels(labels, &[]), c.get());
                    }
                    Series::Gauge(x) => {
                        let _ = writeln!(
                            out,
                            "{name}{} {}",
                            fmt_labels(labels, &[]),
                            fmt_f64(x.get())
                        );
                    }
                    Series::Histogram(h) => {
                        let mut cumulative = 0u64;
                        for (i, b) in h.bounds.iter().enumerate() {
                            cumulative += h.counts[i].load(Ordering::Relaxed);
                            let le = fmt_f64(*b);
                            let _ = writeln!(
                                out,
                                "{name}_bucket{} {cumulative}",
                                fmt_labels(labels, &[("le", &le)])
                            );
                        }
                        cumulative += h.counts[h.bounds.len()].load(Ordering::Relaxed);
                        let _ = writeln!(
                            out,
                            "{name}_bucket{} {cumulative}",
                            fmt_labels(labels, &[("le", "+Inf")])
                        );
                        let _ = writeln!(
                            out,
                            "{name}_sum{} {}",
                            fmt_labels(labels, &[]),
                            fmt_f64(h.sum())
                        );
                        let _ =
                            writeln!(out, "{name}_count{} {}", fmt_labels(labels, &[]), h.count());
                    }
                }
            }
        }
        out
    }
}

fn fmt_f64(v: f64) -> String {
    if v == f64::INFINITY {
        "+Inf".to_string()
    } else {
        format!("{v}")
    }
}

/// Render a label set plus extra pairs as `{k="v",...}`, or empty string.
fn fmt_labels(labels: &Labels, extra: &[(&str, &str)]) -> String {
    if labels.is_empty() && extra.is_empty() {
        return String::new();
    }
    let mut parts: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
        .collect();
    for (k, v) in extra {
        parts.push(format!("{k}=\"{}\"", escape(v)));
    }
    format!("{{{}}}", parts.join(","))
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_and_gauge_render() {
        let m = Metrics::new();
        let hits = m.counter(
            "cache_hits_total",
            "cache hits",
            labels(&[("form", "whole")]),
        );
        hits.inc();
        hits.add(4);
        let disk = m.gauge("disk_used_bytes", "disk usage", labels(&[("worker", "w1")]));
        disk.set(1024.0);

        let out = m.render();
        assert!(out.contains("# TYPE cache_hits_total counter"));
        assert!(out.contains("cache_hits_total{form=\"whole\"} 5"));
        assert!(out.contains("disk_used_bytes{worker=\"w1\"} 1024"));
    }

    #[test]
    fn same_series_is_shared() {
        let m = Metrics::new();
        let a = m.counter("reqs_total", "reqs", labels(&[("backend", "s3")]));
        let b = m.counter("reqs_total", "reqs", labels(&[("backend", "s3")]));
        a.inc();
        b.inc();
        assert_eq!(a.get(), 2, "same name+labels must map to one series");
    }

    #[test]
    fn distinct_labels_are_distinct_series() {
        let m = Metrics::new();
        m.counter("reqs_total", "reqs", labels(&[("backend", "s3")]))
            .add(2);
        m.counter("reqs_total", "reqs", labels(&[("backend", "gcs")]))
            .add(7);
        let out = m.render();
        assert!(out.contains("reqs_total{backend=\"s3\"} 2"));
        assert!(out.contains("reqs_total{backend=\"gcs\"} 7"));
    }

    #[test]
    fn histogram_buckets_are_cumulative() {
        let m = Metrics::new();
        let h = m.histogram_with(
            "load_seconds",
            "load latency",
            Labels::new(),
            vec![0.1, 1.0],
        );
        h.observe(0.05); // <= 0.1
        h.observe(0.5); // <= 1.0
        h.observe(2.0); // +Inf
        let out = m.render();
        assert!(out.contains("load_seconds_bucket{le=\"0.1\"} 1"));
        assert!(out.contains("load_seconds_bucket{le=\"1\"} 2"));
        assert!(out.contains("load_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("load_seconds_count 3"));
        assert_eq!(h.count(), 3);
        assert!((h.sum() - 2.55).abs() < 1e-9);
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::new();
        m.counter("x", "h", labels(&[("k", "a\"b")])).inc();
        assert!(m.render().contains("k=\"a\\\"b\""));
    }
}
