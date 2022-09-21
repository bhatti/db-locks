use prometheus::{self, Registry, IntCounterVec, HistogramVec, HistogramOpts, Opts, TextEncoder, Encoder};
use std::collections::HashMap;
use std::time::Instant;
use crate::domain::models::{LockResult, LockError};

pub(crate) struct LockMetric<'a> {
    metrics: &'a LockMetrics,
    method: String,
    status: String,
    clock: Instant,
}

impl<'a> Drop for LockMetric<'a> {
    fn drop(&mut self) {
        let method = self.method.to_string();
        let status = self.status.to_string();

        let elapsed = self.clock.elapsed();
        let duration =
            (elapsed.as_secs() as f64) + f64::from(elapsed.subsec_nanos()) / 1_000_000_000_f64;
        self.metrics.requests_duration_seconds
            .with_label_values(&[&method, &status])
            .observe(duration);

        self.metrics.requests_total
            .with_label_values(&[&method, &status])
            .inc();
    }
}

impl<'a> LockMetric<'a> {
    pub fn new(
        metrics: &'a LockMetrics,
        method: &str) -> Self {
        LockMetric {
            metrics,
            method: method.to_string(),
            status: String::from(""),
            clock: Instant::now(),
        }
    }
}

pub(crate) struct LockMetrics {
    requests_total: IntCounterVec,
    requests_duration_seconds: HistogramVec,
    acquire_retry_wait_total: IntCounterVec,
}

impl LockMetrics {
    pub fn new(
        registry: Registry) -> LockResult<Self> {
        let const_labels = HashMap::new();
        let buckets = prometheus::DEFAULT_BUCKETS.to_vec();
        let metrics = LockMetrics {
            requests_total: IntCounterVec::new(Opts::new("requests_total", "Total number of lock acquire requests").const_labels(const_labels.clone()), &["method", "status"])?,
            requests_duration_seconds: HistogramVec::new(HistogramOpts::new("requests_duration_seconds", "Lock acquire request duration in seconds for all requests").buckets(buckets.to_vec()).const_labels(const_labels.clone()), &["method", "status"])?,
            acquire_retry_wait_total: IntCounterVec::new(Opts::new("acquire_retry_wait_total", "Total number of lock waits while acquiring").const_labels(const_labels.clone()), &[])?,
        };
        match metrics.register(registry) {
            Ok(_) => {}
            Err(err) => {
                match err {
                    // ignore AlreadyReg errors
                    prometheus::Error::AlreadyReg => {}
                    _ => {
                        return Err(LockError::validation(format!("prometheus validation {:?}", err), None));
                    }
                }
            }
        }
        Ok(metrics)
    }

    fn register(&self, registry: Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.requests_total.clone()))?;
        registry.register(Box::new(self.requests_duration_seconds.clone()))?;
        registry.register(Box::new(self.acquire_retry_wait_total.clone()))?;
        Ok(())
    }

    pub(crate) fn inc_retry_wait(&self) {
        self.acquire_retry_wait_total.with_label_values(&[]).inc();
    }

    pub(crate) fn get_request_total(&self, method: &str) -> i64 {
        match self.requests_total.get_metric_with_label_values(&[method, ""]) {
            Ok(count) => { count.get() as i64 }
            Err(_) => { 0 }
        }
    }

    pub(crate) fn new_metric(&self, method: &str) -> LockMetric {
        LockMetric::new(self, method)
    }

    pub(crate) fn dump(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer.clone()).unwrap()
    }
}
