use std::collections::HashMap;
use std::time::Instant;

use prometheus::{self, Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};
use prometheus::proto::MetricType;

use crate::domain::models::{LockError, LockResult};

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
        prefix: &str,
        registry: &Registry) -> LockResult<Self> {
        let const_labels = HashMap::new();
        let buckets = prometheus::DEFAULT_BUCKETS.to_vec();
        let metrics = LockMetrics {
            requests_total: IntCounterVec::new(Opts::new(
                format!("{}requests_total", prefix).as_str(), "Total number of lock acquire requests")
                                                   .const_labels(const_labels.clone()), &["method", "status"])?,
            requests_duration_seconds: HistogramVec::new(HistogramOpts::new(
                format!("{}requests_duration_seconds", prefix).as_str(), "Lock acquire request duration in seconds for all requests")
                                                             .buckets(buckets.to_vec()).const_labels(const_labels.clone()), &["method", "status"])?,
            acquire_retry_wait_total: IntCounterVec::new(Opts::new(
                format!("{}acquire_retry_wait_total", prefix).as_str(), "Total number of lock waits while acquiring")
                                                             .const_labels(const_labels.clone()), &[])?,
        };
        match metrics.register(registry) {
            Ok(_) => {}
            Err(err) => {
                match err {
                    // ignore AlreadyReg errors
                    prometheus::Error::AlreadyReg => {}
                    _ => {
                        return Err(LockError::validation(
                            format!("prometheus validation {:?}", err).as_str(), None));
                    }
                }
            }
        }
        Ok(metrics)
    }

    fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
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

    pub(crate) fn summary(&self) -> HashMap<String, f64> {
        let mut summary = HashMap::new();
        for metric in prometheus::gather() {
            for m in metric.get_metric() {
                if m.get_label().len() > 0 && metric.get_field_type() == MetricType::HISTOGRAM {
                    if metric.get_field_type() == MetricType::HISTOGRAM {
                        summary.insert(
                            format!("{}[{}]_SUM", metric.get_name(), m.get_label()[0].get_value()),
                            m.get_histogram().get_sample_sum());
                        summary.insert(
                            format!("{}[{}]_TOTAL", metric.get_name(), m.get_label()[0].get_value()),
                            m.get_histogram().get_sample_count() as f64);
                    } else if metric.get_field_type() == MetricType::COUNTER {
                        summary.insert(
                            format!("{}[{}]_COUNTER", metric.get_name(), m.get_label()[0].get_value()),
                            m.get_counter().get_value());
                    }
                }
            }
        }
        summary
    }

    pub(crate) fn dump(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer.clone()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread};
    use std::time::Duration;

    use env_logger::Env;
    use prometheus::default_registry;
    use rand::Rng;

    use crate::utils::lock_metrics::{LockMetrics};

    #[test]
    fn test_should_add_metrics() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).is_test(true).try_init();
        let registry = &default_registry();
        let metrics1 = LockMetrics::new("source1_", registry).unwrap();
        let metrics2 = LockMetrics::new("source2_", registry).unwrap();
        let mut rng = rand::thread_rng();
        for _i in 0..100 {
            metrics1.new_metric("method1");
            thread::sleep(Duration::from_millis(rng.gen_range(1..5)));
        }
        for _i in 0..100 {
            metrics2.new_metric("method2");
            thread::sleep(Duration::from_millis(rng.gen_range(5..10)));
        }
        assert_eq!(100, metrics1.get_request_total("method1"));
        assert_eq!(100, metrics2.get_request_total("method2"));
    }
}


