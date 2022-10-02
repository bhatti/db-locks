use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) mod lock_metrics;

pub fn current_time_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("could not get time").as_millis() as i64
}


