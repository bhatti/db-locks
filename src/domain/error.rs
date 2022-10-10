use std::fmt::{Display, Formatter};
use std::fmt;
use std::error::Error;

//
#[derive(Debug)]
pub enum LockError {
    Database {
        message: String,
        reason_code: Option<String>,
        retryable: bool,
    },
    AccessDenied {
        message: String,
        reason_code: Option<String>,
    },
    NotGranted {
        message: String,
        reason_code: Option<String>,
    },
    DuplicateKey {
        message: String,
    },
    NotFound {
        message: String,
    },

    // This is a retry-able error, which indicates that the lock being requested has already been
    // held by another worker and has not been released yet and the lease duration has not expired
    // since the lock was last updated by the current tenant_id.
    // The caller can retry acquiring the lock with or without a backoff.
    CurrentlyUnavailable {
        message: String,
        reason_code: Option<String>,
        retryable: bool,
    },
    Validation {
        message: String,
        reason_code: Option<String>,
    },
    Serialization {
        message: String,
    },
    Runtime {
        message: String,
        reason_code: Option<String>,
    },
}

impl LockError {
    pub fn database(message: &str, reason_code: Option<String>, retryable: bool) -> LockError {
        LockError::Database { message: message.to_string(), reason_code, retryable }
    }

    pub fn access_denied(message: &str, reason_code: Option<String>) -> LockError {
        LockError::AccessDenied { message: message.to_string(), reason_code }
    }

    pub fn not_granted(message: &str, reason_code: Option<String>) -> LockError {
        LockError::NotGranted { message: message.to_string(), reason_code }
    }

    pub fn duplicate_key(message: &str) -> LockError {
        LockError::DuplicateKey { message: message.to_string() }
    }

    pub fn not_found(message: &str) -> LockError {
        LockError::NotFound { message: message.to_string() }
    }

    pub fn unavailable(message: &str, reason_code: Option<String>, retryable: bool) -> LockError {
        LockError::CurrentlyUnavailable { message: message.to_string(), reason_code, retryable }
    }

    pub fn validation(message: &str, reason_code: Option<String>) -> LockError {
        LockError::Validation { message: message.to_string(), reason_code }
    }

    pub fn serialization(message: &str) -> LockError {
        LockError::Serialization { message: message.to_string() }
    }

    pub fn runtime(message: &str, reason_code: Option<String>) -> LockError {
        LockError::Runtime { message: message.to_string(), reason_code }
    }

    pub fn retryable(&self) -> bool {
        match self {
            LockError::Database { retryable, .. } => { *retryable }
            LockError::AccessDenied { .. } => { false }
            LockError::NotGranted { .. } => { false }
            LockError::DuplicateKey { .. } => { false }
            LockError::NotFound { .. } => { false }
            LockError::CurrentlyUnavailable { retryable, .. } => { *retryable }
            LockError::Validation { .. } => { false }
            LockError::Serialization { .. } => { false }
            LockError::Runtime { .. } => { false }
        }
    }
}

impl std::convert::From<std::io::Error> for LockError {
    fn from(err: std::io::Error) -> Self {
        LockError::runtime(
            format!("serde validation {:?}", err).as_str(), None)
    }
}

impl std::convert::From<serde_json::Error> for LockError {
    fn from(err: serde_json::Error) -> Self {
        LockError::serialization(
            format!("serde validation {:?}", err).as_str())
    }
}

impl std::convert::From<prometheus::Error> for LockError {
    fn from(err: prometheus::Error) -> Self {
        LockError::validation(
            format!("prometheus validation {:?}", err).as_str(), None)
    }
}

impl Display for LockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Database { message, reason_code, retryable } => {
                write!(f, "{} {:?} {}", message, reason_code, retryable)
            }
            LockError::AccessDenied { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::NotGranted { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::DuplicateKey { message } => {
                write!(f, "{}", message)
            }
            LockError::NotFound { message } => {
                write!(f, "{}", message)
            }
            LockError::CurrentlyUnavailable { message, reason_code, retryable } => {
                write!(f, "{} {:?} {}", message, reason_code, retryable)
            }
            LockError::Validation { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::Serialization { message } => {
                write!(f, "{}", message)
            }
            LockError::Runtime { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
        }
    }
}

impl Error for LockError {}
