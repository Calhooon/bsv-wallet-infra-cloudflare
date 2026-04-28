//! Error types for wallet-infra.

use serde::Serialize;
use std::fmt;

#[derive(Debug, Serialize)]
pub enum Error {
    ValidationError(String),
    DatabaseError(String),
    NotFound(String),
    InternalError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            Error::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            Error::NotFound(msg) => write!(f, "Not found: {}", msg),
            Error::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl From<worker::Error> for Error {
    fn from(e: worker::Error) -> Self {
        Error::InternalError(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::ValidationError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Display formatting
    // =========================================================================

    #[test]
    fn display_validation_error() {
        let err = Error::ValidationError("missing field".to_string());
        assert_eq!(format!("{}", err), "Validation error: missing field");
    }

    #[test]
    fn display_database_error() {
        let err = Error::DatabaseError("connection refused".to_string());
        assert_eq!(format!("{}", err), "Database error: connection refused");
    }

    #[test]
    fn display_not_found() {
        let err = Error::NotFound("user 42".to_string());
        assert_eq!(format!("{}", err), "Not found: user 42");
    }

    #[test]
    fn display_internal_error() {
        let err = Error::InternalError("unexpected panic".to_string());
        assert_eq!(format!("{}", err), "Internal error: unexpected panic");
    }

    // =========================================================================
    // From<serde_json::Error> conversion
    // =========================================================================

    #[test]
    fn from_serde_json_error() {
        // Trigger a real serde_json::Error by parsing invalid JSON.
        let serde_err = serde_json::from_str::<serde_json::Value>("not valid json").unwrap_err();
        let err: Error = Error::from(serde_err);
        match &err {
            Error::ValidationError(msg) => {
                assert!(msg.contains("expected"), "message was: {}", msg);
            }
            _ => panic!("expected ValidationError, got {:?}", err),
        }
    }

    // =========================================================================
    // Serialize
    // =========================================================================

    #[test]
    fn serialize_validation_error() {
        let err = Error::ValidationError("bad input".to_string());
        let val = serde_json::to_value(&err).unwrap();
        // Enum serialization: {"ValidationError": "bad input"}
        assert_eq!(val["ValidationError"], "bad input");
    }

    #[test]
    fn serialize_not_found() {
        let err = Error::NotFound("output 99".to_string());
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["NotFound"], "output 99");
    }

    #[test]
    fn serialize_database_error() {
        let err = Error::DatabaseError("timeout".to_string());
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["DatabaseError"], "timeout");
    }

    #[test]
    fn serialize_internal_error() {
        let err = Error::InternalError("oom".to_string());
        let val = serde_json::to_value(&err).unwrap();
        assert_eq!(val["InternalError"], "oom");
    }

    // =========================================================================
    // Error code mapping (mirrors dispatch.rs logic)
    // =========================================================================

    #[test]
    fn error_code_mapping() {
        // This tests the pattern used in dispatch.rs to map errors to JSON-RPC codes.
        let test_cases: Vec<(Error, i32)> = vec![
            (Error::ValidationError("v".into()), -32602),
            (Error::NotFound("n".into()), -32001),
            (Error::DatabaseError("d".into()), -32603),
            (Error::InternalError("i".into()), -32603),
        ];

        for (error, expected_code) in test_cases {
            let (code, _msg) = match &error {
                Error::ValidationError(m) => (-32602, m.clone()),
                Error::NotFound(m) => (-32001, m.clone()),
                Error::DatabaseError(m) => (-32603, m.clone()),
                Error::InternalError(m) => (-32603, m.clone()),
            };
            assert_eq!(code, expected_code, "wrong code for {:?}", error);
        }
    }

    // =========================================================================
    // Debug impl
    // =========================================================================

    #[test]
    fn debug_format_includes_variant_name() {
        let err = Error::ValidationError("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ValidationError"));
        assert!(debug_str.contains("test"));
    }

    // =========================================================================
    // Error messages extracted correctly
    // =========================================================================

    #[test]
    fn error_message_extraction() {
        let errors = vec![
            Error::ValidationError("msg_v".to_string()),
            Error::NotFound("msg_n".to_string()),
            Error::DatabaseError("msg_d".to_string()),
            Error::InternalError("msg_i".to_string()),
        ];

        let messages: Vec<String> = errors
            .iter()
            .map(|e| match e {
                Error::ValidationError(m) => m.clone(),
                Error::NotFound(m) => m.clone(),
                Error::DatabaseError(m) => m.clone(),
                Error::InternalError(m) => m.clone(),
            })
            .collect();

        assert_eq!(messages, vec!["msg_v", "msg_n", "msg_d", "msg_i"]);
    }
}
