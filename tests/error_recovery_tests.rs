//! Error recovery and retry logic tests

use emsqrt_operators::traits::OpError;

#[test]
fn test_recoverable_error_detection() {
    let recoverable = OpError::Recoverable("transient error".to_string());
    assert!(recoverable.is_recoverable());
    
    let non_recoverable = OpError::Exec("permanent error".to_string());
    assert!(!non_recoverable.is_recoverable());
}

#[test]
fn test_error_recovery_suggestions() {
    let recoverable = OpError::Recoverable("network timeout".to_string());
    let suggestions = recoverable.suggestions();
    
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("retry") || s.contains("transient")));
    assert!(suggestions.iter().any(|s| s.contains("network") || s.contains("storage")));
}

// Note: Actual retry logic is tested in integration tests with the runtime
// These are unit tests for the error types themselves

