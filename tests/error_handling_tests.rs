//! Error handling and context tests

use emsqrt_core::error::Error;
use emsqrt_operators::traits::OpError;

#[test]
fn test_error_with_context() {
    let base_error = Error::Schema("unknown column 'xyz'".to_string());
    let contextual_error = base_error.with_context("while processing filter operator");
    
    match contextual_error {
        Error::Context { context, .. } => {
            assert_eq!(context, "while processing filter operator");
        }
        _ => panic!("Expected Context variant"),
    }
}

#[test]
fn test_error_suggestions() {
    let schema_error = Error::Schema("unknown column 'xyz'".to_string());
    let suggestions = schema_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("column")));
}

#[test]
fn test_config_error_suggestions() {
    let config_error = Error::Config("memory cap exceeded".to_string());
    let suggestions = config_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("memory")));
}

#[test]
fn test_operror_with_context() {
    let base_error = OpError::Schema("unknown column".to_string());
    let contextual_error = base_error.with_context("filter operator");
    
    match contextual_error {
        OpError::Schema(msg) => {
            assert!(msg.contains("filter operator"));
            assert!(msg.contains("unknown column"));
        }
        _ => panic!("Expected Schema variant"),
    }
}

#[test]
fn test_operror_is_recoverable() {
    let recoverable = OpError::Recoverable("transient I/O error".to_string());
    assert!(recoverable.is_recoverable());
    
    let non_recoverable = OpError::Exec("permanent error".to_string());
    assert!(!non_recoverable.is_recoverable());
}

#[test]
fn test_operror_suggestions() {
    let schema_error = OpError::Schema("unknown column 'xyz'".to_string());
    let suggestions = schema_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("column")));
    
    let exec_error = OpError::Exec("failed to parse expression".to_string());
    let suggestions = exec_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("expression") || s.contains("syntax")));
    
    let recoverable_error = OpError::Recoverable("network timeout".to_string());
    let suggestions = recoverable_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("retry") || s.contains("transient")));
}

#[test]
fn test_memory_error_with_context() {
    use emsqrt_mem::error::Error as MemError;
    
    let base_error = MemError::BudgetExceeded {
        tag: "test",
        requested: 1000,
        capacity: 500,
        used: 400,
    };
    
    let contextual_error = base_error.with_context("while allocating buffer");
    
    match contextual_error {
        MemError::Budget(msg) => {
            assert!(msg.contains("while allocating buffer"));
            assert!(msg.contains("budget exceeded"));
        }
        _ => panic!("Expected Budget variant"),
    }
}

#[test]
fn test_memory_error_suggestions() {
    use emsqrt_mem::error::Error as MemError;
    
    let budget_error = MemError::BudgetExceeded {
        tag: "test",
        requested: 1000,
        capacity: 500,
        used: 400,
    };
    
    let suggestions = budget_error.suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions.iter().any(|s| s.contains("memory_cap_bytes")));
    assert!(suggestions.iter().any(|s| s.contains("external")));
}

