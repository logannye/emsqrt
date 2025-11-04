//! Expression evaluation tests

use emsqrt_core::expr::Expr;
use emsqrt_core::schema::{DataType, Field, Schema};
use emsqrt_core::types::{Column, RowBatch, Scalar};

fn create_test_batch() -> RowBatch {
    RowBatch {
        columns: vec![
            Column {
                name: "age".to_string(),
                values: vec![
                    Scalar::I32(25),
                    Scalar::I32(18),
                    Scalar::I32(30),
                    Scalar::Null,
                ],
            },
            Column {
                name: "name".to_string(),
                values: vec![
                    Scalar::Str("Alice".to_string()),
                    Scalar::Str("Bob".to_string()),
                    Scalar::Str("Charlie".to_string()),
                    Scalar::Str("David".to_string()),
                ],
            },
            Column {
                name: "price".to_string(),
                values: vec![
                    Scalar::F64(10.5),
                    Scalar::F64(20.0),
                    Scalar::F64(15.75),
                    Scalar::F64(5.0),
                ],
            },
            Column {
                name: "quantity".to_string(),
                values: vec![
                    Scalar::I32(2),
                    Scalar::I32(3),
                    Scalar::I32(1),
                    Scalar::I32(4),
                ],
            },
        ],
    }
}

#[test]
fn test_evaluate_column() {
    let batch = create_test_batch();
    let expr = Expr::parse("age").unwrap();
    
    let result = expr.evaluate(&batch, 0).unwrap();
    assert_eq!(result, Scalar::I32(25));
    
    let result = expr.evaluate(&batch, 1).unwrap();
    assert_eq!(result, Scalar::I32(18));
}

#[test]
fn test_evaluate_literal() {
    let batch = create_test_batch();
    let expr = Expr::parse("42").unwrap();
    
    let result = expr.evaluate(&batch, 0).unwrap();
    assert_eq!(result, Scalar::I32(42));
}

#[test]
fn test_evaluate_comparison() {
    let batch = create_test_batch();
    let expr = Expr::parse("age > 18").unwrap();
    
    // Row 0: age=25 > 18 -> true
    let result = expr.evaluate_bool(&batch, 0).unwrap();
    assert_eq!(result, true);
    
    // Row 1: age=18 > 18 -> false
    let result = expr.evaluate_bool(&batch, 1).unwrap();
    assert_eq!(result, false);
    
    // Row 2: age=30 > 18 -> true
    let result = expr.evaluate_bool(&batch, 2).unwrap();
    assert_eq!(result, true);
}

#[test]
fn test_evaluate_equality() {
    let batch = create_test_batch();
    let expr = Expr::parse("name == \"Alice\"").unwrap();
    
    // Row 0: name="Alice" -> true
    let result = expr.evaluate_bool(&batch, 0).unwrap();
    assert_eq!(result, true);
    
    // Row 1: name="Bob" -> false
    let result = expr.evaluate_bool(&batch, 1).unwrap();
    assert_eq!(result, false);
}

#[test]
fn test_evaluate_arithmetic() {
    let batch = create_test_batch();
    let expr = Expr::parse("price * quantity").unwrap();
    
    // Row 0: 10.5 * 2 = 21.0
    let result = expr.evaluate(&batch, 0).unwrap();
    assert!(matches!(result, Scalar::F64(v) if (v - 21.0).abs() < 0.001));
    
    // Row 1: 20.0 * 3 = 60.0
    let result = expr.evaluate(&batch, 1).unwrap();
    assert!(matches!(result, Scalar::F64(v) if (v - 60.0).abs() < 0.001));
}

#[test]
fn test_evaluate_addition() {
    let batch = create_test_batch();
    let expr = Expr::parse("age + 5").unwrap();
    
    // Row 0: 25 + 5 = 30
    let result = expr.evaluate(&batch, 0).unwrap();
    assert_eq!(result, Scalar::I32(30));
}

#[test]
fn test_evaluate_division() {
    let batch = create_test_batch();
    let expr = Expr::parse("price / 2").unwrap();
    
    // Row 0: 10.5 / 2 = 5.25
    let result = expr.evaluate(&batch, 0).unwrap();
    assert!(matches!(result, Scalar::F64(v) if (v - 5.25).abs() < 0.001));
}

#[test]
fn test_evaluate_logical_and() {
    let batch = create_test_batch();
    let expr = Expr::parse("age > 20 AND price < 15").unwrap();
    
    // Row 0: age=25 > 20 (true) AND price=10.5 < 15 (true) -> true
    let result = expr.evaluate_bool(&batch, 0).unwrap();
    assert_eq!(result, true);
    
    // Row 1: age=18 > 20 (false) AND price=20.0 < 15 (false) -> false
    let result = expr.evaluate_bool(&batch, 1).unwrap();
    assert_eq!(result, false);
}

#[test]
fn test_evaluate_logical_or() {
    let batch = create_test_batch();
    let expr = Expr::parse("age < 20 OR price > 15").unwrap();
    
    // Row 0: age=25 < 20 (false) OR price=10.5 > 15 (false) -> false
    let result = expr.evaluate_bool(&batch, 0).unwrap();
    assert_eq!(result, false);
    
    // Row 1: age=18 < 20 (true) OR price=20.0 > 15 (true) -> true
    let result = expr.evaluate_bool(&batch, 1).unwrap();
    assert_eq!(result, true);
}

#[test]
fn test_evaluate_missing_column() {
    let batch = create_test_batch();
    let expr = Expr::parse("nonexistent").unwrap();
    
    let result = expr.evaluate(&batch, 0);
    assert!(result.is_err());
}

#[test]
fn test_evaluate_out_of_bounds() {
    let batch = create_test_batch();
    let expr = Expr::parse("age").unwrap();
    
    let result = expr.evaluate(&batch, 100);
    assert!(result.is_err());
}

#[test]
fn test_evaluate_null_handling() {
    let batch = create_test_batch();
    let expr = Expr::parse("age > 18").unwrap();
    
    // Row 3 has null age - should evaluate to false
    let result = expr.evaluate_bool(&batch, 3).unwrap();
    assert_eq!(result, false);
}

#[test]
fn test_evaluate_division_by_zero() {
    let batch = RowBatch {
        columns: vec![
            Column {
                name: "value".to_string(),
                values: vec![Scalar::I32(10), Scalar::I32(0)],
            },
        ],
    };
    
    let expr = Expr::parse("value / 0").unwrap();
    
    // Division by zero should error
    let result = expr.evaluate(&batch, 1);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("division by zero"));
}

