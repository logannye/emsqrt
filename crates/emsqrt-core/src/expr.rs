//! Expression engine for SQL-like expressions.
//!
//! Supports arithmetic operations, comparisons, logical operations, and column references.
//! Used by Filter and Project operators for complex expressions.

use serde::{Deserialize, Serialize};

use crate::types::{RowBatch, Scalar};

/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinOp {
    // Comparison operators
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical operators
    And,
    Or,
    // Arithmetic operators
    Add,
    Sub,
    Mul,
    Div,
}

impl BinOp {
    /// Parse a binary operator from a string.
    pub fn parse(op: &str) -> Result<Self, String> {
        match op {
            "==" | "=" => Ok(BinOp::Eq),
            "!=" | "<>" => Ok(BinOp::Ne),
            "<" => Ok(BinOp::Lt),
            "<=" => Ok(BinOp::Le),
            ">" => Ok(BinOp::Gt),
            ">=" => Ok(BinOp::Ge),
            "AND" | "and" | "&&" => Ok(BinOp::And),
            "OR" | "or" | "||" => Ok(BinOp::Or),
            "+" => Ok(BinOp::Add),
            "-" => Ok(BinOp::Sub),
            "*" => Ok(BinOp::Mul),
            "/" => Ok(BinOp::Div),
            _ => Err(format!("unknown binary operator: {}", op)),
        }
    }
}

/// Unary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
}

impl UnaryOp {
    /// Parse a unary operator from a string.
    pub fn parse(op: &str) -> Result<Self, String> {
        match op.to_uppercase().as_str() {
            "NOT" | "!" => Ok(UnaryOp::Not),
            "ISNULL" | "IS NULL" => Ok(UnaryOp::IsNull),
            "ISNOTNULL" | "IS NOT NULL" => Ok(UnaryOp::IsNotNull),
            _ => Err(format!("unknown unary operator: {}", op)),
        }
    }
}

/// Expression AST for SQL-like expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Column reference: "column_name"
    Column(String),
    /// Literal value: 42, "hello", true, etc.
    Literal(Scalar),
    /// Binary operation: left OP right
    BinaryOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation: OP arg
    UnaryOp {
        op: UnaryOp,
        arg: Box<Expr>,
    },
}

impl Expr {
    /// Parse a simple expression string into an Expr AST.
    ///
    /// Currently supports simple predicates like "col OP literal" or "col1 OP col2".
    /// More complex expressions (with parentheses, AND/OR) will be added later.
    pub fn parse(expr_str: &str) -> Result<Self, String> {
        let expr_str = expr_str.trim();
        
        // Try to parse simple binary operations
        let ops = ["==", "!=", "<=", ">=", "<", ">", "+", "-", "*", "/", "AND", "OR"];
        
        for op_str in &ops {
            if let Some(pos) = expr_str.find(op_str) {
                let left_str = expr_str[..pos].trim();
                let right_str = expr_str[pos + op_str.len()..].trim();
                
                if !left_str.is_empty() && !right_str.is_empty() {
                    let op = BinOp::parse(op_str)?;
                    let left = Self::parse_atom(left_str)?;
                    let right = Self::parse_atom(right_str)?;
                    return Ok(Expr::BinaryOp {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    });
                }
            }
        }
        
        // Try to parse as a single atom (column or literal)
        Self::parse_atom(expr_str)
    }

    /// Parse an atomic expression (column or literal).
    fn parse_atom(atom_str: &str) -> Result<Self, String> {
        let atom_str = atom_str.trim();
        
        // Try to parse as literal first
        if let Ok(scalar) = parse_literal(atom_str) {
            return Ok(Expr::Literal(scalar));
        }
        
        // Otherwise, treat as column reference
        Ok(Expr::Column(atom_str.to_string()))
    }

    /// Evaluate an expression against a row in a RowBatch.
    ///
    /// Returns the resulting Scalar value.
    pub fn evaluate(&self, batch: &RowBatch, row_idx: usize) -> Result<Scalar, String> {
        match self {
            Expr::Column(name) => {
                // Find column and get value at row_idx
                let col = batch
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        let available: Vec<String> = batch.columns.iter().map(|c| c.name.clone()).collect();
                        format!("column '{}' not found. Available columns: {:?}", name, available)
                    })?;
                
                if row_idx >= col.values.len() {
                    return Err(format!("row index {} out of bounds", row_idx));
                }
                
                Ok(col.values[row_idx].clone())
            }
            Expr::Literal(scalar) => Ok(scalar.clone()),
            Expr::BinaryOp { op, left, right } => {
                let left_val = left.evaluate(batch, row_idx)?;
                let right_val = right.evaluate(batch, row_idx)?;
                evaluate_binary_op(*op, &left_val, &right_val)
            }
            Expr::UnaryOp { op, arg } => {
                let arg_val = arg.evaluate(batch, row_idx)?;
                evaluate_unary_op(*op, &arg_val)
            }
        }
    }

    /// Evaluate an expression to a boolean (for predicates).
    ///
    /// Returns true if the expression evaluates to a truthy value.
    pub fn evaluate_bool(&self, batch: &RowBatch, row_idx: usize) -> Result<bool, String> {
        let scalar = self.evaluate(batch, row_idx)?;
        scalar_to_bool(&scalar)
    }
}

/// Parse a literal string into a Scalar value.
fn parse_literal(literal: &str) -> Result<Scalar, String> {
    let trimmed = literal.trim();
    
    // Try boolean
    if let Ok(b) = trimmed.parse::<bool>() {
        return Ok(Scalar::Bool(b));
    }
    
    // Try integer (i32 first, then i64)
    if let Ok(i) = trimmed.parse::<i32>() {
        return Ok(Scalar::I32(i));
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Ok(Scalar::I64(i));
    }
    
    // Try float (f32 first, then f64)
    if let Ok(f) = trimmed.parse::<f32>() {
        return Ok(Scalar::F32(f));
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return Ok(Scalar::F64(f));
    }
    
    // Try string (remove quotes if present)
    if (trimmed.starts_with('"') && trimmed.ends_with('"')) ||
       (trimmed.starts_with('\'') && trimmed.ends_with('\'')) {
        let unquoted = &trimmed[1..trimmed.len()-1];
        return Ok(Scalar::Str(unquoted.to_string()));
    }
    
    Err(format!("cannot parse '{}' as literal", literal))
}

/// Evaluate a binary operation.
fn evaluate_binary_op(op: BinOp, left: &Scalar, right: &Scalar) -> Result<Scalar, String> {
    use Scalar::*;
    
    match op {
        BinOp::Eq => Ok(Scalar::Bool(scalar_eq(left, right))),
        BinOp::Ne => Ok(Scalar::Bool(!scalar_eq(left, right))),
        BinOp::Lt => Ok(Scalar::Bool(scalar_cmp(left, right).is_lt())),
        BinOp::Le => Ok(Scalar::Bool(scalar_cmp(left, right).is_le())),
        BinOp::Gt => Ok(Scalar::Bool(scalar_cmp(left, right).is_gt())),
        BinOp::Ge => Ok(Scalar::Bool(scalar_cmp(left, right).is_ge())),
        BinOp::And => {
            let left_bool = scalar_to_bool(left)?;
            let right_bool = scalar_to_bool(right)?;
            Ok(Scalar::Bool(left_bool && right_bool))
        }
        BinOp::Or => {
            let left_bool = scalar_to_bool(left)?;
            let right_bool = scalar_to_bool(right)?;
            Ok(Scalar::Bool(left_bool || right_bool))
        }
        BinOp::Add => {
            match (left, right) {
                (I32(a), I32(b)) => Ok(Scalar::I32(a + b)),
                (I64(a), I64(b)) => Ok(Scalar::I64(a + b)),
                (F32(a), F32(b)) => Ok(Scalar::F32(a + b)),
                (F64(a), F64(b)) => Ok(Scalar::F64(a + b)),
                (Str(a), Str(b)) => Ok(Scalar::Str(format!("{}{}", a, b))),
                _ => Err(format!("unsupported addition: {:?} + {:?}", left, right)),
            }
        }
        BinOp::Sub => {
            match (left, right) {
                (I32(a), I32(b)) => Ok(Scalar::I32(a - b)),
                (I64(a), I64(b)) => Ok(Scalar::I64(a - b)),
                (F32(a), F32(b)) => Ok(Scalar::F32(a - b)),
                (F64(a), F64(b)) => Ok(Scalar::F64(a - b)),
                _ => Err(format!("unsupported subtraction: {:?} - {:?}", left, right)),
            }
        }
        BinOp::Mul => {
            match (left, right) {
                (I32(a), I32(b)) => Ok(Scalar::I32(a * b)),
                (I64(a), I64(b)) => Ok(Scalar::I64(a * b)),
                (F32(a), F32(b)) => Ok(Scalar::F32(a * b)),
                (F64(a), F64(b)) => Ok(Scalar::F64(a * b)),
                _ => Err(format!("unsupported multiplication: {:?} * {:?}", left, right)),
            }
        }
        BinOp::Div => {
            match (left, right) {
                (I32(a), I32(b)) => {
                    if *b == 0 {
                        return Err("division by zero".to_string());
                    }
                    Ok(Scalar::I32(a / b))
                }
                (I64(a), I64(b)) => {
                    if *b == 0 {
                        return Err("division by zero".to_string());
                    }
                    Ok(Scalar::I64(a / b))
                }
                (F32(a), F32(b)) => {
                    if *b == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    Ok(Scalar::F32(a / b))
                }
                (F64(a), F64(b)) => {
                    if *b == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    Ok(Scalar::F64(a / b))
                }
                _ => Err(format!("unsupported division: {:?} / {:?}", left, right)),
            }
        }
    }
}

/// Evaluate a unary operation.
fn evaluate_unary_op(op: UnaryOp, arg: &Scalar) -> Result<Scalar, String> {
    match op {
        UnaryOp::Not => {
            let bool_val = scalar_to_bool(arg)?;
            Ok(Scalar::Bool(!bool_val))
        }
        UnaryOp::IsNull => Ok(Scalar::Bool(matches!(arg, Scalar::Null))),
        UnaryOp::IsNotNull => Ok(Scalar::Bool(!matches!(arg, Scalar::Null))),
    }
}

/// Compare two scalars for equality.
fn scalar_eq(a: &Scalar, b: &Scalar) -> bool {
    use Scalar::*;
    match (a, b) {
        (Null, Null) => true,
        (Null, _) | (_, Null) => false,
        (Bool(x), Bool(y)) => x == y,
        (I32(x), I32(y)) => x == y,
        (I64(x), I64(y)) => x == y,
        // Handle cross-type integer comparisons
        (I32(x), I64(y)) => (*x as i64) == *y,
        (I64(x), I32(y)) => *x == (*y as i64),
        (F32(x), F32(y)) => (x - y).abs() < f32::EPSILON,
        (F64(x), F64(y)) => (x - y).abs() < f64::EPSILON,
        // Handle cross-type float comparisons
        (F32(x), F64(y)) => ((*x as f64) - y).abs() < f64::EPSILON,
        (F64(x), F32(y)) => (x - (*y as f64)).abs() < f64::EPSILON,
        // Handle numeric cross-type comparisons
        (I32(x), F32(y)) => ((*x as f32) - y).abs() < f32::EPSILON,
        (I32(x), F64(y)) => ((*x as f64) - y).abs() < f64::EPSILON,
        (I64(x), F32(y)) => ((*x as f32) - y).abs() < f32::EPSILON,
        (I64(x), F64(y)) => ((*x as f64) - y).abs() < f64::EPSILON,
        (F32(x), I32(y)) => (x - (*y as f32)).abs() < f32::EPSILON,
        (F32(x), I64(y)) => (x - (*y as f32)).abs() < f32::EPSILON,
        (F64(x), I32(y)) => (x - (*y as f64)).abs() < f64::EPSILON,
        (F64(x), I64(y)) => (x - (*y as f64)).abs() < f64::EPSILON,
        (Str(x), Str(y)) => x == y,
        (Bin(x), Bin(y)) => x == y,
        _ => false,
    }
}

/// Compare two scalars (for ordering).
fn scalar_cmp(a: &Scalar, b: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use Scalar::*;
    
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I64(x), I64(y)) => x.cmp(y),
        // Handle cross-type comparisons by converting to common type
        (I32(x), I64(y)) => (*x as i64).cmp(y),
        (I64(x), I32(y)) => x.cmp(&(*y as i64)),
        (F32(x), F32(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (F64(x), F64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        // Handle cross-type float comparisons
        (F32(x), F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (F64(x), F32(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        // Handle numeric cross-type comparisons
        (I32(x), F32(y)) => (*x as f32).partial_cmp(y).unwrap_or(Ordering::Equal),
        (I32(x), F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (I64(x), F32(y)) => (*x as f32).partial_cmp(y).unwrap_or(Ordering::Equal),
        (I64(x), F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (F32(x), I32(y)) => x.partial_cmp(&(*y as f32)).unwrap_or(Ordering::Equal),
        (F32(x), I64(y)) => x.partial_cmp(&(*y as f32)).unwrap_or(Ordering::Equal),
        (F64(x), I32(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (F64(x), I64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Str(x), Str(y)) => x.cmp(y),
        (Bin(x), Bin(y)) => x.cmp(y),
        _ => {
            // Mixed types: compare by type order
            let a_order = scalar_type_order(a);
            let b_order = scalar_type_order(b);
            a_order.cmp(&b_order)
        }
    }
}

/// Get type order for scalar (for mixed-type comparisons).
fn scalar_type_order(s: &Scalar) -> u8 {
    use Scalar::*;
    match s {
        Null => 0,
        Bool(_) => 1,
        I32(_) => 2,
        I64(_) => 3,
        F32(_) => 4,
        F64(_) => 5,
        Str(_) => 6,
        Bin(_) => 7,
    }
}

/// Convert a scalar to a boolean value.
fn scalar_to_bool(scalar: &Scalar) -> Result<bool, String> {
    use Scalar::*;
    match scalar {
        Null => Ok(false),
        Bool(b) => Ok(*b),
        I32(i) => Ok(*i != 0),
        I64(i) => Ok(*i != 0),
        F32(f) => Ok(*f != 0.0),
        F64(f) => Ok(*f != 0.0),
        Str(s) => Ok(!s.is_empty()),
        Bin(b) => Ok(!b.is_empty()),
    }
}

