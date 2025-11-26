//! Parameter handling for prepared statements
//!
//! This module provides a pgrx-independent way to collect and manage
//! query parameters. The actual conversion to pgrx Datums happens
//! in the executor module.

use super::expr::{Expr, ParamRef};
use super::types::SqlType;

/// A parameter value that can be rendered to SQL
///
/// This is intentionally decoupled from pgrx to allow the AST module
/// to be tested independently.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    /// SQL NULL
    Null,
    /// Boolean value
    Bool(bool),
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Floating point value
    Float(f64),
    /// Array of values (for array parameters)
    Array(Vec<ParamValue>),
    /// JSON value (stored as serde_json::Value)
    Json(serde_json::Value),
}

impl ParamValue {
    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Convert to a string representation for SQL
    pub fn to_sql_literal(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Bool(b) => Some(b.to_string()),
            Self::String(s) => Some(s.clone()),
            Self::Integer(n) => Some(n.to_string()),
            Self::Float(f) => Some(f.to_string()),
            Self::Array(arr) => {
                let elements: Vec<String> = arr.iter().filter_map(|v| v.to_sql_literal()).collect();
                Some(format!("{{{}}}", elements.join(",")))
            }
            Self::Json(v) => Some(v.to_string()),
        }
    }
}

impl From<bool> for ParamValue {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<String> for ParamValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for ParamValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<i64> for ParamValue {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<i32> for ParamValue {
    fn from(n: i32) -> Self {
        Self::Integer(n as i64)
    }
}

impl From<f64> for ParamValue {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<serde_json::Value> for ParamValue {
    fn from(v: serde_json::Value) -> Self {
        Self::Json(v)
    }
}

impl<T: Into<ParamValue>> From<Option<T>> for ParamValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

/// A collected parameter with its index, value, and type
#[derive(Debug, Clone)]
pub struct Param {
    /// 1-indexed parameter number
    pub index: usize,
    /// The parameter value
    pub value: ParamValue,
    /// The SQL type for casting
    pub sql_type: SqlType,
}

impl Param {
    pub fn new(index: usize, value: ParamValue, sql_type: SqlType) -> Self {
        Self {
            index,
            value,
            sql_type,
        }
    }
}

/// Collects parameters during AST construction
///
/// This allows building parameterized queries without worrying about
/// parameter numbering. Each call to `add()` returns an expression
/// that references the parameter.
#[derive(Debug, Default)]
pub struct ParamCollector {
    params: Vec<Param>,
}

impl ParamCollector {
    /// Create a new empty parameter collector
    pub fn new() -> Self {
        Self { params: Vec::new() }
    }

    /// Add a parameter and return an expression that references it
    ///
    /// Parameters are 1-indexed in SQL ($1, $2, etc.)
    pub fn add(&mut self, value: ParamValue, sql_type: SqlType) -> Expr {
        let index = self.params.len() + 1; // 1-indexed for SQL
        self.params.push(Param {
            index,
            value,
            sql_type: sql_type.clone(),
        });
        Expr::Param(ParamRef {
            index,
            type_cast: sql_type,
        })
    }

    /// Add a parameter from a serde_json::Value
    ///
    /// This is a convenience method for the common case of converting
    /// GraphQL input values to parameters.
    pub fn add_json(&mut self, value: &serde_json::Value, sql_type: SqlType) -> Expr {
        let param_value = json_to_param_value(value);
        self.add(param_value, sql_type)
    }

    /// Get all collected parameters
    pub fn into_params(self) -> Vec<Param> {
        self.params
    }

    /// Get parameters as a slice
    pub fn params(&self) -> &[Param] {
        &self.params
    }

    /// Get the number of collected parameters
    pub fn len(&self) -> usize {
        self.params.len()
    }

    /// Check if no parameters have been collected
    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    /// Get the next parameter index that would be assigned
    pub fn next_index(&self) -> usize {
        self.params.len() + 1
    }
}

/// Convert a serde_json::Value to a ParamValue
pub fn json_to_param_value(value: &serde_json::Value) -> ParamValue {
    match value {
        serde_json::Value::Null => ParamValue::Null,
        serde_json::Value::Bool(b) => ParamValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ParamValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                ParamValue::Float(f)
            } else {
                // Fallback to string representation
                ParamValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => ParamValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            ParamValue::Array(arr.iter().map(json_to_param_value).collect())
        }
        serde_json::Value::Object(_) => {
            // Store objects as JSON
            ParamValue::Json(value.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_collector() {
        let mut collector = ParamCollector::new();

        let expr1 = collector.add(ParamValue::String("hello".into()), SqlType::text());
        let expr2 = collector.add(ParamValue::Integer(42), SqlType::integer());

        assert_eq!(collector.len(), 2);

        match &expr1 {
            Expr::Param(p) => {
                assert_eq!(p.index, 1);
                assert_eq!(p.type_cast.name, "text");
            }
            _ => panic!("Expected Param"),
        }

        match &expr2 {
            Expr::Param(p) => {
                assert_eq!(p.index, 2);
                assert_eq!(p.type_cast.name, "integer");
            }
            _ => panic!("Expected Param"),
        }

        let params = collector.into_params();
        assert_eq!(params.len(), 2);
        assert!(matches!(params[0].value, ParamValue::String(_)));
        assert!(matches!(params[1].value, ParamValue::Integer(42)));
    }

    #[test]
    fn test_json_to_param_value() {
        assert!(matches!(
            json_to_param_value(&serde_json::Value::Null),
            ParamValue::Null
        ));

        assert!(matches!(
            json_to_param_value(&serde_json::json!(true)),
            ParamValue::Bool(true)
        ));

        assert!(matches!(
            json_to_param_value(&serde_json::json!(42)),
            ParamValue::Integer(42)
        ));

        assert!(matches!(
            json_to_param_value(&serde_json::json!("hello")),
            ParamValue::String(s) if s == "hello"
        ));

        let arr = json_to_param_value(&serde_json::json!([1, 2, 3]));
        match arr {
            ParamValue::Array(v) => assert_eq!(v.len(), 3),
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_param_value_to_sql_literal() {
        assert_eq!(ParamValue::Null.to_sql_literal(), None);
        assert_eq!(
            ParamValue::Bool(true).to_sql_literal(),
            Some("true".to_string())
        );
        assert_eq!(
            ParamValue::Integer(42).to_sql_literal(),
            Some("42".to_string())
        );
        assert_eq!(
            ParamValue::String("hello".into()).to_sql_literal(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_param_value_from() {
        let _: ParamValue = true.into();
        let _: ParamValue = "hello".into();
        let _: ParamValue = 42i32.into();
        let _: ParamValue = 42i64.into();
        let _: ParamValue = 3.14f64.into();
        let _: ParamValue = None::<i32>.into();
        let _: ParamValue = Some(42i32).into();
    }
}
