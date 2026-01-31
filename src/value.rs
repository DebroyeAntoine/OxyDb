use std::sync::Arc;

use crate::data_type::DataType;

/// Represents a single data value stored in the database.
///
/// This enum wraps all supported Rust types into a single type that can be
/// passed around the engine. It includes support for SQL `NULL` values.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// represents an empty or missing value.
    Null,
    /// A 64-bit signed integer value.
    Int(i64),
    /// A 64-bit floating-point value.
    Float(f64),
    /// A UTF-8 string value, wrapped in an [Arc] for efficient,
    /// thread-safe sharing and cheap cloning.
    Text(Arc<str>),
    /// A boolean value.
    Bool(bool),
}

impl Value {
    /// Returns `true` if the value is [Value::Null].
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns the inner integer value if this is a [Value::Int].
    /// Otherwise, returns `None`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the inner float value if this is a [Value::Float].
    /// Otherwise, returns `None`.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns a reference to the inner string slice if this is a [Value::Text].
    /// Otherwise, returns `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the inner boolean value if this is a [Value::Bool].
    /// Otherwise, returns `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the logical [DataType] corresponding to this value.
    ///
    /// Returns `None` if the value is [Value::Null], because in this database
    /// engine, a standalone NULL value is untyped until it is placed in a [Column].
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Int(_) => Some(DataType::Int),
            Self::Float(_) => Some(DataType::Float),
            Self::Text(_) => Some(DataType::Text),
            Self::Bool(_) => Some(DataType::Bool),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::DataType;

    // ─────────────────────────────────────────────────────────────
    // Test 1 : is_null
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(1).is_null());
        assert!(!Value::Float(1.0).is_null());
        assert!(!Value::Text("x".into()).is_null());
        assert!(!Value::Bool(true).is_null());
    }

    // ─────────────────────────────────────────────────────────────
    // Test 2 : as_int
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_as_int() {
        assert_eq!(Value::Int(42).as_int(), Some(42));
        assert_eq!(Value::Null.as_int(), None);
        assert_eq!(Value::Float(1.0).as_int(), None);
        assert_eq!(Value::Text("42".into()).as_int(), None);
        assert_eq!(Value::Bool(true).as_int(), None);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 3 : as_float
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_as_float() {
        assert_eq!(Value::Float(3.14).as_float(), Some(3.14));
        assert_eq!(Value::Null.as_float(), None);
        assert_eq!(Value::Int(1).as_float(), None);
        assert_eq!(Value::Text("3.14".into()).as_float(), None);
        assert_eq!(Value::Bool(false).as_float(), None);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 4 : as_str
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_as_str() {
        let v = Value::Text("hello".into());

        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(Value::Null.as_str(), None);
        assert_eq!(Value::Int(1).as_str(), None);
        assert_eq!(Value::Float(1.0).as_str(), None);
        assert_eq!(Value::Bool(true).as_str(), None);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 5 : as_bool
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_as_bool() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Null.as_bool(), None);
        assert_eq!(Value::Int(1).as_bool(), None);
        assert_eq!(Value::Float(0.0).as_bool(), None);
        assert_eq!(Value::Text("true".into()).as_bool(), None);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 6 : data_type
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_data_type() {
        assert_eq!(Value::Null.data_type(), None);
        assert_eq!(Value::Int(1).data_type(), Some(DataType::Int));
        assert_eq!(Value::Float(1.0).data_type(), Some(DataType::Float));
        assert_eq!(Value::Text("x".into()).data_type(), Some(DataType::Text));
        assert_eq!(Value::Bool(true).data_type(), Some(DataType::Bool));
    }

    // ─────────────────────────────────────────────────────────────
    // Test 7 : PartialEq
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Int(10), Value::Int(10));
        assert_ne!(Value::Int(10), Value::Int(20));
        assert_eq!(Value::Float(1.5), Value::Float(1.5));
        assert_eq!(Value::Text("abc".into()), Value::Text("abc".into()));
        assert_ne!(Value::Bool(true), Value::Bool(false));
    }

    // ─────────────────────────────────────────────────────────────
    // Test 8 : clone
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_clone() {
        let v1 = Value::Text("hello".into());
        let v2 = v1.clone();

        assert_eq!(v1, v2);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 9 : round-trip (pattern matching)
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_value_round_trip() {
        let values = vec![
            Value::Null,
            Value::Int(42),
            Value::Float(3.14),
            Value::Text("hello".into()),
            Value::Bool(true),
        ];

        for v in values {
            match v.clone() {
                Value::Null => assert!(v.is_null()),
                Value::Int(i) => assert_eq!(v.as_int(), Some(i)),
                Value::Float(f) => assert_eq!(v.as_float(), Some(f)),
                Value::Text(s) => assert_eq!(v.as_str(), Some(s.as_ref())),
                Value::Bool(b) => assert_eq!(v.as_bool(), Some(b)),
            }
        }
    }
}
