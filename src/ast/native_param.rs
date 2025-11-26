//! Native parameter binding for PostgreSQL
//!
//! This module provides type-safe conversion of AST parameters to native PostgreSQL
//! datums. It uses native binding where safe and falls back to text representation
//! for custom types and edge cases.
//!
//! # Design Philosophy
//!
//! The key principle is **conservative native binding**: we only use native types
//! when we can guarantee the conversion is safe and equivalent to the text-based
//! approach. This preserves backwards compatibility while improving performance
//! for common cases.
//!
//! # Safe Conversions
//!
//! Native binding is used for:
//! - `boolean` → `BOOLOID`
//! - `integer` (i32 range) → `INT4OID`
//! - `bigint` (i64) → `INT8OID`
//! - `double precision` (f64) → `FLOAT8OID`
//! - `text/varchar` (String) → `TEXTOID`
//! - `NULL` → proper NULL with type OID
//!
//! # Fallback Cases
//!
//! Text-based binding (current behavior) is preserved for:
//! - Custom types (enums, domains, composites)
//! - Schema-qualified types
//! - Arrays (for now)
//! - Types without known OIDs
//! - Edge cases (numeric precision, date parsing)

use super::params::{Param, ParamCollector, ParamValue};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgBuiltInOids;
use pgrx::{IntoDatum, PgOid};

/// Strategy for binding a parameter to PostgreSQL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingStrategy {
    /// Use native datum binding (type matches exactly)
    Native,
    /// Use text representation with SQL-side cast (fallback)
    TextWithCast,
}

/// Determines the optimal binding strategy for a parameter
pub fn determine_binding_strategy(param: &Param) -> BindingStrategy {
    // Never use native binding for arrays (complex element type handling)
    if param.sql_type.is_array {
        return BindingStrategy::TextWithCast;
    }

    // Never use native binding for schema-qualified types (custom types)
    if param.sql_type.schema.is_some() {
        return BindingStrategy::TextWithCast;
    }

    // Check for safe native conversions based on OID and value type
    match (param.sql_type.oid, &param.value) {
        // Boolean: exact match
        (Some(16), ParamValue::Bool(_)) => BindingStrategy::Native,

        // Integer (int4): only if value fits in i32 range
        (Some(23), ParamValue::Integer(i)) => {
            if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                BindingStrategy::Native
            } else {
                // Value too large for int4, use text to let PostgreSQL error properly
                BindingStrategy::TextWithCast
            }
        }

        // Bigint (int8): exact match for i64
        (Some(20), ParamValue::Integer(_)) => BindingStrategy::Native,

        // Smallint (int2): only if value fits in i16 range
        (Some(21), ParamValue::Integer(i)) => {
            if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                BindingStrategy::Native
            } else {
                BindingStrategy::TextWithCast
            }
        }

        // Double precision (float8): f64 matches exactly
        (Some(701), ParamValue::Float(_)) => BindingStrategy::Native,

        // Real (float4): Use text fallback - f64 to f32 conversion needs care
        // The datum size mismatch between f64 and float4 causes corruption
        // (Some(700), ParamValue::Float(_)) => BindingStrategy::Native,

        // Text: String matches exactly
        (Some(25), ParamValue::String(_)) => BindingStrategy::Native,

        // Varchar also maps to text in pgrx
        (None, ParamValue::String(_)) if param.sql_type.name == "varchar" => {
            BindingStrategy::Native
        }

        // NULL: can be natively bound with proper type OID
        (Some(_), ParamValue::Null) => BindingStrategy::Native,

        // All other cases: use text fallback for safety
        _ => BindingStrategy::TextWithCast,
    }
}

/// Convert a parameter to a pgrx DatumWithOid using the appropriate strategy
///
/// # Safety
///
/// This function uses unsafe code to create DatumWithOid values. The safety
/// is maintained by:
/// 1. Only using native binding for known-safe type combinations
/// 2. Proper NULL handling via DatumWithOid::null_oid
/// 3. Falling back to text representation for any uncertain cases
pub fn param_to_datum(param: &Param) -> DatumWithOid<'static> {
    let strategy = determine_binding_strategy(param);

    match strategy {
        BindingStrategy::Native => convert_native(param),
        BindingStrategy::TextWithCast => convert_as_text(param),
    }
}

/// Convert all collected parameters to pgrx datums
pub fn params_to_datums(params: &ParamCollector) -> Vec<DatumWithOid<'static>> {
    params.params().iter().map(param_to_datum).collect()
}

/// Convert a parameter using native PostgreSQL type binding
fn convert_native(param: &Param) -> DatumWithOid<'static> {
    let oid = param.sql_type.oid.unwrap_or(25); // Default to text OID

    match &param.value {
        ParamValue::Null => DatumWithOid::null_oid(PgOid::BuiltIn(oid_to_builtin(oid)).value()),

        ParamValue::Bool(b) => {
            let datum = b.into_datum().expect("bool should always convert");
            unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::BOOLOID).value()) }
        }

        ParamValue::Integer(i) => {
            // Choose the appropriate integer type based on target OID
            match oid {
                21 => {
                    // smallint
                    let datum = (*i as i16).into_datum().expect("i16 should convert");
                    unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::INT2OID).value()) }
                }
                23 => {
                    // integer
                    let datum = (*i as i32).into_datum().expect("i32 should convert");
                    unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::INT4OID).value()) }
                }
                20 | _ => {
                    // bigint (default for integers)
                    let datum = i.into_datum().expect("i64 should convert");
                    unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::INT8OID).value()) }
                }
            }
        }

        ParamValue::Float(f) => {
            // Only float8 (double precision) uses native binding
            // float4 falls back to text due to f64->f32 size mismatch
            let datum = f.into_datum().expect("f64 should convert");
            unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID).value()) }
        }

        ParamValue::String(s) => {
            let datum = s.clone().into_datum().expect("String should convert");
            unsafe { DatumWithOid::new(datum, PgOid::BuiltIn(PgBuiltInOids::TEXTOID).value()) }
        }

        // These should not reach here due to binding strategy, but handle gracefully
        ParamValue::Json(_) | ParamValue::Array(_) => convert_as_text(param),
    }
}

/// Convert a parameter using text representation (fallback path)
///
/// This preserves the original behavior: pass as text and let PostgreSQL
/// cast via the SQL-side `::typename` cast.
fn convert_as_text(param: &Param) -> DatumWithOid<'static> {
    let type_oid = if param.sql_type.is_array {
        PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID)
    } else {
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
    };

    match &param.value {
        ParamValue::Null => DatumWithOid::null_oid(type_oid.value()),

        ParamValue::String(s) => {
            let datum = s.clone().into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }

        ParamValue::Integer(i) => {
            let datum = i.to_string().into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }

        ParamValue::Float(f) => {
            let datum = f.to_string().into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }

        ParamValue::Bool(b) => {
            let datum = b.to_string().into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }

        ParamValue::Json(j) => {
            let datum = j.to_string().into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }

        ParamValue::Array(arr) => {
            // Convert array to PostgreSQL array format via text
            let elements: Vec<Option<String>> = arr.iter().map(param_value_to_string).collect();
            let datum = elements.into_datum();
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()),
            }
        }
    }
}

/// Convert a ParamValue to Option<String> for array element conversion
fn param_value_to_string(value: &ParamValue) -> Option<String> {
    match value {
        ParamValue::Null => None,
        ParamValue::String(s) => Some(s.clone()),
        ParamValue::Integer(i) => Some(i.to_string()),
        ParamValue::Float(f) => Some(f.to_string()),
        ParamValue::Bool(b) => Some(b.to_string()),
        ParamValue::Json(j) => Some(j.to_string()),
        ParamValue::Array(_) => value.to_sql_literal(),
    }
}

/// Convert a raw OID number to a PgBuiltInOids enum value
///
/// This is used for NULL handling where we need to create a typed NULL.
fn oid_to_builtin(oid: u32) -> PgBuiltInOids {
    match oid {
        16 => PgBuiltInOids::BOOLOID,
        20 => PgBuiltInOids::INT8OID,
        21 => PgBuiltInOids::INT2OID,
        23 => PgBuiltInOids::INT4OID,
        25 => PgBuiltInOids::TEXTOID,
        114 => PgBuiltInOids::JSONOID,
        700 => PgBuiltInOids::FLOAT4OID,
        701 => PgBuiltInOids::FLOAT8OID,
        1082 => PgBuiltInOids::DATEOID,
        1083 => PgBuiltInOids::TIMEOID,
        1114 => PgBuiltInOids::TIMESTAMPOID,
        1184 => PgBuiltInOids::TIMESTAMPTZOID,
        1700 => PgBuiltInOids::NUMERICOID,
        2950 => PgBuiltInOids::UUIDOID,
        3802 => PgBuiltInOids::JSONBOID,
        _ => PgBuiltInOids::TEXTOID, // Default to text for unknown types
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::SqlType;

    #[test]
    fn test_binding_strategy_bool() {
        let param = Param::new(1, ParamValue::Bool(true), SqlType::boolean());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_integer_in_range() {
        let param = Param::new(1, ParamValue::Integer(42), SqlType::integer());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_integer_out_of_range() {
        // Value too large for int4
        let param = Param::new(
            1,
            ParamValue::Integer(i64::MAX),
            SqlType::integer(),
        );
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::TextWithCast
        );
    }

    #[test]
    fn test_binding_strategy_bigint() {
        let param = Param::new(1, ParamValue::Integer(i64::MAX), SqlType::bigint());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_custom_type() {
        let param = Param::new(
            1,
            ParamValue::String("happy".to_string()),
            SqlType::with_schema("public", "mood"),
        );
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::TextWithCast
        );
    }

    #[test]
    fn test_binding_strategy_array() {
        let param = Param::new(
            1,
            ParamValue::Array(vec![ParamValue::Integer(1), ParamValue::Integer(2)]),
            SqlType::integer().into_array(),
        );
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::TextWithCast
        );
    }

    #[test]
    fn test_binding_strategy_null() {
        let param = Param::new(1, ParamValue::Null, SqlType::integer());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_text() {
        let param = Param::new(
            1,
            ParamValue::String("hello".to_string()),
            SqlType::text(),
        );
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_float8() {
        let param = Param::new(1, ParamValue::Float(3.14), SqlType::double_precision());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::Native
        );
    }

    #[test]
    fn test_binding_strategy_float4_uses_text() {
        // float4 (real) uses text fallback due to f64->f32 size mismatch
        let param = Param::new(1, ParamValue::Float(3.14), SqlType::real());
        assert_eq!(
            determine_binding_strategy(&param),
            BindingStrategy::TextWithCast
        );
    }
}
