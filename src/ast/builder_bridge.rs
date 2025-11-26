//! Bridge module connecting existing builders to the AST system
//!
//! This module provides the `ToAst` trait and implementations that convert
//! the existing builder structures into AST nodes. This allows incremental
//! migration from the string-based SQL generation to the type-safe AST.
//!
//! # Design Philosophy
//!
//! - Maintain backwards compatibility with existing code
//! - Use proper parameter binding (never string interpolation for values)
//! - Support both the old ParamContext and new ParamCollector
//! - Identifiers are always properly quoted via the AST's Ident type

use super::{
    AggregateExpr, AggregateFunction, ColumnRef, Expr, FunctionArg, FunctionCall, JsonBuildExpr,
    Literal, OrderByExpr, ParamCollector, SqlType,
};
use crate::error::GraphQLResult;

// Re-export json_to_param_value from params module (it's already defined there)
pub use super::params::json_to_param_value;

/// Trait for converting builders to AST nodes
///
/// This trait is implemented by builder types to convert them into
/// type-safe AST representations.
pub trait ToAst {
    /// The AST type this builder produces
    type Ast;

    /// Convert this builder to an AST node, collecting parameters
    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast>;
}

/// Context for AST building that tracks block names and other metadata
#[derive(Debug, Clone)]
pub struct AstBuildContext {
    /// The current block/alias name for table references
    pub block_name: String,
    /// Counter for generating unique block names
    block_counter: usize,
}

impl AstBuildContext {
    pub fn new() -> Self {
        Self {
            block_name: Self::generate_block_name(),
            block_counter: 0,
        }
    }

    /// Create a context with a specific block name
    pub fn with_block_name(block_name: impl Into<String>) -> Self {
        Self {
            block_name: block_name.into(),
            block_counter: 0,
        }
    }

    /// Generate a random block name (like the existing rand_block_name)
    fn generate_block_name() -> String {
        use rand::distributions::Alphanumeric;
        use rand::{thread_rng, Rng};
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect::<String>()
            .to_lowercase()
    }

    /// Get a new unique block name
    pub fn next_block_name(&mut self) -> String {
        self.block_counter += 1;
        format!("{}_{}", self.block_name, self.block_counter)
    }
}

impl Default for AstBuildContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a SQL type name string to a SqlType
///
/// Handles array types (ending with []) and common PostgreSQL types.
pub fn type_name_to_sql_type(type_name: &str) -> SqlType {
    let is_array = type_name.ends_with("[]");
    let base_name = if is_array {
        &type_name[..type_name.len() - 2]
    } else {
        type_name
    };

    let base_type = match base_name.to_lowercase().as_str() {
        "text" | "varchar" | "char" | "character varying" | "character" => SqlType::text(),
        "integer" | "int" | "int4" => SqlType::integer(),
        "bigint" | "int8" => SqlType::bigint(),
        "smallint" | "int2" => SqlType::smallint(),
        "boolean" | "bool" => SqlType::boolean(),
        "real" | "float4" => SqlType::real(),
        "double precision" | "float8" => SqlType::double_precision(),
        "numeric" | "decimal" => SqlType::numeric(),
        "uuid" => SqlType::uuid(),
        "json" => SqlType::json(),
        "jsonb" => SqlType::jsonb(),
        "timestamp" | "timestamp without time zone" => SqlType::timestamp(),
        "timestamptz" | "timestamp with time zone" => SqlType::timestamptz(),
        "date" => SqlType::date(),
        "time" | "time without time zone" => SqlType::time(),
        "timetz" | "time with time zone" => SqlType::new("time with time zone"),
        "bytea" => SqlType::bytea(),
        _ => {
            // For custom types, preserve the original name
            if base_name.contains('.') {
                let parts: Vec<&str> = base_name.splitn(2, '.').collect();
                SqlType::with_schema(parts[0], parts[1])
            } else {
                SqlType::new(base_name)
            }
        }
    };

    if is_array {
        base_type.as_array()
    } else {
        base_type
    }
}

/// Add a JSON value as a parameter and return the expression referencing it
///
/// This is the key function for safe parameter binding. It:
/// 1. Converts the JSON value to a ParamValue
/// 2. Adds it to the ParamCollector with proper type info
/// 3. Returns an Expr that references the parameter with type cast
pub fn add_param_from_json(
    params: &mut ParamCollector,
    value: &serde_json::Value,
    type_name: &str,
) -> GraphQLResult<Expr> {
    let param_value = json_to_param_value(value);
    let sql_type = type_name_to_sql_type(type_name);
    Ok(params.add(param_value, sql_type))
}

// =============================================================================
// Expression Builder - Consolidated helper functions for creating Expr nodes
// =============================================================================

/// Builder for creating SQL expressions with a fluent API.
///
/// This struct consolidates all expression-building helper functions into
/// a single namespace for better discoverability and organization.
///
/// # Example
///
/// ```rust,ignore
/// use pg_graphql::ast::ExprBuilder;
///
/// let col = ExprBuilder::column("users", "id");
/// let lit = ExprBuilder::string("hello");
/// let agg = ExprBuilder::count_star();
/// ```
pub struct ExprBuilder;

impl ExprBuilder {
    // -------------------------------------------------------------------------
    // Column references
    // -------------------------------------------------------------------------

    /// Create a qualified column reference (table.column)
    #[inline]
    pub fn column(table_alias: &str, column_name: &str) -> Expr {
        Expr::Column(ColumnRef::qualified(table_alias, column_name))
    }

    /// Create an unqualified column reference
    #[inline]
    pub fn column_unqualified(column_name: &str) -> Expr {
        Expr::Column(ColumnRef::new(column_name))
    }

    // -------------------------------------------------------------------------
    // Literals
    // -------------------------------------------------------------------------

    /// Create a string literal
    #[inline]
    pub fn string(s: &str) -> Expr {
        Expr::Literal(Literal::String(s.to_string()))
    }

    /// Create an integer literal
    #[inline]
    pub fn int(i: i64) -> Expr {
        Expr::Literal(Literal::Integer(i))
    }

    /// Create a boolean literal
    #[inline]
    pub fn bool(b: bool) -> Expr {
        Expr::Literal(Literal::Bool(b))
    }

    /// Create a NULL literal
    #[inline]
    pub fn null() -> Expr {
        Expr::Literal(Literal::Null)
    }

    /// Create a DEFAULT expression (for INSERT)
    #[inline]
    pub fn default() -> Expr {
        Expr::Literal(Literal::Default)
    }

    // -------------------------------------------------------------------------
    // Function calls
    // -------------------------------------------------------------------------

    /// Create a simple function call
    #[inline]
    pub fn func(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall(FunctionCall::new(
            name,
            args.into_iter().map(FunctionArg::unnamed).collect(),
        ))
    }

    /// Create a schema-qualified function call
    #[inline]
    pub fn func_with_schema(schema: &str, name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall(FunctionCall::with_schema(
            schema,
            name,
            args.into_iter().map(FunctionArg::unnamed).collect(),
        ))
    }

    /// Create a COALESCE expression
    #[inline]
    pub fn coalesce(args: Vec<Expr>) -> Expr {
        Expr::Coalesce(args)
    }

    // -------------------------------------------------------------------------
    // Aggregates
    // -------------------------------------------------------------------------

    /// Create a COUNT(*) expression
    #[inline]
    pub fn count_star() -> Expr {
        Expr::Aggregate(AggregateExpr::count_star())
    }

    /// Create a jsonb_agg expression
    #[inline]
    pub fn jsonb_agg(expr: Expr) -> Expr {
        Expr::Aggregate(AggregateExpr::new(AggregateFunction::JsonbAgg, vec![expr]))
    }

    /// Create a jsonb_agg expression with FILTER clause
    pub fn jsonb_agg_filtered(expr: Expr, filter: Expr) -> Expr {
        let mut agg = AggregateExpr::new(AggregateFunction::JsonbAgg, vec![expr]);
        agg.filter = Some(Box::new(filter));
        Expr::Aggregate(agg)
    }

    /// Create a jsonb_agg expression with ORDER BY and optional FILTER
    pub fn jsonb_agg_ordered(expr: Expr, order_by: Vec<OrderByExpr>, filter: Option<Expr>) -> Expr {
        let mut agg = AggregateExpr::new(AggregateFunction::JsonbAgg, vec![expr]);
        if !order_by.is_empty() {
            agg.order_by = Some(order_by);
        }
        if let Some(f) = filter {
            agg.filter = Some(Box::new(f));
        }
        Expr::Aggregate(agg)
    }

    // -------------------------------------------------------------------------
    // JSON/JSONB helpers
    // -------------------------------------------------------------------------

    /// Create a jsonb_build_object expression
    pub fn jsonb_object(pairs: Vec<(String, Expr)>) -> Expr {
        Expr::JsonBuild(JsonBuildExpr::Object(
            pairs
                .into_iter()
                .map(|(k, v)| (Expr::Literal(Literal::String(k)), v))
                .collect(),
        ))
    }

    /// Create an empty jsonb array: jsonb_build_array()
    #[inline]
    pub fn empty_jsonb_array() -> Expr {
        Self::func("jsonb_build_array", vec![])
    }

    /// Create an empty jsonb object: '{}'::jsonb
    #[inline]
    pub fn empty_jsonb_object() -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::Literal(Literal::String("{}".to_string()))),
            target_type: type_name_to_sql_type("jsonb"),
        }
    }
}

// =============================================================================
// Standalone helper functions (for backwards compatibility)
// =============================================================================

/// Helper to create a column reference expression
#[inline]
pub fn column_ref(table_alias: &str, column_name: &str) -> Expr {
    ExprBuilder::column(table_alias, column_name)
}

/// Helper to create an unqualified column reference
#[inline]
pub fn column_ref_unqualified(column_name: &str) -> Expr {
    ExprBuilder::column_unqualified(column_name)
}

/// Helper to create a simple function call expression
#[inline]
pub fn func_call(name: &str, args: Vec<Expr>) -> Expr {
    ExprBuilder::func(name, args)
}

/// Helper to create a schema-qualified function call
#[inline]
pub fn func_call_schema(schema: &str, name: &str, args: Vec<Expr>) -> Expr {
    ExprBuilder::func_with_schema(schema, name, args)
}

/// Helper to build jsonb_build_object calls
#[inline]
pub fn jsonb_build_object(pairs: Vec<(String, Expr)>) -> Expr {
    ExprBuilder::jsonb_object(pairs)
}

/// Helper to build jsonb_agg calls
#[inline]
pub fn jsonb_agg(expr: Expr) -> Expr {
    ExprBuilder::jsonb_agg(expr)
}

/// Helper to build jsonb_agg calls with a FILTER clause
#[inline]
pub fn jsonb_agg_with_filter(expr: Expr, filter: Expr) -> Expr {
    ExprBuilder::jsonb_agg_filtered(expr, filter)
}

/// Helper to build jsonb_agg calls with ORDER BY and FILTER clauses
#[inline]
pub fn jsonb_agg_with_order_and_filter(
    expr: Expr,
    order_by: Vec<OrderByExpr>,
    filter: Option<Expr>,
) -> Expr {
    ExprBuilder::jsonb_agg_ordered(expr, order_by, filter)
}

/// Helper to build coalesce calls
#[inline]
pub fn coalesce(args: Vec<Expr>) -> Expr {
    ExprBuilder::coalesce(args)
}

/// Helper to build count(*) expression
#[inline]
pub fn count_star() -> Expr {
    ExprBuilder::count_star()
}

/// Helper to create a string literal expression
#[inline]
pub fn string_literal(s: &str) -> Expr {
    ExprBuilder::string(s)
}

/// Helper to create an empty jsonb array expression
#[inline]
pub fn empty_jsonb_array() -> Expr {
    ExprBuilder::empty_jsonb_array()
}

/// Helper to create an empty jsonb object expression
#[inline]
pub fn empty_jsonb_object() -> Expr {
    ExprBuilder::empty_jsonb_object()
}

/// Helper to create an integer literal
#[inline]
pub fn int_literal(i: i64) -> Expr {
    ExprBuilder::int(i)
}

/// Helper to create a boolean literal
#[inline]
pub fn bool_literal(b: bool) -> Expr {
    ExprBuilder::bool(b)
}

/// Helper to create a NULL literal
#[inline]
pub fn null_literal() -> Expr {
    ExprBuilder::null()
}

/// Helper to create DEFAULT expression (for INSERT)
#[inline]
pub fn default_expr() -> Expr {
    ExprBuilder::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ParamValue;

    #[test]
    fn test_type_name_to_sql_type() {
        let t = type_name_to_sql_type("integer");
        assert_eq!(t.name, "integer");
        assert!(!t.is_array);

        let t = type_name_to_sql_type("text[]");
        assert_eq!(t.name, "text");
        assert!(t.is_array);

        let t = type_name_to_sql_type("public.my_type");
        assert_eq!(t.name, "my_type");
        assert_eq!(t.schema, Some("public".to_string()));
    }

    #[test]
    fn test_add_param_from_json() {
        use serde_json::json;

        let mut params = ParamCollector::new();

        let expr = add_param_from_json(&mut params, &json!(42), "integer").unwrap();

        // Should be a parameter reference
        match expr {
            Expr::Param(p) => {
                assert_eq!(p.index, 1);
                assert_eq!(p.type_cast.name, "integer");
            }
            _ => panic!("Expected Param expression"),
        }

        // Check the collected parameter
        let collected = params.into_params();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].index, 1);
        match &collected[0].value {
            ParamValue::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_ast_build_context() {
        let ctx = AstBuildContext::new();
        assert!(!ctx.block_name.is_empty());
        assert_eq!(ctx.block_name.len(), 7);
    }

    #[test]
    fn test_helper_functions() {
        // Test column_ref
        let col = column_ref("t", "id");
        match col {
            Expr::Column(c) => {
                assert_eq!(c.table_alias.unwrap().0, "t");
                assert_eq!(c.column.0, "id");
            }
            _ => panic!("Expected column"),
        }

        // Test func_call
        let f = func_call("count", vec![Expr::Raw("*".to_string())]);
        match f {
            Expr::FunctionCall(fc) => {
                assert_eq!(fc.name.0, "count");
                assert_eq!(fc.args.len(), 1);
            }
            _ => panic!("Expected function call"),
        }

        // Test jsonb_build_object
        let obj = jsonb_build_object(vec![("key".to_string(), string_literal("value"))]);
        match obj {
            Expr::JsonBuild(JsonBuildExpr::Object(pairs)) => {
                assert_eq!(pairs.len(), 1); // 1 key-value pair
            }
            _ => panic!("Expected JsonBuild Object"),
        }
    }
}
