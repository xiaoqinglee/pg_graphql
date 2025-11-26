//! SQL expression types
//!
//! This module defines all SQL expression types that can appear in queries.
//! Expressions are the building blocks of SQL: columns, literals, operators,
//! function calls, etc.

use super::types::SqlType;

/// A quoted SQL identifier (table name, column name, etc.)
///
/// Identifiers are always quoted when rendered to prevent SQL injection
/// and handle special characters correctly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(pub String);

impl Ident {
    /// Create a new identifier from any string-like type
    #[inline]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the identifier as a string slice
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Ident {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Ident {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Reference to a column, optionally qualified with a table alias
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    /// Table alias (e.g., "t" in "t.id")
    pub table_alias: Option<Ident>,
    /// Column name
    pub column: Ident,
}

impl ColumnRef {
    pub fn new(column: impl Into<Ident>) -> Self {
        Self {
            table_alias: None,
            column: column.into(),
        }
    }

    pub fn qualified(table: impl Into<Ident>, column: impl Into<Ident>) -> Self {
        Self {
            table_alias: Some(table.into()),
            column: column.into(),
        }
    }
}

/// Reference to a query parameter ($1, $2, etc.) with type cast
#[derive(Debug, Clone, PartialEq)]
pub struct ParamRef {
    /// 1-indexed parameter number
    pub index: usize,
    /// Type to cast the parameter to
    pub type_cast: SqlType,
}

/// SQL literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// SQL NULL
    Null,
    /// Boolean true/false
    Bool(bool),
    /// Integer literal
    Integer(i64),
    /// Floating point literal
    Float(f64),
    /// String literal (will be properly quoted)
    String(String),
    /// SQL DEFAULT keyword (for INSERT statements)
    Default,
}

impl Literal {
    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    // Array operators
    Contains,    // @>
    ContainedBy, // <@
    Overlap,     // &&
    Any,         // = ANY(...)

    // String operators
    Like,
    ILike,
    RegEx,      // ~
    IRegEx,     // ~*
    StartsWith, // ^@

    // Logical
    And,
    Or,

    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // JSON operators
    JsonExtract,     // ->
    JsonExtractText, // ->>
    JsonPath,        // #>
    JsonPathText,    // #>>

    // JSONB concatenation
    JsonConcat, // ||
}

impl BinaryOperator {
    /// Get the SQL representation of this operator
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Contains => "@>",
            Self::ContainedBy => "<@",
            Self::Overlap => "&&",
            Self::Any => "= any",
            Self::Like => "like",
            Self::ILike => "ilike",
            Self::RegEx => "~",
            Self::IRegEx => "~*",
            Self::StartsWith => "^@",
            Self::And => "and",
            Self::Or => "or",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::JsonExtract => "->",
            Self::JsonExtractText => "->>",
            Self::JsonPath => "#>",
            Self::JsonPathText => "#>>",
            Self::JsonConcat => "||",
        }
    }
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
    BitNot,
}

impl UnaryOperator {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Not => "not",
            Self::Neg => "-",
            Self::BitNot => "~",
        }
    }
}

/// Function argument (can be named or positional)
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    /// Positional argument
    Unnamed(Expr),
    /// Named argument (name => value)
    Named { name: Ident, value: Expr },
}

impl FunctionArg {
    pub fn unnamed(expr: Expr) -> Self {
        Self::Unnamed(expr)
    }

    pub fn named(name: impl Into<Ident>, value: Expr) -> Self {
        Self::Named {
            name: name.into(),
            value,
        }
    }
}

/// A function call expression
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    /// Schema (e.g., "pg_catalog", "public")
    pub schema: Option<Ident>,
    /// Function name
    pub name: Ident,
    /// Arguments
    pub args: Vec<FunctionArg>,
    /// FILTER clause (for aggregate functions)
    pub filter: Option<Box<Expr>>,
    /// ORDER BY within the function (for ordered-set aggregates)
    pub order_by: Option<Vec<OrderByExpr>>,
}

impl FunctionCall {
    pub fn new(name: impl Into<Ident>, args: Vec<FunctionArg>) -> Self {
        Self {
            schema: None,
            name: name.into(),
            args,
            filter: None,
            order_by: None,
        }
    }

    pub fn with_schema(
        schema: impl Into<Ident>,
        name: impl Into<Ident>,
        args: Vec<FunctionArg>,
    ) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
            args,
            filter: None,
            order_by: None,
        }
    }

    /// Add a FILTER clause
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(Box::new(filter));
        self
    }

    /// Add ORDER BY within the function
    pub fn with_order_by(mut self, order_by: Vec<OrderByExpr>) -> Self {
        self.order_by = Some(order_by);
        self
    }
}

/// Aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    JsonAgg,
    JsonbAgg,
    ArrayAgg,
    BoolAnd,
    BoolOr,
    StringAgg,
}

impl AggregateFunction {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::JsonAgg => "json_agg",
            Self::JsonbAgg => "jsonb_agg",
            Self::ArrayAgg => "array_agg",
            Self::BoolAnd => "bool_and",
            Self::BoolOr => "bool_or",
            Self::StringAgg => "string_agg",
        }
    }
}

/// An aggregate expression with optional FILTER and ORDER BY
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<OrderByExpr>>,
}

impl AggregateExpr {
    pub fn new(function: AggregateFunction, args: Vec<Expr>) -> Self {
        Self {
            function,
            args,
            distinct: false,
            filter: None,
            order_by: None,
        }
    }

    pub fn count_star() -> Self {
        Self::new(
            AggregateFunction::Count,
            vec![Expr::Literal(Literal::String("*".to_string()))],
        )
    }

    pub fn count_all() -> Self {
        Self::new(AggregateFunction::Count, vec![])
    }

    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(Box::new(filter));
        self
    }

    pub fn with_order_by(mut self, order_by: Vec<OrderByExpr>) -> Self {
        self.order_by = Some(order_by);
        self
    }
}

/// CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr {
    /// CASE <operand> (simple case) vs CASE WHEN (searched case)
    pub operand: Option<Box<Expr>>,
    /// WHEN ... THEN ... pairs
    pub when_clauses: Vec<(Expr, Expr)>,
    /// ELSE clause
    pub else_clause: Option<Box<Expr>>,
}

impl CaseExpr {
    /// Create a searched CASE expression (CASE WHEN ... THEN ...)
    pub fn searched(when_clauses: Vec<(Expr, Expr)>, else_clause: Option<Expr>) -> Self {
        Self {
            operand: None,
            when_clauses,
            else_clause: else_clause.map(Box::new),
        }
    }

    /// Create a simple CASE expression (CASE x WHEN ... THEN ...)
    pub fn simple(
        operand: Expr,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Expr>,
    ) -> Self {
        Self {
            operand: Some(Box::new(operand)),
            when_clauses,
            else_clause: else_clause.map(Box::new),
        }
    }
}

/// JSON/JSONB building expressions
#[derive(Debug, Clone, PartialEq)]
pub enum JsonBuildExpr {
    /// jsonb_build_object(k1, v1, k2, v2, ...)
    Object(Vec<(Expr, Expr)>),
    /// jsonb_build_array(v1, v2, ...)
    Array(Vec<Expr>),
}

/// ORDER BY expression component
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: Option<OrderDirection>,
    pub nulls: Option<NullsOrder>,
}

impl OrderByExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            direction: None,
            nulls: None,
        }
    }

    pub fn asc(expr: Expr) -> Self {
        Self {
            expr,
            direction: Some(OrderDirection::Asc),
            nulls: None,
        }
    }

    pub fn desc(expr: Expr) -> Self {
        Self {
            expr,
            direction: Some(OrderDirection::Desc),
            nulls: None,
        }
    }

    pub fn with_nulls(mut self, nulls: NullsOrder) -> Self {
        self.nulls = Some(nulls);
        self
    }
}

/// ORDER BY direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl OrderDirection {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

/// NULLS FIRST/LAST in ORDER BY
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

impl NullsOrder {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::First => "nulls first",
            Self::Last => "nulls last",
        }
    }
}

/// The main expression enum encompassing all SQL expression types
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: table.column or just column
    Column(ColumnRef),

    /// Literal value
    Literal(Literal),

    /// Parameterized value: $1, $2, etc.
    Param(ParamRef),

    /// Binary operation: expr op expr
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    /// Unary operation: op expr (e.g., NOT)
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },

    /// Function call
    FunctionCall(FunctionCall),

    /// Aggregate function
    Aggregate(AggregateExpr),

    /// CASE expression
    Case(CaseExpr),

    /// Subquery: (SELECT ...)
    Subquery(Box<super::stmt::SelectStmt>),

    /// Array literal: ARRAY[...]
    Array(Vec<Expr>),

    /// Type cast: expr::type
    Cast {
        expr: Box<Expr>,
        target_type: SqlType,
    },

    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },

    /// expr IN (values) - for a list
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// expr BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// EXISTS (subquery)
    Exists {
        subquery: Box<super::stmt::SelectStmt>,
        negated: bool,
    },

    /// JSON/JSONB building
    JsonBuild(JsonBuildExpr),

    /// Coalesce function: COALESCE(expr1, expr2, ...)
    Coalesce(Vec<Expr>),

    /// Parenthesized expression (for explicit grouping)
    Nested(Box<Expr>),

    /// Raw SQL string - **DEPRECATED**: Use only in tests.
    ///
    /// This is a security-sensitive escape hatch that bypasses SQL injection protection.
    /// All production code should use type-safe AST nodes instead. If you need SQL
    /// functionality not yet supported by the AST, add a proper node type.
    ///
    /// # Security Warning
    ///
    /// Never use this with user-provided input. The string is rendered directly
    /// to SQL without any escaping or validation.
    #[cfg(test)]
    Raw(String),

    /// Array index access: array[index]
    ArrayIndex { array: Box<Expr>, index: Box<Expr> },

    /// Function call with ORDER BY clause (e.g., array_agg(x ORDER BY y))
    FunctionCallWithOrderBy {
        name: String,
        args: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
    },

    /// ROW constructor: ROW(expr1, expr2, ...) or (expr1, expr2, ...)
    Row(Vec<Expr>),
}

impl Expr {
    // Convenience constructors

    /// Create a column reference
    pub fn column(name: impl Into<Ident>) -> Self {
        Self::Column(ColumnRef::new(name))
    }

    /// Create a qualified column reference (table.column)
    pub fn qualified_column(table: impl Into<Ident>, column: impl Into<Ident>) -> Self {
        Self::Column(ColumnRef::qualified(table, column))
    }

    /// Create a NULL literal
    pub fn null() -> Self {
        Self::Literal(Literal::Null)
    }

    /// Create a boolean literal
    pub fn bool(b: bool) -> Self {
        Self::Literal(Literal::Bool(b))
    }

    /// Create an integer literal
    pub fn int(n: i64) -> Self {
        Self::Literal(Literal::Integer(n))
    }

    /// Create a string literal
    pub fn string(s: impl Into<String>) -> Self {
        Self::Literal(Literal::String(s.into()))
    }

    /// Create a binary operation
    pub fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a NOT expression
    pub fn not(expr: Expr) -> Self {
        Self::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        }
    }

    /// Create an IS NULL expression
    pub fn is_null(expr: Expr) -> Self {
        Self::IsNull {
            expr: Box::new(expr),
            negated: false,
        }
    }

    /// Create an IS NOT NULL expression
    pub fn is_not_null(expr: Expr) -> Self {
        Self::IsNull {
            expr: Box::new(expr),
            negated: true,
        }
    }

    /// Create a type cast
    pub fn cast(expr: Expr, target_type: SqlType) -> Self {
        Self::Cast {
            expr: Box::new(expr),
            target_type,
        }
    }

    /// Create a function call
    pub fn function(name: impl Into<Ident>, args: Vec<Expr>) -> Self {
        Self::FunctionCall(FunctionCall::new(
            name,
            args.into_iter().map(FunctionArg::Unnamed).collect(),
        ))
    }

    /// Create a COALESCE expression
    pub fn coalesce(exprs: Vec<Expr>) -> Self {
        Self::Coalesce(exprs)
    }

    /// Create jsonb_build_object
    pub fn jsonb_build_object(pairs: Vec<(Expr, Expr)>) -> Self {
        Self::JsonBuild(JsonBuildExpr::Object(pairs))
    }

    /// Create jsonb_build_array
    pub fn jsonb_build_array(exprs: Vec<Expr>) -> Self {
        Self::JsonBuild(JsonBuildExpr::Array(exprs))
    }

    /// Wrap in parentheses
    pub fn nested(self) -> Self {
        Self::Nested(Box::new(self))
    }

    /// Combine with AND
    pub fn and(self, other: Expr) -> Self {
        Self::binary(self, BinaryOperator::And, other)
    }

    /// Combine with OR
    pub fn or(self, other: Expr) -> Self {
        Self::binary(self, BinaryOperator::Or, other)
    }

    /// Check equality
    pub fn eq(self, other: Expr) -> Self {
        Self::binary(self, BinaryOperator::Eq, other)
    }

    /// Create raw SQL - **DEPRECATED**: Use only in tests.
    ///
    /// # Security Warning
    ///
    /// This bypasses SQL injection protection. Never use with user input.
    #[cfg(test)]
    pub fn raw(sql: impl Into<String>) -> Self {
        Self::Raw(sql.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref() {
        let col = ColumnRef::new("id");
        assert_eq!(col.column.as_str(), "id");
        assert!(col.table_alias.is_none());

        let col = ColumnRef::qualified("users", "id");
        assert_eq!(col.table_alias.unwrap().as_str(), "users");
        assert_eq!(col.column.as_str(), "id");
    }

    #[test]
    fn test_expr_constructors() {
        let expr = Expr::qualified_column("t", "id");
        match expr {
            Expr::Column(c) => {
                assert_eq!(c.table_alias.unwrap().as_str(), "t");
                assert_eq!(c.column.as_str(), "id");
            }
            _ => panic!("Expected Column"),
        }

        let expr = Expr::int(42);
        match expr {
            Expr::Literal(Literal::Integer(n)) => assert_eq!(n, 42),
            _ => panic!("Expected Integer"),
        }
    }

    #[test]
    fn test_binary_op() {
        let expr = Expr::qualified_column("t", "id").eq(Expr::int(1));
        match expr {
            Expr::BinaryOp { op, .. } => assert_eq!(op, BinaryOperator::Eq),
            _ => panic!("Expected BinaryOp"),
        }
    }
}
