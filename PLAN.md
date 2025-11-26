# SQL AST Refactoring Plan for pg_graphql

## Executive Summary

This plan outlines a comprehensive refactoring of the pg_graphql transpiler to use a type-safe SQL Abstract Syntax Tree (AST). The new architecture will provide:

1. **Type Safety**: Compile-time guarantees for SQL structure validity
2. **Modularity**: Clean separation between AST, rendering, and execution
3. **Future Extensibility**: Support for nested inserts requiring multiple SQL executions
4. **Debuggability**: Rich logging and telemetry throughout the pipeline
5. **Testability**: Each layer independently testable

---

## Current Architecture Analysis

### Current Flow
```
GraphQL Query → Parser (resolve.rs)
              → Builder Creation (builder.rs)
              → SQL String Generation (transpile.rs) - uses format!() strings
              → Execution via pgrx SPI
```

### Problems with Current Approach

1. **String-based SQL Generation**: Uses `format!()` macros throughout `transpile.rs`, making it easy to create malformed SQL
2. **Tight pgrx Coupling**: `ParamContext` is tightly coupled to pgrx's `DatumWithOid`
3. **No Intermediate Representation**: No way to inspect/validate/transform SQL before rendering
4. **Single Execution Assumption**: Current design assumes one SQL statement per request
5. **Limited Debugging**: No structured way to log generated SQL or execution plans

---

## Proposed Architecture

### New Flow
```
GraphQL Query → Parser (resolve.rs)
              → Builder Creation (builder.rs)
              → AST Construction (NEW: src/ast/mod.rs)
              → SQL Rendering (NEW: src/ast/render.rs)
              → Execution Plan (NEW: src/executor/mod.rs)
              → Execution via pgrx SPI
```

### Module Structure

```
src/
├── ast/                    # NEW: Standalone SQL AST module
│   ├── mod.rs             # Public API, re-exports
│   ├── expr.rs            # Expression types (columns, literals, operators)
│   ├── stmt.rs            # Statement types (SELECT, INSERT, UPDATE, DELETE)
│   ├── cte.rs             # CTE (WITH clause) support
│   ├── types.rs           # SQL type representations
│   ├── render.rs          # SQL string rendering (the only place SQL strings are built)
│   ├── params.rs          # Parameter handling (decoupled from pgrx)
│   └── validate.rs        # AST validation utilities
│
├── executor/              # NEW: Execution layer
│   ├── mod.rs             # Public API
│   ├── plan.rs            # Execution plans (single or multi-statement)
│   ├── pgrx_backend.rs    # pgrx-specific execution
│   └── telemetry.rs       # Logging and metrics
│
├── transpile.rs           # REFACTOR: Now builds AST instead of strings
├── builder.rs             # UNCHANGED initially
└── ...
```

---

## Phase 1: Core AST Module (Standalone, No pgrx Dependencies)

### 1.1 Expression Types (`src/ast/expr.rs`)

```rust
/// SQL expressions - the building blocks
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: table.column or just column
    Column(ColumnRef),

    /// Literal value with type information
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
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },

    /// Function call: func(args)
    FunctionCall(FunctionCall),

    /// CASE WHEN ... THEN ... ELSE ... END
    Case(CaseExpr),

    /// Subquery: (SELECT ...)
    Subquery(Box<SelectStmt>),

    /// Array construction: ARRAY[...]
    Array(Vec<Expr>),

    /// Type cast: expr::type
    Cast {
        expr: Box<Expr>,
        target_type: SqlType,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    /// expr IN (values) or expr = ANY(array)
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// Aggregate function with optional filter
    Aggregate(AggregateExpr),

    /// JSON/JSONB building functions
    JsonBuild(JsonBuildExpr),

    /// Raw SQL (escape hatch, should be minimized)
    Raw(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table_alias: Option<Ident>,
    pub column: Ident,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParamRef {
    pub index: usize,
    pub type_cast: SqlType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Default, // SQL DEFAULT keyword
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Eq, NotEq, Lt, LtEq, Gt, GtEq,
    // Array
    Contains, ContainedBy, Overlap, Any,
    // String
    Like, ILike, RegEx, IRegEx, StartsWith,
    // Logical
    And, Or,
    // Arithmetic
    Add, Sub, Mul, Div,
    // JSON
    JsonExtract, JsonExtractText,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub schema: Option<Ident>,
    pub name: Ident,
    pub args: Vec<FunctionArg>,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<OrderByExpr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    Unnamed(Expr),
    Named { name: Ident, value: Expr },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub args: Vec<Expr>,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<OrderByExpr>>,
}

#[derive(Debug, Clone, PartialEq)]
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
    Coalesce,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonBuildExpr {
    Object(Vec<(Expr, Expr)>),  // key-value pairs
    Array(Vec<Expr>),
}

/// A quoted identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(pub String);
```

### 1.2 Statement Types (`src/ast/stmt.rs`)

```rust
/// SQL statements
#[derive(Debug, Clone)]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub ctes: Vec<Cte>,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    Expr { expr: Expr, alias: Option<Ident> },
    AllFrom { table: Ident },
}

#[derive(Debug, Clone)]
pub enum FromClause {
    Table {
        schema: Option<Ident>,
        name: Ident,
        alias: Option<Ident>,
    },
    Subquery {
        query: Box<SelectStmt>,
        alias: Ident,
    },
    Function {
        call: FunctionCall,
        alias: Ident,
    },
    Join {
        left: Box<FromClause>,
        join_type: JoinType,
        right: Box<FromClause>,
        on: Option<Expr>,
    },
    CrossJoin {
        left: Box<FromClause>,
        right: Box<FromClause>,
    },
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: Option<OrderDirection>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub ctes: Vec<Cte>,
    pub schema: Option<Ident>,
    pub table: Ident,
    pub columns: Vec<Ident>,
    pub values: InsertValues,
    pub returning: Vec<SelectColumn>,
}

#[derive(Debug, Clone)]
pub enum InsertValues {
    Values(Vec<Vec<Expr>>),  // Multiple rows
    Query(Box<SelectStmt>),
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub ctes: Vec<Cte>,
    pub schema: Option<Ident>,
    pub table: Ident,
    pub alias: Option<Ident>,
    pub set: Vec<(Ident, Expr)>,
    pub where_clause: Option<Expr>,
    pub returning: Vec<SelectColumn>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub ctes: Vec<Cte>,
    pub schema: Option<Ident>,
    pub table: Ident,
    pub alias: Option<Ident>,
    pub where_clause: Option<Expr>,
    pub returning: Vec<SelectColumn>,
}
```

### 1.3 CTE Support (`src/ast/cte.rs`)

```rust
#[derive(Debug, Clone)]
pub struct Cte {
    pub name: Ident,
    pub columns: Option<Vec<Ident>>,
    pub query: CteQuery,
    pub materialized: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum CteQuery {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}
```

### 1.4 Type System (`src/ast/types.rs`)

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct SqlType {
    pub schema: Option<String>,
    pub name: String,
    pub oid: Option<u32>,  // PostgreSQL OID when known
    pub is_array: bool,
}

impl SqlType {
    pub fn text() -> Self {
        Self { schema: None, name: "text".into(), oid: Some(25), is_array: false }
    }

    pub fn integer() -> Self {
        Self { schema: None, name: "integer".into(), oid: Some(23), is_array: false }
    }

    pub fn bigint() -> Self {
        Self { schema: None, name: "bigint".into(), oid: Some(20), is_array: false }
    }

    pub fn jsonb() -> Self {
        Self { schema: None, name: "jsonb".into(), oid: Some(3802), is_array: false }
    }

    pub fn boolean() -> Self {
        Self { schema: None, name: "boolean".into(), oid: Some(16), is_array: false }
    }

    pub fn array_of(base: Self) -> Self {
        Self { is_array: true, ..base }
    }

    pub fn custom(schema: Option<String>, name: String) -> Self {
        Self { schema, name, oid: None, is_array: false }
    }
}
```

### 1.5 Parameter Handling (`src/ast/params.rs`)

```rust
/// A parameter value that can be rendered to SQL
/// Decoupled from pgrx - this is the AST's view of parameters
#[derive(Debug, Clone)]
pub struct Param {
    pub index: usize,
    pub value: ParamValue,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Bool(bool),
    String(String),
    Integer(i64),
    Float(f64),
    Array(Vec<ParamValue>),
    Json(serde_json::Value),
}

/// Collects parameters during AST construction
#[derive(Debug, Default)]
pub struct ParamCollector {
    params: Vec<Param>,
}

impl ParamCollector {
    pub fn new() -> Self {
        Self { params: Vec::new() }
    }

    /// Add a parameter and return its reference expression
    pub fn add(&mut self, value: ParamValue, sql_type: SqlType) -> Expr {
        let index = self.params.len() + 1; // 1-indexed for SQL
        self.params.push(Param { index, value, sql_type: sql_type.clone() });
        Expr::Param(ParamRef { index, type_cast: sql_type })
    }

    /// Get all collected parameters
    pub fn into_params(self) -> Vec<Param> {
        self.params
    }

    /// Get parameters as a slice
    pub fn params(&self) -> &[Param] {
        &self.params
    }
}
```

### 1.6 SQL Rendering (`src/ast/render.rs`)

```rust
use std::fmt::Write;

pub struct SqlRenderer {
    output: String,
    indent_level: usize,
    pretty: bool,
}

impl SqlRenderer {
    pub fn new() -> Self {
        Self { output: String::new(), indent_level: 0, pretty: false }
    }

    pub fn pretty() -> Self {
        Self { output: String::new(), indent_level: 0, pretty: true }
    }

    pub fn render_stmt(&mut self, stmt: &Stmt) -> &str {
        match stmt {
            Stmt::Select(s) => self.render_select(s),
            Stmt::Insert(s) => self.render_insert(s),
            Stmt::Update(s) => self.render_update(s),
            Stmt::Delete(s) => self.render_delete(s),
        }
        &self.output
    }

    fn render_select(&mut self, stmt: &SelectStmt) {
        self.render_ctes(&stmt.ctes);
        self.write("SELECT ");
        self.render_select_columns(&stmt.columns);
        if let Some(from) = &stmt.from {
            self.newline();
            self.write("FROM ");
            self.render_from(from);
        }
        if let Some(where_clause) = &stmt.where_clause {
            self.newline();
            self.write("WHERE ");
            self.render_expr(where_clause);
        }
        // ... group_by, having, order_by, limit, offset
    }

    fn render_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(col) => {
                if let Some(table) = &col.table_alias {
                    self.write_ident(table);
                    self.write(".");
                }
                self.write_ident(&col.column);
            }
            Expr::Param(p) => {
                write!(self.output, "(${}", p.index).unwrap();
                self.write("::");
                self.render_type(&p.type_cast);
                self.write(")");
            }
            Expr::BinaryOp { left, op, right } => {
                self.render_expr(left);
                self.write(" ");
                self.render_binary_op(op);
                self.write(" ");
                self.render_expr(right);
            }
            // ... all other expression types
            _ => todo!("Render {:?}", expr),
        }
    }

    fn write_ident(&mut self, ident: &Ident) {
        // Use PostgreSQL's quote_ident rules
        // For now, always quote to be safe
        write!(self.output, "\"{}\"", ident.0.replace('"', "\"\"")).unwrap();
    }

    fn write(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn newline(&mut self) {
        if self.pretty {
            self.output.push('\n');
            for _ in 0..self.indent_level {
                self.output.push_str("    ");
            }
        } else {
            self.output.push(' ');
        }
    }
}

/// Convenience function for quick rendering
pub fn render(stmt: &Stmt) -> String {
    let mut renderer = SqlRenderer::new();
    renderer.render_stmt(stmt);
    renderer.output
}

pub fn render_pretty(stmt: &Stmt) -> String {
    let mut renderer = SqlRenderer::pretty();
    renderer.render_stmt(stmt);
    renderer.output
}
```

---

## Phase 2: Executor Module

### 2.1 Execution Plans (`src/executor/plan.rs`)

```rust
use crate::ast::{Stmt, Param};

/// An execution plan that may contain one or more SQL statements
#[derive(Debug)]
pub struct ExecutionPlan {
    pub steps: Vec<ExecutionStep>,
    pub telemetry: PlanTelemetry,
}

#[derive(Debug)]
pub struct ExecutionStep {
    pub id: String,
    pub stmt: Stmt,
    pub params: Vec<Param>,
    pub description: String,
    pub depends_on: Vec<String>,  // IDs of steps this depends on
}

#[derive(Debug, Default)]
pub struct PlanTelemetry {
    pub graphql_query: Option<String>,
    pub operation_name: Option<String>,
    pub created_at: std::time::Instant,
}

impl ExecutionPlan {
    pub fn single(stmt: Stmt, params: Vec<Param>, description: &str) -> Self {
        Self {
            steps: vec![ExecutionStep {
                id: "main".to_string(),
                stmt,
                params,
                description: description.to_string(),
                depends_on: vec![],
            }],
            telemetry: PlanTelemetry::default(),
        }
    }

    /// For future nested inserts: create a multi-step plan
    pub fn multi(steps: Vec<ExecutionStep>) -> Self {
        Self {
            steps,
            telemetry: PlanTelemetry::default(),
        }
    }

    pub fn with_graphql_context(mut self, query: &str, operation_name: Option<&str>) -> Self {
        self.telemetry.graphql_query = Some(query.to_string());
        self.telemetry.operation_name = operation_name.map(|s| s.to_string());
        self
    }
}
```

### 2.2 pgrx Backend (`src/executor/pgrx_backend.rs`)

```rust
use crate::ast::{Param, ParamValue, SqlType, render};
use crate::executor::ExecutionPlan;
use crate::error::{GraphQLError, GraphQLResult};
use pgrx::prelude::*;
use pgrx::datum::DatumWithOid;
use pgrx::spi::SpiClient;

/// Converts AST parameters to pgrx Datums
pub fn params_to_datums(params: &[Param]) -> GraphQLResult<Vec<DatumWithOid<'static>>> {
    params.iter().map(param_to_datum).collect()
}

fn param_to_datum(param: &Param) -> GraphQLResult<DatumWithOid<'static>> {
    let datum = match &param.value {
        ParamValue::Null => None::<String>.into_datum(),
        ParamValue::Bool(b) => b.to_string().into_datum(),
        ParamValue::String(s) => s.clone().into_datum(),
        ParamValue::Integer(i) => i.to_string().into_datum(),
        ParamValue::Float(f) => f.to_string().into_datum(),
        ParamValue::Array(arr) => {
            let strings: Vec<Option<String>> = arr.iter()
                .map(|v| match v {
                    ParamValue::Null => None,
                    ParamValue::String(s) => Some(s.clone()),
                    ParamValue::Integer(i) => Some(i.to_string()),
                    ParamValue::Float(f) => Some(f.to_string()),
                    ParamValue::Bool(b) => Some(b.to_string()),
                    _ => None, // Nested arrays not supported
                })
                .collect();
            strings.into_datum()
        }
        ParamValue::Json(v) => v.to_string().into_datum(),
    };

    let oid = if param.sql_type.is_array {
        pgrx::pg_sys::TEXTARRAYOID
    } else {
        pgrx::pg_sys::TEXTOID
    };

    Ok(unsafe { DatumWithOid::new(datum, oid) })
}

/// Execute a query plan and return JSON result
pub fn execute_query(plan: &ExecutionPlan) -> GraphQLResult<serde_json::Value> {
    // Log the plan for debugging
    log_execution_plan(plan);

    if plan.steps.len() != 1 {
        return Err(GraphQLError::internal(
            "Query execution currently only supports single-step plans"
        ));
    }

    let step = &plan.steps[0];
    let sql = render(&step.stmt);
    let datums = params_to_datums(&step.params)?;

    log_sql_execution(&sql, &step.params);

    Spi::connect(|client| {
        let result = client.select(&sql, Some(1), &datums)?;
        if result.is_empty() {
            Ok(serde_json::Value::Null)
        } else {
            let jsonb: pgrx::JsonB = result.first().get(1)?
                .ok_or_else(|| GraphQLError::internal("No result from query"))?;
            Ok(jsonb.0)
        }
    }).map_err(|e| GraphQLError::sql_execution(format!("SPI error: {:?}", e)))
}

/// Execute a mutation plan and return JSON result
pub fn execute_mutation<'conn>(
    plan: &ExecutionPlan,
    client: &mut SpiClient<'conn>,
) -> GraphQLResult<serde_json::Value> {
    log_execution_plan(plan);

    if plan.steps.len() != 1 {
        // TODO: For nested inserts, we'll need to execute multiple steps
        return Err(GraphQLError::internal(
            "Mutation execution currently only supports single-step plans"
        ));
    }

    let step = &plan.steps[0];
    let sql = render(&step.stmt);
    let datums = params_to_datums(&step.params)?;

    log_sql_execution(&sql, &step.params);

    let result = client.update(&sql, None, &datums)
        .map_err(|_| GraphQLError::sql_execution("Failed to execute mutation"))?;

    let jsonb: pgrx::JsonB = result.first().get(1)?
        .ok_or_else(|| GraphQLError::internal("No result from mutation"))?;

    Ok(jsonb.0)
}
```

### 2.3 Telemetry (`src/executor/telemetry.rs`)

```rust
use crate::ast::{Param, render_pretty};
use crate::executor::ExecutionPlan;

/// Log level for SQL telemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Off,
    Basic,    // Just SQL and timing
    Detailed, // SQL + parameters
    Debug,    // Everything including AST dump
}

/// Get current log level from GUC or environment
pub fn get_log_level() -> LogLevel {
    // TODO: Read from pg_graphql.log_level GUC
    // For now, check environment variable
    match std::env::var("PG_GRAPHQL_LOG_LEVEL").as_deref() {
        Ok("off") => LogLevel::Off,
        Ok("basic") => LogLevel::Basic,
        Ok("detailed") => LogLevel::Detailed,
        Ok("debug") => LogLevel::Debug,
        _ => LogLevel::Off,
    }
}

pub fn log_execution_plan(plan: &ExecutionPlan) {
    let level = get_log_level();
    if level == LogLevel::Off {
        return;
    }

    pgrx::info!(
        "pg_graphql: Executing plan with {} step(s)",
        plan.steps.len()
    );

    if level >= LogLevel::Detailed {
        if let Some(query) = &plan.telemetry.graphql_query {
            pgrx::info!("pg_graphql: GraphQL query:\n{}", query);
        }
    }
}

pub fn log_sql_execution(sql: &str, params: &[Param]) {
    let level = get_log_level();
    if level == LogLevel::Off {
        return;
    }

    pgrx::info!("pg_graphql: Executing SQL:\n{}", sql);

    if level >= LogLevel::Detailed {
        for param in params {
            pgrx::info!(
                "pg_graphql: Param ${}: {:?} ({})",
                param.index,
                param.value,
                param.sql_type.name
            );
        }
    }
}

pub fn log_execution_result(duration_ms: u64, row_count: usize) {
    let level = get_log_level();
    if level >= LogLevel::Basic {
        pgrx::info!(
            "pg_graphql: Execution completed in {}ms, {} rows",
            duration_ms,
            row_count
        );
    }
}
```

---

## Phase 3: Refactor Transpile Module

### 3.1 New Transpiler Architecture

The transpiler will be refactored to build AST nodes instead of strings. Each builder's `to_sql()` method becomes `to_ast()`.

```rust
// src/transpile.rs - new structure

use crate::ast::*;
use crate::builder::*;
use crate::error::GraphQLResult;
use crate::executor::{ExecutionPlan, ExecutionStep};

/// Trait for types that can be transpiled to an execution plan
pub trait ToExecutionPlan {
    fn to_plan(&self) -> GraphQLResult<ExecutionPlan>;
}

/// Trait for types that can be transpiled to an AST expression or statement fragment
pub trait ToAst {
    type Output;
    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Output>;
}

impl ToExecutionPlan for InsertBuilder {
    fn to_plan(&self) -> GraphQLResult<ExecutionPlan> {
        let mut params = ParamCollector::new();
        let stmt = self.to_ast(&mut params)?;
        Ok(ExecutionPlan::single(
            Stmt::Select(stmt),  // INSERT wrapped in CTE returns via SELECT
            params.into_params(),
            "Insert mutation"
        ))
    }
}

impl ToAst for InsertBuilder {
    type Output = SelectStmt;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<SelectStmt> {
        let block_name = Ident(rand_block_name_raw());

        // Build the INSERT CTE
        let insert_cte = self.build_insert_cte(params)?;

        // Build the SELECT that reads from the CTE
        let select_columns = self.build_select_columns(&block_name, params)?;

        Ok(SelectStmt {
            ctes: vec![insert_cte],
            columns: select_columns,
            from: Some(FromClause::Table {
                schema: None,
                name: Ident("affected".to_string()),
                alias: Some(block_name),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        })
    }
}

impl InsertBuilder {
    fn build_insert_cte(&self, params: &mut ParamCollector) -> GraphQLResult<Cte> {
        let columns: Vec<Ident> = self.referenced_columns()
            .iter()
            .map(|c| Ident(c.name.clone()))
            .collect();

        let values: Vec<Vec<Expr>> = self.objects
            .iter()
            .map(|row| self.row_to_exprs(row, params))
            .collect::<Result<_, _>>()?;

        let returning = self.table.columns
            .iter()
            .filter(|c| c.permissions.is_selectable)
            .map(|c| SelectColumn::Expr {
                expr: Expr::Column(ColumnRef {
                    table_alias: None,
                    column: Ident(c.name.clone()),
                }),
                alias: None,
            })
            .collect();

        Ok(Cte {
            name: Ident("affected".to_string()),
            columns: None,
            query: CteQuery::Insert(InsertStmt {
                ctes: vec![],
                schema: Some(Ident(self.table.schema.clone())),
                table: Ident(self.table.name.clone()),
                columns,
                values: InsertValues::Values(values),
                returning,
            }),
            materialized: None,
        })
    }

    fn row_to_exprs(
        &self,
        row: &InsertRowBuilder,
        params: &mut ParamCollector,
    ) -> GraphQLResult<Vec<Expr>> {
        self.referenced_columns()
            .iter()
            .map(|col| {
                match row.row.get(&col.name) {
                    None | Some(InsertElemValue::Default) => Ok(Expr::Literal(Literal::Default)),
                    Some(InsertElemValue::Value(val)) => {
                        let param_value = json_to_param_value(val)?;
                        let sql_type = SqlType::custom(None, col.type_name.clone());
                        Ok(params.add(param_value, sql_type))
                    }
                }
            })
            .collect()
    }
}
```

### 3.2 Filter Expression Building

```rust
impl ToAst for FilterBuilder {
    type Output = Expr;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Expr> {
        if self.elems.is_empty() {
            return Ok(Expr::Literal(Literal::Bool(true)));
        }

        let exprs: Vec<Expr> = self.elems
            .iter()
            .map(|elem| elem.to_ast(params))
            .collect::<Result<_, _>>()?;

        // Combine with AND
        Ok(exprs.into_iter().reduce(|acc, expr| {
            Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOperator::And,
                right: Box::new(expr),
            }
        }).unwrap_or(Expr::Literal(Literal::Bool(true))))
    }
}

impl ToAst for FilterBuilderElem {
    type Output = Expr;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Expr> {
        match self {
            Self::Column { column, op, value } => {
                let col_expr = Expr::Column(ColumnRef {
                    table_alias: None, // Will be set by caller
                    column: Ident(column.name.clone()),
                });

                match op {
                    FilterOp::Is => {
                        let is_null = match value.as_str() {
                            Some("NULL") => true,
                            Some("NOT_NULL") => false,
                            _ => return Err(GraphQLError::sql_generation("Invalid IS filter value")),
                        };
                        Ok(Expr::IsNull {
                            expr: Box::new(col_expr),
                            negated: !is_null,
                        })
                    }
                    _ => {
                        let sql_type = self.determine_type(column, op);
                        let param_value = json_to_param_value(value)?;
                        let param_expr = params.add(param_value, sql_type);

                        Ok(Expr::BinaryOp {
                            left: Box::new(col_expr),
                            op: filter_op_to_binary_op(op),
                            right: Box::new(param_expr),
                        })
                    }
                }
            }
            Self::NodeId(node_id) => node_id.to_ast(params),
            Self::Compound(compound) => compound.to_ast(params),
        }
    }
}

fn filter_op_to_binary_op(op: &FilterOp) -> BinaryOperator {
    match op {
        FilterOp::Equal => BinaryOperator::Eq,
        FilterOp::NotEqual => BinaryOperator::NotEq,
        FilterOp::LessThan => BinaryOperator::Lt,
        FilterOp::LessThanEqualTo => BinaryOperator::LtEq,
        FilterOp::GreaterThan => BinaryOperator::Gt,
        FilterOp::GreaterThanEqualTo => BinaryOperator::GtEq,
        FilterOp::In => BinaryOperator::Any,
        FilterOp::StartsWith => BinaryOperator::StartsWith,
        FilterOp::Like => BinaryOperator::Like,
        FilterOp::ILike => BinaryOperator::ILike,
        FilterOp::RegEx => BinaryOperator::RegEx,
        FilterOp::IRegEx => BinaryOperator::IRegEx,
        FilterOp::Contains => BinaryOperator::Contains,
        FilterOp::ContainedBy => BinaryOperator::ContainedBy,
        FilterOp::Overlap => BinaryOperator::Overlap,
        FilterOp::Is => unreachable!("Is handled separately"),
    }
}
```

---

## Phase 4: Testing Strategy

### 4.1 Unit Tests for AST Module

Create `src/ast/tests.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_simple_select() {
        let stmt = Stmt::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Expr {
                expr: Expr::Column(ColumnRef {
                    table_alias: Some(Ident("t".to_string())),
                    column: Ident("id".to_string()),
                }),
                alias: None,
            }],
            from: Some(FromClause::Table {
                schema: Some(Ident("public".to_string())),
                name: Ident("users".to_string()),
                alias: Some(Ident("t".to_string())),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        });

        let sql = render(&stmt);
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("\"t\".\"id\""));
        assert!(sql.contains("FROM \"public\".\"users\""));
    }

    #[test]
    fn test_render_insert_with_returning() {
        // Test INSERT statement rendering
    }

    #[test]
    fn test_param_collector() {
        let mut collector = ParamCollector::new();
        let expr1 = collector.add(ParamValue::String("hello".into()), SqlType::text());
        let expr2 = collector.add(ParamValue::Integer(42), SqlType::integer());

        assert_eq!(collector.params().len(), 2);
        match &expr1 {
            Expr::Param(p) => assert_eq!(p.index, 1),
            _ => panic!("Expected Param"),
        }
    }

    #[test]
    fn test_jsonb_build_object_chunking() {
        // Test that large objects are properly chunked with ||
    }
}
```

### 4.2 Integration with Existing pg_regress Tests

All existing tests in `test/sql/` remain unchanged. The refactoring should produce **identical SQL output** (modulo whitespace) for all existing queries.

We'll add a test helper that compares old and new transpiler output:

```rust
// In development only: compare outputs
#[cfg(feature = "transpiler_validation")]
fn validate_transpiler_output(
    old_sql: &str,
    new_sql: &str,
) {
    // Normalize whitespace and compare
    let normalize = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(normalize(old_sql), normalize(new_sql));
}
```

### 4.3 New AST-Specific Tests

Create new test files:

- `test/sql/ast_basic.sql` - Basic AST rendering tests
- `test/sql/ast_cte.sql` - CTE generation tests
- `test/sql/ast_params.sql` - Parameter handling tests
- `test/sql/telemetry.sql` - Logging/telemetry tests

---

## Phase 5: Implementation Order

### Step 1: Create AST Module Foundation (No Breaking Changes)
1. Create `src/ast/mod.rs` with module structure
2. Implement `expr.rs` with all expression types
3. Implement `stmt.rs` with statement types
4. Implement `cte.rs` for CTE support
5. Implement `types.rs` for SQL types
6. Implement `params.rs` for parameter collection
7. Implement `render.rs` for SQL generation
8. Add comprehensive unit tests

**Verification**: Run `cargo test` - all AST unit tests pass

### Step 2: Create Executor Module (No Breaking Changes)
1. Create `src/executor/mod.rs`
2. Implement `plan.rs` for execution plans
3. Implement `telemetry.rs` for logging
4. Implement `pgrx_backend.rs` for pgrx integration
5. Add unit tests

**Verification**: Run `cargo test` - all executor unit tests pass

### Step 3: Add Parallel Transpilation Path
1. Add `ToAst` trait implementations alongside existing `to_sql()` methods
2. Start with `InsertBuilder` as it's well-understood
3. Add feature flag to switch between old and new paths
4. Compare outputs in development mode

**Verification**:
- `cargo pgrx install --features pg18`
- `./bin/installcheck mutation_insert`

### Step 4: Incrementally Migrate Builders
1. Migrate `FilterBuilder` and `FilterBuilderElem`
2. Migrate `UpdateBuilder`
3. Migrate `DeleteBuilder`
4. Migrate `NodeBuilder`
5. Migrate `ConnectionBuilder` (most complex)
6. Migrate `FunctionCallBuilder`

For each builder:
- Implement `ToAst` trait
- Run corresponding tests
- Compare output with old implementation

**Verification after each**: Run relevant `./bin/installcheck` tests

### Step 5: Remove Old String-Based Code
1. Remove feature flag
2. Delete old `to_sql()` methods
3. Update `resolve.rs` to use new executor
4. Clean up any remaining string-based SQL generation

**Verification**: Run full test suite: `./bin/installcheck`

### Step 6: Add Multi-Statement Support (Future Extensibility)
1. Extend `ExecutionPlan` for dependent steps
2. Implement step ordering in executor
3. Add transaction handling for multi-statement plans
4. Document API for nested inserts

---

## Design Decisions

### Why Text-Based Parameters?
The current approach converts all parameters to text and lets PostgreSQL cast them. We maintain this pattern because:
1. It's proven to work reliably
2. PostgreSQL's type coercion is well-tested
3. It avoids complex Datum construction
4. The generated SQL is readable and debuggable

### Why CTE-Based Mutations?
CTEs provide atomic execution and allow us to:
1. Check affected row counts before committing
2. Return data from the modified rows
3. Compose complex operations
4. Support future features like nested inserts

### Why Separate Render Phase?
Having a dedicated render phase allows:
1. AST inspection/validation before execution
2. Different rendering styles (compact vs pretty)
3. SQL logging and debugging
4. Future optimizations at the AST level

### Identifier Quoting Strategy
All identifiers are quoted by default using PostgreSQL's rules:
- Double quotes around the identifier
- Internal double quotes escaped by doubling
- This prevents SQL injection and handles special characters

---

## PostgreSQL Version Compatibility

The AST module must work with PostgreSQL 14-18. Key considerations:

1. **Standard SQL constructs only**: The AST uses standard PostgreSQL features available in all supported versions
2. **No version-specific features**: Avoid using features only in newer PostgreSQL versions
3. **Test matrix**: CI runs against all supported versions

---

## Risk Mitigation

### Risk: Breaking Existing Functionality
**Mitigation**:
- Parallel implementation with feature flag
- Output comparison in development
- Full regression test suite
- Incremental migration

### Risk: Performance Regression
**Mitigation**:
- Benchmark key queries before/after
- AST construction adds minimal overhead
- Rendering is O(n) in SQL length
- No runtime overhead once SQL is generated

### Risk: Complex Nested Queries
**Mitigation**:
- ConnectionBuilder tests cover complex CTEs
- Start with simpler builders
- Extensive unit tests for edge cases

---

## Success Criteria

1. **All existing tests pass**: `./bin/installcheck` returns 0
2. **No SQL changes**: Generated SQL is semantically identical
3. **Type safety**: No string interpolation for SQL identifiers or values
4. **Modularity**: AST module has no pgrx dependencies
5. **Debuggability**: Telemetry shows generated SQL and parameters
6. **Documentation**: All new types and functions documented
7. **Future-ready**: Clear path for multi-statement execution

---

## Appendix: Full Module Structure

```
src/
├── ast/
│   ├── mod.rs           # 50 lines - re-exports
│   ├── expr.rs          # 400 lines - expression types
│   ├── stmt.rs          # 300 lines - statement types
│   ├── cte.rs           # 50 lines - CTE types
│   ├── types.rs         # 100 lines - SQL type system
│   ├── params.rs        # 100 lines - parameter handling
│   ├── render.rs        # 600 lines - SQL rendering
│   ├── validate.rs      # 100 lines - validation utilities
│   └── tests.rs         # 400 lines - unit tests
│
├── executor/
│   ├── mod.rs           # 50 lines - re-exports
│   ├── plan.rs          # 150 lines - execution plans
│   ├── pgrx_backend.rs  # 200 lines - pgrx execution
│   └── telemetry.rs     # 150 lines - logging
│
├── transpile.rs         # REFACTORED: 1500 lines - builds AST
├── builder.rs           # UNCHANGED: ~800 lines
├── resolve.rs           # MINOR CHANGES: uses executor
├── graphql.rs           # UNCHANGED
├── lib.rs               # MINOR CHANGES: module declarations
├── error.rs             # MINOR ADDITIONS: new error types
├── sql_types.rs         # UNCHANGED
├── constants.rs         # UNCHANGED
├── gson.rs              # UNCHANGED
├── omit.rs              # UNCHANGED
├── parser_util.rs       # UNCHANGED
└── merge.rs             # UNCHANGED

Estimated new code: ~2,500 lines
Estimated refactored code: ~1,500 lines
Total: ~4,000 lines of changes
```
