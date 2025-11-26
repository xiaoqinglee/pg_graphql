//! SQL string rendering
//!
//! This module converts AST nodes to SQL strings. It is the only place
//! in the codebase where SQL strings are constructed.
//!
//! # Architecture
//!
//! The rendering system is built around two key components:
//!
//! - [`Render`] trait: Implemented by all AST nodes to define how they render to SQL
//! - [`SqlRenderer`]: The rendering context that handles output buffering and formatting
//!
//! # Safety
//!
//! All identifiers are properly quoted using PostgreSQL's quoting rules.
//! All literals are properly escaped.

use super::cte::{Cte, CteQuery};
use super::expr::*;
use super::stmt::*;
use super::types::SqlType;
use std::fmt::Write;

// =============================================================================
// Render Trait
// =============================================================================

/// Trait for AST nodes that can be rendered to SQL.
///
/// This trait allows each AST node type to define its own rendering logic
/// while sharing the common [`SqlRenderer`] infrastructure.
///
/// # Example
///
/// ```rust,ignore
/// use pg_graphql::ast::{Render, SqlRenderer, Expr};
///
/// let expr = Expr::int(42);
/// let mut renderer = SqlRenderer::new();
/// expr.render(&mut renderer);
/// let sql = renderer.into_sql();
/// assert_eq!(sql, "42");
/// ```
pub trait Render {
    /// Render this node to the given SQL renderer
    fn render(&self, renderer: &mut SqlRenderer);
}

// Implement Render for Stmt
impl Render for Stmt {
    fn render(&self, renderer: &mut SqlRenderer) {
        match self {
            Stmt::Select(s) => s.render(renderer),
            Stmt::Insert(s) => s.render(renderer),
            Stmt::Update(s) => s.render(renderer),
            Stmt::Delete(s) => s.render(renderer),
        }
    }
}

// Implement Render for SelectStmt
impl Render for SelectStmt {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_select(self);
    }
}

// Implement Render for InsertStmt
impl Render for InsertStmt {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_insert(self);
    }
}

// Implement Render for UpdateStmt
impl Render for UpdateStmt {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_update(self);
    }
}

// Implement Render for DeleteStmt
impl Render for DeleteStmt {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_delete(self);
    }
}

// Implement Render for Expr
impl Render for Expr {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_expr(self);
    }
}

// Implement Render for Literal
impl Render for Literal {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.render_literal(self);
    }
}

// Implement Render for Ident
impl Render for Ident {
    fn render(&self, renderer: &mut SqlRenderer) {
        renderer.write_ident(self);
    }
}

// =============================================================================
// Constants
// =============================================================================

/// Default buffer capacity for simple queries
const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// Buffer capacity for queries with CTEs (e.g., connection queries)
const CTE_BUFFER_CAPACITY: usize = 4096;

/// Buffer capacity for complex queries with many CTEs
const LARGE_BUFFER_CAPACITY: usize = 8192;

/// SQL renderer with optional pretty-printing
pub struct SqlRenderer {
    output: String,
    indent_level: usize,
    pretty: bool,
}

impl SqlRenderer {
    /// Create a new renderer with compact output
    pub fn new() -> Self {
        Self {
            output: String::with_capacity(DEFAULT_BUFFER_CAPACITY),
            indent_level: 0,
            pretty: false,
        }
    }

    /// Create a new renderer with a specific buffer capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            output: String::with_capacity(capacity),
            indent_level: 0,
            pretty: false,
        }
    }

    /// Create a new renderer with pretty-printed output
    pub fn pretty() -> Self {
        Self {
            output: String::with_capacity(DEFAULT_BUFFER_CAPACITY),
            indent_level: 0,
            pretty: true,
        }
    }

    /// Estimate appropriate buffer capacity based on statement complexity
    pub fn estimate_capacity(stmt: &Stmt) -> usize {
        match stmt {
            Stmt::Select(s) => {
                let cte_count = s.ctes.len();
                if cte_count >= 5 {
                    LARGE_BUFFER_CAPACITY
                } else if cte_count > 0 {
                    CTE_BUFFER_CAPACITY
                } else {
                    DEFAULT_BUFFER_CAPACITY
                }
            }
            Stmt::Insert(s) => {
                let cte_count = s.ctes.len();
                if cte_count > 0 {
                    CTE_BUFFER_CAPACITY
                } else {
                    DEFAULT_BUFFER_CAPACITY
                }
            }
            Stmt::Update(s) => {
                let cte_count = s.ctes.len();
                if cte_count > 0 {
                    CTE_BUFFER_CAPACITY
                } else {
                    DEFAULT_BUFFER_CAPACITY
                }
            }
            Stmt::Delete(s) => {
                let cte_count = s.ctes.len();
                if cte_count > 0 {
                    CTE_BUFFER_CAPACITY
                } else {
                    DEFAULT_BUFFER_CAPACITY
                }
            }
        }
    }

    /// Render a statement and return the SQL string
    pub fn render_stmt(&mut self, stmt: &Stmt) -> &str {
        match stmt {
            Stmt::Select(s) => self.render_select(s),
            Stmt::Insert(s) => self.render_insert(s),
            Stmt::Update(s) => self.render_update(s),
            Stmt::Delete(s) => self.render_delete(s),
        }
        &self.output
    }

    /// Take ownership of the rendered SQL string
    pub fn into_sql(self) -> String {
        self.output
    }

    // =========================================================================
    // Statement rendering
    // =========================================================================

    fn render_select(&mut self, stmt: &SelectStmt) {
        self.render_ctes(&stmt.ctes);
        self.write("select ");
        self.render_select_columns(&stmt.columns);

        if let Some(from) = &stmt.from {
            self.newline();
            self.write("from ");
            self.render_from(from);
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.newline();
            self.write("where ");
            self.render_expr(where_clause);
        }

        if !stmt.group_by.is_empty() {
            self.newline();
            self.write("group by ");
            self.render_expr_list(&stmt.group_by);
        }

        if let Some(having) = &stmt.having {
            self.newline();
            self.write("having ");
            self.render_expr(having);
        }

        if !stmt.order_by.is_empty() {
            self.newline();
            self.write("order by ");
            self.render_order_by(&stmt.order_by);
        }

        if let Some(limit) = stmt.limit {
            self.newline();
            write!(self.output, "limit {}", limit).unwrap();
        }

        if let Some(offset) = stmt.offset {
            self.newline();
            write!(self.output, "offset {}", offset).unwrap();
        }
    }

    fn render_insert(&mut self, stmt: &InsertStmt) {
        self.render_ctes(&stmt.ctes);
        self.write("insert into ");
        self.render_table_name(stmt.schema.as_ref(), &stmt.table);

        if !stmt.columns.is_empty() {
            self.write("(");
            for (i, col) in stmt.columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write_ident(col);
            }
            self.write(")");
        }

        self.newline();
        match &stmt.values {
            InsertValues::Values(rows) => {
                self.write("values ");
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write("(");
                    self.render_expr_list(row);
                    self.write(")");
                }
            }
            InsertValues::Query(query) => {
                self.render_select(query);
            }
            InsertValues::DefaultValues => {
                self.write("default values");
            }
        }

        if let Some(on_conflict) = &stmt.on_conflict {
            self.newline();
            self.render_on_conflict(on_conflict);
        }

        if !stmt.returning.is_empty() {
            self.newline();
            self.write("returning ");
            self.render_select_columns(&stmt.returning);
        }
    }

    fn render_update(&mut self, stmt: &UpdateStmt) {
        self.render_ctes(&stmt.ctes);
        self.write("update ");
        self.render_table_name(stmt.schema.as_ref(), &stmt.table);

        if let Some(alias) = &stmt.alias {
            self.write(" as ");
            self.write_ident(alias);
        }

        self.newline();
        self.write("set ");
        for (i, (col, expr)) in stmt.set.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write_ident(col);
            self.write(" = ");
            self.render_expr(expr);
        }

        if let Some(from) = &stmt.from {
            self.newline();
            self.write("from ");
            self.render_from(from);
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.newline();
            self.write("where ");
            self.render_expr(where_clause);
        }

        if !stmt.returning.is_empty() {
            self.newline();
            self.write("returning ");
            self.render_select_columns(&stmt.returning);
        }
    }

    fn render_delete(&mut self, stmt: &DeleteStmt) {
        self.render_ctes(&stmt.ctes);
        self.write("delete from ");
        self.render_table_name(stmt.schema.as_ref(), &stmt.table);

        if let Some(alias) = &stmt.alias {
            self.write(" as ");
            self.write_ident(alias);
        }

        if let Some(using) = &stmt.using {
            self.newline();
            self.write("using ");
            self.render_from(using);
        }

        if let Some(where_clause) = &stmt.where_clause {
            self.newline();
            self.write("where ");
            self.render_expr(where_clause);
        }

        if !stmt.returning.is_empty() {
            self.newline();
            self.write("returning ");
            self.render_select_columns(&stmt.returning);
        }
    }

    // =========================================================================
    // CTE rendering
    // =========================================================================

    fn render_ctes(&mut self, ctes: &[Cte]) {
        if ctes.is_empty() {
            return;
        }

        self.write("with ");
        for (i, cte) in ctes.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.render_cte(cte);
        }
        self.newline();
    }

    fn render_cte(&mut self, cte: &Cte) {
        self.write_ident(&cte.name);

        if let Some(columns) = &cte.columns {
            self.write("(");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    self.write(", ");
                }
                self.write_ident(col);
            }
            self.write(")");
        }

        self.write(" as ");

        if let Some(materialized) = cte.materialized {
            if materialized {
                self.write("materialized ");
            } else {
                self.write("not materialized ");
            }
        }

        self.write("(");
        self.indent();
        self.newline();

        match &cte.query {
            CteQuery::Select(s) => self.render_select(s),
            CteQuery::Insert(s) => self.render_insert(s),
            CteQuery::Update(s) => self.render_update(s),
            CteQuery::Delete(s) => self.render_delete(s),
        }

        self.dedent();
        self.newline();
        self.write(")");
    }

    // =========================================================================
    // FROM clause rendering
    // =========================================================================

    fn render_from(&mut self, from: &FromClause) {
        match from {
            FromClause::Table {
                schema,
                name,
                alias,
            } => {
                self.render_table_name(schema.as_ref(), name);
                if let Some(alias) = alias {
                    self.write(" ");
                    self.write_ident(alias);
                }
            }
            FromClause::Subquery { query, alias } => {
                self.write("(");
                self.render_select(query);
                self.write(") ");
                self.write_ident(alias);
            }
            FromClause::Function { call, alias } => {
                self.render_function_call(call);
                self.write(" ");
                self.write_ident(alias);
            }
            FromClause::Join {
                left,
                join_type,
                right,
                on,
            } => {
                self.render_from(left);
                self.newline();
                self.write(join_type.as_sql());
                self.write(" ");
                self.render_from(right);
                if let Some(on_expr) = on {
                    self.write(" on ");
                    self.render_expr(on_expr);
                }
            }
            FromClause::CrossJoin { left, right } => {
                self.render_from(left);
                self.write(" cross join ");
                self.render_from(right);
            }
            FromClause::Lateral { subquery, alias } => {
                self.write("lateral (");
                self.render_select(subquery);
                self.write(") ");
                self.write_ident(alias);
            }
        }
    }

    // =========================================================================
    // Expression rendering
    // =========================================================================

    fn render_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(col) => {
                if let Some(table) = &col.table_alias {
                    self.write_ident(table);
                    self.write(".");
                }
                self.write_ident(&col.column);
            }

            Expr::Literal(lit) => self.render_literal(lit),

            Expr::Param(p) => {
                write!(self.output, "(${}", p.index).unwrap();
                self.write("::");
                self.render_type(&p.type_cast);
                self.write(")");
            }

            Expr::BinaryOp { left, op, right } => {
                self.render_expr(left);
                self.write(" ");
                self.write(op.as_sql());
                // The ANY operator needs parentheses around the right operand: col = any(arr)
                if *op == BinaryOperator::Any {
                    self.write("(");
                    self.render_expr(right);
                    self.write(")");
                } else {
                    self.write(" ");
                    self.render_expr(right);
                }
            }

            Expr::UnaryOp { op, expr } => {
                self.write(op.as_sql());
                self.write("(");
                self.render_expr(expr);
                self.write(")");
            }

            Expr::FunctionCall(call) => {
                self.render_function_call(call);
            }

            Expr::Aggregate(agg) => {
                self.render_aggregate(agg);
            }

            Expr::Case(case) => {
                self.render_case(case);
            }

            Expr::Subquery(query) => {
                self.write("(");
                self.render_select(query);
                self.write(")");
            }

            Expr::Array(exprs) => {
                self.write("array[");
                self.render_expr_list(exprs);
                self.write("]");
            }

            Expr::Cast { expr, target_type } => {
                self.render_expr(expr);
                self.write("::");
                self.render_type(target_type);
            }

            Expr::IsNull { expr, negated } => {
                self.render_expr(expr);
                if *negated {
                    self.write(" is not null");
                } else {
                    self.write(" is null");
                }
            }

            Expr::InList {
                expr,
                list,
                negated,
            } => {
                self.render_expr(expr);
                if *negated {
                    self.write(" not in (");
                } else {
                    self.write(" in (");
                }
                self.render_expr_list(list);
                self.write(")");
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                self.render_expr(expr);
                if *negated {
                    self.write(" not between ");
                } else {
                    self.write(" between ");
                }
                self.render_expr(low);
                self.write(" and ");
                self.render_expr(high);
            }

            Expr::Exists { subquery, negated } => {
                if *negated {
                    self.write("not ");
                }
                self.write("exists (");
                self.render_select(subquery);
                self.write(")");
            }

            Expr::JsonBuild(json) => {
                self.render_json_build(json);
            }

            Expr::Coalesce(exprs) => {
                self.write("coalesce(");
                self.render_expr_list(exprs);
                self.write(")");
            }

            Expr::Nested(inner) => {
                self.write("(");
                self.render_expr(inner);
                self.write(")");
            }

            // Raw SQL - only available in tests
            #[cfg(test)]
            Expr::Raw(sql) => {
                self.write(sql);
            }

            Expr::ArrayIndex { array, index } => {
                self.write("(");
                self.render_expr(array);
                self.write(")[");
                self.render_expr(index);
                self.write("]");
            }

            Expr::FunctionCallWithOrderBy {
                name,
                args,
                order_by,
            } => {
                self.write(name);
                self.write("(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.render_expr(arg);
                }
                if !order_by.is_empty() {
                    self.write(" order by ");
                    self.render_order_by(order_by);
                }
                self.write(")");
            }

            Expr::Row(exprs) => {
                // ROW constructor: ROW(expr1, expr2, ...) or just (expr1, expr2, ...)
                // We use the explicit ROW keyword for clarity
                self.write("row(");
                self.render_expr_list(exprs);
                self.write(")");
            }
        }
    }

    fn render_literal(&mut self, lit: &Literal) {
        match lit {
            Literal::Null => self.write("null"),
            Literal::Bool(b) => self.write(if *b { "true" } else { "false" }),
            Literal::Integer(n) => write!(self.output, "{}", n).unwrap(),
            Literal::Float(f) => write!(self.output, "{}", f).unwrap(),
            Literal::String(s) => self.write_literal(s),
            Literal::Default => self.write("default"),
        }
    }

    fn render_function_call(&mut self, call: &FunctionCall) {
        if let Some(schema) = &call.schema {
            self.write_ident(schema);
            self.write(".");
        }
        self.write_ident(&call.name);
        self.write("(");

        for (i, arg) in call.args.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            match arg {
                FunctionArg::Unnamed(expr) => self.render_expr(expr),
                FunctionArg::Named { name, value } => {
                    self.write_ident(name);
                    self.write(" => ");
                    self.render_expr(value);
                }
            }
        }

        if let Some(order_by) = &call.order_by {
            if !order_by.is_empty() {
                self.write(" order by ");
                self.render_order_by(order_by);
            }
        }

        self.write(")");

        if let Some(filter) = &call.filter {
            self.write(" filter (where ");
            self.render_expr(filter);
            self.write(")");
        }
    }

    fn render_aggregate(&mut self, agg: &AggregateExpr) {
        self.write(agg.function.as_sql());
        self.write("(");

        if agg.distinct {
            self.write("distinct ");
        }

        // Special case for count(*)
        if agg.args.is_empty() && matches!(agg.function, AggregateFunction::Count) {
            self.write("*");
        } else {
            self.render_expr_list(&agg.args);
        }

        if let Some(order_by) = &agg.order_by {
            if !order_by.is_empty() {
                self.write(" order by ");
                self.render_order_by(order_by);
            }
        }

        self.write(")");

        if let Some(filter) = &agg.filter {
            self.write(" filter (where ");
            self.render_expr(filter);
            self.write(")");
        }
    }

    fn render_case(&mut self, case: &CaseExpr) {
        self.write("case");

        if let Some(operand) = &case.operand {
            self.write(" ");
            self.render_expr(operand);
        }

        for (when_expr, then_expr) in &case.when_clauses {
            self.write(" when ");
            self.render_expr(when_expr);
            self.write(" then ");
            self.render_expr(then_expr);
        }

        if let Some(else_clause) = &case.else_clause {
            self.write(" else ");
            self.render_expr(else_clause);
        }

        self.write(" end");
    }

    fn render_json_build(&mut self, json: &JsonBuildExpr) {
        match json {
            JsonBuildExpr::Object(pairs) => {
                self.write("jsonb_build_object(");
                for (i, (key, value)) in pairs.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.render_expr(key);
                    self.write(", ");
                    self.render_expr(value);
                }
                self.write(")");
            }
            JsonBuildExpr::Array(exprs) => {
                self.write("jsonb_build_array(");
                self.render_expr_list(exprs);
                self.write(")");
            }
        }
    }

    fn render_on_conflict(&mut self, on_conflict: &OnConflict) {
        self.write("on conflict ");

        match &on_conflict.target {
            OnConflictTarget::Columns(cols) => {
                self.write("(");
                for (i, col) in cols.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write_ident(col);
                }
                self.write(") ");
            }
            OnConflictTarget::Constraint(name) => {
                self.write("on constraint ");
                self.write_ident(name);
                self.write(" ");
            }
        }

        match &on_conflict.action {
            OnConflictAction::DoNothing => {
                self.write("do nothing");
            }
            OnConflictAction::DoUpdate { set, where_clause } => {
                self.write("do update set ");
                for (i, (col, expr)) in set.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.write_ident(col);
                    self.write(" = ");
                    self.render_expr(expr);
                }
                if let Some(where_clause) = where_clause {
                    self.write(" where ");
                    self.render_expr(where_clause);
                }
            }
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    fn render_select_columns(&mut self, columns: &[SelectColumn]) {
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            match col {
                SelectColumn::Expr { expr, alias } => {
                    self.render_expr(expr);
                    if let Some(alias) = alias {
                        self.write(" as ");
                        self.write_ident(alias);
                    }
                }
                SelectColumn::Star => self.write("*"),
                SelectColumn::QualifiedStar { table } => {
                    self.write_ident(table);
                    self.write(".*");
                }
            }
        }
    }

    fn render_expr_list(&mut self, exprs: &[Expr]) {
        for (i, expr) in exprs.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.render_expr(expr);
        }
    }

    fn render_order_by(&mut self, order_by: &[OrderByExpr]) {
        for (i, ob) in order_by.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.render_expr(&ob.expr);
            if let Some(dir) = &ob.direction {
                self.write(" ");
                self.write(dir.as_sql());
            }
            if let Some(nulls) = &ob.nulls {
                self.write(" ");
                self.write(nulls.as_sql());
            }
        }
    }

    fn render_table_name(&mut self, schema: Option<&Ident>, name: &Ident) {
        if let Some(schema) = schema {
            self.write_ident(schema);
            self.write(".");
        }
        self.write_ident(name);
    }

    fn render_type(&mut self, sql_type: &SqlType) {
        if let Some(schema) = &sql_type.schema {
            self.output.push_str(schema);
            self.output.push('.');
        }
        self.output.push_str(&sql_type.name);
        if sql_type.is_array {
            self.output.push_str("[]");
        }
    }

    // =========================================================================
    // Low-level output methods
    // =========================================================================

    fn write(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn write_ident(&mut self, ident: &Ident) {
        // Always quote identifiers to prevent SQL injection and handle special chars
        self.output.push('"');
        // Escape any double quotes in the identifier by doubling them
        for c in ident.as_str().chars() {
            if c == '"' {
                self.output.push('"');
            }
            self.output.push(c);
        }
        self.output.push('"');
    }

    fn write_literal(&mut self, s: &str) {
        // Use dollar quoting for strings that might contain special characters
        // This matches PostgreSQL's quote_literal behavior
        if s.contains('\'') || s.contains('\\') {
            // Use dollar quoting
            self.output.push_str("$__$");
            self.output.push_str(s);
            self.output.push_str("$__$");
        } else {
            self.output.push('\'');
            self.output.push_str(s);
            self.output.push('\'');
        }
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

    fn indent(&mut self) {
        self.indent_level += 1;
    }

    fn dedent(&mut self) {
        if self.indent_level > 0 {
            self.indent_level -= 1;
        }
    }
}

impl Default for SqlRenderer {
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// Convenience functions
// =========================================================================

/// Render a statement to a compact SQL string
///
/// Uses estimated buffer capacity based on statement complexity to reduce allocations.
pub fn render(stmt: &Stmt) -> String {
    let capacity = SqlRenderer::estimate_capacity(stmt);
    let mut renderer = SqlRenderer::with_capacity(capacity);
    renderer.render_stmt(stmt);
    renderer.into_sql()
}

/// Render a statement to a pretty-printed SQL string
pub fn render_pretty(stmt: &Stmt) -> String {
    let capacity = SqlRenderer::estimate_capacity(stmt);
    let mut renderer = SqlRenderer {
        output: String::with_capacity(capacity),
        indent_level: 0,
        pretty: true,
    };
    renderer.render_stmt(stmt);
    renderer.into_sql()
}

/// Render just a SELECT statement
pub fn render_select(stmt: &SelectStmt) -> String {
    render(&Stmt::Select(stmt.clone()))
}

/// Render just an expression
pub fn render_expr(expr: &Expr) -> String {
    let mut renderer = SqlRenderer::new();
    renderer.render_expr(expr);
    renderer.into_sql()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_render_simple_select() {
        let stmt =
            SelectStmt::columns(vec![SelectColumn::star()]).with_from(FromClause::table("users"));

        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("select *"));
        assert!(sql.contains("from \"users\""));
    }

    #[test]
    fn test_render_select_with_where() {
        let stmt = SelectStmt::columns(vec![
            SelectColumn::expr(Expr::qualified_column("t", "id")),
            SelectColumn::expr(Expr::qualified_column("t", "name")),
        ])
        .with_from(FromClause::table("users").with_alias("t"))
        .with_where(Expr::qualified_column("t", "active").eq(Expr::bool(true)));

        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("\"t\".\"id\""));
        assert!(sql.contains("\"t\".\"name\""));
        assert!(sql.contains("where"));
        assert!(sql.contains("\"t\".\"active\" = true"));
    }

    #[test]
    fn test_render_insert() {
        let stmt = InsertStmt::new(
            "users",
            vec![Ident::new("name"), Ident::new("email")],
            InsertValues::Values(vec![vec![
                Expr::string("Alice"),
                Expr::string("alice@example.com"),
            ]]),
        )
        .with_schema("public")
        .with_returning(vec![SelectColumn::expr(Expr::column("id"))]);

        let sql = render(&Stmt::Insert(stmt));
        assert!(sql.contains("insert into \"public\".\"users\""));
        assert!(sql.contains("(\"name\", \"email\")"));
        assert!(sql.contains("values"));
        assert!(sql.contains("returning"));
    }

    #[test]
    fn test_render_update() {
        let stmt = UpdateStmt::new("users", vec![(Ident::new("name"), Expr::string("Bob"))])
            .with_where(Expr::column("id").eq(Expr::int(1)));

        let sql = render(&Stmt::Update(stmt));
        assert!(sql.contains("update \"users\""));
        assert!(sql.contains("set \"name\" ="));
        assert!(sql.contains("where"));
    }

    #[test]
    fn test_render_delete() {
        let stmt = DeleteStmt::new("users").with_where(Expr::column("id").eq(Expr::int(1)));

        let sql = render(&Stmt::Delete(stmt));
        assert!(sql.contains("delete from \"users\""));
        assert!(sql.contains("where"));
    }

    #[test]
    fn test_render_cte() {
        let inner_select = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_where(Expr::column("active").eq(Expr::bool(true)));

        let outer_select = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("active_users"))
            .with_ctes(vec![Cte::select("active_users", inner_select)]);

        let sql = render(&Stmt::Select(outer_select));
        assert!(sql.contains("with \"active_users\" as"));
        assert!(sql.contains("from \"active_users\""));
    }

    #[test]
    fn test_render_param() {
        let param_ref = ParamRef {
            index: 1,
            type_cast: SqlType::text(),
        };
        let expr = Expr::Param(param_ref);
        let sql = render_expr(&expr);
        assert_eq!(sql, "($1::text)");
    }

    #[test]
    fn test_render_jsonb_build_object() {
        let expr = Expr::jsonb_build_object(vec![
            (Expr::string("name"), Expr::string("Alice")),
            (Expr::string("age"), Expr::int(30)),
        ]);
        let sql = render_expr(&expr);
        assert!(sql.contains("jsonb_build_object"));
        assert!(sql.contains("'name'"));
        assert!(sql.contains("'Alice'"));
    }

    #[test]
    fn test_render_aggregate() {
        let agg = AggregateExpr::new(AggregateFunction::Count, vec![]);
        let expr = Expr::Aggregate(agg);
        let sql = render_expr(&expr);
        assert_eq!(sql, "count(*)");
    }

    #[test]
    fn test_render_case() {
        let case = CaseExpr::searched(
            vec![
                (
                    Expr::column("status").eq(Expr::string("active")),
                    Expr::int(1),
                ),
                (
                    Expr::column("status").eq(Expr::string("inactive")),
                    Expr::int(0),
                ),
            ],
            Some(Expr::int(-1)),
        );
        let expr = Expr::Case(case);
        let sql = render_expr(&expr);
        assert!(sql.contains("case"));
        assert!(sql.contains("when"));
        assert!(sql.contains("then"));
        assert!(sql.contains("else"));
        assert!(sql.contains("end"));
    }

    #[test]
    fn test_ident_quoting() {
        let ident = Ident::new("user\"name");
        let mut renderer = SqlRenderer::new();
        renderer.write_ident(&ident);
        let sql = renderer.into_sql();
        assert_eq!(sql, "\"user\"\"name\"");
    }

    #[test]
    fn test_string_literal_with_quotes() {
        let mut renderer = SqlRenderer::new();
        renderer.write_literal("it's a test");
        let sql = renderer.into_sql();
        assert!(sql.contains("$__$"));
    }
}
