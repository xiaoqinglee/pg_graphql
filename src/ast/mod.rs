//! SQL Abstract Syntax Tree (AST) module
//!
//! This module provides a type-safe representation of SQL statements that can be
//! constructed programmatically and rendered to SQL strings. It is intentionally
//! decoupled from pgrx to allow for:
//!
//! - Independent testing without PostgreSQL
//! - Potential reuse in other contexts
//! - Clear separation of concerns
//!
//! # Architecture
//!
//! The AST is built from several components:
//!
//! - [`expr`]: SQL expressions (columns, literals, operators, functions)
//! - [`stmt`]: SQL statements (SELECT, INSERT, UPDATE, DELETE)
//! - [`cte`]: Common Table Expressions (WITH clauses)
//! - [`types`]: SQL type representations
//! - [`params`]: Parameter handling for prepared statements
//! - [`render`]: SQL string generation
//!
//! # Example
//!
//! ```rust,ignore
//! use pg_graphql::ast::*;
//!
//! let mut params = ParamCollector::new();
//! let stmt = SelectStmt {
//!     columns: vec![SelectColumn::star()],
//!     from: Some(FromClause::table("public", "users", "u")),
//!     where_clause: Some(Expr::binary(
//!         Expr::column("u", "id"),
//!         BinaryOperator::Eq,
//!         params.add(ParamValue::Integer(1), SqlType::integer()),
//!     )),
//!     ..Default::default()
//! };
//!
//! let sql = render(&Stmt::Select(stmt));
//! // SELECT * FROM "public"."users" "u" WHERE "u"."id" = ($1::integer)
//! ```

mod builder_bridge;
mod cte;
mod execute;
mod expr;
mod native_param;
mod params;
mod render;
mod stmt;
mod transpile_connection;
mod transpile_delete;
mod transpile_filter;
mod transpile_function_call;
mod transpile_insert;
mod transpile_node;
mod transpile_update;
mod types;

// Re-export all public types
pub use builder_bridge::*;
pub use cte::*;
pub use execute::*;
pub use expr::*;
pub use params::*;
pub use render::*;
pub use stmt::*;
pub use transpile_connection::*;
pub use transpile_delete::*;
pub use transpile_filter::*;
pub use transpile_function_call::*;
pub use transpile_insert::*;
pub use transpile_node::*;
pub use transpile_update::*;
pub use types::*;

#[cfg(test)]
mod tests;
