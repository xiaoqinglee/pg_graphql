//! AST-based query execution
//!
//! This module provides execution methods for builders using the new AST system.
//! It bridges the gap between the existing builder infrastructure and the new
//! type-safe SQL generation and execution.
//!
//! # Parameter Binding
//!
//! Parameters are bound using native PostgreSQL types where safe, falling back
//! to text representation for custom types. See [`super::native_param`] for details.

use super::native_param::params_to_datums;
use crate::ast::{render, ParamCollector};
use crate::error::{GraphQLError, GraphQLResult};
use pgrx::spi::{self, Spi, SpiClient};
use pgrx::JsonB;

/// Execute a query builder using the AST path
///
/// This trait can be implemented alongside existing execute methods
/// to allow gradual migration to the new AST system.
pub trait AstExecutable {
    type Ast;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast>;

    fn execute_via_ast(&self) -> GraphQLResult<serde_json::Value>
    where
        Self::Ast: AsStatement,
    {
        let mut params = ParamCollector::new();
        let ast = self.to_ast(&mut params)?;

        // Render the AST to SQL
        let sql = ast.render_sql();

        // Convert parameters to pgrx format using native binding where safe
        let pgrx_params = params_to_datums(&params);

        // Execute via SPI
        let spi_result: Result<Option<JsonB>, spi::Error> = Spi::connect(|c| {
            let val = c.select(&sql, Some(1), &pgrx_params)?;
            if val.is_empty() {
                Ok(None)
            } else {
                val.first().get::<JsonB>(1)
            }
        });

        match spi_result {
            Ok(Some(jsonb)) => Ok(jsonb.0),
            Ok(None) => Ok(serde_json::Value::Null),
            Err(e) => Err(GraphQLError::internal(format!("{}", e))),
        }
    }

    fn execute_mutation_via_ast<'conn, 'c>(
        &self,
        conn: &'c mut SpiClient<'conn>,
    ) -> GraphQLResult<(serde_json::Value, &'c mut SpiClient<'conn>)>
    where
        Self::Ast: AsStatement,
    {
        let mut params = ParamCollector::new();
        let ast = self.to_ast(&mut params)?;

        // Render the AST to SQL
        let sql = ast.render_sql();

        // Convert parameters to pgrx format using native binding where safe
        let pgrx_params = params_to_datums(&params);

        // Execute via SPI update (for mutations)
        let res_q = conn.update(&sql, None, &pgrx_params).map_err(|_| {
            GraphQLError::sql_execution("Internal Error: Failed to execute AST-generated mutation")
        })?;

        let res: JsonB = match res_q.first().get::<JsonB>(1) {
            Ok(Some(dat)) => dat,
            Ok(None) => JsonB(serde_json::Value::Null),
            Err(e) => {
                return Err(GraphQLError::sql_generation(format!(
                    "Internal Error: Failed to load result from AST query: {e}"
                )));
            }
        };

        Ok((res.0, conn))
    }
}

/// Trait for types that can be rendered as SQL statements
pub trait AsStatement {
    fn render_sql(&self) -> String;
}

// Implement AsStatement for our AST result types
impl AsStatement for crate::ast::InsertAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

impl AsStatement for crate::ast::UpdateAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

impl AsStatement for crate::ast::DeleteAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

impl AsStatement for crate::ast::NodeAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

impl AsStatement for crate::ast::ConnectionAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

impl AsStatement for crate::ast::FunctionCallAst {
    fn render_sql(&self) -> String {
        render(&self.stmt)
    }
}

// Implement AstExecutable for builders
// These connect the ToAst implementations from transpile_*.rs to the execution trait

use crate::ast::ToAst;
use crate::builder::{
    ConnectionBuilder, DeleteBuilder, FunctionCallBuilder, InsertBuilder, NodeBuilder,
    UpdateBuilder,
};

impl AstExecutable for InsertBuilder {
    type Ast = crate::ast::InsertAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

impl AstExecutable for UpdateBuilder {
    type Ast = crate::ast::UpdateAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

impl AstExecutable for DeleteBuilder {
    type Ast = crate::ast::DeleteAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

impl AstExecutable for NodeBuilder {
    type Ast = crate::ast::NodeAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

impl AstExecutable for ConnectionBuilder {
    type Ast = crate::ast::ConnectionAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

impl AstExecutable for FunctionCallBuilder {
    type Ast = crate::ast::FunctionCallAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        <Self as ToAst>::to_ast(self, params)
    }
}

#[cfg(test)]
mod tests {
    // Tests that don't require pgrx runtime can go here
    // Full integration tests require pg_regress
}
