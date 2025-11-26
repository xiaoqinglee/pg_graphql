//! AST-based query execution
//!
//! This module provides execution methods for builders using the new AST system.
//! It bridges the gap between the existing builder infrastructure and the new
//! type-safe SQL generation and execution.

use crate::ast::{render, ParamCollector, ParamValue};
use crate::error::{GraphQLError, GraphQLResult};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgBuiltInOids;
use pgrx::spi::{self, Spi, SpiClient};
use pgrx::{IntoDatum, JsonB, PgOid};

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

        // Convert parameters to pgrx format
        let pgrx_params = convert_params_to_pgrx(&params);

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

        // Convert parameters to pgrx format
        let pgrx_params = convert_params_to_pgrx(&params);

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

/// Convert AST parameters to pgrx-compatible DatumWithOid format
fn convert_params_to_pgrx(params: &ParamCollector) -> Vec<DatumWithOid<'static>> {
    params
        .params()
        .iter()
        .map(|param| {
            // Determine OID based on whether it's an array
            let type_oid = if param.sql_type.is_array {
                PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID)
            } else {
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
            };

            // Convert value to text datum (all values passed as TEXT and cast at SQL level)
            let datum = match &param.value {
                ParamValue::Null => None,
                ParamValue::String(s) => s.clone().into_datum(),
                ParamValue::Integer(i) => i.to_string().into_datum(),
                ParamValue::Float(f) => f.to_string().into_datum(),
                ParamValue::Bool(b) => b.to_string().into_datum(),
                ParamValue::Json(j) => j.to_string().into_datum(),
                ParamValue::Array(arr) => {
                    // Convert array elements to Vec<Option<String>> and use pgrx's into_datum
                    // This properly handles PostgreSQL array formatting (including quoting)
                    let elements: Vec<Option<String>> =
                        arr.iter().map(|v| param_value_to_string(v)).collect();
                    elements.into_datum()
                }
            };

            // Create DatumWithOid - this requires unsafe since we're managing the datum lifetime
            match datum {
                Some(d) => unsafe { DatumWithOid::new(d, type_oid.value()) },
                None => DatumWithOid::null_oid(type_oid.value()), // Proper NULL representation
            }
        })
        .collect()
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
        ParamValue::Array(_) => {
            // Nested arrays are not supported, return stringified form
            value.to_sql_literal()
        }
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
