//! AST-based transpilation for FunctionCallBuilder
//!
//! This module implements the ToAst trait for FunctionCallBuilder, converting it
//! to a type-safe AST that can be rendered to SQL. FunctionCallBuilder handles
//! top-level function calls in queries and mutations.

use super::{
    add_param_from_json, apply_type_cast, build_function_connection_subquery_full,
    build_node_object_expr, coalesce, AstBuildContext, ToAst,
};
use crate::ast::{
    Expr, FromClause, FunctionArg, FunctionCall, Ident, Literal, ParamCollector, SelectColumn,
    SelectStmt, Stmt,
};
use crate::builder::{FuncCallReturnTypeBuilder, FunctionCallBuilder};
use crate::error::GraphQLResult;

/// The result of transpiling a FunctionCallBuilder to AST
pub struct FunctionCallAst {
    /// The complete SQL statement
    pub stmt: Stmt,
}

impl ToAst for FunctionCallBuilder {
    type Ast = FunctionCallAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        let ctx = AstBuildContext::new();
        let block_name = ctx.block_name.clone();

        // Build the function call arguments
        let args = build_function_args(&self.args_builder, params)?;

        // Build the function call expression
        let func_call = FunctionCall::with_schema(
            self.function.schema_name.clone(),
            self.function.name.clone(),
            args,
        );

        // Build the query based on return type
        let select_expr = match &self.return_type_builder {
            FuncCallReturnTypeBuilder::Scalar | FuncCallReturnTypeBuilder::List => {
                // SELECT to_jsonb(schema.func(args)::type_adjustment)
                let func_expr = Expr::FunctionCall(func_call);
                let adjusted_expr = apply_type_cast(func_expr, self.function.type_oid);
                func_call_expr("to_jsonb", vec![adjusted_expr])
            }
            FuncCallReturnTypeBuilder::Node(node_builder) => {
                // SELECT coalesce((SELECT node_object FROM schema.func(args) AS block WHERE NOT (block IS NULL)), null::jsonb)
                let object_expr =
                    build_node_object_expr(&node_builder.selections, &block_name, params)?;

                // Handle empty selections
                let object_expr = if node_builder.selections.is_empty() {
                    func_call_expr("jsonb_build_object", vec![])
                } else {
                    object_expr
                };

                // Build: NOT (block_name IS NULL)
                let not_null_check = Expr::UnaryOp {
                    op: super::UnaryOperator::Not,
                    expr: Box::new(Expr::IsNull {
                        expr: Box::new(Expr::Column(super::ColumnRef::new(block_name.as_str()))),
                        negated: false,
                    }),
                };

                // Build the inner subquery
                let inner_subquery = SelectStmt {
                    ctes: vec![],
                    columns: vec![SelectColumn::expr(object_expr)],
                    from: Some(FromClause::Function {
                        call: func_call,
                        alias: Ident::new(&block_name),
                    }),
                    where_clause: Some(not_null_check),
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                };

                // Wrap in coalesce with null::jsonb fallback
                coalesce(vec![
                    Expr::Subquery(Box::new(inner_subquery)),
                    Expr::Cast {
                        expr: Box::new(Expr::Literal(Literal::Null)),
                        target_type: super::type_name_to_sql_type("jsonb"),
                    },
                ])
            }
            FuncCallReturnTypeBuilder::Connection(conn_builder) => {
                // Build a connection query using the function as the FROM source
                // This returns an Expr::Subquery containing the full connection query
                build_function_connection_subquery_full(conn_builder, func_call, params)?
            }
        };

        // For Connection type, the select_expr is already a subquery that returns the result
        // For other types, we need to wrap in SELECT

        // Build the final SELECT statement
        let select = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(select_expr)],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        Ok(FunctionCallAst {
            stmt: Stmt::Select(select),
        })
    }
}

/// Build function call arguments from FuncCallArgsBuilder
fn build_function_args(
    args_builder: &crate::builder::FuncCallArgsBuilder,
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<FunctionArg>> {
    let mut args = Vec::new();

    for (arg_meta, arg_value) in &args_builder.args {
        if let Some(arg) = arg_meta {
            // Build the parameter expression with type cast
            let param_expr = add_param_from_json(params, arg_value, &arg.type_name)?;

            // Create a named argument: name => value
            args.push(FunctionArg::named(arg.name.as_str(), param_expr));
        }
    }

    Ok(args)
}

/// Helper to create a function call expression
fn func_call_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall(FunctionCall::new(
        name,
        args.into_iter().map(FunctionArg::unnamed).collect(),
    ))
}

#[cfg(test)]
mod tests {
    // Integration tests via pg_regress are more appropriate for this module
    // since it requires a full GraphQL schema and function setup.
}
