//! AST-based transpilation for UpdateBuilder
//!
//! This module implements the ToAst trait for UpdateBuilder, converting it
//! to a type-safe AST that can be rendered to SQL.

use super::{
    add_param_from_json, build_connection_subquery, build_filter_expr,
    build_relation_subquery_expr, coalesce, column_ref, count_star, empty_jsonb_array, func_call,
    func_call_schema, jsonb_agg, jsonb_build_object, string_literal, AstBuildContext, ToAst,
};
use crate::ast::{
    BinaryOperator, CaseExpr, ColumnRef, Cte, CteQuery, Expr, FromClause, Ident, Literal,
    ParamCollector, SelectColumn, SelectStmt, Stmt, UpdateStmt,
};
use crate::builder::{
    FunctionBuilder, FunctionSelection, NodeBuilder, NodeSelection, UpdateBuilder, UpdateSelection,
};
use crate::error::GraphQLResult;

/// The result of transpiling an UpdateBuilder to AST
pub struct UpdateAst {
    /// The complete SQL statement
    pub stmt: Stmt,
}

impl ToAst for UpdateBuilder {
    type Ast = UpdateAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        let ctx = AstBuildContext::new();
        let block_name = ctx.block_name.clone();

        // Build SET clause
        let set_clauses = build_set_clauses(&self.set.set, &self.table, params)?;

        // Build WHERE clause from filter
        let where_clause = build_filter_expr(&self.filter, &self.table, &block_name, params)?;

        // Build RETURNING clause - all selectable columns
        let returning: Vec<SelectColumn> = self
            .table
            .columns
            .iter()
            .filter(|c| c.permissions.is_selectable)
            .map(|c| SelectColumn::expr(Expr::Column(ColumnRef::new(c.name.clone()))))
            .collect();

        // Build the UPDATE statement for the 'impacted' CTE
        let update_stmt = UpdateStmt {
            ctes: vec![],
            schema: Some(Ident::new(self.table.schema.clone())),
            table: Ident::new(self.table.name.clone()),
            alias: Some(Ident::new(block_name.clone())),
            set: set_clauses,
            from: None,
            where_clause,
            returning,
        };

        // Build the select columns for jsonb_build_object
        let select_columns = build_select_columns(&self.selections, &block_name, params)?;

        // Build the complex CTE structure:
        // WITH impacted AS (UPDATE ...), total(total_count) AS (...), req(res) AS (...), wrapper(res) AS (...)
        let stmt =
            build_update_with_at_most(update_stmt, select_columns, &block_name, self.at_most);

        Ok(UpdateAst { stmt })
    }
}

/// Build SET clauses from the SetBuilder
fn build_set_clauses(
    set_map: &std::collections::HashMap<String, serde_json::Value>,
    table: &crate::sql_types::Table,
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<(Ident, Expr)>> {
    let mut clauses = Vec::new();

    for (column_name, value) in set_map {
        let column = table
            .columns
            .iter()
            .find(|c| &c.name == column_name)
            .expect("Failed to find column in update builder");

        let value_expr = add_param_from_json(params, value, &column.type_name)?;

        clauses.push((Ident::new(column_name.clone()), value_expr));
    }

    Ok(clauses)
}

/// Build the select columns for jsonb_build_object from UpdateSelection
fn build_select_columns(
    selections: &[UpdateSelection],
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<(Expr, Expr)>> {
    let mut pairs = Vec::new();

    for selection in selections {
        match selection {
            UpdateSelection::AffectedCount { alias } => {
                pairs.push((string_literal(alias), count_star()));
            }
            UpdateSelection::Records(node_builder) => {
                let node_expr = build_node_builder_expr(node_builder, block_name, params)?;
                pairs.push((
                    string_literal(&node_builder.alias),
                    coalesce(vec![jsonb_agg(node_expr), empty_jsonb_array()]),
                ));
            }
            UpdateSelection::Typename { alias, typename } => {
                pairs.push((string_literal(alias), string_literal(typename)));
            }
        }
    }

    Ok(pairs)
}

/// Build expression for a NodeBuilder (simplified)
fn build_node_builder_expr(
    node_builder: &NodeBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let mut pairs = Vec::new();

    for selection in &node_builder.selections {
        match selection {
            NodeSelection::Column(col_builder) => {
                // Use build_column_expr which handles enum mappings
                let col_expr = super::build_column_expr(col_builder, block_name);
                pairs.push((col_builder.alias.clone(), col_expr));
            }
            NodeSelection::Typename { alias, typename } => {
                pairs.push((alias.clone(), string_literal(typename)));
            }
            NodeSelection::NodeId(node_id_builder) => {
                let pk_exprs: Vec<Expr> = node_id_builder
                    .columns
                    .iter()
                    .map(|c| func_call("to_jsonb", vec![column_ref(block_name, &c.name)]))
                    .collect();

                let mut array_args = vec![
                    string_literal(&node_id_builder.schema_name),
                    string_literal(&node_id_builder.table_name),
                ];
                array_args.extend(pk_exprs);

                let jsonb_array = func_call("jsonb_build_array", array_args);
                let as_text = Expr::Cast {
                    expr: Box::new(jsonb_array),
                    target_type: super::type_name_to_sql_type("text"),
                };
                let converted = func_call("convert_to", vec![as_text, string_literal("utf-8")]);
                let encoded = func_call("encode", vec![converted, string_literal("base64")]);
                let translated = func_call(
                    "translate",
                    vec![encoded, string_literal("\n"), string_literal("")],
                );

                pairs.push((node_id_builder.alias.clone(), translated));
            }
            NodeSelection::Function(func_builder) => {
                // Build function expression: schema.function(row::schema.table)
                let func_expr = build_function_expr(func_builder, block_name)?;
                pairs.push((func_builder.alias.clone(), func_expr));
            }
            NodeSelection::Connection(conn_builder) => {
                // Connection selections - build subquery
                let conn_expr = build_connection_subquery(conn_builder, block_name, params)?;
                pairs.push((conn_builder.alias.clone(), conn_expr));
            }
            NodeSelection::Node(nested_node) => {
                // Nested node selections - build subquery
                let node_expr = build_relation_subquery_expr(nested_node, block_name, params)?;
                pairs.push((nested_node.alias.clone(), node_expr));
            }
        }
    }

    Ok(jsonb_build_object(pairs))
}

/// Build the full UPDATE with at_most check using CTEs
fn build_update_with_at_most(
    update_stmt: UpdateStmt,
    select_columns: Vec<(Expr, Expr)>,
    block_name: &str,
    at_most: i64,
) -> Stmt {
    // CTE 1: impacted AS (UPDATE ...)
    let impacted_cte = Cte {
        name: Ident::new("impacted"),
        columns: None,
        query: CteQuery::Update(update_stmt),
        materialized: None,
    };

    // CTE 2: total(total_count) AS (SELECT count(*) FROM impacted)
    let total_cte = Cte {
        name: Ident::new("total"),
        columns: Some(vec![Ident::new("total_count")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(count_star())],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("impacted"),
                alias: None,
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    };

    // CTE 3: req(res) AS (SELECT jsonb_build_object(...) FROM impacted LIMIT 1)
    let req_cte = Cte {
        name: Ident::new("req"),
        columns: Some(vec![Ident::new("res")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(Expr::JsonBuild(
                super::JsonBuildExpr::Object(select_columns),
            ))],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("impacted"),
                alias: Some(Ident::new(block_name)),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: Some(1),
            offset: None,
        }),
        materialized: None,
    };

    // CTE 4: wrapper(res) AS (SELECT CASE WHEN total.total_count > at_most THEN exception ELSE req.res END ...)
    let case_expr = Expr::Case(CaseExpr::searched(
        vec![(
            // WHEN total.total_count > at_most
            Expr::BinaryOp {
                left: Box::new(column_ref("total", "total_count")),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(at_most))),
            },
            // THEN graphql.exception(...)::jsonb
            Expr::Cast {
                expr: Box::new(func_call_schema(
                    "graphql",
                    "exception",
                    vec![string_literal("update impacts too many records")],
                )),
                target_type: super::type_name_to_sql_type("jsonb"),
            },
        )],
        // ELSE req.res
        Some(column_ref("req", "res")),
    ));

    let wrapper_cte = Cte {
        name: Ident::new("wrapper"),
        columns: Some(vec![Ident::new("res")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(case_expr)],
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("total"),
                    alias: None,
                }),
                join_type: super::JoinType::Left,
                right: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("req"),
                    alias: None,
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: Some(1),
            offset: None,
        }),
        materialized: None,
    };

    // Final SELECT from wrapper
    Stmt::Select(SelectStmt {
        ctes: vec![impacted_cte, total_cte, req_cte, wrapper_cte],
        columns: vec![SelectColumn::expr(column_ref("wrapper", "res"))],
        from: Some(FromClause::Table {
            schema: None,
            name: Ident::new("wrapper"),
            alias: None,
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    })
}

/// Build a function expression for scalar function calls
fn build_function_expr(func_builder: &FunctionBuilder, block_name: &str) -> GraphQLResult<Expr> {
    use super::apply_type_cast;
    use crate::ast::{FunctionArg, FunctionCall, SqlType};

    // Build the row argument with type cast: block_name::schema.table
    let row_arg = Expr::Cast {
        expr: Box::new(Expr::Column(ColumnRef::new(block_name))),
        target_type: SqlType::with_schema(
            func_builder.table.schema.clone(),
            func_builder.table.name.clone(),
        ),
    };

    let args = vec![FunctionArg::unnamed(row_arg)];

    let func_expr = Expr::FunctionCall(FunctionCall::with_schema(
        func_builder.function.schema_name.clone(),
        func_builder.function.name.clone(),
        args,
    ));

    // Apply type cast for special types
    match &func_builder.selection {
        FunctionSelection::ScalarSelf | FunctionSelection::Array => {
            Ok(apply_type_cast(func_expr, func_builder.function.type_oid))
        }
        FunctionSelection::Connection(_) | FunctionSelection::Node(_) => {
            // These are complex selections - return raw function call
            Ok(func_expr)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_clause_structure() {
        // Basic structure test - full testing via pg_regress
        // SET clauses are represented as tuples (Ident, Expr)
        let clause: (Ident, Expr) = (
            Ident::new("name"),
            Expr::Literal(Literal::String("test".to_string())),
        );
        assert_eq!(clause.0 .0, "name");
    }
}
