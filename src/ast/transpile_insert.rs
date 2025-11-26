//! AST-based transpilation for InsertBuilder
//!
//! This module implements the ToAst trait for InsertBuilder, converting it
//! to a type-safe AST that can be rendered to SQL.

use super::{
    add_param_from_json, apply_type_cast, build_connection_subquery, build_relation_subquery_expr,
    coalesce, column_ref, count_star, default_expr, empty_jsonb_array, jsonb_agg,
    jsonb_build_object, string_literal, type_name_to_sql_type, AstBuildContext, ToAst,
};
use crate::ast::{
    Cte, CteQuery, Expr, FromClause, Ident, InsertStmt, InsertValues, ParamCollector, SelectColumn,
    SelectStmt, Stmt,
};
use crate::builder::{
    FunctionBuilder, FunctionSelection, InsertBuilder, InsertElemValue, InsertRowBuilder,
    InsertSelection,
};
use crate::error::GraphQLResult;
use crate::sql_types::Column;
use std::collections::HashSet;
use std::sync::Arc;

/// The result of transpiling an InsertBuilder to AST
pub struct InsertAst {
    /// The complete SQL statement
    pub stmt: Stmt,
}

impl ToAst for InsertBuilder {
    type Ast = InsertAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        let ctx = AstBuildContext::new();
        let block_name = &ctx.block_name;

        // Build the select columns for the outer query
        let select_columns = build_select_columns(&self.selections, block_name, params)?;

        // Identify all columns provided in any of `object` rows
        let referenced_column_names: HashSet<&String> =
            self.objects.iter().flat_map(|x| x.row.keys()).collect();

        let referenced_columns: Vec<&Arc<Column>> = self
            .table
            .columns
            .iter()
            .filter(|c| referenced_column_names.contains(&c.name))
            .collect();

        // Build column names for the INSERT
        let column_names: Vec<Ident> = referenced_columns
            .iter()
            .map(|c| Ident::new(c.name.clone()))
            .collect();

        // Build VALUES rows
        let mut values_rows = Vec::with_capacity(self.objects.len());
        for row_builder in &self.objects {
            let row = build_insert_row(row_builder, &referenced_columns, params)?;
            values_rows.push(row);
        }

        // Build the RETURNING clause - all selectable columns as SelectColumn
        let returning: Vec<SelectColumn> = self
            .table
            .columns
            .iter()
            .filter(|c| c.permissions.is_selectable)
            .map(|c| SelectColumn::expr(Expr::Column(super::ColumnRef::new(c.name.clone()))))
            .collect();

        // Build the INSERT statement
        let insert_stmt = InsertStmt {
            ctes: vec![],
            schema: Some(Ident::new(self.table.schema.clone())),
            table: Ident::new(self.table.name.clone()),
            columns: column_names,
            values: InsertValues::Values(values_rows),
            on_conflict: None,
            returning,
        };

        // Wrap in CTE: WITH affected AS (INSERT ...) SELECT jsonb_build_object(...) FROM affected
        let cte = Cte {
            name: Ident::new("affected"),
            columns: None,
            query: CteQuery::Insert(insert_stmt),
            materialized: None,
        };

        // Build the outer SELECT
        let outer_select = SelectStmt {
            ctes: vec![cte],
            columns: vec![SelectColumn::Expr {
                expr: Expr::JsonBuild(super::JsonBuildExpr::Object(select_columns)),
                alias: None,
            }],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("affected"),
                alias: Some(Ident::new(block_name.clone())),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        Ok(InsertAst {
            stmt: Stmt::Select(outer_select),
        })
    }
}

/// Build the select columns for jsonb_build_object from InsertSelection
fn build_select_columns(
    selections: &[InsertSelection],
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<(Expr, Expr)>> {
    let mut pairs = Vec::new();

    for selection in selections {
        match selection {
            InsertSelection::AffectedCount { alias } => {
                pairs.push((string_literal(alias), count_star()));
            }
            InsertSelection::Records(node_builder) => {
                // For now, use a simplified approach - we'll fully implement NodeBuilder later
                // This creates: coalesce(jsonb_agg(jsonb_build_object(...)), '[]')
                let node_expr = build_node_builder_expr(node_builder, block_name, params)?;
                pairs.push((
                    string_literal(&node_builder.alias),
                    coalesce(vec![jsonb_agg(node_expr), empty_jsonb_array()]),
                ));
            }
            InsertSelection::Typename { alias, typename } => {
                pairs.push((string_literal(alias), string_literal(typename)));
            }
        }
    }

    Ok(pairs)
}

/// Build expression for a NodeBuilder (simplified for now)
fn build_node_builder_expr(
    node_builder: &crate::builder::NodeBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    use crate::builder::NodeSelection;

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
                // NodeId is base64 encoded JSON array of [schema, table, pk_values...]
                let pk_exprs: Vec<Expr> = node_id_builder
                    .columns
                    .iter()
                    .map(|c| super::func_call("to_jsonb", vec![column_ref(block_name, &c.name)]))
                    .collect();

                // Build: translate(encode(convert_to(jsonb_build_array(schema, table, pk_vals...)::text, 'utf-8'), 'base64'), E'\n', '')
                let mut array_args = vec![
                    string_literal(&node_id_builder.schema_name),
                    string_literal(&node_id_builder.table_name),
                ];
                array_args.extend(pk_exprs);

                let jsonb_array = super::func_call("jsonb_build_array", array_args);
                let as_text = Expr::Cast {
                    expr: Box::new(jsonb_array),
                    target_type: type_name_to_sql_type("text"),
                };
                let converted =
                    super::func_call("convert_to", vec![as_text, string_literal("utf-8")]);
                let encoded = super::func_call("encode", vec![converted, string_literal("base64")]);
                let translated = super::func_call(
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

/// Build a single row of values for INSERT
fn build_insert_row(
    row_builder: &InsertRowBuilder,
    columns: &[&Arc<Column>],
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<Expr>> {
    let mut row = Vec::with_capacity(columns.len());

    for column in columns {
        let expr = match row_builder.row.get(&column.name) {
            None => default_expr(),
            Some(elem) => match elem {
                InsertElemValue::Default => default_expr(),
                InsertElemValue::Value(val) => add_param_from_json(params, val, &column.type_name)?,
            },
        };
        row.push(expr);
    }

    Ok(row)
}

/// Build a function expression for scalar function calls
fn build_function_expr(func_builder: &FunctionBuilder, block_name: &str) -> GraphQLResult<Expr> {
    use crate::ast::{ColumnRef, FunctionArg, FunctionCall, SqlType};

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

    // Note: Full testing requires setting up Table, Column, etc. which is complex.
    // Integration tests via pg_regress are more appropriate for full coverage.

    #[test]
    fn test_build_insert_row_default() {
        // Test that default values are handled correctly
        let row_builder = InsertRowBuilder {
            row: std::collections::HashMap::new(),
        };

        // We can't easily test without Column instances, but we can verify
        // the function compiles and handles empty cases
        let columns: Vec<&Arc<Column>> = vec![];
        let mut params = ParamCollector::new();

        let result = build_insert_row(&row_builder, &columns, &mut params);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
