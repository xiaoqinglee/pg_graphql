//! AST-based transpilation for NodeBuilder
//!
//! This module implements the ToAst trait for NodeBuilder, converting it
//! to a type-safe AST that can be rendered to SQL.

use super::{
    add_param_from_json, column_ref, func_call, jsonb_build_object, string_literal,
    AstBuildContext, ToAst,
};
use crate::ast::{
    BinaryOperator, Expr, FromClause, FunctionArg, Ident, ParamCollector, SelectColumn, SelectStmt,
    Stmt,
};
use crate::builder::{
    ColumnBuilder, FunctionSelection, NodeBuilder, NodeIdBuilder, NodeIdInstance, NodeSelection,
};
use crate::error::{GraphQLError, GraphQLResult};
use crate::sql_types::{Table, TypeDetails};

/// The result of transpiling a NodeBuilder to AST for entrypoint queries
pub struct NodeAst {
    /// The complete SQL statement
    pub stmt: Stmt,
}

/// The result of transpiling a NodeBuilder to an expression (for nested selections)
pub struct NodeExprAst {
    /// The expression representing this node
    pub expr: Expr,
}

impl ToAst for NodeBuilder {
    type Ast = NodeAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        let ctx = AstBuildContext::new();
        let block_name = ctx.block_name.clone();

        // Build the object clause from selections
        let object_expr = build_node_object_expr(&self.selections, &block_name, params)?;

        // Build WHERE clause from node_id
        let node_id = self
            .node_id
            .as_ref()
            .ok_or("Expected nodeId argument missing")?;

        let where_clause = build_node_id_filter(node_id, &self.table, &block_name, params)?;

        // Build the SELECT statement
        let select = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(object_expr)],
            from: Some(FromClause::Table {
                schema: Some(Ident::new(self.table.schema.clone())),
                name: Ident::new(self.table.name.clone()),
                alias: Some(Ident::new(block_name)),
            }),
            where_clause: Some(where_clause),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        // Wrap in a subquery expression
        Ok(NodeAst {
            stmt: Stmt::Select(select),
        })
    }
}

/// Build the node_id filter as a WHERE clause expression
fn build_node_id_filter(
    node_id: &NodeIdInstance,
    table: &Table,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    // Validate that nodeId belongs to this table
    if (&node_id.schema_name, &node_id.table_name) != (&table.schema, &table.name) {
        return Err(GraphQLError::validation(
            "nodeId belongs to a different collection",
        ));
    }

    let pk_columns = table.primary_key_columns();
    let mut conditions = Vec::new();

    for (col, val) in pk_columns.iter().zip(node_id.values.iter()) {
        let col_expr = column_ref(block_name, &col.name);
        let val_expr = add_param_from_json(params, val, &col.type_name)?;

        conditions.push(Expr::BinaryOp {
            left: Box::new(col_expr),
            op: BinaryOperator::Eq,
            right: Box::new(val_expr),
        });
    }

    // Combine with AND
    if conditions.len() == 1 {
        Ok(conditions.remove(0))
    } else {
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(cond),
            };
        }
        Ok(combined)
    }
}

/// Build expression for a NodeBuilder's selections (jsonb_build_object)
pub fn build_node_object_expr(
    selections: &[NodeSelection],
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let mut all_pairs: Vec<(String, Expr)> = Vec::new();

    for selection in selections {
        match selection {
            NodeSelection::Column(col_builder) => {
                let col_expr = build_column_expr(col_builder, block_name);
                all_pairs.push((col_builder.alias.clone(), col_expr));
            }
            NodeSelection::Typename { alias, typename } => {
                all_pairs.push((alias.clone(), string_literal(typename)));
            }
            NodeSelection::NodeId(node_id_builder) => {
                let node_id_expr = build_node_id_expr(node_id_builder, block_name);
                all_pairs.push((node_id_builder.alias.clone(), node_id_expr));
            }
            NodeSelection::Function(func_builder) => {
                let func_expr = build_function_expr(func_builder, block_name, params)?;
                all_pairs.push((func_builder.alias.clone(), func_expr));
            }
            NodeSelection::Connection(conn_builder) => {
                // Connection needs a subquery - will be implemented in transpile_connection
                let conn_expr = build_connection_subquery_expr(conn_builder, block_name, params)?;
                all_pairs.push((conn_builder.alias.clone(), conn_expr));
            }
            NodeSelection::Node(nested_node) => {
                // Nested node relation - build as subquery
                let node_expr = build_relation_subquery_expr(nested_node, block_name, params)?;
                all_pairs.push((nested_node.alias.clone(), node_expr));
            }
        }
    }

    // jsonb_build_object has a limit of 100 arguments (50 pairs)
    // If we have more, we need to chunk and concatenate with ||
    const MAX_PAIRS_PER_CALL: usize = 50;

    if all_pairs.len() <= MAX_PAIRS_PER_CALL {
        Ok(jsonb_build_object(all_pairs))
    } else {
        // Chunk into multiple jsonb_build_object calls and concatenate
        let mut chunks: Vec<Expr> = all_pairs
            .chunks(MAX_PAIRS_PER_CALL)
            .map(|chunk| jsonb_build_object(chunk.to_vec()))
            .collect();

        // Concatenate with || operator (JsonConcat for JSONB)
        let mut result = chunks.remove(0);
        for chunk in chunks {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: BinaryOperator::JsonConcat,
                right: Box::new(chunk),
            };
        }
        Ok(result)
    }
}

/// Build expression for a column selection
///
/// This handles enum mappings by generating CASE expressions when the column
/// has an enum type with custom mappings defined.
pub fn build_column_expr(col_builder: &ColumnBuilder, block_name: &str) -> Expr {
    let col_ref = column_ref(block_name, &col_builder.column.name);

    // Check if this is an enum column with mappings
    let maybe_enum = col_builder
        .column
        .type_
        .as_ref()
        .and_then(|t| match &t.details {
            Some(TypeDetails::Enum(enum_)) => Some(enum_),
            _ => None,
        });

    if let Some(enum_) = maybe_enum {
        if let Some(ref mappings) = enum_.directives.mappings {
            // Build CASE expression for enum mappings
            // case when col = 'pg_value1' then 'graphql_value1' when col = 'pg_value2' then 'graphql_value2' else col::text end
            let when_clauses: Vec<(Expr, Expr)> = mappings
                .iter()
                .map(|(pg_val, graphql_val)| {
                    (
                        Expr::BinaryOp {
                            left: Box::new(col_ref.clone()),
                            op: BinaryOperator::Eq,
                            right: Box::new(string_literal(pg_val)),
                        },
                        string_literal(graphql_val),
                    )
                })
                .collect();

            let else_clause = Expr::Cast {
                expr: Box::new(col_ref),
                target_type: super::type_name_to_sql_type("text"),
            };

            return Expr::Case(super::CaseExpr::searched(when_clauses, Some(else_clause)));
        }
    }

    // Apply type adjustment for special OIDs
    apply_type_cast(col_ref, col_builder.column.type_oid)
}

/// Apply suffix casts for types that need special handling
///
/// This handles types that need to be converted for GraphQL output:
/// - bigint (20) -> text (prevents precision loss in JSON)
/// - json/jsonb (114/3802) -> text via #>> '{}'
/// - numeric (1700) -> text (prevents precision loss)
/// - bigint[] (1016) -> text[]
/// - json[]/jsonb[] (199/3807) -> text[]
/// - numeric[] (1231) -> text[]
pub fn apply_type_cast(expr: Expr, type_oid: u32) -> Expr {
    match type_oid {
        20 => Expr::Cast {
            // bigints as text
            expr: Box::new(expr),
            target_type: super::type_name_to_sql_type("text"),
        },
        114 | 3802 => Expr::BinaryOp {
            // json/b as stringified using #>> '{}' (empty path extracts root as text)
            // Use string literal '{}' which PostgreSQL interprets as text[] for the path
            left: Box::new(expr),
            op: BinaryOperator::JsonPathText,
            right: Box::new(string_literal("{}")),
        },
        1700 => Expr::Cast {
            // numeric as text
            expr: Box::new(expr),
            target_type: super::type_name_to_sql_type("text"),
        },
        1016 => Expr::Cast {
            // bigint arrays as array of text
            expr: Box::new(expr),
            target_type: super::type_name_to_sql_type("text[]"),
        },
        199 | 3807 => Expr::Cast {
            // json/b array as array of text
            expr: Box::new(expr),
            target_type: super::type_name_to_sql_type("text[]"),
        },
        1231 => Expr::Cast {
            // numeric array as array of text
            expr: Box::new(expr),
            target_type: super::type_name_to_sql_type("text[]"),
        },
        _ => expr,
    }
}

/// Build expression for nodeId (base64 encoded JSON array)
fn build_node_id_expr(node_id_builder: &NodeIdBuilder, block_name: &str) -> Expr {
    // Build: translate(encode(convert_to(jsonb_build_array(schema, table, pk_vals...)::text, 'utf-8'), 'base64'), E'\n', '')
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

    func_call(
        "translate",
        vec![encoded, string_literal("\n"), string_literal("")],
    )
}

/// Build expression for a function call
fn build_function_expr(
    func_builder: &crate::builder::FunctionBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    // The function takes the row as a typed argument: schema.function(block_name::schema.table)
    // Build the row argument with type cast
    let row_arg = Expr::Cast {
        expr: Box::new(Expr::Column(super::ColumnRef::new(block_name))),
        target_type: super::SqlType::with_schema(
            func_builder.table.schema.clone(),
            func_builder.table.name.clone(),
        ),
    };

    let args = vec![FunctionArg::unnamed(row_arg)];

    let func_call = super::FunctionCall::with_schema(
        func_builder.function.schema_name.clone(),
        func_builder.function.name.clone(),
        args,
    );

    match &func_builder.selection {
        FunctionSelection::ScalarSelf | FunctionSelection::Array => {
            // For scalar/array selections, the result is the function call with type cast
            let func_expr = Expr::FunctionCall(func_call);
            Ok(apply_type_cast(func_expr, func_builder.function.type_oid))
        }
        FunctionSelection::Node(node_builder) => {
            // For node selection (function returning a single row), wrap in a subquery:
            // (SELECT node_object FROM schema.func(block_name::schema.table) AS func_block WHERE NOT (func_block IS NULL))
            let func_block_name = AstBuildContext::new().block_name;

            // Build the node object expression
            let object_expr =
                build_node_object_expr(&node_builder.selections, &func_block_name, params)?;

            // Build: NOT (func_block IS NULL)
            let not_null_check = Expr::UnaryOp {
                op: super::UnaryOperator::Not,
                expr: Box::new(Expr::IsNull {
                    expr: Box::new(Expr::Column(super::ColumnRef::new(func_block_name.as_str()))),
                    negated: false,
                }),
            };

            // Build the subquery
            let subquery = SelectStmt {
                ctes: vec![],
                columns: vec![SelectColumn::expr(object_expr)],
                from: Some(FromClause::Function {
                    call: func_call,
                    alias: Ident::new(&func_block_name),
                }),
                where_clause: Some(not_null_check),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            };

            Ok(Expr::Subquery(Box::new(subquery)))
        }
        FunctionSelection::Connection(conn_builder) => {
            // For connection selection (function returning setof), build a connection subquery
            // that uses the function as its FROM clause instead of a table
            build_function_connection_subquery(func_builder, conn_builder, block_name, params)
        }
    }
}

/// Build a connection subquery for a function that returns setof <type>
///
/// This is used when a function returns a set of rows (setof) and we want to
/// expose that as a connection. The key difference from a regular connection
/// is that the FROM clause uses the function call instead of a table.
fn build_function_connection_subquery(
    func_builder: &crate::builder::FunctionBuilder,
    conn_builder: &crate::builder::ConnectionBuilder,
    parent_block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    // Build the function call argument: parent_block_name::schema.table
    let row_arg = Expr::Cast {
        expr: Box::new(Expr::Column(super::ColumnRef::new(parent_block_name))),
        target_type: super::SqlType::with_schema(
            func_builder.table.schema.clone(),
            func_builder.table.name.clone(),
        ),
    };

    let func_call = super::FunctionCall::with_schema(
        func_builder.function.schema_name.clone(),
        func_builder.function.name.clone(),
        vec![FunctionArg::unnamed(row_arg)],
    );

    // Build the connection subquery using the function as the FROM source
    super::build_function_connection_subquery_full(conn_builder, func_call, params)
}

/// Build a subquery expression for a connection selection
fn build_connection_subquery_expr(
    conn_builder: &crate::builder::ConnectionBuilder,
    parent_block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    // Use the full connection subquery implementation from transpile_connection
    super::build_connection_subquery(conn_builder, parent_block_name, params)
}

/// Build a subquery expression for a nested node relation
pub fn build_relation_subquery_expr(
    nested_node: &NodeBuilder,
    parent_block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let ctx = AstBuildContext::new();
    let block_name = ctx.block_name.clone();

    // Build the object clause from nested node's selections
    let object_expr = build_node_object_expr(&nested_node.selections, &block_name, params)?;

    // Get the foreign key and direction
    let fkey = nested_node
        .fkey
        .as_ref()
        .ok_or("Internal Error: relation key")?;
    let reverse_reference = nested_node
        .reverse_reference
        .ok_or("Internal Error: relation reverse reference")?;

    // Build the join condition
    let join_condition = build_join_condition(
        fkey,
        reverse_reference,
        &block_name,
        parent_block_name,
        &nested_node.table,
    )?;

    // Build the subquery
    let subquery = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(object_expr)],
        from: Some(FromClause::Table {
            schema: Some(Ident::new(nested_node.table.schema.clone())),
            name: Ident::new(nested_node.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause: Some(join_condition),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Expr::Subquery(Box::new(subquery)))
}

/// Build a join condition for a foreign key relationship
fn build_join_condition(
    fkey: &crate::sql_types::ForeignKey,
    reverse_reference: bool,
    child_block_name: &str,
    parent_block_name: &str,
    _table: &Table,
) -> GraphQLResult<Expr> {
    let mut conditions = Vec::new();

    // ForeignKey has local_table_meta and referenced_table_meta, each with column_names
    // Depending on direction, pair up the columns
    let pairs: Vec<(&String, &String)> = if reverse_reference {
        // Parent has the referenced columns, child has the local columns
        fkey.local_table_meta
            .column_names
            .iter()
            .zip(fkey.referenced_table_meta.column_names.iter())
            .collect()
    } else {
        // Parent has the local columns, child has the referenced columns
        fkey.referenced_table_meta
            .column_names
            .iter()
            .zip(fkey.local_table_meta.column_names.iter())
            .collect()
    };

    for (child_col, parent_col) in pairs {
        let child_expr = column_ref(child_block_name, child_col);
        let parent_expr = column_ref(parent_block_name, parent_col);

        conditions.push(Expr::BinaryOp {
            left: Box::new(child_expr),
            op: BinaryOperator::Eq,
            right: Box::new(parent_expr),
        });
    }

    // Combine with AND
    if conditions.len() == 1 {
        Ok(conditions.remove(0))
    } else {
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(cond),
            };
        }
        Ok(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ColumnRef;

    #[test]
    fn test_type_cast_bigint() {
        let expr = Expr::Column(ColumnRef::new("test"));
        let casted = apply_type_cast(expr, 20);
        match casted {
            Expr::Cast { target_type, .. } => {
                assert_eq!(target_type.name, "text");
            }
            _ => panic!("Expected Cast expression"),
        }
    }

    #[test]
    fn test_type_cast_no_change() {
        let expr = Expr::Column(ColumnRef::new("test"));
        let result = apply_type_cast(expr.clone(), 25); // text OID, no cast needed
        match result {
            Expr::Column(_) => {}
            _ => panic!("Expected no cast for text type"),
        }
    }
}
