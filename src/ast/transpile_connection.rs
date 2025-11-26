//! AST-based transpilation for ConnectionBuilder
//!
//! This module implements the ToAst trait for ConnectionBuilder, converting it
//! to a type-safe AST that can be rendered to SQL. ConnectionBuilder generates
//! complex pagination queries with multiple CTEs for:
//! - Record fetching with pagination
//! - Total count
//! - Has next/previous page detection
//! - Aggregate computations

use super::{
    add_param_from_json, build_filter_expr, build_node_object_expr, coalesce, column_ref,
    count_star, empty_jsonb_object, func_call, jsonb_agg_with_order_and_filter, jsonb_build_object,
    string_literal, AstBuildContext, ToAst,
};
use crate::ast::{
    BinaryOperator, ColumnRef, Cte, CteQuery, Expr, FromClause, Ident, Literal, NullsOrder,
    OrderByExpr, OrderDirection, ParamCollector, SelectColumn, SelectStmt, Stmt,
};
use crate::builder::{
    AggregateBuilder, AggregateSelection, ConnectionBuilder, ConnectionSelection, Cursor,
    EdgeBuilder, EdgeSelection, OrderByBuilder, PageInfoBuilder, PageInfoSelection,
};
use crate::error::{GraphQLError, GraphQLResult};
use crate::sql_types::Table;

/// The result of transpiling a ConnectionBuilder to AST
pub struct ConnectionAst {
    /// The complete SQL statement
    pub stmt: Stmt,
}

impl ToAst for ConnectionBuilder {
    type Ast = ConnectionAst;

    fn to_ast(&self, params: &mut ParamCollector) -> GraphQLResult<Self::Ast> {
        let ctx = AstBuildContext::new();
        let block_name = ctx.block_name.clone();

        // Build the __records CTE - main data fetch with pagination
        let records_cte = build_records_cte(self, &block_name, params)?;

        // Build the __total_count CTE
        let total_count_cte = build_total_count_cte(self, &block_name, params)?;

        // Build __has_next_page and __has_previous_page CTEs
        let (has_next_cte, has_prev_cte) = build_pagination_ctes(self, &block_name, params)?;

        // Build the __has_records CTE
        let has_records_cte = build_has_records_cte();

        // Check if aggregates are requested and build the aggregate CTE
        let aggregate_builder = self.selections.iter().find_map(|sel| match sel {
            ConnectionSelection::Aggregate(builder) => Some(builder),
            _ => None,
        });
        let aggregates_cte = build_aggregates_cte(self, &block_name, aggregate_builder, params)?;

        // Build the main selection object (excluding aggregates - they're handled separately)
        let object_columns = build_connection_object(self, &block_name, params)?;

        // Build the __base_object CTE that combines everything
        let base_object_cte = build_base_object_cte(&object_columns, &block_name);

        // Build the final SELECT expression with aggregate merge if needed
        let final_expr = if let Some(agg_builder) = aggregate_builder {
            // Merge: coalesce(__base_object.obj, '{}'::jsonb) || jsonb_build_object(agg_alias, coalesce(__aggregates.agg_result, '{}'::jsonb))
            Expr::BinaryOp {
                left: Box::new(coalesce(vec![
                    column_ref("__base_object", "obj"),
                    empty_jsonb_object(),
                ])),
                op: BinaryOperator::JsonConcat,
                right: Box::new(jsonb_build_object(vec![(
                    agg_builder.alias.clone(),
                    coalesce(vec![
                        column_ref("__aggregates", "agg_result"),
                        empty_jsonb_object(),
                    ]),
                )])),
            }
        } else {
            coalesce(vec![
                column_ref("__base_object", "obj"),
                empty_jsonb_object(),
            ])
        };

        // Combine all CTEs
        let stmt = Stmt::Select(SelectStmt {
            ctes: vec![
                records_cte,
                total_count_cte,
                has_next_cte,
                has_prev_cte,
                has_records_cte,
                aggregates_cte,
                base_object_cte,
            ],
            columns: vec![SelectColumn::expr(final_expr)],
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Join {
                    left: Box::new(FromClause::Subquery {
                        query: Box::new(SelectStmt {
                            ctes: vec![],
                            columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
                            from: None,
                            where_clause: None,
                            group_by: vec![],
                            having: None,
                            order_by: vec![],
                            limit: None,
                            offset: None,
                        }),
                        alias: Ident::new("__dummy_for_left_join"),
                    }),
                    join_type: super::JoinType::Left,
                    right: Box::new(FromClause::Table {
                        schema: None,
                        name: Ident::new("__base_object"),
                        alias: None,
                    }),
                    on: Some(Expr::Literal(Literal::Bool(true))),
                }),
                join_type: super::JoinType::Inner,
                right: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("__aggregates"),
                    alias: None,
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        });

        Ok(ConnectionAst { stmt })
    }
}

/// Build the __records CTE that fetches the actual data
fn build_records_cte(
    conn: &ConnectionBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    // Build filter expression
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    // Get cursor (before or after)
    let cursor = conn.before.as_ref().or(conn.after.as_ref());

    // Determine if this is reverse pagination (using 'before' or 'last')
    let is_reverse = conn.before.is_some() || (conn.last.is_some() && conn.first.is_none());

    // Get order by (reversed if using reverse pagination)
    let order_by_builder = if is_reverse {
        conn.order_by.reverse()
    } else {
        conn.order_by.clone()
    };

    // Build ORDER BY expressions
    let order_by = build_order_by_exprs(&order_by_builder, block_name);

    // Build cursor pagination clause if cursor exists
    let pagination_clause = if let Some(cursor) = cursor {
        build_cursor_pagination_clause(
            &conn.source.table,
            &order_by_builder,
            cursor,
            block_name,
            params,
            false, // Don't include cursor's own record
        )?
    } else {
        None
    };

    // Combine filter and pagination clauses
    let where_clause = match (filter_clause, pagination_clause) {
        (Some(f), Some(p)) => Some(Expr::BinaryOp {
            left: Box::new(f),
            op: BinaryOperator::And,
            right: Box::new(p),
        }),
        (Some(f), None) => Some(f),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    };

    // Calculate limit
    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    // Select all selectable columns
    let columns: Vec<SelectColumn> = conn
        .source
        .table
        .columns
        .iter()
        .filter(|c| c.permissions.is_selectable)
        .map(|c| SelectColumn::expr(Expr::Column(ColumnRef::new(c.name.clone()))))
        .collect();

    let select = SelectStmt {
        ctes: vec![],
        columns,
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause,
        group_by: vec![],
        having: None,
        order_by,
        limit: Some(limit),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    Ok(Cte {
        name: Ident::new("__records"),
        columns: None,
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build cursor pagination clause
///
/// Generates a WHERE clause expression for cursor-based pagination using the
/// after or before cursor. The clause filters records based on the cursor's
/// position in the ordered result set.
fn build_cursor_pagination_clause(
    table: &Table,
    order_by: &OrderByBuilder,
    cursor: &Cursor,
    block_name: &str,
    params: &mut ParamCollector,
    allow_equality: bool,
) -> GraphQLResult<Option<Expr>> {
    if cursor.elems.is_empty() {
        return Ok(if allow_equality {
            Some(Expr::Literal(Literal::Bool(true)))
        } else {
            None
        });
    }

    // Build the recursive pagination clause
    build_cursor_pagination_clause_recursive(
        table,
        order_by,
        cursor,
        block_name,
        params,
        allow_equality,
        0,
    )
}

/// Recursive helper for building cursor pagination clause
fn build_cursor_pagination_clause_recursive(
    table: &Table,
    order_by: &OrderByBuilder,
    cursor: &Cursor,
    block_name: &str,
    params: &mut ParamCollector,
    allow_equality: bool,
    depth: usize,
) -> GraphQLResult<Option<Expr>> {
    // Check if cursor has more elements than order_by columns
    if depth < cursor.elems.len() && depth >= order_by.elems.len() {
        return Err(GraphQLError::validation(
            "orderBy clause incompatible with pagination cursor",
        ));
    }

    if depth >= cursor.elems.len() {
        return Ok(if allow_equality {
            Some(Expr::Literal(Literal::Bool(true)))
        } else {
            None
        });
    }

    let cursor_elem = &cursor.elems[depth];
    let order_elem = &order_by.elems[depth];
    let column = &order_elem.column;

    // Find the column type for parameter casting
    let col_expr = column_ref(block_name, &column.name);
    let val_expr = add_param_from_json(params, &cursor_elem.value, &column.type_name)?;

    // Determine comparison operator based on sort direction
    let op = if order_elem.direction.is_asc() {
        BinaryOperator::Gt
    } else {
        BinaryOperator::Lt
    };

    let nulls_first = order_elem.direction.nulls_first();

    // Build: (col > val OR (col IS NOT NULL AND val IS NULL AND nulls_first))
    let main_compare = Expr::BinaryOp {
        left: Box::new(col_expr.clone()),
        op,
        right: Box::new(val_expr.clone()),
    };

    let null_handling = if nulls_first {
        // When nulls first: non-null values come after null values
        // So (col IS NOT NULL AND val IS NULL) means we're past the cursor
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::IsNull {
                    expr: Box::new(col_expr.clone()),
                    negated: true, // IS NOT NULL
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::IsNull {
                    expr: Box::new(val_expr.clone()),
                    negated: false, // IS NULL
                }),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Literal::Bool(true))),
        }
    } else {
        // When nulls last: null values come after non-null values
        // So (col IS NULL AND val IS NOT NULL) means we're past the cursor
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::IsNull {
                    expr: Box::new(col_expr.clone()),
                    negated: false, // IS NULL
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::IsNull {
                    expr: Box::new(val_expr.clone()),
                    negated: true, // IS NOT NULL
                }),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Literal::Bool(true))),
        }
    };

    // First condition: (col > val) OR (col IS NOT NULL AND val IS NULL AND nulls_first)
    // Wrap in Nested for proper parentheses
    let first_condition = Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(main_compare),
        op: BinaryOperator::Or,
        right: Box::new(null_handling),
    }));

    // Build equality check for recursion: (col = val OR (col IS NULL AND val IS NULL))
    let equality_check = Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOperator::Eq,
            right: Box::new(val_expr.clone()),
        }),
        op: BinaryOperator::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::IsNull {
                expr: Box::new(col_expr),
                negated: false,
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::IsNull {
                expr: Box::new(val_expr),
                negated: false,
            }),
        }),
    }));

    // Recurse to next level
    let recurse = build_cursor_pagination_clause_recursive(
        table,
        order_by,
        cursor,
        block_name,
        params,
        allow_equality,
        depth + 1,
    )?;

    // Build: first_condition OR (equality_check AND recurse)
    // Wrap in Nested for proper grouping
    let result = match recurse {
        Some(recurse_expr) => Expr::Nested(Box::new(Expr::BinaryOp {
            left: Box::new(first_condition),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
                left: Box::new(equality_check),
                op: BinaryOperator::And,
                right: Box::new(recurse_expr),
            }))),
        })),
        None => first_condition,
    };

    Ok(Some(result))
}

/// Build the __total_count CTE
fn build_total_count_cte(
    conn: &ConnectionBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let where_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    let select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(count_star())],
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Cte {
        name: Ident::new("__total_count"),
        columns: Some(vec![Ident::new("___total_count")]),
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build pagination detection CTEs
fn build_pagination_ctes(
    conn: &ConnectionBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<(Cte, Cte)> {
    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    // Determine if this is reverse pagination
    let is_reverse = conn.before.is_some() || (conn.last.is_some() && conn.first.is_none());

    // Get order by (reversed if using reverse pagination)
    let order_by_builder = if is_reverse {
        conn.order_by.reverse()
    } else {
        conn.order_by.clone()
    };

    let order_by = build_order_by_exprs(&order_by_builder, block_name);

    // Get cursor (before or after)
    let cursor = conn.before.as_ref().or(conn.after.as_ref());

    // Build filter expression
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    // Build cursor pagination clause if cursor exists
    let pagination_clause = if let Some(cursor) = cursor {
        build_cursor_pagination_clause(
            &conn.source.table,
            &order_by_builder,
            cursor,
            block_name,
            params,
            false,
        )?
    } else {
        None
    };

    // Combine filter and pagination clauses
    let where_clause = match (filter_clause.clone(), pagination_clause) {
        (Some(f), Some(p)) => Some(Expr::BinaryOp {
            left: Box::new(f),
            op: BinaryOperator::And,
            right: Box::new(p),
        }),
        (Some(f), None) => Some(f),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    };

    // __has_next_page: select count(*) > limit from (limited subquery)
    let page_plus_1_select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause: where_clause.clone(),
        group_by: vec![],
        having: None,
        order_by: order_by.clone(),
        limit: Some(limit + 1),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    // __has_previous_page: check if there's a record that would have come before __records
    // We check if the first record (by original order) is NOT in __records
    // If offset > 0, then by definition there's a previous page
    let has_prev_select = if offset > 0 {
        // Simple case: offset > 0 means there's definitely a previous page
        SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(Expr::Literal(Literal::Bool(true)))],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }
    } else {
        // Complex case: check if first record (by order) is NOT in __records
        // This handles cursor-based pagination correctly
        //
        // Query structure:
        // with page_minus_1 as (
        //     select not (pk_tuple = any(__records.seen)) is_pkey_in_records
        //     from table
        //         left join (select array_agg(pk_tuple) from __records) __records(seen) on true
        //     where filter_clause
        //     order by order_by_clause
        //     limit 1
        // )
        // select coalesce(bool_and(is_pkey_in_records), false) from page_minus_1

        // Build pk tuple expression: (pk_col1, pk_col2, ...)
        let pk_columns = conn.source.table.primary_key_columns();
        let pk_tuple_from_table: Expr = if pk_columns.len() == 1 {
            column_ref(block_name, &pk_columns[0].name)
        } else {
            // For multi-column pk, create a ROW expression
            Expr::Row(
                pk_columns
                    .iter()
                    .map(|c| column_ref(block_name, &c.name))
                    .collect(),
            )
        };

        let pk_tuple_from_records: Expr = if pk_columns.len() == 1 {
            column_ref("__records", &pk_columns[0].name)
        } else {
            Expr::Row(
                pk_columns
                    .iter()
                    .map(|c| column_ref("__records", &c.name))
                    .collect(),
            )
        };

        // Build: select array_agg(pk_tuple) as seen from __records
        let seen_subquery = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Expr {
                expr: Expr::Aggregate(super::AggregateExpr::new(
                    super::AggregateFunction::ArrayAgg,
                    vec![pk_tuple_from_records],
                )),
                alias: Some(Ident::new("seen")),
            }],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("__records"),
                alias: None,
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        // Build: not (pk_tuple = any(__seen_records.seen))
        let is_pkey_in_records = Expr::UnaryOp {
            op: super::UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(pk_tuple_from_table),
                op: BinaryOperator::Any,
                right: Box::new(column_ref("__seen_records", "seen")),
            }),
        };

        // Build page_minus_1 CTE
        let page_minus_1_select = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Expr {
                expr: is_pkey_in_records,
                alias: Some(Ident::new("is_pkey_in_records")),
            }],
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Table {
                    schema: Some(Ident::new(conn.source.table.schema.clone())),
                    name: Ident::new(conn.source.table.name.clone()),
                    alias: Some(Ident::new(block_name)),
                }),
                join_type: super::JoinType::Left,
                right: Box::new(FromClause::Subquery {
                    query: Box::new(seen_subquery),
                    alias: Ident::new("__seen_records"),
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            where_clause: filter_clause.clone(),
            group_by: vec![],
            having: None,
            order_by: order_by.clone(),
            limit: Some(1),
            offset: None,
        };

        // Build final select: coalesce(bool_and(is_pkey_in_records), false)
        SelectStmt {
            ctes: vec![Cte {
                name: Ident::new("page_minus_1"),
                columns: None,
                query: CteQuery::Select(page_minus_1_select),
                materialized: None,
            }],
            columns: vec![SelectColumn::expr(coalesce(vec![
                Expr::Aggregate(super::AggregateExpr::new(
                    super::AggregateFunction::BoolAnd,
                    vec![column_ref("page_minus_1", "is_pkey_in_records")],
                )),
                Expr::Literal(Literal::Bool(false)),
            ]))],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("page_minus_1"),
                alias: None,
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }
    };

    // For reverse pagination (before/last), swap hasNextPage and hasPreviousPage queries
    // This is because when paginating backwards, "next" in the query direction
    // is actually "previous" in the logical ordering
    let (next_query, prev_query) = if is_reverse {
        (
            CteQuery::Select(has_prev_select),
            CteQuery::Select(SelectStmt {
                ctes: vec![Cte {
                    name: Ident::new("page_plus_1"),
                    columns: None,
                    query: CteQuery::Select(page_plus_1_select),
                    materialized: None,
                }],
                columns: vec![SelectColumn::expr(Expr::BinaryOp {
                    left: Box::new(count_star()),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Literal(Literal::Integer(limit as i64))),
                })],
                from: Some(FromClause::Table {
                    schema: None,
                    name: Ident::new("page_plus_1"),
                    alias: None,
                }),
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        )
    } else {
        (
            CteQuery::Select(SelectStmt {
                ctes: vec![Cte {
                    name: Ident::new("page_plus_1"),
                    columns: None,
                    query: CteQuery::Select(page_plus_1_select),
                    materialized: None,
                }],
                columns: vec![SelectColumn::expr(Expr::BinaryOp {
                    left: Box::new(count_star()),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Literal(Literal::Integer(limit as i64))),
                })],
                from: Some(FromClause::Table {
                    schema: None,
                    name: Ident::new("page_plus_1"),
                    alias: None,
                }),
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            CteQuery::Select(has_prev_select),
        )
    };

    let has_next_cte = Cte {
        name: Ident::new("__has_next_page"),
        columns: Some(vec![Ident::new("___has_next_page")]),
        query: next_query,
        materialized: None,
    };

    let has_prev_cte = Cte {
        name: Ident::new("__has_previous_page"),
        columns: Some(vec![Ident::new("___has_previous_page")]),
        query: prev_query,
        materialized: None,
    };

    Ok((has_next_cte, has_prev_cte))
}

/// Build the __has_records CTE
fn build_has_records_cte() -> Cte {
    Cte {
        name: Ident::new("__has_records"),
        columns: Some(vec![Ident::new("has_records")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(Expr::Exists {
                subquery: Box::new(SelectStmt {
                    ctes: vec![],
                    columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
                    from: Some(FromClause::Table {
                        schema: None,
                        name: Ident::new("__records"),
                        alias: None,
                    }),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                }),
                negated: false,
            })],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    }
}

/// Build the __aggregates CTE for aggregate computations
fn build_aggregates_cte(
    conn: &ConnectionBuilder,
    block_name: &str,
    agg_builder: Option<&AggregateBuilder>,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let Some(agg_builder) = agg_builder else {
        // No aggregates requested - return a dummy CTE with null
        return Ok(Cte {
            name: Ident::new("__aggregates"),
            columns: Some(vec![Ident::new("agg_result")]),
            query: CteQuery::Select(SelectStmt {
                ctes: vec![],
                columns: vec![SelectColumn::expr(Expr::Cast {
                    expr: Box::new(Expr::Literal(Literal::Null)),
                    target_type: super::type_name_to_sql_type("jsonb"),
                })],
                from: None,
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            materialized: None,
        });
    };

    // Build the aggregate select list
    let agg_pairs = build_aggregate_select_list(agg_builder, block_name);

    // Build WHERE clause (same filter as records)
    let where_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    Ok(Cte {
        name: Ident::new("__aggregates"),
        columns: Some(vec![Ident::new("agg_result")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(jsonb_build_object(agg_pairs))],
            from: Some(FromClause::Table {
                schema: Some(Ident::new(conn.source.table.schema.clone())),
                name: Ident::new(conn.source.table.name.clone()),
                alias: Some(Ident::new(block_name)),
            }),
            where_clause,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    })
}

/// Build the aggregate select list (count, sum, avg, min, max)
fn build_aggregate_select_list(
    agg_builder: &AggregateBuilder,
    block_name: &str,
) -> Vec<(String, Expr)> {
    let mut pairs = Vec::new();

    for selection in &agg_builder.selections {
        match selection {
            AggregateSelection::Count { alias } => {
                pairs.push((alias.clone(), count_star()));
            }
            AggregateSelection::Sum {
                alias,
                column_builders,
            } => {
                let field_pairs =
                    build_aggregate_field_pairs(column_builders, block_name, "sum", false);
                pairs.push((alias.clone(), jsonb_build_object(field_pairs)));
            }
            AggregateSelection::Avg {
                alias,
                column_builders,
            } => {
                // AVG needs numeric cast for precision
                let field_pairs =
                    build_aggregate_field_pairs(column_builders, block_name, "avg", true);
                pairs.push((alias.clone(), jsonb_build_object(field_pairs)));
            }
            AggregateSelection::Min {
                alias,
                column_builders,
            } => {
                let field_pairs =
                    build_aggregate_field_pairs(column_builders, block_name, "min", false);
                pairs.push((alias.clone(), jsonb_build_object(field_pairs)));
            }
            AggregateSelection::Max {
                alias,
                column_builders,
            } => {
                let field_pairs =
                    build_aggregate_field_pairs(column_builders, block_name, "max", false);
                pairs.push((alias.clone(), jsonb_build_object(field_pairs)));
            }
            AggregateSelection::Typename { alias, typename } => {
                pairs.push((alias.clone(), string_literal(typename)));
            }
        }
    }

    pairs
}

/// Build field pairs for aggregate functions (sum/avg/min/max)
fn build_aggregate_field_pairs(
    column_builders: &[crate::builder::ColumnBuilder],
    block_name: &str,
    func_name: &str,
    cast_to_numeric: bool,
) -> Vec<(String, Expr)> {
    column_builders
        .iter()
        .map(|col_builder| {
            let col_expr = column_ref(block_name, &col_builder.column.name);
            let arg_expr = if cast_to_numeric {
                Expr::Cast {
                    expr: Box::new(col_expr),
                    target_type: super::type_name_to_sql_type("numeric"),
                }
            } else {
                col_expr
            };
            let agg_expr = func_call(func_name, vec![arg_expr]);
            (col_builder.alias.clone(), agg_expr)
        })
        .collect()
}

/// Build the connection object columns from selections
fn build_connection_object(
    conn: &ConnectionBuilder,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Vec<(String, Expr)>> {
    let mut pairs = Vec::new();

    for selection in &conn.selections {
        match selection {
            ConnectionSelection::TotalCount { alias } => {
                pairs.push((alias.clone(), column_ref("__total_count", "___total_count")));
            }
            ConnectionSelection::Edge(edges_builder) => {
                let edges_expr = build_edges_expr(
                    edges_builder,
                    block_name,
                    &conn.order_by,
                    &conn.source.table,
                    params,
                )?;
                pairs.push((edges_builder.alias.clone(), edges_expr));
            }
            ConnectionSelection::PageInfo(page_info_builder) => {
                let page_info_expr =
                    build_page_info_expr(page_info_builder, block_name, &conn.order_by)?;
                pairs.push((page_info_builder.alias.clone(), page_info_expr));
            }
            ConnectionSelection::Typename { alias, typename } => {
                pairs.push((alias.clone(), string_literal(typename)));
            }
            ConnectionSelection::Aggregate(_) => {
                // Aggregate handling would be added here
                // For now, skip
            }
        }
    }

    Ok(pairs)
}

/// Build edges expression
fn build_edges_expr(
    edges: &EdgeBuilder,
    block_name: &str,
    order_by: &crate::builder::OrderByBuilder,
    table: &Table,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let mut edge_pairs = Vec::new();

    for selection in &edges.selections {
        match selection {
            EdgeSelection::Cursor { alias } => {
                // Build cursor: translate(encode(convert_to(jsonb_build_array(to_jsonb(col1), to_jsonb(col2), ...)::text, 'utf-8'), 'base64'), E'\n', '')
                let cursor_expr = build_cursor_expr(block_name, order_by);
                edge_pairs.push((alias.clone(), cursor_expr));
            }
            EdgeSelection::Node(node_builder) => {
                let node_expr =
                    build_node_object_expr(&node_builder.selections, block_name, params)?;
                edge_pairs.push((node_builder.alias.clone(), node_expr));
            }
            EdgeSelection::Typename { alias, typename } => {
                edge_pairs.push((alias.clone(), string_literal(typename)));
            }
        }
    }

    // Build: coalesce(jsonb_agg(jsonb_build_object(...) order by ... ) filter (where pk is not null), '[]')
    // The filter clause excludes null rows from LEFT JOIN when no matching records exist
    // The ORDER BY re-reverses results that were fetched in reverse order for backward pagination
    let edge_object = jsonb_build_object(edge_pairs);

    // Build ORDER BY expressions for jsonb_agg using NORMAL order (not reversed)
    // This re-sorts results that were fetched in reverse order for backward pagination
    let order_by_exprs: Vec<OrderByExpr> = order_by
        .elems
        .iter()
        .map(|elem| {
            // Convert the combined OrderDirection enum to separate direction and nulls
            let (direction, nulls) = match elem.direction {
                crate::builder::OrderDirection::AscNullsFirst => {
                    (OrderDirection::Asc, NullsOrder::First)
                }
                crate::builder::OrderDirection::AscNullsLast => {
                    (OrderDirection::Asc, NullsOrder::Last)
                }
                crate::builder::OrderDirection::DescNullsFirst => {
                    (OrderDirection::Desc, NullsOrder::First)
                }
                crate::builder::OrderDirection::DescNullsLast => {
                    (OrderDirection::Desc, NullsOrder::Last)
                }
            };
            OrderByExpr {
                expr: column_ref(block_name, &elem.column.name),
                direction: Some(direction),
                nulls: Some(nulls),
            }
        })
        .collect();

    // Get the first primary key column to use in the filter
    let pk_columns = table.primary_key_columns();
    let filter_expr = pk_columns.first().map(|pk_col| {
        // Build: block_name.pk_col is not null
        Expr::IsNull {
            expr: Box::new(column_ref(block_name, &pk_col.name)),
            negated: true, // IS NOT NULL
        }
    });

    // Build jsonb_agg with order by and optional filter
    let agg_expr = jsonb_agg_with_order_and_filter(edge_object, order_by_exprs, filter_expr);

    // Wrap with CASE WHEN for safety: return empty [] when no records exist
    // This handles edge cases where LEFT JOIN produces NULL rows
    let has_records_ref = column_ref("__has_records", "has_records");
    let case_expr = Expr::Case(super::CaseExpr {
        operand: None,
        when_clauses: vec![(has_records_ref, coalesce(vec![agg_expr, super::empty_jsonb_array()]))],
        else_clause: Some(Box::new(super::empty_jsonb_array())),
    });

    Ok(case_expr)
}

/// Build cursor expression from order_by columns
/// Format: translate(encode(convert_to(jsonb_build_array(to_jsonb(col1), ...)::text, 'utf-8'), 'base64'), E'\n', '')
fn build_cursor_expr(block_name: &str, order_by: &crate::builder::OrderByBuilder) -> Expr {
    // Build to_jsonb(block_name.column) for each order_by column
    let jsonb_cols: Vec<Expr> = order_by
        .elems
        .iter()
        .map(|elem| func_call("to_jsonb", vec![column_ref(block_name, &elem.column.name)]))
        .collect();

    // Build jsonb_build_array(...)
    let jsonb_array = func_call("jsonb_build_array", jsonb_cols);

    // Cast to text
    let as_text = Expr::Cast {
        expr: Box::new(jsonb_array),
        target_type: super::type_name_to_sql_type("text"),
    };

    // convert_to(..., 'utf-8')
    let converted = func_call("convert_to", vec![as_text, string_literal("utf-8")]);

    // encode(..., 'base64')
    let encoded = func_call("encode", vec![converted, string_literal("base64")]);

    // translate(..., E'\n', '')
    func_call(
        "translate",
        vec![encoded, string_literal("\n"), string_literal("")],
    )
}

/// Build page info expression
fn build_page_info_expr(
    page_info: &PageInfoBuilder,
    block_name: &str,
    order_by: &crate::builder::OrderByBuilder,
) -> GraphQLResult<Expr> {
    let mut pairs = Vec::new();

    // Build cursor expression for start/end cursor
    let cursor_expr = build_cursor_expr(block_name, order_by);

    // Build forward order by expressions (for startCursor)
    let forward_order_by = build_order_by_exprs(order_by, block_name);

    // Build reversed order by expressions (for endCursor)
    let reversed_order_by: Vec<OrderByExpr> = order_by
        .elems
        .iter()
        .map(|elem| {
            let (direction, nulls) = match elem.direction {
                crate::builder::OrderDirection::AscNullsFirst => {
                    (Some(OrderDirection::Desc), Some(NullsOrder::Last))
                }
                crate::builder::OrderDirection::AscNullsLast => {
                    (Some(OrderDirection::Desc), Some(NullsOrder::First))
                }
                crate::builder::OrderDirection::DescNullsFirst => {
                    (Some(OrderDirection::Asc), Some(NullsOrder::Last))
                }
                crate::builder::OrderDirection::DescNullsLast => {
                    (Some(OrderDirection::Asc), Some(NullsOrder::First))
                }
            };
            OrderByExpr {
                expr: column_ref(block_name, &elem.column.name),
                direction,
                nulls,
            }
        })
        .collect();

    for selection in &page_info.selections {
        match selection {
            PageInfoSelection::HasNextPage { alias } => {
                pairs.push((
                    alias.clone(),
                    coalesce(vec![
                        func_call(
                            "bool_and",
                            vec![column_ref("__has_next_page", "___has_next_page")],
                        ),
                        Expr::Literal(Literal::Bool(false)),
                    ]),
                ));
            }
            PageInfoSelection::HasPreviousPage { alias } => {
                pairs.push((
                    alias.clone(),
                    coalesce(vec![
                        func_call(
                            "bool_and",
                            vec![column_ref("__has_previous_page", "___has_previous_page")],
                        ),
                        Expr::Literal(Literal::Bool(false)),
                    ]),
                ));
            }
            PageInfoSelection::StartCursor { alias } => {
                // case when __has_records.has_records then (array_agg(cursor order by order_by))[1] else null end
                let array_agg_expr = Expr::ArrayIndex {
                    array: Box::new(Expr::FunctionCallWithOrderBy {
                        name: "array_agg".to_string(),
                        args: vec![cursor_expr.clone()],
                        order_by: forward_order_by.clone(),
                    }),
                    index: Box::new(Expr::Literal(Literal::Integer(1))),
                };
                let case_expr = Expr::Case(crate::ast::CaseExpr::searched(
                    vec![(column_ref("__has_records", "has_records"), array_agg_expr)],
                    Some(Expr::Literal(Literal::Null)),
                ));
                pairs.push((alias.clone(), case_expr));
            }
            PageInfoSelection::EndCursor { alias } => {
                // case when __has_records.has_records then (array_agg(cursor order by order_by_reversed))[1] else null end
                let array_agg_expr = Expr::ArrayIndex {
                    array: Box::new(Expr::FunctionCallWithOrderBy {
                        name: "array_agg".to_string(),
                        args: vec![cursor_expr.clone()],
                        order_by: reversed_order_by.clone(),
                    }),
                    index: Box::new(Expr::Literal(Literal::Integer(1))),
                };
                let case_expr = Expr::Case(crate::ast::CaseExpr::searched(
                    vec![(column_ref("__has_records", "has_records"), array_agg_expr)],
                    Some(Expr::Literal(Literal::Null)),
                ));
                pairs.push((alias.clone(), case_expr));
            }
            PageInfoSelection::Typename { alias, typename } => {
                pairs.push((alias.clone(), string_literal(typename)));
            }
        }
    }

    Ok(jsonb_build_object(pairs))
}

/// Build the __base_object CTE
fn build_base_object_cte(object_columns: &[(String, Expr)], block_name: &str) -> Cte {
    let object_expr = jsonb_build_object(object_columns.to_vec());

    // The key insight: __records is LEFT JOINed and aliased as block_name
    // This allows object_clause expressions (which use block_name) to work correctly
    Cte {
        name: Ident::new("__base_object"),
        columns: Some(vec![Ident::new("obj")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Expr {
                expr: object_expr,
                alias: None,
            }],
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Join {
                    left: Box::new(FromClause::Join {
                        left: Box::new(FromClause::Join {
                            left: Box::new(FromClause::Table {
                                schema: None,
                                name: Ident::new("__total_count"),
                                alias: None,
                            }),
                            join_type: super::JoinType::Inner,
                            right: Box::new(FromClause::Table {
                                schema: None,
                                name: Ident::new("__has_next_page"),
                                alias: None,
                            }),
                            on: Some(Expr::Literal(Literal::Bool(true))),
                        }),
                        join_type: super::JoinType::Inner,
                        right: Box::new(FromClause::Table {
                            schema: None,
                            name: Ident::new("__has_previous_page"),
                            alias: None,
                        }),
                        on: Some(Expr::Literal(Literal::Bool(true))),
                    }),
                    join_type: super::JoinType::Inner,
                    right: Box::new(FromClause::Table {
                        schema: None,
                        name: Ident::new("__has_records"),
                        alias: None,
                    }),
                    on: Some(Expr::Literal(Literal::Bool(true))),
                }),
                // LEFT JOIN __records aliased as block_name
                join_type: super::JoinType::Left,
                right: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("__records"),
                    alias: Some(Ident::new(block_name)),
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            where_clause: None,
            group_by: vec![
                column_ref("__total_count", "___total_count"),
                column_ref("__has_next_page", "___has_next_page"),
                column_ref("__has_previous_page", "___has_previous_page"),
                column_ref("__has_records", "has_records"),
            ],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    }
}

/// Build ORDER BY expressions from the order_by builder
fn build_order_by_exprs(
    order_by: &crate::builder::OrderByBuilder,
    block_name: &str,
) -> Vec<OrderByExpr> {
    order_by
        .elems
        .iter()
        .map(|elem| {
            let (direction, nulls) = match elem.direction {
                crate::builder::OrderDirection::AscNullsFirst => {
                    (Some(OrderDirection::Asc), Some(NullsOrder::First))
                }
                crate::builder::OrderDirection::AscNullsLast => {
                    (Some(OrderDirection::Asc), Some(NullsOrder::Last))
                }
                crate::builder::OrderDirection::DescNullsFirst => {
                    (Some(OrderDirection::Desc), Some(NullsOrder::First))
                }
                crate::builder::OrderDirection::DescNullsLast => {
                    (Some(OrderDirection::Desc), Some(NullsOrder::Last))
                }
            };
            OrderByExpr {
                expr: column_ref(block_name, &elem.column.name),
                direction,
                nulls,
            }
        })
        .collect()
}

/// Build a connection query as a subquery expression (for nested connections inside nodes)
///
/// This is the public interface for building a connection when it's nested inside a node selection.
/// It builds the same complex CTE-based query but returns it as a subquery expression.
pub fn build_connection_subquery(
    conn: &ConnectionBuilder,
    parent_block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let ctx = AstBuildContext::new();
    let block_name = ctx.block_name.clone();

    // Build the join clause for the foreign key relationship
    let join_clause = build_fkey_join_clause(conn, &block_name, parent_block_name);

    // Build the __records CTE - main data fetch with pagination and FK join
    let records_cte = build_records_cte_with_join(conn, &block_name, &join_clause, params)?;

    // Build the __total_count CTE with FK join
    let total_count_cte = build_total_count_cte_with_join(conn, &block_name, &join_clause, params)?;

    // Build __has_next_page and __has_previous_page CTEs with FK join
    let (has_next_cte, has_prev_cte) =
        build_pagination_ctes_with_join(conn, &block_name, &join_clause, params)?;

    // Build the __has_records CTE
    let has_records_cte = build_has_records_cte();

    // Check if aggregates are requested and build the aggregate CTE
    let aggregate_builder = conn.selections.iter().find_map(|sel| match sel {
        ConnectionSelection::Aggregate(builder) => Some(builder),
        _ => None,
    });
    let aggregates_cte =
        build_aggregates_cte_with_join(conn, &block_name, &join_clause, aggregate_builder, params)?;

    // Build the main selection object (excluding aggregates - they're handled separately)
    let object_columns = build_connection_object(conn, &block_name, params)?;

    // Build the __base_object CTE that combines everything
    let base_object_cte = build_base_object_cte(&object_columns, &block_name);

    // Build the final SELECT expression with aggregate merge if needed
    let final_expr = if let Some(agg_builder) = aggregate_builder {
        Expr::BinaryOp {
            left: Box::new(coalesce(vec![
                column_ref("__base_object", "obj"),
                empty_jsonb_object(),
            ])),
            op: BinaryOperator::JsonConcat,
            right: Box::new(jsonb_build_object(vec![(
                agg_builder.alias.clone(),
                coalesce(vec![
                    column_ref("__aggregates", "agg_result"),
                    empty_jsonb_object(),
                ]),
            )])),
        }
    } else {
        coalesce(vec![
            column_ref("__base_object", "obj"),
            empty_jsonb_object(),
        ])
    };

    // Build the full select statement as a subquery
    let select = SelectStmt {
        ctes: vec![
            records_cte,
            total_count_cte,
            has_next_cte,
            has_prev_cte,
            has_records_cte,
            aggregates_cte,
            base_object_cte,
        ],
        columns: vec![SelectColumn::expr(final_expr)],
        from: Some(FromClause::Join {
            left: Box::new(FromClause::Join {
                left: Box::new(FromClause::Subquery {
                    query: Box::new(SelectStmt {
                        ctes: vec![],
                        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
                        from: None,
                        where_clause: None,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    }),
                    alias: Ident::new("__dummy_for_left_join"),
                }),
                join_type: super::JoinType::Left,
                right: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("__base_object"),
                    alias: None,
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            join_type: super::JoinType::Inner,
            right: Box::new(FromClause::Table {
                schema: None,
                name: Ident::new("__aggregates"),
                alias: None,
            }),
            on: Some(Expr::Literal(Literal::Bool(true))),
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Expr::Subquery(Box::new(select)))
}

/// Build the join clause for a foreign key relationship
fn build_fkey_join_clause(
    conn: &ConnectionBuilder,
    block_name: &str,
    parent_block_name: &str,
) -> Option<Expr> {
    let fkey_reversible = conn.source.fkey.as_ref()?;
    let fkey = &fkey_reversible.fkey;
    let reverse = fkey_reversible.reverse_reference;

    let (local_cols, parent_cols) = if reverse {
        (
            &fkey.local_table_meta.column_names,
            &fkey.referenced_table_meta.column_names,
        )
    } else {
        (
            &fkey.referenced_table_meta.column_names,
            &fkey.local_table_meta.column_names,
        )
    };

    let mut conditions: Vec<Expr> = Vec::new();
    for (local_col, parent_col) in local_cols.iter().zip(parent_cols.iter()) {
        conditions.push(Expr::BinaryOp {
            left: Box::new(column_ref(block_name, local_col)),
            op: BinaryOperator::Eq,
            right: Box::new(column_ref(parent_block_name, parent_col)),
        });
    }

    if conditions.is_empty() {
        None
    } else {
        Some(super::combine_with_and(conditions))
    }
}

/// Build __records CTE with join clause for nested connections
fn build_records_cte_with_join(
    conn: &ConnectionBuilder,
    block_name: &str,
    join_clause: &Option<Expr>,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;
    let order_by = build_order_by_exprs(&conn.order_by, block_name);

    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    // Combine join clause with filter clause
    let where_clause = match (join_clause.clone(), filter_clause) {
        (Some(join), Some(filter)) => Some(Expr::BinaryOp {
            left: Box::new(join),
            op: BinaryOperator::And,
            right: Box::new(filter),
        }),
        (Some(join), None) => Some(join),
        (None, Some(filter)) => Some(filter),
        (None, None) => None,
    };

    let columns: Vec<SelectColumn> = conn
        .source
        .table
        .columns
        .iter()
        .filter(|c| c.permissions.is_selectable)
        .map(|c| SelectColumn::expr(Expr::Column(ColumnRef::new(c.name.clone()))))
        .collect();

    let select = SelectStmt {
        ctes: vec![],
        columns,
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause,
        group_by: vec![],
        having: None,
        order_by,
        limit: Some(limit),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    Ok(Cte {
        name: Ident::new("__records"),
        columns: None,
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build __total_count CTE with join clause for nested connections
fn build_total_count_cte_with_join(
    conn: &ConnectionBuilder,
    block_name: &str,
    join_clause: &Option<Expr>,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    let where_clause = match (join_clause.clone(), filter_clause) {
        (Some(join), Some(filter)) => Some(Expr::BinaryOp {
            left: Box::new(join),
            op: BinaryOperator::And,
            right: Box::new(filter),
        }),
        (Some(join), None) => Some(join),
        (None, Some(filter)) => Some(filter),
        (None, None) => None,
    };

    let select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(count_star())],
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Cte {
        name: Ident::new("__total_count"),
        columns: Some(vec![Ident::new("___total_count")]),
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build pagination CTEs with join clause for nested connections
fn build_pagination_ctes_with_join(
    conn: &ConnectionBuilder,
    block_name: &str,
    join_clause: &Option<Expr>,
    params: &mut ParamCollector,
) -> GraphQLResult<(Cte, Cte)> {
    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;
    let order_by = build_order_by_exprs(&conn.order_by, block_name);

    let where_clause = match (join_clause.clone(), filter_clause) {
        (Some(join), Some(filter)) => Some(Expr::BinaryOp {
            left: Box::new(join),
            op: BinaryOperator::And,
            right: Box::new(filter),
        }),
        (Some(join), None) => Some(join),
        (None, Some(filter)) => Some(filter),
        (None, None) => None,
    };

    let page_plus_1_select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
        from: Some(FromClause::Table {
            schema: Some(Ident::new(conn.source.table.schema.clone())),
            name: Ident::new(conn.source.table.name.clone()),
            alias: Some(Ident::new(block_name)),
        }),
        where_clause: where_clause.clone(),
        group_by: vec![],
        having: None,
        order_by: order_by.clone(),
        limit: Some(limit + 1),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    let has_next_cte = Cte {
        name: Ident::new("__has_next_page"),
        columns: Some(vec![Ident::new("___has_next_page")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![Cte {
                name: Ident::new("page_plus_1"),
                columns: None,
                query: CteQuery::Select(page_plus_1_select),
                materialized: None,
            }],
            columns: vec![SelectColumn::expr(Expr::BinaryOp {
                left: Box::new(count_star()),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(limit as i64))),
            })],
            from: Some(FromClause::Table {
                schema: None,
                name: Ident::new("page_plus_1"),
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

    let has_prev_cte = Cte {
        name: Ident::new("__has_previous_page"),
        columns: Some(vec![Ident::new("___has_previous_page")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(Expr::Literal(Literal::Bool(offset > 0)))],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    };

    Ok((has_next_cte, has_prev_cte))
}

/// Build aggregates CTE with join clause for nested connections
fn build_aggregates_cte_with_join(
    conn: &ConnectionBuilder,
    block_name: &str,
    join_clause: &Option<Expr>,
    agg_builder: Option<&AggregateBuilder>,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let Some(agg_builder) = agg_builder else {
        return Ok(Cte {
            name: Ident::new("__aggregates"),
            columns: Some(vec![Ident::new("agg_result")]),
            query: CteQuery::Select(SelectStmt {
                ctes: vec![],
                columns: vec![SelectColumn::expr(Expr::Cast {
                    expr: Box::new(Expr::Literal(Literal::Null)),
                    target_type: super::type_name_to_sql_type("jsonb"),
                })],
                from: None,
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            materialized: None,
        });
    };

    let agg_pairs = build_aggregate_select_list(agg_builder, block_name);
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    let where_clause = match (join_clause.clone(), filter_clause) {
        (Some(join), Some(filter)) => Some(Expr::BinaryOp {
            left: Box::new(join),
            op: BinaryOperator::And,
            right: Box::new(filter),
        }),
        (Some(join), None) => Some(join),
        (None, Some(filter)) => Some(filter),
        (None, None) => None,
    };

    Ok(Cte {
        name: Ident::new("__aggregates"),
        columns: Some(vec![Ident::new("agg_result")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(jsonb_build_object(agg_pairs))],
            from: Some(FromClause::Table {
                schema: Some(Ident::new(conn.source.table.schema.clone())),
                name: Ident::new(conn.source.table.name.clone()),
                alias: Some(Ident::new(block_name)),
            }),
            where_clause,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    })
}

/// Build a connection query using a function call as the FROM source
///
/// This is used when a function returns `setof <type>` and we want to expose
/// that as a connection. The function call replaces the table in the FROM clause.
pub fn build_function_connection_subquery_full(
    conn: &ConnectionBuilder,
    func_call: super::FunctionCall,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    let ctx = AstBuildContext::new();
    let block_name = ctx.block_name.clone();

    // Build the from clause using the function call
    let from_clause = FromClause::Function {
        call: func_call.clone(),
        alias: Ident::new(&block_name),
    };

    // Build the __records CTE - main data fetch with pagination
    let records_cte = build_records_cte_from_function(conn, &block_name, &from_clause, params)?;

    // Build the __total_count CTE
    let total_count_cte =
        build_total_count_cte_from_function(conn, &block_name, &from_clause, params)?;

    // Build __has_next_page and __has_previous_page CTEs
    let (has_next_cte, has_prev_cte) =
        build_pagination_ctes_from_function(conn, &block_name, &from_clause, params)?;

    // Build the __has_records CTE
    let has_records_cte = build_has_records_cte();

    // Check if aggregates are requested and build the aggregate CTE
    let aggregate_builder = conn.selections.iter().find_map(|sel| match sel {
        ConnectionSelection::Aggregate(builder) => Some(builder),
        _ => None,
    });
    let aggregates_cte = build_aggregates_cte_from_function(
        conn,
        &block_name,
        &from_clause,
        aggregate_builder,
        params,
    )?;

    // Build the main selection object (excluding aggregates - they're handled separately)
    let object_columns = build_connection_object(conn, &block_name, params)?;

    // Build the __base_object CTE that combines everything
    let base_object_cte = build_base_object_cte(&object_columns, &block_name);

    // Build the final SELECT expression with aggregate merge if needed
    let final_expr = if let Some(agg_builder) = aggregate_builder {
        Expr::BinaryOp {
            left: Box::new(coalesce(vec![
                column_ref("__base_object", "obj"),
                empty_jsonb_object(),
            ])),
            op: BinaryOperator::JsonConcat,
            right: Box::new(jsonb_build_object(vec![(
                agg_builder.alias.clone(),
                coalesce(vec![
                    column_ref("__aggregates", "agg_result"),
                    empty_jsonb_object(),
                ]),
            )])),
        }
    } else {
        coalesce(vec![
            column_ref("__base_object", "obj"),
            empty_jsonb_object(),
        ])
    };

    // Build the full select statement as a subquery
    let select = SelectStmt {
        ctes: vec![
            records_cte,
            total_count_cte,
            has_next_cte,
            has_prev_cte,
            has_records_cte,
            aggregates_cte,
            base_object_cte,
        ],
        columns: vec![SelectColumn::expr(final_expr)],
        from: Some(FromClause::Join {
            left: Box::new(FromClause::Join {
                left: Box::new(FromClause::Subquery {
                    query: Box::new(SelectStmt {
                        ctes: vec![],
                        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
                        from: None,
                        where_clause: None,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    }),
                    alias: Ident::new("__dummy_for_left_join"),
                }),
                join_type: super::JoinType::Left,
                right: Box::new(FromClause::Table {
                    schema: None,
                    name: Ident::new("__base_object"),
                    alias: None,
                }),
                on: Some(Expr::Literal(Literal::Bool(true))),
            }),
            join_type: super::JoinType::Inner,
            right: Box::new(FromClause::Table {
                schema: None,
                name: Ident::new("__aggregates"),
                alias: None,
            }),
            on: Some(Expr::Literal(Literal::Bool(true))),
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Expr::Subquery(Box::new(select)))
}

/// Build __records CTE using a function as the FROM source
fn build_records_cte_from_function(
    conn: &ConnectionBuilder,
    block_name: &str,
    from_clause: &FromClause,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    // Determine if this is reverse pagination (using 'before' or 'last')
    let is_reverse = conn.before.is_some() || (conn.last.is_some() && conn.first.is_none());

    // Get order by (reversed if using reverse pagination)
    let order_by_builder = if is_reverse {
        conn.order_by.reverse()
    } else {
        conn.order_by.clone()
    };

    let order_by = build_order_by_exprs(&order_by_builder, block_name);

    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    let columns: Vec<SelectColumn> = conn
        .source
        .table
        .columns
        .iter()
        .filter(|c| c.permissions.is_selectable)
        .map(|c| SelectColumn::expr(Expr::Column(ColumnRef::new(c.name.clone()))))
        .collect();

    let select = SelectStmt {
        ctes: vec![],
        columns,
        from: Some(from_clause.clone()),
        where_clause: filter_clause,
        group_by: vec![],
        having: None,
        order_by,
        limit: Some(limit),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    Ok(Cte {
        name: Ident::new("__records"),
        columns: None,
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build __total_count CTE using a function as the FROM source
fn build_total_count_cte_from_function(
    conn: &ConnectionBuilder,
    block_name: &str,
    from_clause: &FromClause,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    let select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(count_star())],
        from: Some(from_clause.clone()),
        where_clause: filter_clause,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    Ok(Cte {
        name: Ident::new("__total_count"),
        columns: Some(vec![Ident::new("___total_count")]),
        query: CteQuery::Select(select),
        materialized: None,
    })
}

/// Build pagination CTEs using a function as the FROM source
fn build_pagination_ctes_from_function(
    conn: &ConnectionBuilder,
    block_name: &str,
    from_clause: &FromClause,
    params: &mut ParamCollector,
) -> GraphQLResult<(Cte, Cte)> {
    let limit = conn
        .first
        .or(conn.last)
        .map(|l| std::cmp::min(l, conn.max_rows))
        .unwrap_or(conn.max_rows);

    let offset = conn.offset.unwrap_or(0);

    // Determine if this is reverse pagination
    let is_reverse = conn.before.is_some() || (conn.last.is_some() && conn.first.is_none());

    // Get order by (reversed if using reverse pagination)
    let order_by_builder = if is_reverse {
        conn.order_by.reverse()
    } else {
        conn.order_by.clone()
    };

    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;
    let order_by = build_order_by_exprs(&order_by_builder, block_name);

    let page_plus_1_select = SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Integer(1)))],
        from: Some(from_clause.clone()),
        where_clause: filter_clause.clone(),
        group_by: vec![],
        having: None,
        order_by: order_by.clone(),
        limit: Some(limit + 1),
        offset: if offset > 0 { Some(offset) } else { None },
    };

    let has_more_query = CteQuery::Select(SelectStmt {
        ctes: vec![Cte {
            name: Ident::new("page_plus_1"),
            columns: None,
            query: CteQuery::Select(page_plus_1_select),
            materialized: None,
        }],
        columns: vec![SelectColumn::expr(Expr::BinaryOp {
            left: Box::new(count_star()),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(limit as i64))),
        })],
        from: Some(FromClause::Table {
            schema: None,
            name: Ident::new("page_plus_1"),
            alias: None,
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    });

    let offset_check_query = CteQuery::Select(SelectStmt {
        ctes: vec![],
        columns: vec![SelectColumn::expr(Expr::Literal(Literal::Bool(offset > 0)))],
        from: None,
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    });

    // For reverse pagination (before/last), swap hasNextPage and hasPreviousPage
    let (next_query, prev_query) = if is_reverse {
        (offset_check_query, has_more_query)
    } else {
        (has_more_query, offset_check_query)
    };

    let has_next_cte = Cte {
        name: Ident::new("__has_next_page"),
        columns: Some(vec![Ident::new("___has_next_page")]),
        query: next_query,
        materialized: None,
    };

    let has_prev_cte = Cte {
        name: Ident::new("__has_previous_page"),
        columns: Some(vec![Ident::new("___has_previous_page")]),
        query: prev_query,
        materialized: None,
    };

    Ok((has_next_cte, has_prev_cte))
}

/// Build aggregates CTE using a function as the FROM source
fn build_aggregates_cte_from_function(
    conn: &ConnectionBuilder,
    block_name: &str,
    from_clause: &FromClause,
    agg_builder: Option<&AggregateBuilder>,
    params: &mut ParamCollector,
) -> GraphQLResult<Cte> {
    let Some(agg_builder) = agg_builder else {
        return Ok(Cte {
            name: Ident::new("__aggregates"),
            columns: Some(vec![Ident::new("agg_result")]),
            query: CteQuery::Select(SelectStmt {
                ctes: vec![],
                columns: vec![SelectColumn::expr(Expr::Cast {
                    expr: Box::new(Expr::Literal(Literal::Null)),
                    target_type: super::type_name_to_sql_type("jsonb"),
                })],
                from: None,
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            materialized: None,
        });
    };

    let agg_pairs = build_aggregate_select_list(agg_builder, block_name);
    let filter_clause = build_filter_expr(&conn.filter, &conn.source.table, block_name, params)?;

    Ok(Cte {
        name: Ident::new("__aggregates"),
        columns: Some(vec![Ident::new("agg_result")]),
        query: CteQuery::Select(SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::expr(jsonb_build_object(agg_pairs))],
            from: Some(from_clause.clone()),
            where_clause: filter_clause,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
        materialized: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_records_cte() {
        let cte = build_has_records_cte();
        assert_eq!(cte.name.0, "__has_records");
    }
}
