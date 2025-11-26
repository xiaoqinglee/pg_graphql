//! AST-based transpilation for FilterBuilder
//!
//! This module implements filter expression building using the AST.
//! Filters are used in WHERE clauses for queries and mutations.

use super::{add_param_from_json, column_ref};
use crate::ast::{BinaryOperator, Expr, Literal, ParamCollector, UnaryOperator};
use crate::builder::{CompoundFilterBuilder, FilterBuilder, FilterBuilderElem};
use crate::error::{GraphQLError, GraphQLResult};
use crate::graphql::FilterOp;
use crate::sql_types::Table;

/// Build a WHERE clause expression from a FilterBuilder
pub fn build_filter_expr(
    filter: &FilterBuilder,
    table: &Table,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Option<Expr>> {
    if filter.elems.is_empty() {
        return Ok(None);
    }

    let mut conditions = Vec::new();

    for elem in &filter.elems {
        let expr = build_filter_elem_expr(elem, table, block_name, params)?;
        conditions.push(expr);
    }

    // Combine all conditions with AND
    Ok(Some(combine_with_and(conditions)))
}

/// Combine multiple expressions with AND
pub fn combine_with_and(mut conditions: Vec<Expr>) -> Expr {
    if conditions.len() == 1 {
        conditions.remove(0)
    } else {
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(cond),
            };
        }
        combined
    }
}

/// Combine multiple expressions with OR
fn combine_with_or(mut conditions: Vec<Expr>) -> Expr {
    if conditions.len() == 1 {
        conditions.remove(0)
    } else {
        let mut combined = conditions.remove(0);
        for cond in conditions {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::Or,
                right: Box::new(cond),
            };
        }
        combined
    }
}

/// Build expression for a single filter element
fn build_filter_elem_expr(
    elem: &FilterBuilderElem,
    table: &Table,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    match elem {
        FilterBuilderElem::Column { column, op, value } => {
            let col_expr = column_ref(block_name, &column.name);

            match op {
                FilterOp::Is => {
                    // IS NULL / IS NOT NULL
                    let is_null = match value {
                        serde_json::Value::String(s) => match s.as_str() {
                            "NULL" => true,
                            "NOT_NULL" => false,
                            _ => {
                                return Err(GraphQLError::sql_generation(
                                    "Error transpiling Is filter value",
                                ))
                            }
                        },
                        _ => {
                            return Err(GraphQLError::sql_generation(
                                "Error transpiling Is filter value type",
                            ))
                        }
                    };

                    Ok(Expr::IsNull {
                        expr: Box::new(col_expr),
                        negated: !is_null,
                    })
                }
                FilterOp::In | FilterOp::Contains | FilterOp::ContainedBy | FilterOp::Overlap => {
                    // Array operations use array cast
                    let cast_type_name = match op {
                        FilterOp::In | FilterOp::Contains => format!("{}[]", column.type_name),
                        _ => column.type_name.clone(),
                    };

                    let param_expr = add_param_from_json(params, value, &cast_type_name)?;

                    let binary_op = match op {
                        FilterOp::In => BinaryOperator::Any,
                        FilterOp::Contains => BinaryOperator::Contains,
                        FilterOp::ContainedBy => BinaryOperator::ContainedBy,
                        FilterOp::Overlap => BinaryOperator::Overlap,
                        _ => unreachable!(),
                    };

                    Ok(Expr::BinaryOp {
                        left: Box::new(col_expr),
                        op: binary_op,
                        right: Box::new(param_expr),
                    })
                }
                _ => {
                    // If the value is null for comparison operators, comparing with NULL
                    // always produces NULL (unknown), so we should return FALSE for
                    // consistent filtering semantics
                    if value.is_null() {
                        return Ok(Expr::Literal(Literal::Bool(false)));
                    }

                    // Standard comparison operators
                    let param_expr = add_param_from_json(params, value, &column.type_name)?;

                    let binary_op = match op {
                        FilterOp::Equal => BinaryOperator::Eq,
                        FilterOp::NotEqual => BinaryOperator::NotEq,
                        FilterOp::LessThan => BinaryOperator::Lt,
                        FilterOp::LessThanEqualTo => BinaryOperator::LtEq,
                        FilterOp::GreaterThan => BinaryOperator::Gt,
                        FilterOp::GreaterThanEqualTo => BinaryOperator::GtEq,
                        FilterOp::Like => BinaryOperator::Like,
                        FilterOp::ILike => BinaryOperator::ILike,
                        FilterOp::RegEx => BinaryOperator::RegEx,
                        FilterOp::IRegEx => BinaryOperator::IRegEx,
                        FilterOp::StartsWith => BinaryOperator::StartsWith,
                        _ => unreachable!(),
                    };

                    Ok(Expr::BinaryOp {
                        left: Box::new(col_expr),
                        op: binary_op,
                        right: Box::new(param_expr),
                    })
                }
            }
        }
        FilterBuilderElem::NodeId(node_id_instance) => {
            // Validate that nodeId belongs to this table
            if (&node_id_instance.schema_name, &node_id_instance.table_name)
                != (&table.schema, &table.name)
            {
                return Err(GraphQLError::validation(
                    "nodeId belongs to a different collection",
                ));
            }

            // Get primary key columns
            let pk_columns = table.primary_key_columns();

            if pk_columns.len() != node_id_instance.values.len() {
                return Err(GraphQLError::validation(
                    "NodeId value count doesn't match primary key columns",
                ));
            }

            // Build conditions for each primary key column
            let mut conditions = Vec::new();
            for (col, value) in pk_columns.iter().zip(&node_id_instance.values) {
                let col_expr = column_ref(block_name, &col.name);
                let param_expr = add_param_from_json(params, value, &col.type_name)?;

                conditions.push(Expr::BinaryOp {
                    left: Box::new(col_expr),
                    op: BinaryOperator::Eq,
                    right: Box::new(param_expr),
                });
            }

            Ok(combine_with_and(conditions))
        }
        FilterBuilderElem::Compound(compound) => {
            build_compound_filter_expr(compound, table, block_name, params)
        }
    }
}

/// Build expression for a compound filter (AND/OR/NOT)
fn build_compound_filter_expr(
    compound: &CompoundFilterBuilder,
    table: &Table,
    block_name: &str,
    params: &mut ParamCollector,
) -> GraphQLResult<Expr> {
    match compound {
        CompoundFilterBuilder::And(elems) => {
            let mut conditions = Vec::new();
            for elem in elems {
                let expr = build_filter_elem_expr(elem, table, block_name, params)?;
                conditions.push(expr);
            }

            if conditions.is_empty() {
                // Empty AND is true
                Ok(Expr::Literal(Literal::Bool(true)))
            } else {
                Ok(combine_with_and(conditions))
            }
        }
        CompoundFilterBuilder::Or(elems) => {
            let mut conditions = Vec::new();
            for elem in elems {
                let expr = build_filter_elem_expr(elem, table, block_name, params)?;
                conditions.push(expr);
            }

            if conditions.is_empty() {
                // Empty OR is false
                Ok(Expr::Literal(Literal::Bool(false)))
            } else {
                Ok(combine_with_or(conditions))
            }
        }
        CompoundFilterBuilder::Not(elem) => {
            let expr = build_filter_elem_expr(elem, table, block_name, params)?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full testing requires setting up Table, Column, FilterBuilder etc.
    // Integration tests via pg_regress are more appropriate.

    #[test]
    fn test_filter_ops_mapping() {
        // Just verify the operator mappings compile correctly
        let ops = [
            (FilterOp::Equal, BinaryOperator::Eq),
            (FilterOp::NotEqual, BinaryOperator::NotEq),
            (FilterOp::LessThan, BinaryOperator::Lt),
            (FilterOp::GreaterThan, BinaryOperator::Gt),
        ];

        for (filter_op, binary_op) in ops {
            // Just check the mapping exists
            let _ = format!("{:?} -> {:?}", filter_op, binary_op);
        }
    }

    #[test]
    fn test_combine_with_and() {
        let exprs = vec![
            Expr::Literal(Literal::Bool(true)),
            Expr::Literal(Literal::Bool(false)),
        ];

        let combined = combine_with_and(exprs);
        match combined {
            Expr::BinaryOp { op, .. } => assert_eq!(op, BinaryOperator::And),
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_combine_with_or() {
        let exprs = vec![
            Expr::Literal(Literal::Bool(true)),
            Expr::Literal(Literal::Bool(false)),
        ];

        let combined = combine_with_or(exprs);
        match combined {
            Expr::BinaryOp { op, .. } => assert_eq!(op, BinaryOperator::Or),
            _ => panic!("Expected BinaryOp"),
        }
    }
}
