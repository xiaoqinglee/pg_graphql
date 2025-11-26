//! pgrx-specific execution backend
//!
//! This module handles the actual execution of SQL statements using pgrx's
//! SPI (Server Programming Interface). It converts AST parameters to pgrx
//! Datums and executes queries.

use crate::ast::{render, Param, ParamValue};
use crate::error::{GraphQLError, GraphQLResult};
use crate::executor::{log_result, log_sql, ExecutionPlan};
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use std::time::Instant;

/// Convert AST parameters to pgrx Datums
///
/// All parameters are converted to text and cast at the SQL level.
/// This matches the existing behavior in transpile.rs.
pub fn params_to_datums(params: &[Param]) -> GraphQLResult<Vec<DatumWithOid<'static>>> {
    params.iter().map(param_to_datum).collect()
}

fn param_to_datum(param: &Param) -> GraphQLResult<DatumWithOid<'static>> {
    let datum = match &param.value {
        ParamValue::Null => {
            let null: Option<String> = None;
            null.into_datum()
        }
        ParamValue::Bool(b) => b.to_string().into_datum(),
        ParamValue::String(s) => s.clone().into_datum(),
        ParamValue::Integer(i) => i.to_string().into_datum(),
        ParamValue::Float(f) => f.to_string().into_datum(),
        ParamValue::Array(arr) => {
            let strings: Vec<Option<String>> = arr
                .iter()
                .map(|v| match v {
                    ParamValue::Null => None,
                    ParamValue::String(s) => Some(s.clone()),
                    ParamValue::Integer(i) => Some(i.to_string()),
                    ParamValue::Float(f) => Some(f.to_string()),
                    ParamValue::Bool(b) => Some(b.to_string()),
                    ParamValue::Array(_) => None, // Nested arrays not supported
                    ParamValue::Json(j) => Some(j.to_string()),
                })
                .collect();
            strings.into_datum()
        }
        ParamValue::Json(v) => v.to_string().into_datum(),
    };

    // Use text OID for all parameters - PostgreSQL will cast as needed
    let oid = if param.sql_type.is_array {
        pgrx::pg_sys::TEXTARRAYOID
    } else {
        pgrx::pg_sys::TEXTOID
    };

    Ok(unsafe { DatumWithOid::new(datum, oid) })
}

/// Execute a query plan and return JSON result
///
/// This is used for SELECT queries (including those with CTEs).
pub fn execute_query(plan: &ExecutionPlan) -> GraphQLResult<serde_json::Value> {
    if !plan.is_single_step() {
        return Err(GraphQLError::internal(
            "Query execution currently only supports single-step plans",
        ));
    }

    let step = plan.main_step().ok_or_else(|| {
        GraphQLError::internal("No main step in execution plan")
    })?;

    let sql = render(&step.stmt);
    let datums = params_to_datums(&step.params)?;

    log_sql(&sql, &step.params);
    let start = Instant::now();

    let result = Spi::connect(|client| {
        let result = client.select(&sql, Some(1), &datums)?;
        if result.is_empty() {
            Ok(serde_json::Value::Null)
        } else {
            let jsonb: pgrx::JsonB = result
                .first()
                .get(1)?
                .ok_or_else(|| spi::Error::InvalidPosition)?;
            Ok(jsonb.0)
        }
    });

    match &result {
        Ok(_) => log_result(start, true),
        Err(e) => {
            log_result(start, false);
            return Err(GraphQLError::sql_execution(format!("SPI error: {:?}", e)));
        }
    }

    result.map_err(|e: spi::Error| GraphQLError::sql_execution(format!("SPI error: {:?}", e)))
}

/// Execute a mutation plan and return JSON result
///
/// This is used for INSERT, UPDATE, DELETE operations.
/// It takes a mutable SPI client to participate in the current transaction.
pub fn execute_mutation<'conn>(
    plan: &ExecutionPlan,
    client: &'conn mut SpiClient<'conn>,
) -> GraphQLResult<(serde_json::Value, &'conn mut SpiClient<'conn>)> {
    if !plan.is_single_step() {
        // TODO: For nested inserts, implement multi-step execution
        return Err(GraphQLError::internal(
            "Mutation execution currently only supports single-step plans",
        ));
    }

    let step = plan.main_step().ok_or_else(|| {
        GraphQLError::internal("No main step in execution plan")
    })?;

    let sql = render(&step.stmt);
    let datums = params_to_datums(&step.params)?;

    log_sql(&sql, &step.params);
    let start = Instant::now();

    let result = client.update(&sql, None, &datums);

    match &result {
        Ok(_) => {}
        Err(_) => {
            log_result(start, false);
            return Err(GraphQLError::sql_execution(
                "Failed to execute mutation",
            ));
        }
    }

    let res_q = result.map_err(|_| {
        GraphQLError::sql_execution("Internal Error: Failed to execute transpiled query")
    })?;

    let jsonb: pgrx::JsonB = match res_q.first().get::<pgrx::JsonB>(1) {
        Ok(Some(dat)) => dat,
        Ok(None) => pgrx::JsonB(serde_json::Value::Null),
        Err(e) => {
            log_result(start, false);
            return Err(GraphQLError::sql_generation(format!(
                "Internal Error: Failed to load result from transpiled query: {e}"
            )));
        }
    };

    log_result(start, true);
    Ok((jsonb.0, client))
}

/// Execute a multi-step mutation plan (for future nested inserts)
///
/// This executes all steps in dependency order within the same transaction.
#[allow(dead_code)]
pub fn execute_multi_step_mutation<'conn>(
    plan: &ExecutionPlan,
    client: &'conn mut SpiClient<'conn>,
) -> GraphQLResult<(Vec<serde_json::Value>, &'conn mut SpiClient<'conn>)> {
    let mut results = Vec::with_capacity(plan.steps.len());

    for step in plan.steps_in_order() {
        let sql = render(&step.stmt);
        let datums = params_to_datums(&step.params)?;

        log_sql(&sql, &step.params);
        let start = Instant::now();

        let result = client.update(&sql, None, &datums).map_err(|_| {
            log_result(start, false);
            GraphQLError::sql_execution(format!(
                "Failed to execute step '{}': {}",
                step.id, step.description
            ))
        })?;

        let jsonb: pgrx::JsonB = match result.first().get::<pgrx::JsonB>(1) {
            Ok(Some(dat)) => dat,
            Ok(None) => pgrx::JsonB(serde_json::Value::Null),
            Err(e) => {
                log_result(start, false);
                return Err(GraphQLError::sql_generation(format!(
                    "Failed to get result from step '{}': {e}",
                    step.id
                )));
            }
        };

        log_result(start, true);
        results.push(jsonb.0);
    }

    Ok((results, client))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_value_conversion() {
        // Test that ParamValue conversions produce the expected string representations
        let string_val = ParamValue::String("hello".into());
        assert!(!string_val.is_null());

        let int_val = ParamValue::Integer(42);
        assert_eq!(int_val.to_sql_literal(), Some("42".to_string()));

        let null_val = ParamValue::Null;
        assert!(null_val.is_null());
        assert_eq!(null_val.to_sql_literal(), None);

        let array_val = ParamValue::Array(vec![
            ParamValue::Integer(1),
            ParamValue::Integer(2),
        ]);
        let literal = array_val.to_sql_literal();
        assert!(literal.is_some());
        assert!(literal.unwrap().contains("1"));
    }

    // Note: pgrx-specific tests (params_to_datums, execute_*) require
    // a PostgreSQL connection and are tested via pg_regress instead.
}
