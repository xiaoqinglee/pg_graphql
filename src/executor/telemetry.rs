//! Telemetry and logging for SQL execution
//!
//! This module provides configurable logging for debugging SQL generation
//! and execution. It can be controlled via environment variables or
//! PostgreSQL GUC settings.
//!
//! # Configuration
//!
//! Set the `PG_GRAPHQL_LOG_LEVEL` environment variable to one of:
//! - `off` - No logging (default)
//! - `basic` - Log SQL and timing only
//! - `detailed` - Log SQL, parameters, and GraphQL context
//! - `debug` - Log everything including AST dumps
//!
//! # Example
//!
//! ```bash
//! export PG_GRAPHQL_LOG_LEVEL=detailed
//! ```

use crate::ast::Param;
use std::time::Instant;

/// Log level for SQL telemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// No logging
    Off = 0,
    /// Basic info: SQL and timing
    Basic = 1,
    /// Detailed: SQL, parameters, GraphQL context
    Detailed = 2,
    /// Debug: Everything including AST
    Debug = 3,
}

impl LogLevel {
    /// Parse from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "basic" => Self::Basic,
            "detailed" => Self::Detailed,
            "debug" => Self::Debug,
            _ => Self::Off,
        }
    }
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Off
    }
}

/// Get current log level from environment
///
/// Checks `PG_GRAPHQL_LOG_LEVEL` environment variable.
pub fn get_log_level() -> LogLevel {
    std::env::var("PG_GRAPHQL_LOG_LEVEL")
        .map(|s| LogLevel::from_str(&s))
        .unwrap_or(LogLevel::Off)
}

/// Log an execution plan
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
pub fn log_plan(plan: &super::ExecutionPlan) {
    let level = get_log_level();
    if level < LogLevel::Basic {
        return;
    }

    pgrx::info!(
        "pg_graphql: Execution plan with {} step(s), elapsed: {}ms",
        plan.steps.len(),
        plan.telemetry.elapsed_ms()
    );

    if level >= LogLevel::Detailed {
        if let Some(query) = &plan.telemetry.graphql_query {
            let truncated = if query.len() > 500 {
                format!("{}...", &query[..500])
            } else {
                query.clone()
            };
            pgrx::info!("pg_graphql: GraphQL: {}", truncated);
        }

        if let Some(op_name) = &plan.telemetry.operation_name {
            pgrx::info!("pg_graphql: Operation: {}", op_name);
        }

        for (key, value) in &plan.telemetry.tags {
            pgrx::info!("pg_graphql: Tag {}: {}", key, value);
        }
    }
}

/// Log SQL execution
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
pub fn log_sql(sql: &str, params: &[Param]) {
    let level = get_log_level();
    if level < LogLevel::Basic {
        return;
    }

    // Truncate very long SQL for basic logging
    let sql_display = if level >= LogLevel::Detailed {
        sql.to_string()
    } else if sql.len() > 1000 {
        format!("{}...", &sql[..1000])
    } else {
        sql.to_string()
    };

    pgrx::info!("pg_graphql: SQL:\n{}", sql_display);

    if level >= LogLevel::Detailed && !params.is_empty() {
        for param in params {
            pgrx::info!(
                "pg_graphql: Param ${}: {:?} ({})",
                param.index,
                param.value,
                param.sql_type
            );
        }
    }
}

/// Log execution result
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
pub fn log_result(start: Instant, success: bool) {
    let level = get_log_level();
    if level < LogLevel::Basic {
        return;
    }

    let duration_ms = start.elapsed().as_millis();
    let status = if success { "completed" } else { "failed" };

    pgrx::info!("pg_graphql: Execution {} in {}ms", status, duration_ms);
}

/// Log an error
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
pub fn log_error(context: &str, error: &str) {
    let level = get_log_level();
    if level < LogLevel::Basic {
        return;
    }

    pgrx::warning!("pg_graphql: Error in {}: {}", context, error);
}

// Non-pgrx versions for testing

#[cfg(not(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
)))]
pub fn log_plan(_plan: &super::ExecutionPlan) {}

#[cfg(not(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
)))]
pub fn log_sql(_sql: &str, _params: &[Param]) {}

#[cfg(not(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
)))]
pub fn log_result(_start: Instant, _success: bool) {}

#[cfg(not(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
)))]
pub fn log_error(_context: &str, _error: &str) {}

/// A guard that logs execution timing on drop
pub struct ExecutionTimer {
    start: Instant,
    context: String,
    #[allow(dead_code)]
    logged: bool,
}

impl ExecutionTimer {
    /// Start a new execution timer
    pub fn new(context: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            context: context.into(),
            logged: false,
        }
    }

    /// Get elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    /// Mark as successful and log
    pub fn success(mut self) {
        self.logged = true;
        log_result(self.start, true);
    }

    /// Mark as failed and log
    pub fn failure(mut self, error: &str) {
        self.logged = true;
        log_error(&self.context, error);
        log_result(self.start, false);
    }
}

impl Drop for ExecutionTimer {
    fn drop(&mut self) {
        // Log if not already logged (implicit failure)
        if !self.logged {
            log_result(self.start, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_parsing() {
        assert_eq!(LogLevel::from_str("off"), LogLevel::Off);
        assert_eq!(LogLevel::from_str("basic"), LogLevel::Basic);
        assert_eq!(LogLevel::from_str("detailed"), LogLevel::Detailed);
        assert_eq!(LogLevel::from_str("debug"), LogLevel::Debug);
        assert_eq!(LogLevel::from_str("BASIC"), LogLevel::Basic);
        assert_eq!(LogLevel::from_str("invalid"), LogLevel::Off);
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Off < LogLevel::Basic);
        assert!(LogLevel::Basic < LogLevel::Detailed);
        assert!(LogLevel::Detailed < LogLevel::Debug);
    }

    #[test]
    fn test_execution_timer() {
        let timer = ExecutionTimer::new("test");
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(timer.elapsed_ms() >= 10);
    }
}
