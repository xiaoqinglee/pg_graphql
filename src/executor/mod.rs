//! Execution module for pg_graphql
//!
//! This module handles the execution of SQL statements generated from the AST.
//! It provides:
//!
//! - Execution plans that can contain one or more SQL statements
//! - Telemetry and logging for debugging
//! - pgrx-specific execution backend
//!
//! # Architecture
//!
//! The executor is designed to support future features like nested inserts
//! that require multiple SQL statements to be executed in a single transaction.
//!
//! ```text
//! ExecutionPlan
//!   └── ExecutionStep[]
//!         ├── stmt: Stmt (AST)
//!         ├── params: Vec<Param>
//!         └── depends_on: Vec<StepId>
//! ```
//!
//! For now, all operations use single-step plans, but the infrastructure
//! is in place for multi-step execution.

mod plan;
mod telemetry;

#[cfg(feature = "pg18")]
mod pgrx_backend;
#[cfg(feature = "pg17")]
mod pgrx_backend;
#[cfg(feature = "pg16")]
mod pgrx_backend;
#[cfg(feature = "pg15")]
mod pgrx_backend;
#[cfg(feature = "pg14")]
mod pgrx_backend;

pub use plan::*;
pub use telemetry::*;

// Re-export pgrx_backend when building with pgrx
#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
pub use pgrx_backend::*;
