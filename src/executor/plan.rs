//! Execution plans for SQL statements
//!
//! An execution plan represents one or more SQL statements that need to be
//! executed together. Currently, all GraphQL operations result in single-step
//! plans, but the infrastructure supports multi-step plans for future features
//! like nested inserts.

use crate::ast::{Param, Stmt};
use std::time::Instant;

/// An execution plan containing one or more SQL statements
#[derive(Debug)]
pub struct ExecutionPlan {
    /// The steps to execute
    pub steps: Vec<ExecutionStep>,
    /// Telemetry information for debugging
    pub telemetry: PlanTelemetry,
}

/// A single step in an execution plan
#[derive(Debug)]
pub struct ExecutionStep {
    /// Unique identifier for this step
    pub id: String,
    /// The SQL statement (as AST)
    pub stmt: Stmt,
    /// Parameters for this statement
    pub params: Vec<Param>,
    /// Human-readable description of what this step does
    pub description: String,
    /// IDs of steps that must complete before this one
    pub depends_on: Vec<String>,
}

/// Telemetry information attached to an execution plan
#[derive(Debug)]
pub struct PlanTelemetry {
    /// The original GraphQL query (if available)
    pub graphql_query: Option<String>,
    /// The operation name (if specified)
    pub operation_name: Option<String>,
    /// When the plan was created
    pub created_at: Instant,
    /// Custom tags for categorization
    pub tags: Vec<(String, String)>,
}

impl Default for PlanTelemetry {
    fn default() -> Self {
        Self {
            graphql_query: None,
            operation_name: None,
            created_at: Instant::now(),
            tags: Vec::new(),
        }
    }
}

impl PlanTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the GraphQL query
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.graphql_query = Some(query.into());
        self
    }

    /// Set the operation name
    pub fn with_operation_name(mut self, name: impl Into<String>) -> Self {
        self.operation_name = Some(name.into());
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push((key.into(), value.into()));
        self
    }

    /// Get elapsed time since plan creation
    pub fn elapsed_ms(&self) -> u128 {
        self.created_at.elapsed().as_millis()
    }
}

impl ExecutionPlan {
    /// Create a single-step execution plan
    ///
    /// This is the most common case: one GraphQL operation = one SQL statement.
    pub fn single(stmt: Stmt, params: Vec<Param>, description: impl Into<String>) -> Self {
        Self {
            steps: vec![ExecutionStep {
                id: "main".to_string(),
                stmt,
                params,
                description: description.into(),
                depends_on: vec![],
            }],
            telemetry: PlanTelemetry::default(),
        }
    }

    /// Create a multi-step execution plan
    ///
    /// Use this for operations that require multiple SQL statements,
    /// such as nested inserts.
    pub fn multi(steps: Vec<ExecutionStep>) -> Self {
        Self {
            steps,
            telemetry: PlanTelemetry::default(),
        }
    }

    /// Attach GraphQL context to the plan
    pub fn with_graphql_context(
        mut self,
        query: impl Into<String>,
        operation_name: Option<impl Into<String>>,
    ) -> Self {
        self.telemetry.graphql_query = Some(query.into());
        self.telemetry.operation_name = operation_name.map(|n| n.into());
        self
    }

    /// Attach telemetry to the plan
    pub fn with_telemetry(mut self, telemetry: PlanTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Add a tag to the plan's telemetry
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.telemetry.tags.push((key.into(), value.into()));
        self
    }

    /// Check if this is a single-step plan
    pub fn is_single_step(&self) -> bool {
        self.steps.len() == 1
    }

    /// Get the number of steps in the plan
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Get the main step (for single-step plans)
    pub fn main_step(&self) -> Option<&ExecutionStep> {
        if self.steps.len() == 1 {
            self.steps.first()
        } else {
            self.steps.iter().find(|s| s.id == "main")
        }
    }

    /// Get a step by ID
    pub fn get_step(&self, id: &str) -> Option<&ExecutionStep> {
        self.steps.iter().find(|s| s.id == id)
    }

    /// Get steps in execution order (respecting dependencies)
    ///
    /// Returns steps sorted so that dependencies come before dependents.
    /// Panics if there are circular dependencies.
    pub fn steps_in_order(&self) -> Vec<&ExecutionStep> {
        // Simple topological sort
        let mut result = Vec::new();
        let mut completed: std::collections::HashSet<&str> = std::collections::HashSet::new();

        while result.len() < self.steps.len() {
            let mut made_progress = false;

            for step in &self.steps {
                if completed.contains(step.id.as_str()) {
                    continue;
                }

                let deps_satisfied = step
                    .depends_on
                    .iter()
                    .all(|dep| completed.contains(dep.as_str()));

                if deps_satisfied {
                    result.push(step);
                    completed.insert(&step.id);
                    made_progress = true;
                }
            }

            if !made_progress && result.len() < self.steps.len() {
                panic!("Circular dependency detected in execution plan");
            }
        }

        result
    }
}

impl ExecutionStep {
    /// Create a new execution step
    pub fn new(
        id: impl Into<String>,
        stmt: Stmt,
        params: Vec<Param>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            stmt,
            params,
            description: description.into(),
            depends_on: vec![],
        }
    }

    /// Add a dependency on another step
    pub fn depends_on(mut self, step_id: impl Into<String>) -> Self {
        self.depends_on.push(step_id.into());
        self
    }

    /// Add multiple dependencies
    pub fn depends_on_all(mut self, step_ids: Vec<impl Into<String>>) -> Self {
        for id in step_ids {
            self.depends_on.push(id.into());
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    fn dummy_stmt() -> Stmt {
        Stmt::Select(SelectStmt::columns(vec![SelectColumn::star()]))
    }

    #[test]
    fn test_single_step_plan() {
        let plan = ExecutionPlan::single(dummy_stmt(), vec![], "test query");

        assert!(plan.is_single_step());
        assert_eq!(plan.step_count(), 1);
        assert!(plan.main_step().is_some());
    }

    #[test]
    fn test_multi_step_plan() {
        let steps = vec![
            ExecutionStep::new("step1", dummy_stmt(), vec![], "first"),
            ExecutionStep::new("step2", dummy_stmt(), vec![], "second").depends_on("step1"),
        ];

        let plan = ExecutionPlan::multi(steps);

        assert!(!plan.is_single_step());
        assert_eq!(plan.step_count(), 2);
    }

    #[test]
    fn test_steps_in_order() {
        let steps = vec![
            ExecutionStep::new("c", dummy_stmt(), vec![], "third")
                .depends_on("a")
                .depends_on("b"),
            ExecutionStep::new("a", dummy_stmt(), vec![], "first"),
            ExecutionStep::new("b", dummy_stmt(), vec![], "second").depends_on("a"),
        ];

        let plan = ExecutionPlan::multi(steps);
        let ordered: Vec<&str> = plan.steps_in_order().iter().map(|s| s.id.as_str()).collect();

        // a must come before b, b must come before c
        let a_idx = ordered.iter().position(|&id| id == "a").unwrap();
        let b_idx = ordered.iter().position(|&id| id == "b").unwrap();
        let c_idx = ordered.iter().position(|&id| id == "c").unwrap();

        assert!(a_idx < b_idx);
        assert!(b_idx < c_idx);
    }

    #[test]
    fn test_telemetry() {
        let plan = ExecutionPlan::single(dummy_stmt(), vec![], "test")
            .with_graphql_context("query { users { id } }", Some("GetUsers"))
            .with_tag("type", "query");

        assert!(plan.telemetry.graphql_query.is_some());
        assert_eq!(
            plan.telemetry.operation_name,
            Some("GetUsers".to_string())
        );
        assert!(!plan.telemetry.tags.is_empty());
    }
}
