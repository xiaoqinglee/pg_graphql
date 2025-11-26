//! Common Table Expression (CTE) support
//!
//! CTEs are used extensively in pg_graphql for atomic mutations and
//! complex query composition.

use super::expr::Ident;
use super::stmt::{DeleteStmt, InsertStmt, SelectStmt, UpdateStmt};

/// A Common Table Expression (CTE) in a WITH clause
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    /// Name of the CTE
    pub name: Ident,
    /// Optional column list: WITH cte(col1, col2) AS (...)
    pub columns: Option<Vec<Ident>>,
    /// The query that defines the CTE
    pub query: CteQuery,
    /// MATERIALIZED / NOT MATERIALIZED hint
    pub materialized: Option<bool>,
}

impl Cte {
    /// Create a new CTE with a SELECT query
    pub fn select(name: impl Into<Ident>, query: SelectStmt) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: CteQuery::Select(query),
            materialized: None,
        }
    }

    /// Create a new CTE with an INSERT query
    pub fn insert(name: impl Into<Ident>, query: InsertStmt) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: CteQuery::Insert(query),
            materialized: None,
        }
    }

    /// Create a new CTE with an UPDATE query
    pub fn update(name: impl Into<Ident>, query: UpdateStmt) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: CteQuery::Update(query),
            materialized: None,
        }
    }

    /// Create a new CTE with a DELETE query
    pub fn delete(name: impl Into<Ident>, query: DeleteStmt) -> Self {
        Self {
            name: name.into(),
            columns: None,
            query: CteQuery::Delete(query),
            materialized: None,
        }
    }

    /// Add column aliases to the CTE
    pub fn with_columns(mut self, columns: Vec<impl Into<Ident>>) -> Self {
        self.columns = Some(columns.into_iter().map(|c| c.into()).collect());
        self
    }

    /// Mark the CTE as MATERIALIZED
    pub fn materialized(mut self) -> Self {
        self.materialized = Some(true);
        self
    }

    /// Mark the CTE as NOT MATERIALIZED
    pub fn not_materialized(mut self) -> Self {
        self.materialized = Some(false);
        self
    }
}

/// The query that defines a CTE
///
/// CTEs can contain any DML statement (SELECT, INSERT, UPDATE, DELETE).
/// This is particularly useful for data-modifying CTEs.
#[derive(Debug, Clone, PartialEq)]
pub enum CteQuery {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

impl From<SelectStmt> for CteQuery {
    fn from(stmt: SelectStmt) -> Self {
        Self::Select(stmt)
    }
}

impl From<InsertStmt> for CteQuery {
    fn from(stmt: InsertStmt) -> Self {
        Self::Insert(stmt)
    }
}

impl From<UpdateStmt> for CteQuery {
    fn from(stmt: UpdateStmt) -> Self {
        Self::Update(stmt)
    }
}

impl From<DeleteStmt> for CteQuery {
    fn from(stmt: DeleteStmt) -> Self {
        Self::Delete(stmt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{InsertValues, SelectColumn};

    #[test]
    fn test_cte_select() {
        let cte = Cte::select(
            "active_users",
            SelectStmt::columns(vec![SelectColumn::star()]),
        );

        assert_eq!(cte.name.0, "active_users");
        assert!(matches!(cte.query, CteQuery::Select(_)));
    }

    #[test]
    fn test_cte_insert() {
        let insert = InsertStmt::new("users", vec![], InsertValues::DefaultValues);
        let cte = Cte::insert("new_user", insert).with_columns(vec!["id", "name"]);

        assert_eq!(cte.columns.unwrap().len(), 2);
    }

    #[test]
    fn test_cte_materialized() {
        let cte = Cte::select("temp", SelectStmt::new()).materialized();
        assert_eq!(cte.materialized, Some(true));

        let cte = Cte::select("temp", SelectStmt::new()).not_materialized();
        assert_eq!(cte.materialized, Some(false));
    }
}
