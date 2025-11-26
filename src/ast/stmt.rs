//! SQL statement types
//!
//! This module defines the top-level SQL statement types: SELECT, INSERT, UPDATE, DELETE.

use super::cte::Cte;
use super::expr::{Expr, FunctionCall, Ident, OrderByExpr};

/// Top-level SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

impl Stmt {
    pub fn select(stmt: SelectStmt) -> Self {
        Self::Select(stmt)
    }

    pub fn insert(stmt: InsertStmt) -> Self {
        Self::Insert(stmt)
    }

    pub fn update(stmt: UpdateStmt) -> Self {
        Self::Update(stmt)
    }

    pub fn delete(stmt: DeleteStmt) -> Self {
        Self::Delete(stmt)
    }
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SelectStmt {
    /// WITH clause (CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT columns
    pub columns: Vec<SelectColumn>,
    /// FROM clause
    pub from: Option<FromClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// GROUP BY clause
    pub group_by: Vec<Expr>,
    /// HAVING clause
    pub having: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT
    pub limit: Option<u64>,
    /// OFFSET
    pub offset: Option<u64>,
}

impl SelectStmt {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a simple SELECT with columns
    pub fn columns(columns: Vec<SelectColumn>) -> Self {
        Self {
            columns,
            ..Default::default()
        }
    }

    pub fn with_from(mut self, from: FromClause) -> Self {
        self.from = Some(from);
        self
    }

    pub fn with_where(mut self, expr: Expr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    pub fn with_ctes(mut self, ctes: Vec<Cte>) -> Self {
        self.ctes = ctes;
        self
    }

    pub fn with_order_by(mut self, order_by: Vec<OrderByExpr>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_group_by(mut self, group_by: Vec<Expr>) -> Self {
        self.group_by = group_by;
        self
    }
}

/// A column in a SELECT clause
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    /// An expression with optional alias: expr AS alias
    Expr { expr: Expr, alias: Option<Ident> },
    /// All columns: *
    Star,
    /// All columns from a table: table.*
    QualifiedStar { table: Ident },
}

impl SelectColumn {
    /// Create an expression column without alias
    pub fn expr(expr: Expr) -> Self {
        Self::Expr { expr, alias: None }
    }

    /// Create an expression column with alias
    pub fn expr_as(expr: Expr, alias: impl Into<Ident>) -> Self {
        Self::Expr {
            expr,
            alias: Some(alias.into()),
        }
    }

    /// Create a star (SELECT *)
    pub fn star() -> Self {
        Self::Star
    }

    /// Create a qualified star (SELECT table.*)
    pub fn qualified_star(table: impl Into<Ident>) -> Self {
        Self::QualifiedStar {
            table: table.into(),
        }
    }
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    /// Simple table reference
    Table {
        schema: Option<Ident>,
        name: Ident,
        alias: Option<Ident>,
    },
    /// Subquery
    Subquery {
        query: Box<SelectStmt>,
        alias: Ident,
    },
    /// Function call as table source
    Function { call: FunctionCall, alias: Ident },
    /// JOIN clause
    Join {
        left: Box<FromClause>,
        join_type: JoinType,
        right: Box<FromClause>,
        on: Option<Expr>,
    },
    /// CROSS JOIN
    CrossJoin {
        left: Box<FromClause>,
        right: Box<FromClause>,
    },
    /// LATERAL subquery
    Lateral {
        subquery: Box<SelectStmt>,
        alias: Ident,
    },
}

impl FromClause {
    /// Create a simple table reference
    pub fn table(name: impl Into<Ident>) -> Self {
        Self::Table {
            schema: None,
            name: name.into(),
            alias: None,
        }
    }

    /// Create a schema-qualified table reference
    pub fn qualified_table(schema: impl Into<Ident>, name: impl Into<Ident>) -> Self {
        Self::Table {
            schema: Some(schema.into()),
            name: name.into(),
            alias: None,
        }
    }

    /// Create a table reference with alias
    pub fn table_alias(
        schema: Option<impl Into<Ident>>,
        name: impl Into<Ident>,
        alias: impl Into<Ident>,
    ) -> Self {
        Self::Table {
            schema: schema.map(|s| s.into()),
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    /// Create a subquery source
    pub fn subquery(query: SelectStmt, alias: impl Into<Ident>) -> Self {
        Self::Subquery {
            query: Box::new(query),
            alias: alias.into(),
        }
    }

    /// Create a function call source
    pub fn function(call: FunctionCall, alias: impl Into<Ident>) -> Self {
        Self::Function {
            call,
            alias: alias.into(),
        }
    }

    /// Add an alias to this FROM clause
    pub fn with_alias(self, alias: impl Into<Ident>) -> Self {
        match self {
            Self::Table { schema, name, .. } => Self::Table {
                schema,
                name,
                alias: Some(alias.into()),
            },
            _ => self,
        }
    }

    /// Create a LEFT JOIN
    pub fn left_join(self, right: FromClause, on: Expr) -> Self {
        Self::Join {
            left: Box::new(self),
            join_type: JoinType::Left,
            right: Box::new(right),
            on: Some(on),
        }
    }

    /// Create an INNER JOIN
    pub fn inner_join(self, right: FromClause, on: Expr) -> Self {
        Self::Join {
            left: Box::new(self),
            join_type: JoinType::Inner,
            right: Box::new(right),
            on: Some(on),
        }
    }

    /// Create a CROSS JOIN
    pub fn cross_join(self, right: FromClause) -> Self {
        Self::CrossJoin {
            left: Box::new(self),
            right: Box::new(right),
        }
    }
}

/// JOIN type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Inner => "inner join",
            Self::Left => "left join",
            Self::Right => "right join",
            Self::Full => "full join",
        }
    }
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    /// WITH clause (CTEs)
    pub ctes: Vec<Cte>,
    /// Target schema
    pub schema: Option<Ident>,
    /// Target table
    pub table: Ident,
    /// Target columns
    pub columns: Vec<Ident>,
    /// Values to insert
    pub values: InsertValues,
    /// RETURNING clause
    pub returning: Vec<SelectColumn>,
    /// ON CONFLICT clause (for upserts)
    pub on_conflict: Option<OnConflict>,
}

impl InsertStmt {
    pub fn new(table: impl Into<Ident>, columns: Vec<Ident>, values: InsertValues) -> Self {
        Self {
            ctes: vec![],
            schema: None,
            table: table.into(),
            columns,
            values,
            returning: vec![],
            on_conflict: None,
        }
    }

    pub fn with_schema(mut self, schema: impl Into<Ident>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_returning(mut self, returning: Vec<SelectColumn>) -> Self {
        self.returning = returning;
        self
    }

    pub fn with_ctes(mut self, ctes: Vec<Cte>) -> Self {
        self.ctes = ctes;
        self
    }
}

/// Values for INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub enum InsertValues {
    /// VALUES (row1), (row2), ...
    Values(Vec<Vec<Expr>>),
    /// INSERT ... SELECT ...
    Query(Box<SelectStmt>),
    /// DEFAULT VALUES
    DefaultValues,
}

/// ON CONFLICT clause for upserts
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub target: OnConflictTarget,
    pub action: OnConflictAction,
}

/// Target for ON CONFLICT
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictTarget {
    /// ON CONFLICT (column1, column2)
    Columns(Vec<Ident>),
    /// ON CONFLICT ON CONSTRAINT constraint_name
    Constraint(Ident),
}

/// Action for ON CONFLICT
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    /// DO NOTHING
    DoNothing,
    /// DO UPDATE SET ...
    DoUpdate {
        set: Vec<(Ident, Expr)>,
        where_clause: Option<Expr>,
    },
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    /// WITH clause (CTEs)
    pub ctes: Vec<Cte>,
    /// Target schema
    pub schema: Option<Ident>,
    /// Target table
    pub table: Ident,
    /// Table alias
    pub alias: Option<Ident>,
    /// SET clause: column = value pairs
    pub set: Vec<(Ident, Expr)>,
    /// FROM clause (for UPDATE ... FROM ...)
    pub from: Option<FromClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// RETURNING clause
    pub returning: Vec<SelectColumn>,
}

impl UpdateStmt {
    pub fn new(table: impl Into<Ident>, set: Vec<(Ident, Expr)>) -> Self {
        Self {
            ctes: vec![],
            schema: None,
            table: table.into(),
            alias: None,
            set,
            from: None,
            where_clause: None,
            returning: vec![],
        }
    }

    pub fn with_schema(mut self, schema: impl Into<Ident>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_alias(mut self, alias: impl Into<Ident>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn with_where(mut self, expr: Expr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    pub fn with_returning(mut self, returning: Vec<SelectColumn>) -> Self {
        self.returning = returning;
        self
    }

    pub fn with_ctes(mut self, ctes: Vec<Cte>) -> Self {
        self.ctes = ctes;
        self
    }
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    /// WITH clause (CTEs)
    pub ctes: Vec<Cte>,
    /// Target schema
    pub schema: Option<Ident>,
    /// Target table
    pub table: Ident,
    /// Table alias
    pub alias: Option<Ident>,
    /// USING clause (for DELETE ... USING ...)
    pub using: Option<FromClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// RETURNING clause
    pub returning: Vec<SelectColumn>,
}

impl DeleteStmt {
    pub fn new(table: impl Into<Ident>) -> Self {
        Self {
            ctes: vec![],
            schema: None,
            table: table.into(),
            alias: None,
            using: None,
            where_clause: None,
            returning: vec![],
        }
    }

    pub fn with_schema(mut self, schema: impl Into<Ident>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_alias(mut self, alias: impl Into<Ident>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn with_where(mut self, expr: Expr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    pub fn with_returning(mut self, returning: Vec<SelectColumn>) -> Self {
        self.returning = returning;
        self
    }

    pub fn with_ctes(mut self, ctes: Vec<Cte>) -> Self {
        self.ctes = ctes;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_builder() {
        let stmt = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_limit(10);

        assert!(matches!(stmt.from, Some(FromClause::Table { .. })));
        assert_eq!(stmt.limit, Some(10));
    }

    #[test]
    fn test_from_clause_join() {
        let from = FromClause::table("users").with_alias("u").left_join(
            FromClause::table("orders").with_alias("o"),
            Expr::qualified_column("u", "id").eq(Expr::qualified_column("o", "user_id")),
        );

        assert!(matches!(
            from,
            FromClause::Join {
                join_type: JoinType::Left,
                ..
            }
        ));
    }

    #[test]
    fn test_insert_stmt() {
        let stmt = InsertStmt::new(
            "users",
            vec![Ident::new("name"), Ident::new("email")],
            InsertValues::Values(vec![vec![
                Expr::string("Alice"),
                Expr::string("alice@example.com"),
            ]]),
        )
        .with_schema("public")
        .with_returning(vec![SelectColumn::star()]);

        assert_eq!(stmt.table.0, "users");
        assert_eq!(stmt.columns.len(), 2);
    }
}
