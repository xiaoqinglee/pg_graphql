//! Comprehensive tests for the AST module
//!
//! These tests verify that the AST correctly represents SQL constructs
//! and renders them properly.

use super::*;

mod expr_tests {
    use super::*;

    #[test]
    fn test_column_expressions() {
        // Simple column
        let col = Expr::column("id");
        let sql = render_expr(&col);
        assert_eq!(sql, "\"id\"");

        // Qualified column
        let col = Expr::qualified_column("users", "email");
        let sql = render_expr(&col);
        assert_eq!(sql, "\"users\".\"email\"");
    }

    #[test]
    fn test_literal_expressions() {
        assert_eq!(render_expr(&Expr::null()), "null");
        assert_eq!(render_expr(&Expr::bool(true)), "true");
        assert_eq!(render_expr(&Expr::bool(false)), "false");
        assert_eq!(render_expr(&Expr::int(42)), "42");
        assert_eq!(render_expr(&Expr::int(-100)), "-100");
        assert_eq!(render_expr(&Expr::string("hello")), "'hello'");
    }

    #[test]
    fn test_binary_operations() {
        // Equality
        let expr = Expr::column("id").eq(Expr::int(1));
        assert!(render_expr(&expr).contains("= 1"));

        // Comparison
        let expr = Expr::binary(Expr::column("age"), BinaryOperator::GtEq, Expr::int(18));
        assert!(render_expr(&expr).contains(">= 18"));

        // Logical AND
        let expr = Expr::column("a")
            .eq(Expr::int(1))
            .and(Expr::column("b").eq(Expr::int(2)));
        let sql = render_expr(&expr);
        assert!(sql.contains("and"));

        // Logical OR
        let expr = Expr::column("a")
            .eq(Expr::int(1))
            .or(Expr::column("b").eq(Expr::int(2)));
        let sql = render_expr(&expr);
        assert!(sql.contains("or"));
    }

    #[test]
    fn test_is_null() {
        let expr = Expr::is_null(Expr::column("deleted_at"));
        assert!(render_expr(&expr).contains("is null"));

        let expr = Expr::is_not_null(Expr::column("deleted_at"));
        assert!(render_expr(&expr).contains("is not null"));
    }

    #[test]
    fn test_in_list() {
        let expr = Expr::InList {
            expr: Box::new(Expr::column("status")),
            list: vec![Expr::string("active"), Expr::string("pending")],
            negated: false,
        };
        let sql = render_expr(&expr);
        assert!(sql.contains("in ("));
        assert!(sql.contains("'active'"));
        assert!(sql.contains("'pending'"));

        let expr = Expr::InList {
            expr: Box::new(Expr::column("status")),
            list: vec![Expr::string("deleted")],
            negated: true,
        };
        let sql = render_expr(&expr);
        assert!(sql.contains("not in ("));
    }

    #[test]
    fn test_between() {
        let expr = Expr::Between {
            expr: Box::new(Expr::column("age")),
            low: Box::new(Expr::int(18)),
            high: Box::new(Expr::int(65)),
            negated: false,
        };
        let sql = render_expr(&expr);
        assert!(sql.contains("between"));
        assert!(sql.contains("18"));
        assert!(sql.contains("65"));
    }

    #[test]
    fn test_type_cast() {
        let expr = Expr::cast(Expr::column("id"), SqlType::text());
        let sql = render_expr(&expr);
        assert!(sql.contains("::text"));
    }

    #[test]
    fn test_function_call() {
        let expr = Expr::function("lower", vec![Expr::column("name")]);
        let sql = render_expr(&expr);
        assert!(sql.contains("\"lower\"("));
    }

    #[test]
    fn test_coalesce() {
        let expr = Expr::coalesce(vec![
            Expr::column("nickname"),
            Expr::column("name"),
            Expr::string("Unknown"),
        ]);
        let sql = render_expr(&expr);
        assert!(sql.contains("coalesce("));
    }

    #[test]
    fn test_case_expression() {
        let case = CaseExpr::searched(
            vec![
                (Expr::column("x").eq(Expr::int(1)), Expr::string("one")),
                (Expr::column("x").eq(Expr::int(2)), Expr::string("two")),
            ],
            Some(Expr::string("other")),
        );
        let expr = Expr::Case(case);
        let sql = render_expr(&expr);
        assert!(sql.contains("case"));
        assert!(sql.contains("when"));
        assert!(sql.contains("then"));
        assert!(sql.contains("else"));
        assert!(sql.contains("end"));
    }

    #[test]
    fn test_jsonb_build() {
        // Object
        let expr = Expr::jsonb_build_object(vec![(Expr::string("key"), Expr::string("value"))]);
        let sql = render_expr(&expr);
        assert!(sql.contains("jsonb_build_object("));

        // Array
        let expr = Expr::jsonb_build_array(vec![Expr::int(1), Expr::int(2)]);
        let sql = render_expr(&expr);
        assert!(sql.contains("jsonb_build_array("));
    }

    #[test]
    fn test_aggregate_expressions() {
        // COUNT(*)
        let agg = AggregateExpr::count_all();
        let expr = Expr::Aggregate(agg);
        assert_eq!(render_expr(&expr), "count(*)");

        // SUM with column
        let agg = AggregateExpr::new(AggregateFunction::Sum, vec![Expr::column("amount")]);
        let expr = Expr::Aggregate(agg);
        assert!(render_expr(&expr).contains("sum("));

        // COUNT with DISTINCT
        let agg = AggregateExpr::new(AggregateFunction::Count, vec![Expr::column("user_id")])
            .with_distinct();
        let expr = Expr::Aggregate(agg);
        let sql = render_expr(&expr);
        assert!(sql.contains("distinct"));

        // Aggregate with FILTER
        let agg = AggregateExpr::new(AggregateFunction::Count, vec![])
            .with_filter(Expr::column("active").eq(Expr::bool(true)));
        let expr = Expr::Aggregate(agg);
        let sql = render_expr(&expr);
        assert!(sql.contains("filter (where"));
    }

    #[test]
    fn test_array_operators() {
        // Contains
        let expr = Expr::binary(
            Expr::column("tags"),
            BinaryOperator::Contains,
            Expr::Array(vec![Expr::string("rust")]),
        );
        let sql = render_expr(&expr);
        assert!(sql.contains("@>"));

        // Overlap
        let expr = Expr::binary(
            Expr::column("tags"),
            BinaryOperator::Overlap,
            Expr::column("other_tags"),
        );
        let sql = render_expr(&expr);
        assert!(sql.contains("&&"));
    }
}

mod stmt_tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let stmt =
            SelectStmt::columns(vec![SelectColumn::star()]).with_from(FromClause::table("users"));
        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("select *"));
        assert!(sql.contains("from \"users\""));
    }

    #[test]
    fn test_select_with_join() {
        let stmt = SelectStmt::columns(vec![
            SelectColumn::expr(Expr::qualified_column("u", "name")),
            SelectColumn::expr(Expr::qualified_column("o", "total")),
        ])
        .with_from(FromClause::table("users").with_alias("u").left_join(
            FromClause::table("orders").with_alias("o"),
            Expr::qualified_column("u", "id").eq(Expr::qualified_column("o", "user_id")),
        ));
        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("left join"));
        assert!(sql.contains("on"));
    }

    #[test]
    fn test_select_with_subquery() {
        let inner = SelectStmt::columns(vec![SelectColumn::expr(Expr::column("user_id"))])
            .with_from(FromClause::table("orders"));
        let outer = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_where(Expr::InList {
                expr: Box::new(Expr::column("id")),
                list: vec![Expr::Subquery(Box::new(inner))],
                negated: false,
            });
        let sql = render(&Stmt::Select(outer));
        assert!(sql.contains("in ("));
        assert!(sql.contains("select"));
    }

    #[test]
    fn test_select_with_order_limit_offset() {
        let stmt = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_order_by(vec![
                OrderByExpr::desc(Expr::column("created_at")).with_nulls(NullsOrder::Last)
            ])
            .with_limit(10)
            .with_offset(20);
        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("order by"));
        assert!(sql.contains("desc"));
        assert!(sql.contains("nulls last"));
        assert!(sql.contains("limit 10"));
        assert!(sql.contains("offset 20"));
    }

    #[test]
    fn test_insert_single_row() {
        let stmt = InsertStmt::new(
            "users",
            vec![Ident::new("name"), Ident::new("email")],
            InsertValues::Values(vec![vec![
                Expr::string("Alice"),
                Expr::string("alice@example.com"),
            ]]),
        );
        let sql = render(&Stmt::Insert(stmt));
        assert!(sql.contains("insert into"));
        assert!(sql.contains("values"));
    }

    #[test]
    fn test_insert_multiple_rows() {
        let stmt = InsertStmt::new(
            "users",
            vec![Ident::new("name")],
            InsertValues::Values(vec![vec![Expr::string("Alice")], vec![Expr::string("Bob")]]),
        );
        let sql = render(&Stmt::Insert(stmt));
        assert!(sql.contains("'Alice'"));
        assert!(sql.contains("'Bob'"));
    }

    #[test]
    fn test_insert_with_returning() {
        let stmt = InsertStmt::new(
            "users",
            vec![Ident::new("name")],
            InsertValues::Values(vec![vec![Expr::string("Alice")]]),
        )
        .with_returning(vec![SelectColumn::star()]);
        let sql = render(&Stmt::Insert(stmt));
        assert!(sql.contains("returning *"));
    }

    #[test]
    fn test_insert_default_values() {
        let stmt = InsertStmt::new("users", vec![], InsertValues::DefaultValues);
        let sql = render(&Stmt::Insert(stmt));
        assert!(sql.contains("default values"));
    }

    #[test]
    fn test_update_basic() {
        let stmt = UpdateStmt::new(
            "users",
            vec![
                (Ident::new("name"), Expr::string("Bob")),
                (Ident::new("updated_at"), Expr::raw("now()")),
            ],
        )
        .with_where(Expr::column("id").eq(Expr::int(1)));
        let sql = render(&Stmt::Update(stmt));
        assert!(sql.contains("update"));
        assert!(sql.contains("set"));
        assert!(sql.contains("where"));
    }

    #[test]
    fn test_delete_basic() {
        let stmt =
            DeleteStmt::new("users").with_where(Expr::column("active").eq(Expr::bool(false)));
        let sql = render(&Stmt::Delete(stmt));
        assert!(sql.contains("delete from"));
        assert!(sql.contains("where"));
    }
}

mod cte_tests {
    use super::*;

    #[test]
    fn test_simple_cte() {
        let inner = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_where(Expr::column("active").eq(Expr::bool(true)));

        let outer = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("active_users"))
            .with_ctes(vec![Cte::select("active_users", inner)]);

        let sql = render(&Stmt::Select(outer));
        assert!(sql.contains("with"));
        assert!(sql.contains("\"active_users\""));
        assert!(sql.contains("as ("));
    }

    #[test]
    fn test_cte_with_columns() {
        let inner = SelectStmt::columns(vec![
            SelectColumn::expr(Expr::column("id")),
            SelectColumn::expr(Expr::column("name")),
        ])
        .with_from(FromClause::table("users"));

        let cte = Cte::select("user_info", inner).with_columns(vec!["user_id", "user_name"]);

        let outer = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("user_info"))
            .with_ctes(vec![cte]);

        let sql = render(&Stmt::Select(outer));
        assert!(sql.contains("\"user_info\"(\"user_id\", \"user_name\")"));
    }

    #[test]
    fn test_data_modifying_cte() {
        let insert = InsertStmt::new(
            "users",
            vec![Ident::new("name")],
            InsertValues::Values(vec![vec![Expr::string("New User")]]),
        )
        .with_returning(vec![SelectColumn::star()]);

        let outer = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("new_user"))
            .with_ctes(vec![Cte::insert("new_user", insert)]);

        let sql = render(&Stmt::Select(outer));
        assert!(sql.contains("with"));
        assert!(sql.contains("insert into"));
        assert!(sql.contains("returning"));
    }

    #[test]
    fn test_multiple_ctes() {
        let cte1 = Cte::select(
            "a",
            SelectStmt::columns(vec![SelectColumn::expr(Expr::int(1))]),
        );
        let cte2 = Cte::select(
            "b",
            SelectStmt::columns(vec![SelectColumn::expr(Expr::int(2))]),
        );

        let outer = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("a").cross_join(FromClause::table("b")))
            .with_ctes(vec![cte1, cte2]);

        let sql = render(&Stmt::Select(outer));
        assert!(sql.contains("with"));
        assert!(sql.contains("\"a\""));
        assert!(sql.contains("\"b\""));
    }
}

mod params_tests {
    use super::*;

    #[test]
    fn test_param_collection() {
        let mut collector = ParamCollector::new();

        let _e1 = collector.add(ParamValue::String("test".into()), SqlType::text());
        let _e2 = collector.add(ParamValue::Integer(42), SqlType::integer());
        let _e3 = collector.add(ParamValue::Null, SqlType::text());

        assert_eq!(collector.len(), 3);
        assert!(!collector.is_empty());

        let params = collector.into_params();
        assert_eq!(params[0].index, 1);
        assert_eq!(params[1].index, 2);
        assert_eq!(params[2].index, 3);
    }

    #[test]
    fn test_param_in_query() {
        let mut collector = ParamCollector::new();
        let param = collector.add(
            ParamValue::String("alice@example.com".into()),
            SqlType::text(),
        );

        let stmt = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_where(Expr::column("email").eq(param));

        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("($1::text)"));
    }

    #[test]
    fn test_array_param() {
        let mut collector = ParamCollector::new();
        let param = collector.add(
            ParamValue::Array(vec![
                ParamValue::Integer(1),
                ParamValue::Integer(2),
                ParamValue::Integer(3),
            ]),
            SqlType::integer().into_array(),
        );

        let stmt = SelectStmt::columns(vec![SelectColumn::star()])
            .with_from(FromClause::table("users"))
            .with_where(Expr::binary(Expr::column("id"), BinaryOperator::Any, param));

        let sql = render(&Stmt::Select(stmt));
        assert!(sql.contains("($1::integer[])"));
    }
}

mod type_tests {
    use super::*;

    #[test]
    fn test_builtin_types() {
        assert_eq!(SqlType::text().to_sql_string(), "text");
        assert_eq!(SqlType::integer().to_sql_string(), "integer");
        assert_eq!(SqlType::bigint().to_sql_string(), "bigint");
        assert_eq!(SqlType::boolean().to_sql_string(), "boolean");
        assert_eq!(SqlType::jsonb().to_sql_string(), "jsonb");
        assert_eq!(SqlType::uuid().to_sql_string(), "uuid");
    }

    #[test]
    fn test_array_types() {
        assert_eq!(SqlType::text().into_array().to_sql_string(), "text[]");
        assert_eq!(SqlType::integer().as_array().to_sql_string(), "integer[]");
    }

    #[test]
    fn test_custom_types() {
        let t = SqlType::with_schema("public", "my_enum");
        assert_eq!(t.to_sql_string(), "public.my_enum");

        let t = SqlType::from_name("public.my_type[]");
        assert_eq!(t.schema, Some("public".to_string()));
        assert_eq!(t.name, "my_type");
        assert!(t.is_array);
    }
}

mod integration_tests {
    use super::*;

    /// Test a realistic mutation pattern used by pg_graphql
    #[test]
    fn test_insert_mutation_pattern() {
        // This mirrors the INSERT pattern in transpile.rs
        let mut params = ParamCollector::new();

        // Simulate inserting a user
        let email_param = params.add(
            ParamValue::String("user@example.com".into()),
            SqlType::text(),
        );
        let name_param = params.add(ParamValue::String("Test User".into()), SqlType::text());

        // Build the INSERT CTE
        let insert = InsertStmt::new(
            "account",
            vec![Ident::new("email"), Ident::new("name")],
            InsertValues::Values(vec![vec![email_param, name_param]]),
        )
        .with_schema("public")
        .with_returning(vec![
            SelectColumn::expr(Expr::column("id")),
            SelectColumn::expr(Expr::column("email")),
            SelectColumn::expr(Expr::column("name")),
        ]);

        // Build the outer SELECT with jsonb_build_object
        let select = SelectStmt::columns(vec![SelectColumn::expr(Expr::jsonb_build_object(vec![
            (
                Expr::string("affectedCount"),
                Expr::Aggregate(AggregateExpr::count_all()),
            ),
            (
                Expr::string("records"),
                Expr::coalesce(vec![
                    Expr::Aggregate(AggregateExpr::new(
                        AggregateFunction::JsonbAgg,
                        vec![Expr::jsonb_build_object(vec![
                            (Expr::string("id"), Expr::column("id")),
                            (Expr::string("email"), Expr::column("email")),
                        ])],
                    )),
                    Expr::raw("jsonb_build_array()"),
                ]),
            ),
        ]))])
        .with_from(FromClause::table("affected").with_alias("affected"))
        .with_ctes(vec![Cte::insert("affected", insert)]);

        let sql = render(&Stmt::Select(select));

        // Verify the structure
        assert!(sql.contains("with"));
        assert!(sql.contains("insert into"));
        assert!(sql.contains("returning"));
        assert!(sql.contains("jsonb_build_object"));
        assert!(sql.contains("jsonb_agg"));
        assert!(sql.contains("($1::text)"));
        assert!(sql.contains("($2::text)"));
    }

    /// Test the update mutation pattern with at_most check
    #[test]
    fn test_update_mutation_pattern() {
        let mut params = ParamCollector::new();

        let new_name = params.add(ParamValue::String("Updated".into()), SqlType::text());
        let filter_id = params.add(ParamValue::Integer(1), SqlType::integer());

        // Build UPDATE CTE
        let update = UpdateStmt::new("users", vec![(Ident::new("name"), new_name)])
            .with_schema("public")
            .with_alias("t")
            .with_where(Expr::qualified_column("t", "id").eq(filter_id))
            .with_returning(vec![SelectColumn::star()]);

        // Total count CTE
        let total_count = SelectStmt::columns(vec![SelectColumn::expr_as(
            Expr::Aggregate(AggregateExpr::count_all()),
            "total_count",
        )])
        .with_from(FromClause::table("impacted"));

        // Main select with safety check
        let safety_check = Expr::Case(CaseExpr::searched(
            vec![(
                Expr::binary(
                    Expr::column("total_count"),
                    BinaryOperator::Gt,
                    Expr::int(1), // at_most = 1
                ),
                Expr::raw("graphql.exception($a$update impacts too many records$a$)::jsonb"),
            )],
            Some(Expr::jsonb_build_object(vec![(
                Expr::string("affectedCount"),
                Expr::column("total_count"),
            )])),
        ));

        let main_select = SelectStmt::columns(vec![SelectColumn::expr(safety_check)])
            .with_from(FromClause::table("total"))
            .with_ctes(vec![
                Cte::update("impacted", update),
                Cte::select("total", total_count).with_columns(vec!["total_count"]),
            ]);

        let sql = render(&Stmt::Select(main_select));

        assert!(sql.contains("with"));
        assert!(sql.contains("update"));
        assert!(sql.contains("case"));
        assert!(sql.contains("graphql.exception"));
    }
}
