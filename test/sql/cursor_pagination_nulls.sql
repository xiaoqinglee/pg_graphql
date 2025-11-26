-- Test cursor pagination with nulls first/last ordering
-- This test verifies correct behavior when paginating through data with NULL values
-- using different null ordering strategies (NULLS FIRST vs NULLS LAST)

begin;
    comment on schema public is '@graphql({"inflect_names": false})';

    create table items(
        id int primary key,
        priority int,  -- nullable column for testing null ordering
        name text
    );

    -- Insert test data with strategic NULL placement
    -- IDs 1-5: have priority values
    -- IDs 6-8: NULL priority (to test null ordering)
    insert into items(id, priority, name) values
        (1, 10, 'low'),
        (2, 20, 'medium'),
        (3, 30, 'high'),
        (4, 20, 'medium-alt'),  -- duplicate priority
        (5, 10, 'low-alt'),     -- duplicate priority
        (6, null, 'unset-a'),
        (7, null, 'unset-b'),
        (8, null, 'unset-c');

    -- Show the data for reference
    select * from items order by id;

    -- ==========================================================================
    -- Test 1: NULLS LAST with ASC (default behavior)
    -- Order should be: 1,5 (priority=10), 2,4 (priority=20), 3 (priority=30), 6,7,8 (null)
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                    name
                  }
                }
              }
            }
        $$)
    );

    -- ==========================================================================
    -- Test 2: NULLS FIRST with ASC
    -- Order should be: 6,7,8 (null), 1,5 (priority=10), 2,4 (priority=20), 3 (priority=30)
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                    name
                  }
                }
              }
            }
        $$)
    );

    -- ==========================================================================
    -- Test 3: Cursor pagination with NULLS LAST - first 3, then next page
    -- Should get: first page [1,5,2], then from cursor after id=2 get [4,3,6]
    -- ==========================================================================

    -- First page
    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                first: 3
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  cursor
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Next page using cursor [20, 2] (priority=20, id=2)
    select jsonb_pretty(
        graphql.resolve($$
            query NextPage($afterCursor: Cursor) {
              itemsCollection(
                first: 3
                after: $afterCursor
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[20, 2]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 4: Cursor pagination with NULLS FIRST - first 3, then next page
    -- Should get: first page [6,7,8], then from cursor after id=8 get [1,5,2]
    -- ==========================================================================

    -- First page (nulls come first)
    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                first: 3
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  cursor
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Next page using cursor [null, 8] (priority=null, id=8)
    select jsonb_pretty(
        graphql.resolve($$
            query NextPage($afterCursor: Cursor) {
              itemsCollection(
                first: 3
                after: $afterCursor
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[null, 8]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 5: Reverse pagination (last/before) with NULLS LAST
    -- Get last 3, then previous page
    -- ==========================================================================

    -- Last 3 items (should be 3, 6, 7, 8 area - reversed from end)
    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                last: 3
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  cursor
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Previous page using before cursor
    select jsonb_pretty(
        graphql.resolve($$
            query PrevPage($beforeCursor: Cursor) {
              itemsCollection(
                last: 3
                before: $beforeCursor
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('beforeCursor', graphql.encode('[null, 6]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 6: Reverse pagination (last/before) with NULLS FIRST
    -- ==========================================================================

    -- Last 3 items with nulls first ordering
    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                last: 3
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  cursor
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Previous page
    select jsonb_pretty(
        graphql.resolve($$
            query PrevPage($beforeCursor: Cursor) {
              itemsCollection(
                last: 3
                before: $beforeCursor
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('beforeCursor', graphql.encode('[20, 4]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 7: DESC with NULLS FIRST (nulls at start of descending order)
    -- Order: 6,7,8 (null), 3 (30), 2,4 (20), 1,5 (10)
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                orderBy: [{priority: DescNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Paginate through with cursor
    select jsonb_pretty(
        graphql.resolve($$
            query NextPage($afterCursor: Cursor) {
              itemsCollection(
                first: 3
                after: $afterCursor
                orderBy: [{priority: DescNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[null, 8]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 8: DESC with NULLS LAST (nulls at end of descending order)
    -- Order: 3 (30), 2,4 (20), 1,5 (10), 6,7,8 (null)
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            {
              itemsCollection(
                orderBy: [{priority: DescNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$)
    );

    -- Paginate - after priority=20, id=4, should get 1,5 then nulls
    select jsonb_pretty(
        graphql.resolve($$
            query NextPage($afterCursor: Cursor) {
              itemsCollection(
                first: 4
                after: $afterCursor
                orderBy: [{priority: DescNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[20, 4]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 9: Edge case - cursor at NULL value boundary (transitioning from null to non-null)
    -- With NULLS FIRST, cursor at last null should give first non-null items
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            query AfterLastNull($afterCursor: Cursor) {
              itemsCollection(
                first: 2
                after: $afterCursor
                orderBy: [{priority: AscNullsFirst}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                    name
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[null, 8]'::jsonb))
        )
    );

    -- ==========================================================================
    -- Test 10: Edge case - cursor at non-NULL value boundary (transitioning to nulls)
    -- With NULLS LAST, cursor at last non-null should give null items
    -- ==========================================================================

    select jsonb_pretty(
        graphql.resolve($$
            query AfterLastNonNull($afterCursor: Cursor) {
              itemsCollection(
                first: 3
                after: $afterCursor
                orderBy: [{priority: AscNullsLast}, {id: AscNullsLast}]
              ) {
                edges {
                  node {
                    id
                    priority
                    name
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[30, 3]'::jsonb))
        )
    );

rollback;
