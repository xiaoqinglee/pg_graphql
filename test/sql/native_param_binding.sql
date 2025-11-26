-- Test native parameter binding edge cases
-- This test exercises the boundary conditions for native vs text-based parameter binding

begin;
    comment on schema public is '@graphql({"inflect_names": false})';

    -- Create enum type (custom type - should use text binding)
    create type mood as enum ('sad', 'happy', 'neutral');

    -- Create domain type (custom type - should use text binding)
    create domain positive_int as integer check (value > 0);

    -- Create test table with various types
    create table param_test (
        id serial primary key,
        -- Native binding types
        bool_col boolean,
        int4_col integer,
        int8_col bigint,
        int2_col smallint,
        float8_col double precision,
        float4_col real,
        text_col text,
        varchar_col varchar(100),
        -- Text binding types (custom/special)
        enum_col mood,
        domain_col positive_int,
        numeric_col numeric(20,10),
        uuid_col uuid,
        date_col date,
        time_col time,
        timestamptz_col timestamptz,
        json_col json,
        jsonb_col jsonb,
        -- Array types (always text binding)
        int_array integer[],
        text_array text[],
        enum_array mood[]
    );

    -- Test 1: Insert with native types (bool, int, float, text)
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                bool_col: true
                int4_col: 42
                int8_col: "9223372036854775807"
                int2_col: 32767
                float8_col: 3.141592653589793
                float4_col: 2.71828
                text_col: "hello world"
                varchar_col: "varchar test"
              }]) {
                affectedCount
                records {
                  id
                  bool_col
                  int4_col
                  int8_col
                  int2_col
                  float8_col
                  float4_col
                  text_col
                  varchar_col
                }
              }
            }
        $$)
    );

    -- Test 2: Insert with custom types (enum, domain, uuid, dates)
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                enum_col: happy
                numeric_col: "12345678901234567890.1234567890"
                uuid_col: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                date_col: "2024-12-31"
                time_col: "23:59:59.999999"
                timestamptz_col: "2024-12-31T23:59:59.999999+00:00"
                json_col: "{\"key\": \"value\"}"
                jsonb_col: "{\"nested\": {\"deep\": true}}"
              }]) {
                affectedCount
                records {
                  id
                  enum_col
                  numeric_col
                  uuid_col
                  date_col
                  time_col
                  timestamptz_col
                  json_col
                  jsonb_col
                }
              }
            }
        $$)
    );

    -- Test 3: Insert with array types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                int_array: [1, 2, 3, 2147483647, -2147483648]
                text_array: ["foo", "bar", "baz with spaces", "special\"chars"]
                enum_array: [sad, happy, neutral]
              }]) {
                affectedCount
                records {
                  id
                  int_array
                  text_array
                  enum_array
                }
              }
            }
        $$)
    );

    -- Test 4: Integer boundary values
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [
                { int4_col: 2147483647 }
                { int4_col: -2147483648 }
                { int2_col: 32767 }
                { int2_col: -32768 }
              ]) {
                affectedCount
                records {
                  id
                  int4_col
                  int2_col
                }
              }
            }
        $$)
    );

    -- Test 5: Float special values and precision
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [
                { float8_col: 1.7976931348623157e308 }
                { float8_col: 2.2250738585072014e-308 }
                { float8_col: 0 }
                { float8_col: -0 }
              ]) {
                affectedCount
                records {
                  id
                  float8_col
                }
              }
            }
        $$)
    );

    -- Test 6: NULL handling across all types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                bool_col: null
                int4_col: null
                int8_col: null
                float8_col: null
                text_col: null
                enum_col: null
                numeric_col: null
                uuid_col: null
                date_col: null
                json_col: null
                int_array: null
              }]) {
                affectedCount
                records {
                  id
                  bool_col
                  int4_col
                  int8_col
                  float8_col
                  text_col
                  enum_col
                  numeric_col
                  uuid_col
                  date_col
                  json_col
                  int_array
                }
              }
            }
        $$)
    );

    -- Test 7: Filter with native types
    select jsonb_pretty(
        graphql.resolve($$
            {
              param_testCollection(
                filter: {
                  bool_col: {eq: true}
                  int4_col: {eq: 42}
                  text_col: {eq: "hello world"}
                }
              ) {
                edges {
                  node {
                    id
                    bool_col
                    int4_col
                    text_col
                  }
                }
              }
            }
        $$)
    );

    -- Test 8: Filter with custom types
    select jsonb_pretty(
        graphql.resolve($$
            {
              param_testCollection(
                filter: {
                  enum_col: {eq: happy}
                  uuid_col: {eq: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}
                }
              ) {
                edges {
                  node {
                    id
                    enum_col
                    uuid_col
                  }
                }
              }
            }
        $$)
    );

    -- Test 9: Update with mixed native and text types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              updateParam_testCollection(
                filter: {id: {eq: 1}}
                set: {
                  bool_col: false
                  int4_col: 100
                  text_col: "updated"
                  enum_col: sad
                }
              ) {
                affectedCount
                records {
                  id
                  bool_col
                  int4_col
                  text_col
                  enum_col
                }
              }
            }
        $$)
    );

    -- Test 10: Delete with filter on various types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              deleteFromParam_testCollection(
                filter: {
                  int4_col: {eq: 100}
                }
              ) {
                affectedCount
                records {
                  id
                }
              }
            }
        $$)
    );

    -- Test 11: Numeric precision (high precision values)
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                numeric_col: "99999999999999999999.9999999999"
              }]) {
                records {
                  numeric_col
                }
              }
            }
        $$)
    );

    -- Test 12: Text with special characters (SQL injection attempt)
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [{
                text_col: "'; DROP TABLE param_test; --"
              }]) {
                records {
                  text_col
                }
              }
            }
        $$)
    );

    -- Verify table still exists and has data
    select count(*) as row_count from param_test;

    -- Test 13: Empty string vs NULL
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [
                { text_col: "" }
                { text_col: null }
              ]) {
                records {
                  id
                  text_col
                }
              }
            }
        $$)
    );

    -- Test 14: Boolean edge cases
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoParam_testCollection(objects: [
                { bool_col: true }
                { bool_col: false }
              ]) {
                records {
                  id
                  bool_col
                }
              }
            }
        $$)
    );

    -- Test 15: Query filtering on boolean
    select jsonb_pretty(
        graphql.resolve($$
            {
              param_testCollection(
                filter: { bool_col: {eq: false} }
              ) {
                edges {
                  node {
                    id
                    bool_col
                  }
                }
              }
            }
        $$)
    );

rollback;
