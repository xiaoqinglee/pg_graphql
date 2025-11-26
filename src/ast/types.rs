//! SQL type representations
//!
//! This module defines SQL types used for parameter casting and type safety.

/// A SQL type with optional schema qualification and array support
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlType {
    /// Schema name (e.g., "public", "pg_catalog")
    pub schema: Option<String>,
    /// Type name (e.g., "text", "integer", "my_enum")
    pub name: String,
    /// PostgreSQL OID when known (for optimization)
    pub oid: Option<u32>,
    /// Whether this is an array type
    pub is_array: bool,
}

impl SqlType {
    /// Create a new SQL type
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
            oid: None,
            is_array: false,
        }
    }

    /// Create a type with schema qualification
    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
            oid: None,
            is_array: false,
        }
    }

    /// Create a custom type (user-defined)
    pub fn custom(schema: Option<String>, name: String) -> Self {
        Self {
            schema,
            name,
            oid: None,
            is_array: false,
        }
    }

    /// Create an array version of this type
    pub fn into_array(self) -> Self {
        Self {
            is_array: true,
            ..self
        }
    }

    /// Create an array version of this type (non-consuming)
    pub fn as_array(&self) -> Self {
        Self {
            is_array: true,
            ..self.clone()
        }
    }

    // Common PostgreSQL types with known OIDs

    /// PostgreSQL `text` type (OID 25)
    pub fn text() -> Self {
        Self {
            schema: None,
            name: "text".into(),
            oid: Some(25),
            is_array: false,
        }
    }

    /// PostgreSQL `integer` type (OID 23)
    pub fn integer() -> Self {
        Self {
            schema: None,
            name: "integer".into(),
            oid: Some(23),
            is_array: false,
        }
    }

    /// PostgreSQL `bigint` type (OID 20)
    pub fn bigint() -> Self {
        Self {
            schema: None,
            name: "bigint".into(),
            oid: Some(20),
            is_array: false,
        }
    }

    /// PostgreSQL `smallint` type (OID 21)
    pub fn smallint() -> Self {
        Self {
            schema: None,
            name: "smallint".into(),
            oid: Some(21),
            is_array: false,
        }
    }

    /// PostgreSQL `boolean` type (OID 16)
    pub fn boolean() -> Self {
        Self {
            schema: None,
            name: "boolean".into(),
            oid: Some(16),
            is_array: false,
        }
    }

    /// PostgreSQL `real` type (OID 700)
    pub fn real() -> Self {
        Self {
            schema: None,
            name: "real".into(),
            oid: Some(700),
            is_array: false,
        }
    }

    /// PostgreSQL `double precision` type (OID 701)
    pub fn double_precision() -> Self {
        Self {
            schema: None,
            name: "double precision".into(),
            oid: Some(701),
            is_array: false,
        }
    }

    /// PostgreSQL `numeric` type (OID 1700)
    pub fn numeric() -> Self {
        Self {
            schema: None,
            name: "numeric".into(),
            oid: Some(1700),
            is_array: false,
        }
    }

    /// PostgreSQL `json` type (OID 114)
    pub fn json() -> Self {
        Self {
            schema: None,
            name: "json".into(),
            oid: Some(114),
            is_array: false,
        }
    }

    /// PostgreSQL `jsonb` type (OID 3802)
    pub fn jsonb() -> Self {
        Self {
            schema: None,
            name: "jsonb".into(),
            oid: Some(3802),
            is_array: false,
        }
    }

    /// PostgreSQL `uuid` type (OID 2950)
    pub fn uuid() -> Self {
        Self {
            schema: None,
            name: "uuid".into(),
            oid: Some(2950),
            is_array: false,
        }
    }

    /// PostgreSQL `timestamp` type (OID 1114)
    pub fn timestamp() -> Self {
        Self {
            schema: None,
            name: "timestamp".into(),
            oid: Some(1114),
            is_array: false,
        }
    }

    /// PostgreSQL `timestamptz` type (OID 1184)
    pub fn timestamptz() -> Self {
        Self {
            schema: None,
            name: "timestamptz".into(),
            oid: Some(1184),
            is_array: false,
        }
    }

    /// PostgreSQL `date` type (OID 1082)
    pub fn date() -> Self {
        Self {
            schema: None,
            name: "date".into(),
            oid: Some(1082),
            is_array: false,
        }
    }

    /// PostgreSQL `time` type (OID 1083)
    pub fn time() -> Self {
        Self {
            schema: None,
            name: "time".into(),
            oid: Some(1083),
            is_array: false,
        }
    }

    /// PostgreSQL `bytea` type (OID 17)
    pub fn bytea() -> Self {
        Self {
            schema: None,
            name: "bytea".into(),
            oid: Some(17),
            is_array: false,
        }
    }

    /// Create from a type name string (e.g., "text", "integer[]", "public.my_type")
    pub fn from_name(type_name: &str) -> Self {
        let is_array = type_name.ends_with("[]");
        let base_name = if is_array {
            &type_name[..type_name.len() - 2]
        } else {
            type_name
        };

        // Check for schema qualification
        let (schema, name) = if let Some(dot_pos) = base_name.find('.') {
            (
                Some(base_name[..dot_pos].to_string()),
                base_name[dot_pos + 1..].to_string(),
            )
        } else {
            (None, base_name.to_string())
        };

        Self {
            schema,
            name,
            oid: None,
            is_array,
        }
    }

    /// Get the full type name for SQL rendering
    pub fn to_sql_string(&self) -> String {
        let mut result = String::new();

        if let Some(schema) = &self.schema {
            result.push_str(schema);
            result.push('.');
        }

        result.push_str(&self.name);

        if self.is_array {
            result.push_str("[]");
        }

        result
    }

    /// Check if this type requires special handling for JSON serialization
    /// (e.g., bigint needs to be converted to text to avoid precision loss)
    pub fn needs_text_cast_for_json(&self) -> bool {
        matches!(
            self.oid,
            Some(20)   // bigint
            | Some(1700) // numeric
        )
    }

    /// Check if this is a JSON/JSONB type
    pub fn is_json(&self) -> bool {
        matches!(self.oid, Some(114) | Some(3802)) || self.name == "json" || self.name == "jsonb"
    }
}

impl Default for SqlType {
    fn default() -> Self {
        Self::text()
    }
}

impl std::fmt::Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_types() {
        assert_eq!(SqlType::text().to_sql_string(), "text");
        assert_eq!(SqlType::integer().to_sql_string(), "integer");
        assert_eq!(SqlType::jsonb().to_sql_string(), "jsonb");
    }

    #[test]
    fn test_array_types() {
        assert_eq!(SqlType::text().into_array().to_sql_string(), "text[]");
        assert_eq!(SqlType::integer().as_array().to_sql_string(), "integer[]");
    }

    #[test]
    fn test_from_name() {
        let t = SqlType::from_name("text");
        assert_eq!(t.name, "text");
        assert!(!t.is_array);

        let t = SqlType::from_name("integer[]");
        assert_eq!(t.name, "integer");
        assert!(t.is_array);

        let t = SqlType::from_name("public.my_type");
        assert_eq!(t.schema, Some("public".to_string()));
        assert_eq!(t.name, "my_type");
    }

    #[test]
    fn test_schema_qualified() {
        let t = SqlType::with_schema("myschema", "mytype");
        assert_eq!(t.to_sql_string(), "myschema.mytype");
    }
}
