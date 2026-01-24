//! ExactTable - O(1) exact-match lookup table using HashMap.

use crate::EnrichmentRow;
use ahash::RandomState;
use hashbrown::HashMap;
use std::sync::Arc;

/// O(1) exact-match lookup table using hashbrown with ahash.
///
/// Stores enrichment rows indexed by a key string for fast lookups.
#[derive(Debug)]
pub struct ExactTable {
    /// Maps key string to row data.
    data: HashMap<String, EnrichmentRow, RandomState>,

    /// Column names from CSV header (excluding the key column).
    columns: Arc<Vec<String>>,

    /// Name of the key field.
    key_field: String,
}

impl ExactTable {
    /// Creates a new empty exact table.
    pub fn new(columns: Vec<String>, key_field: String) -> Self {
        Self {
            data: HashMap::with_hasher(RandomState::new()),
            columns: Arc::new(columns),
            key_field,
        }
    }

    /// Inserts a row into the table.
    pub fn insert(&mut self, key: String, values: Vec<String>) {
        let row = EnrichmentRow::new(values, Arc::clone(&self.columns));
        self.data.insert(key, row);
    }

    /// Looks up a key and returns the associated row.
    pub fn get(&self, key: &str) -> Option<&EnrichmentRow> {
        self.data.get(key)
    }

    /// Returns the number of entries in the table.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the key field name.
    pub fn key_field(&self) -> &str {
        &self.key_field
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_table_insert_and_get() {
        let columns = vec![
            "user_id".to_string(),
            "email".to_string(),
            "department".to_string(),
        ];
        let mut table = ExactTable::new(columns, "user_id".to_string());

        table.insert(
            "alice".to_string(),
            vec![
                "alice".to_string(),
                "alice@example.com".to_string(),
                "engineering".to_string(),
            ],
        );

        table.insert(
            "bob".to_string(),
            vec![
                "bob".to_string(),
                "bob@example.com".to_string(),
                "sales".to_string(),
            ],
        );

        assert_eq!(table.len(), 2);

        let alice = table.get("alice").unwrap();
        assert_eq!(alice.get("email"), Some("alice@example.com"));
        assert_eq!(alice.get("department"), Some("engineering"));

        let bob = table.get("bob").unwrap();
        assert_eq!(bob.get("department"), Some("sales"));

        assert!(table.get("charlie").is_none());
    }

    #[test]
    fn test_exact_table_empty() {
        let table = ExactTable::new(vec!["id".to_string()], "id".to_string());
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }
}
