//! EnrichmentRow - a row of enrichment data that can be converted to Rhai Dynamic.

use rhai::Dynamic;
use std::sync::Arc;

/// A row of enrichment data that can be converted to Rhai Dynamic.
///
/// Stores column values indexed by position, with shared column names.
#[derive(Debug, Clone)]
pub struct EnrichmentRow {
    /// Column values indexed by position.
    values: Vec<String>,

    /// Reference to column names (shared across all rows in table).
    columns: Arc<Vec<String>>,
}

impl EnrichmentRow {
    /// Creates a new enrichment row.
    pub fn new(values: Vec<String>, columns: Arc<Vec<String>>) -> Self {
        Self { values, columns }
    }

    /// Returns the number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Gets a value by column name.
    pub fn get(&self, column: &str) -> Option<&str> {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|idx| self.values.get(idx))
            .map(|s| s.as_str())
    }

    /// Gets a value by column index.
    pub fn get_by_index(&self, index: usize) -> Option<&str> {
        self.values.get(index).map(|s| s.as_str())
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Converts to a Rhai Dynamic map for script access.
    pub fn to_dynamic(&self) -> Dynamic {
        let mut map = rhai::Map::new();
        for (col, val) in self.columns.iter().zip(self.values.iter()) {
            map.insert(col.clone().into(), val.clone().into());
        }
        map.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrichment_row_get() {
        let columns = Arc::new(vec![
            "country".to_string(),
            "city".to_string(),
            "asn".to_string(),
        ]);
        let row = EnrichmentRow::new(
            vec![
                "US".to_string(),
                "New York".to_string(),
                "15169".to_string(),
            ],
            columns,
        );

        assert_eq!(row.get("country"), Some("US"));
        assert_eq!(row.get("city"), Some("New York"));
        assert_eq!(row.get("asn"), Some("15169"));
        assert_eq!(row.get("nonexistent"), None);
    }

    #[test]
    fn test_enrichment_row_to_dynamic() {
        let columns = Arc::new(vec!["key".to_string(), "value".to_string()]);
        let row = EnrichmentRow::new(vec!["foo".to_string(), "bar".to_string()], columns);

        let dynamic = row.to_dynamic();
        assert!(dynamic.is_map());

        let map = dynamic.cast::<rhai::Map>();
        assert_eq!(
            map.get("key").unwrap().clone().into_string().unwrap(),
            "foo"
        );
        assert_eq!(
            map.get("value").unwrap().clone().into_string().unwrap(),
            "bar"
        );
    }
}
