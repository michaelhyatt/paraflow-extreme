//! EnrichmentRegistry - holds all enrichment tables and provides Rhai integration.

use crate::loader::{load_cidr_table, load_exact_table};
use crate::{CidrTable, EnrichmentTableConfig, ExactTable, MatchType};
use aws_sdk_s3::Client as S3Client;
use hashbrown::HashMap;
use pf_error::Result;
use rhai::{Dynamic, Engine};
use std::sync::Arc;
use tracing::info;

/// Registry of enrichment tables, shared across all worker threads.
///
/// Holds both exact-match and CIDR-match tables, and provides methods
/// to register Rhai functions for script access.
#[derive(Debug)]
pub struct EnrichmentRegistry {
    /// Exact-match tables (HashMap-based, O(1) lookup).
    exact_tables: HashMap<String, ExactTable>,

    /// CIDR-match tables (trie-based, O(log n) longest-prefix match).
    cidr_tables: HashMap<String, CidrTable>,
}

impl EnrichmentRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            exact_tables: HashMap::new(),
            cidr_tables: HashMap::new(),
        }
    }

    /// Loads all tables from configuration.
    ///
    /// # Arguments
    ///
    /// * `configs` - List of table configurations
    /// * `s3_client` - Optional S3 client for loading from S3
    ///
    /// # Returns
    ///
    /// A fully loaded EnrichmentRegistry
    pub async fn load(
        configs: &[EnrichmentTableConfig],
        s3_client: Option<&S3Client>,
    ) -> Result<Self> {
        let mut registry = Self::new();

        for config in configs {
            match config.match_type {
                MatchType::Exact => {
                    let table =
                        load_exact_table(&config.source, &config.key_field, s3_client).await?;
                    registry.add_exact_table(config.name.clone(), table);
                }
                MatchType::Cidr => {
                    let table =
                        load_cidr_table(&config.source, &config.key_field, s3_client).await?;
                    registry.add_cidr_table(config.name.clone(), table);
                }
            }
        }

        info!(
            exact_tables = registry.exact_tables.len(),
            cidr_tables = registry.cidr_tables.len(),
            "Loaded enrichment registry"
        );

        Ok(registry)
    }

    /// Adds an exact-match table to the registry.
    pub fn add_exact_table(&mut self, name: String, table: ExactTable) {
        self.exact_tables.insert(name, table);
    }

    /// Adds a CIDR-match table to the registry.
    pub fn add_cidr_table(&mut self, name: String, table: CidrTable) {
        self.cidr_tables.insert(name, table);
    }

    /// Gets an exact-match table by name.
    pub fn get_exact_table(&self, name: &str) -> Option<&ExactTable> {
        self.exact_tables.get(name)
    }

    /// Gets a CIDR-match table by name.
    pub fn get_cidr_table(&self, name: &str) -> Option<&CidrTable> {
        self.cidr_tables.get(name)
    }

    /// Returns the number of exact tables.
    pub fn exact_table_count(&self) -> usize {
        self.exact_tables.len()
    }

    /// Returns the number of CIDR tables.
    pub fn cidr_table_count(&self) -> usize {
        self.cidr_tables.len()
    }

    /// Returns true if the registry has no tables.
    pub fn is_empty(&self) -> bool {
        self.exact_tables.is_empty() && self.cidr_tables.is_empty()
    }

    /// Registers enrichment lookup functions in a Rhai engine.
    ///
    /// Registers:
    /// - `enrich_exact(table, key)` - Exact match lookup
    /// - `enrich_cidr(table, ip)` - CIDR/longest-prefix match lookup
    pub fn register_functions(self: &Arc<Self>, engine: &mut Engine) {
        // Register enrich_exact function
        let registry_for_exact = Arc::clone(self);
        engine.register_fn("enrich_exact", move |table: &str, key: &str| -> Dynamic {
            match registry_for_exact.get_exact_table(table) {
                Some(t) => t
                    .get(key)
                    .map(|row| row.to_dynamic())
                    .unwrap_or(Dynamic::UNIT),
                None => Dynamic::UNIT,
            }
        });

        // Register enrich_cidr function
        let registry_for_cidr = Arc::clone(self);
        engine.register_fn("enrich_cidr", move |table: &str, ip: &str| -> Dynamic {
            match registry_for_cidr.get_cidr_table(table) {
                Some(t) => t
                    .lookup(ip)
                    .map(|row| row.to_dynamic())
                    .unwrap_or(Dynamic::UNIT),
                None => Dynamic::UNIT,
            }
        });
    }

    /// Returns summary statistics about the registry.
    pub fn stats(&self) -> RegistryStats {
        let exact_entries: usize = self.exact_tables.values().map(|t| t.len()).sum();
        let cidr_entries: usize = self.cidr_tables.values().map(|t| t.len()).sum();

        RegistryStats {
            exact_table_count: self.exact_tables.len(),
            cidr_table_count: self.cidr_tables.len(),
            exact_entry_count: exact_entries,
            cidr_entry_count: cidr_entries,
        }
    }
}

impl Default for EnrichmentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the enrichment registry.
#[derive(Debug, Clone)]
pub struct RegistryStats {
    /// Number of exact-match tables.
    pub exact_table_count: usize,
    /// Number of CIDR-match tables.
    pub cidr_table_count: usize,
    /// Total entries across all exact tables.
    pub exact_entry_count: usize,
    /// Total entries across all CIDR tables.
    pub cidr_entry_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[tokio::test]
    async fn test_registry_load() {
        let users_csv = "user_id,department\nalice,engineering\nbob,sales\n";
        let geo_csv = "cidr,country\n10.0.0.0/8,PRIVATE\n8.8.8.0/24,US\n";

        let users_file = create_test_csv(users_csv);
        let geo_file = create_test_csv(geo_csv);

        let configs = vec![
            EnrichmentTableConfig {
                name: "users".to_string(),
                source: users_file.path().to_str().unwrap().to_string(),
                match_type: MatchType::Exact,
                key_field: "user_id".to_string(),
            },
            EnrichmentTableConfig {
                name: "geo_ip".to_string(),
                source: geo_file.path().to_str().unwrap().to_string(),
                match_type: MatchType::Cidr,
                key_field: "cidr".to_string(),
            },
        ];

        let registry = EnrichmentRegistry::load(&configs, None).await.unwrap();

        assert_eq!(registry.exact_table_count(), 1);
        assert_eq!(registry.cidr_table_count(), 1);

        let users = registry.get_exact_table("users").unwrap();
        assert_eq!(users.len(), 2);

        let geo = registry.get_cidr_table("geo_ip").unwrap();
        assert_eq!(geo.len(), 2);
    }

    #[tokio::test]
    async fn test_registry_rhai_integration() {
        let users_csv = "user_id,department\nalice,engineering\n";
        let geo_csv = "cidr,country\n10.0.0.0/8,PRIVATE\n";

        let users_file = create_test_csv(users_csv);
        let geo_file = create_test_csv(geo_csv);

        let configs = vec![
            EnrichmentTableConfig {
                name: "users".to_string(),
                source: users_file.path().to_str().unwrap().to_string(),
                match_type: MatchType::Exact,
                key_field: "user_id".to_string(),
            },
            EnrichmentTableConfig {
                name: "geo".to_string(),
                source: geo_file.path().to_str().unwrap().to_string(),
                match_type: MatchType::Cidr,
                key_field: "cidr".to_string(),
            },
        ];

        let registry = Arc::new(EnrichmentRegistry::load(&configs, None).await.unwrap());

        let mut engine = Engine::new();
        registry.register_functions(&mut engine);

        // Test enrich_exact
        let result: Dynamic = engine.eval(r#"enrich_exact("users", "alice")"#).unwrap();
        assert!(result.is_map());
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("department")
                .unwrap()
                .clone()
                .into_string()
                .unwrap(),
            "engineering"
        );

        // Test enrich_exact with missing key
        let result: Dynamic = engine
            .eval(r#"enrich_exact("users", "nonexistent")"#)
            .unwrap();
        assert!(result.is_unit());

        // Test enrich_cidr
        let result: Dynamic = engine.eval(r#"enrich_cidr("geo", "10.1.2.3")"#).unwrap();
        assert!(result.is_map());
        let map = result.cast::<rhai::Map>();
        assert_eq!(
            map.get("country").unwrap().clone().into_string().unwrap(),
            "PRIVATE"
        );

        // Test enrich_cidr with no match
        let result: Dynamic = engine.eval(r#"enrich_cidr("geo", "8.8.8.8")"#).unwrap();
        assert!(result.is_unit());
    }

    #[test]
    fn test_registry_stats() {
        let mut registry = EnrichmentRegistry::new();

        let mut users = ExactTable::new(
            vec!["user_id".to_string(), "dept".to_string()],
            "user_id".to_string(),
        );
        users.insert(
            "alice".to_string(),
            vec!["alice".to_string(), "eng".to_string()],
        );
        users.insert(
            "bob".to_string(),
            vec!["bob".to_string(), "sales".to_string()],
        );
        registry.add_exact_table("users".to_string(), users);

        let mut geo = CidrTable::new(
            vec!["cidr".to_string(), "country".to_string()],
            "cidr".to_string(),
        );
        geo.insert(
            "10.0.0.0/8",
            vec!["10.0.0.0/8".to_string(), "PRIVATE".to_string()],
        )
        .unwrap();
        registry.add_cidr_table("geo".to_string(), geo);

        let stats = registry.stats();
        assert_eq!(stats.exact_table_count, 1);
        assert_eq!(stats.cidr_table_count, 1);
        assert_eq!(stats.exact_entry_count, 2);
        assert_eq!(stats.cidr_entry_count, 1);
    }
}
