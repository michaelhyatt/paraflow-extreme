//! Configuration types for enrichment tables.

use serde::{Deserialize, Serialize};

/// Configuration for an enrichment table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentTableConfig {
    /// Table name used in Rhai scripts (e.g., "geo_ip", "users").
    pub name: String,

    /// Source file path (S3 URI or local path).
    pub source: String,

    /// Match type: exact or CIDR.
    pub match_type: MatchType,

    /// Column name in CSV that serves as the lookup key.
    pub key_field: String,
}

/// Match type for enrichment lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchType {
    /// Exact string match using HashMap (O(1)).
    Exact,
    /// CIDR/IP prefix match using trie (O(log n) longest-prefix match).
    Cidr,
}

impl Default for MatchType {
    fn default() -> Self {
        Self::Exact
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_type_default() {
        assert_eq!(MatchType::default(), MatchType::Exact);
    }

    #[test]
    fn test_config_serde() {
        let config = EnrichmentTableConfig {
            name: "geo_ip".to_string(),
            source: "s3://bucket/geo.csv".to_string(),
            match_type: MatchType::Cidr,
            key_field: "cidr".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: EnrichmentTableConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, "geo_ip");
        assert_eq!(parsed.match_type, MatchType::Cidr);
    }
}
