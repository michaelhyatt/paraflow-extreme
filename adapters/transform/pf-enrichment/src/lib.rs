//! Enrichment tables for Paraflow Extreme transforms.
//!
//! This crate provides lookup tables for enriching records during transformation:
//! - [`ExactTable`] - O(1) exact-match lookups using HashMap
//! - [`CidrTable`] - O(log n) longest-prefix match for IP addresses
//! - [`EnrichmentRegistry`] - Registry of all enrichment tables
//!
//! # Example
//!
//! ```rust,ignore
//! use pf_enrichment::{EnrichmentRegistry, EnrichmentTableConfig, MatchType};
//!
//! let configs = vec![
//!     EnrichmentTableConfig {
//!         name: "geo_ip".to_string(),
//!         source: "s3://bucket/geo.csv".to_string(),
//!         match_type: MatchType::Cidr,
//!         key_field: "cidr".to_string(),
//!     },
//! ];
//!
//! let registry = EnrichmentRegistry::load(&configs).await?;
//! ```

mod cidr_table;
mod config;
mod exact_table;
mod loader;
mod registry;
mod row;

pub use cidr_table::CidrTable;
pub use config::{EnrichmentTableConfig, MatchType};
pub use exact_table::ExactTable;
pub use loader::load_csv;
pub use registry::EnrichmentRegistry;
pub use row::EnrichmentRow;
