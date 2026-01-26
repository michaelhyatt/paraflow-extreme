//! CidrTable - O(log n) CIDR lookup table with longest-prefix matching.

use crate::EnrichmentRow;
use ip_network::IpNetwork;
use ip_network_table::IpNetworkTable;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::warn;

/// O(log n) CIDR lookup table with longest-prefix matching.
///
/// Supports both IPv4 and IPv6 addresses and CIDR ranges.
/// Uses `ip_network_table` for efficient trie-based lookups.
pub struct CidrTable {
    /// Combined IPv4/IPv6 prefix table.
    table: IpNetworkTable<EnrichmentRow>,

    /// Column names from CSV header.
    columns: Arc<Vec<String>>,

    /// Name of the key field (CIDR column).
    key_field: String,

    /// Count of IPv4 entries.
    ipv4_count: usize,

    /// Count of IPv6 entries.
    ipv6_count: usize,
}

impl std::fmt::Debug for CidrTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CidrTable")
            .field("columns", &self.columns)
            .field("key_field", &self.key_field)
            .field("ipv4_count", &self.ipv4_count)
            .field("ipv6_count", &self.ipv6_count)
            .finish_non_exhaustive()
    }
}

impl CidrTable {
    /// Creates a new empty CIDR table.
    pub fn new(columns: Vec<String>, key_field: String) -> Self {
        Self {
            table: IpNetworkTable::new(),
            columns: Arc::new(columns),
            key_field,
            ipv4_count: 0,
            ipv6_count: 0,
        }
    }

    /// Inserts a CIDR entry into the table.
    ///
    /// Supports:
    /// - Single IPs: "1.2.3.4" (treated as /32 for IPv4, /128 for IPv6)
    /// - CIDR notation: "10.0.0.0/8", "2001:db8::/32"
    pub fn insert(&mut self, cidr: &str, values: Vec<String>) -> Result<(), String> {
        let row = EnrichmentRow::new(values, Arc::clone(&self.columns));

        // Try parsing as IpNetwork (CIDR notation)
        if let Ok(network) = cidr.parse::<IpNetwork>() {
            match network {
                IpNetwork::V4(_) => self.ipv4_count += 1,
                IpNetwork::V6(_) => self.ipv6_count += 1,
            }
            self.table.insert(network, row);
            return Ok(());
        }

        // Try parsing as bare IP address (convert to /32 or /128)
        if let Ok(ip) = cidr.parse::<IpAddr>() {
            match ip {
                IpAddr::V4(v4) => {
                    let network = IpNetwork::new(v4, 32)
                        .map_err(|e| format!("Failed to create /32 network: {e}"))?;
                    self.table.insert(network, row);
                    self.ipv4_count += 1;
                }
                IpAddr::V6(v6) => {
                    let network = IpNetwork::new(v6, 128)
                        .map_err(|e| format!("Failed to create /128 network: {e}"))?;
                    self.table.insert(network, row);
                    self.ipv6_count += 1;
                }
            }
            return Ok(());
        }

        Err(format!("Invalid IP or CIDR notation: {cidr}"))
    }

    /// Looks up an IP address and returns the longest-prefix match.
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address string to look up
    ///
    /// # Returns
    ///
    /// The enrichment row for the longest matching prefix, or None.
    pub fn lookup(&self, ip: &str) -> Option<&EnrichmentRow> {
        let addr: IpAddr = match ip.parse() {
            Ok(a) => a,
            Err(_) => {
                warn!(ip = %ip, "Failed to parse IP address for CIDR lookup");
                return None;
            }
        };

        self.lookup_addr(addr)
    }

    /// Looks up a parsed IP address.
    pub fn lookup_addr(&self, addr: IpAddr) -> Option<&EnrichmentRow> {
        self.table.longest_match(addr).map(|(_, row)| row)
    }

    /// Returns the total number of entries in the table.
    pub fn len(&self) -> usize {
        self.ipv4_count + self.ipv6_count
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.ipv4_count == 0 && self.ipv6_count == 0
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the key field name.
    pub fn key_field(&self) -> &str {
        &self.key_field
    }

    /// Returns the number of IPv4 entries.
    pub fn ipv4_count(&self) -> usize {
        self.ipv4_count
    }

    /// Returns the number of IPv6 entries.
    pub fn ipv6_count(&self) -> usize {
        self.ipv6_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table() -> CidrTable {
        let columns = vec![
            "cidr".to_string(),
            "country".to_string(),
            "city".to_string(),
        ];
        let mut table = CidrTable::new(columns, "cidr".to_string());

        // Private ranges
        table
            .insert(
                "10.0.0.0/8",
                vec![
                    "10.0.0.0/8".to_string(),
                    "PRIVATE".to_string(),
                    "RFC1918".to_string(),
                ],
            )
            .unwrap();
        table
            .insert(
                "192.168.0.0/16",
                vec![
                    "192.168.0.0/16".to_string(),
                    "PRIVATE".to_string(),
                    "RFC1918".to_string(),
                ],
            )
            .unwrap();

        // More specific match
        table
            .insert(
                "10.1.0.0/16",
                vec![
                    "10.1.0.0/16".to_string(),
                    "INTERNAL".to_string(),
                    "DataCenter".to_string(),
                ],
            )
            .unwrap();

        // Public ranges
        table
            .insert(
                "8.8.8.0/24",
                vec![
                    "8.8.8.0/24".to_string(),
                    "US".to_string(),
                    "Mountain View".to_string(),
                ],
            )
            .unwrap();

        // Single IP
        table
            .insert(
                "1.1.1.1",
                vec![
                    "1.1.1.1/32".to_string(),
                    "AU".to_string(),
                    "Sydney".to_string(),
                ],
            )
            .unwrap();

        table
    }

    #[test]
    fn test_cidr_table_exact_match() {
        let table = create_test_table();

        let result = table.lookup("8.8.8.8").unwrap();
        assert_eq!(result.get("country"), Some("US"));
        assert_eq!(result.get("city"), Some("Mountain View"));
    }

    #[test]
    fn test_cidr_table_longest_prefix_match() {
        let table = create_test_table();

        // 10.1.2.3 should match 10.1.0.0/16 (more specific) over 10.0.0.0/8
        let result = table.lookup("10.1.2.3").unwrap();
        assert_eq!(result.get("country"), Some("INTERNAL"));
        assert_eq!(result.get("city"), Some("DataCenter"));

        // 10.2.0.1 should match 10.0.0.0/8 (less specific, but still matches)
        let result = table.lookup("10.2.0.1").unwrap();
        assert_eq!(result.get("country"), Some("PRIVATE"));
    }

    #[test]
    fn test_cidr_table_single_ip() {
        let table = create_test_table();

        let result = table.lookup("1.1.1.1").unwrap();
        assert_eq!(result.get("country"), Some("AU"));

        // 1.1.1.2 should not match the /32
        assert!(table.lookup("1.1.1.2").is_none());
    }

    #[test]
    fn test_cidr_table_no_match() {
        let table = create_test_table();

        // Should not match any range
        assert!(table.lookup("172.16.0.1").is_none());
        assert!(table.lookup("203.0.113.1").is_none());
    }

    #[test]
    fn test_cidr_table_invalid_ip() {
        let table = create_test_table();

        assert!(table.lookup("not-an-ip").is_none());
        assert!(table.lookup("").is_none());
    }

    #[test]
    fn test_cidr_table_ipv6() {
        let columns = vec!["cidr".to_string(), "country".to_string()];
        let mut table = CidrTable::new(columns, "cidr".to_string());

        table
            .insert(
                "2001:db8::/32",
                vec!["2001:db8::/32".to_string(), "TEST".to_string()],
            )
            .unwrap();

        table
            .insert("::1", vec!["::1/128".to_string(), "LOCALHOST".to_string()])
            .unwrap();

        let result = table.lookup("2001:db8::1").unwrap();
        assert_eq!(result.get("country"), Some("TEST"));

        let result = table.lookup("::1").unwrap();
        assert_eq!(result.get("country"), Some("LOCALHOST"));

        assert!(table.lookup("2001:db9::1").is_none());
    }

    #[test]
    fn test_cidr_table_counts() {
        let table = create_test_table();

        assert_eq!(table.ipv4_count(), 5);
        assert_eq!(table.ipv6_count(), 0);
        assert_eq!(table.len(), 5);
        assert!(!table.is_empty());
    }
}
