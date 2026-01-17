//! CSV loading utilities for enrichment tables.

use crate::{CidrTable, ExactTable, MatchType};
use aws_sdk_s3::Client as S3Client;
use pf_error::{ReaderError, Result};
use std::path::Path;
use tracing::{debug, info};

/// Loads a CSV file and returns (headers, records).
///
/// Supports both local files and S3 URIs.
pub async fn load_csv(
    source: &str,
    s3_client: Option<&S3Client>,
) -> Result<(Vec<String>, Vec<csv::StringRecord>)> {
    let content = if source.starts_with("s3://") {
        load_from_s3(source, s3_client).await?
    } else {
        load_from_file(source).await?
    };

    parse_csv(&content)
}

/// Loads an ExactTable from a CSV source.
pub async fn load_exact_table(
    source: &str,
    key_field: &str,
    s3_client: Option<&S3Client>,
) -> Result<ExactTable> {
    let (headers, records) = load_csv(source, s3_client).await?;

    let key_idx = headers.iter().position(|h| h == key_field).ok_or_else(|| {
        ReaderError::Schema(format!(
            "Key field '{}' not found in CSV headers: {:?}",
            key_field, headers
        ))
    })?;

    let mut table = ExactTable::new(headers.clone(), key_field.to_string());

    for record in records {
        let key = record.get(key_idx).unwrap_or("").to_string();
        let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        table.insert(key, values);
    }

    info!(
        source = %source,
        key_field = %key_field,
        entries = table.len(),
        "Loaded ExactTable"
    );

    Ok(table)
}

/// Loads a CidrTable from a CSV source.
pub async fn load_cidr_table(
    source: &str,
    key_field: &str,
    s3_client: Option<&S3Client>,
) -> Result<CidrTable> {
    let (headers, records) = load_csv(source, s3_client).await?;

    let key_idx = headers.iter().position(|h| h == key_field).ok_or_else(|| {
        ReaderError::Schema(format!(
            "Key field '{}' not found in CSV headers: {:?}",
            key_field, headers
        ))
    })?;

    let mut table = CidrTable::new(headers.clone(), key_field.to_string());
    let mut errors = 0;

    for (line_num, record) in records.iter().enumerate() {
        let cidr = record.get(key_idx).unwrap_or("");
        let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();

        if let Err(e) = table.insert(cidr, values) {
            debug!(
                line = line_num + 2, // +2 for 1-indexed and header row
                cidr = %cidr,
                error = %e,
                "Skipping invalid CIDR entry"
            );
            errors += 1;
        }
    }

    info!(
        source = %source,
        key_field = %key_field,
        entries = table.len(),
        ipv4_entries = table.ipv4_count(),
        ipv6_entries = table.ipv6_count(),
        errors = errors,
        "Loaded CidrTable"
    );

    Ok(table)
}

/// Loads a table based on match type.
#[allow(dead_code)]
pub async fn load_table(
    source: &str,
    key_field: &str,
    match_type: MatchType,
    s3_client: Option<&S3Client>,
) -> Result<TableType> {
    match match_type {
        MatchType::Exact => {
            let table = load_exact_table(source, key_field, s3_client).await?;
            Ok(TableType::Exact(table))
        }
        MatchType::Cidr => {
            let table = load_cidr_table(source, key_field, s3_client).await?;
            Ok(TableType::Cidr(Box::new(table)))
        }
    }
}

/// Enum to hold either table type.
#[allow(dead_code)]
pub enum TableType {
    Exact(ExactTable),
    Cidr(Box<CidrTable>),
}

async fn load_from_file(path: &str) -> Result<String> {
    let path = Path::new(path);
    if !path.exists() {
        return Err(ReaderError::NotFound(path.display().to_string()).into());
    }

    tokio::fs::read_to_string(path)
        .await
        .map_err(|e| ReaderError::Io(format!("Failed to read {}: {}", path.display(), e)).into())
}

async fn load_from_s3(uri: &str, s3_client: Option<&S3Client>) -> Result<String> {
    let client = s3_client
        .ok_or_else(|| ReaderError::S3Error("S3 client not provided for S3 URI".to_string()))?;

    let (bucket, key) = parse_s3_uri(uri)?;

    debug!(bucket = %bucket, key = %key, "Loading enrichment table from S3");

    let response = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .map_err(|e| ReaderError::S3Error(format!("Failed to get object: {e}")))?;

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|e| ReaderError::S3Error(format!("Failed to read body: {e}")))?;

    String::from_utf8(bytes.to_vec())
        .map_err(|e| ReaderError::InvalidFormat(format!("CSV is not valid UTF-8: {e}")).into())
}

fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let uri = uri
        .strip_prefix("s3://")
        .ok_or_else(|| ReaderError::InvalidUri(format!("Not an S3 URI: {uri}")))?;

    let (bucket, key) = uri
        .split_once('/')
        .ok_or_else(|| ReaderError::InvalidUri(format!("Missing key in S3 URI: {uri}")))?;

    Ok((bucket.to_string(), key.to_string()))
}

fn parse_csv(content: &str) -> Result<(Vec<String>, Vec<csv::StringRecord>)> {
    let mut reader = csv::Reader::from_reader(content.as_bytes());

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| ReaderError::ParseError(format!("Failed to parse CSV headers: {e}")))?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let records: Vec<csv::StringRecord> = reader.records().filter_map(|r| r.ok()).collect();

    Ok((headers, records))
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
    async fn test_load_exact_table() {
        let csv_content = "user_id,email,department\n\
                           alice,alice@example.com,engineering\n\
                           bob,bob@example.com,sales\n";
        let file = create_test_csv(csv_content);

        let table = load_exact_table(file.path().to_str().unwrap(), "user_id", None)
            .await
            .unwrap();

        assert_eq!(table.len(), 2);

        let alice = table.get("alice").unwrap();
        assert_eq!(alice.get("email"), Some("alice@example.com"));
        assert_eq!(alice.get("department"), Some("engineering"));
    }

    #[tokio::test]
    async fn test_load_cidr_table() {
        let csv_content = "cidr,country,city\n\
                           10.0.0.0/8,PRIVATE,RFC1918\n\
                           8.8.8.0/24,US,Mountain View\n\
                           1.1.1.1,AU,Sydney\n";
        let file = create_test_csv(csv_content);

        let table = load_cidr_table(file.path().to_str().unwrap(), "cidr", None)
            .await
            .unwrap();

        assert_eq!(table.len(), 3);

        let result = table.lookup("10.1.2.3").unwrap();
        assert_eq!(result.get("country"), Some("PRIVATE"));

        let result = table.lookup("8.8.8.8").unwrap();
        assert_eq!(result.get("country"), Some("US"));
    }

    #[tokio::test]
    async fn test_load_csv_file_not_found() {
        let result = load_csv("/nonexistent/path.csv", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_exact_table_missing_key_field() {
        let csv_content = "email,department\n\
                           alice@example.com,engineering\n";
        let file = create_test_csv(csv_content);

        let result = load_exact_table(file.path().to_str().unwrap(), "user_id", None).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri() {
        let (bucket, key) = parse_s3_uri("s3://my-bucket/path/to/file.csv").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.csv");
    }

    #[test]
    fn test_parse_s3_uri_invalid() {
        assert!(parse_s3_uri("https://example.com/file.csv").is_err());
        assert!(parse_s3_uri("s3://bucket-only").is_err());
    }
}
